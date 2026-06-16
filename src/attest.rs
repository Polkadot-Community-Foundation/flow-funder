//! People-chain `PeopleLite::attest` decoder.
//!
//! The bot reserves dotns names by replaying, into `DotnsGateway::reserve_name`
//! on Asset Hub, the exact inputs the identity-backend already passed to
//! `PeopleLite::attest` on the People chain. This works because both calls verify
//! the byte-identical signed message
//! (`b"pop:people-lite:register using" ++ candidate ++ ring_vrf_key`), so the
//! `candidate_signature` and `proof_of_ownership` from `attest` also validate
//! `reserve_name`. Every input therefore lives in the `attest` extrinsic's call
//! data — no DB, no API, no user secrets.
//!
//! `reservations_in_block` walks a finalized block, finds every `attest` call
//! (including those nested in `Utility::batch`/`batch_all`/`force_batch`, which is
//! how identity-backend submits them), and harvests one `ReservationInputs` per
//! call that carries a username (`consumer_registration = Some`).

use subxt::client::OnlineClientAtBlock;
use subxt::config::SubstrateConfig;
use subxt::ext::scale_value::{Composite, Primitive, Value, ValueDef};

/// Everything `DotnsGateway::reserve_name` needs, harvested from one
/// `PeopleLite::attest` call. The first four are passed through verbatim as
/// decoded `Value`s (their concrete crypto types — `AttestationSignature`,
/// `MemberOf`, `SignatureOf` — never need to exist in this crate; they re-encode
/// structurally against Asset Hub metadata, which shares the same runtime types).
#[derive(Debug, Clone, PartialEq)]
pub struct ReservationInputs {
    pub candidate: Value,
    pub candidate_signature: Value,
    pub ring_vrf_key: Value,
    pub proof_of_ownership: Value,
    /// `consumer_registration.username` (e.g. "alice.11") → `reserve_name.lite_label`.
    pub lite_label: Vec<u8>,
    /// `consumer_registration.identifier_key` → `reserve_name.chat_key`.
    pub chat_key: [u8; 65],
    /// `consumer_registration.reserved_username` → `reserve_name.reserved_base_label`.
    pub reserved_base_label: Option<Vec<u8>>,
}

/// Walk a finalized People block and return one `ReservationInputs` per
/// `PeopleLite::attest` call that carries `consumer_registration = Some`. Calls
/// without a consumer registration (no username) are skipped. Returns `Err` if the
/// block body can't be fetched or a call fails to decode — the caller pins the
/// cursor and retries, never silently advancing past a registration it couldn't read.
///
/// Takes an `OnlineClientAtBlock` (from `block.at()` for live blocks or
/// `api.at_block(n)` for catch-up), so the same decoder serves both paths.
pub async fn reservations_in_block(
    at: &OnlineClientAtBlock<SubstrateConfig>,
) -> Result<Vec<ReservationInputs>, Box<dyn std::error::Error + Send + Sync>> {
    let extrinsics = at.extrinsics().fetch().await?;

    let mut out = Vec::new();
    for ext in extrinsics.iter() {
        let ext = ext?;
        // Only attest (direct) or wrappers that might contain one (Utility batches,
        // Proxy.proxy) are worth decoding.
        match ext.pallet_name() {
            "PeopleLite" | "Utility" | "Proxy" => {
                // scale_value only decodes into the contextless `Value` (= Value<()>).
                let call: Value = ext.decode_call_data_as()?;
                collect_attest(&call, &mut out);
            }
            _ => {}
        }
    }
    Ok(out)
}

/// Recursively collect `attest` reservations from a decoded `RuntimeCall` value,
/// unwrapping `Utility::batch`/`batch_all`/`force_batch`. One code path handles
/// both top-level attests and attests nested (arbitrarily deep) inside batches.
fn collect_attest(call: &Value, out: &mut Vec<ReservationInputs>) {
    let Some((pallet, name, fields)) = as_pallet_call(call) else {
        return;
    };
    match (pallet, name) {
        ("Utility", "batch" | "batch_all" | "force_batch") => {
            // batch/batch_all/force_batch each take a single `calls: Vec<RuntimeCall>`.
            if let Some(calls) = fields.values().next() {
                if let ValueDef::Composite(seq) = &calls.value {
                    for inner in seq.values() {
                        collect_attest(inner, out);
                    }
                }
            }
        }
        ("Proxy", "proxy" | "proxy_announced") => {
            // The wrapped `call` is the last arg of proxy(real, force_proxy_type, call)
            // and proxy_announced(delegate, real, force_proxy_type, call). The
            // identity-backend submits attests through a proxy.
            if let Some(inner) = fields.values().last() {
                collect_attest(inner, out);
            }
        }
        ("PeopleLite", "attest") => {
            if let Some(inputs) = extract_reservation(fields) {
                out.push(inputs);
            }
        }
        _ => {}
    }
}

/// Decode a `RuntimeCall` value into `(pallet_name, call_name, call_fields)`.
/// A `RuntimeCall` is an outer enum keyed by pallet whose single field is the
/// pallet's own call enum, so we drill two variant levels.
fn as_pallet_call(v: &Value) -> Option<(&str, &str, &Composite<()>)> {
    let ValueDef::Variant(outer) = &v.value else {
        return None;
    };
    let inner = outer.values.values().next()?;
    let ValueDef::Variant(call) = &inner.value else {
        return None;
    };
    Some((outer.name.as_str(), call.name.as_str(), &call.values))
}

/// Build `ReservationInputs` from the positional fields of an `attest` call:
/// `candidate, candidate_signature, ring_vrf_key, proof_of_ownership,
/// consumer_registration: Option<LiteConsumerRegistrationParams>`.
/// Positional access avoids any dependence on metadata field names. Returns
/// `None` when `consumer_registration` is `None` (no username to reserve).
fn extract_reservation(fields: &Composite<()>) -> Option<ReservationInputs> {
    let mut args = fields.values();
    let candidate = args.next()?.clone();
    let candidate_signature = args.next()?.clone();
    let ring_vrf_key = args.next()?.clone();
    let proof_of_ownership = args.next()?.clone();
    let consumer_registration = args.next()?;

    // consumer_registration: Option<LiteConsumerRegistrationParams>
    let ValueDef::Variant(option) = &consumer_registration.value else {
        return None;
    };
    if option.name != "Some" {
        return None; // None → attest without a username; nothing to reserve.
    }
    let params = option.values.values().next()?;
    let ValueDef::Composite(params) = &params.value else {
        return None;
    };

    // LiteConsumerRegistrationParams: signature, account, identifier_key, username,
    // reserved_username — positional.
    let mut p = params.values();
    let _signature = p.next()?;
    let _account = p.next()?;
    let identifier_key = value_to_bytes(p.next()?)?;
    let lite_label = value_to_bytes(p.next()?)?;
    let reserved_base_label = option_bytes(p.next()?);

    let chat_key: [u8; 65] = identifier_key.try_into().ok()?;

    Some(ReservationInputs {
        candidate,
        candidate_signature,
        ring_vrf_key,
        proof_of_ownership,
        lite_label,
        chat_key,
        reserved_base_label,
    })
}

/// Read a decoded byte container back into raw bytes. `BaseLabel`/`Username`
/// (`BoundedVec<u8, _>`) and `[u8; N]` decode as a composite of `u8` primitives,
/// but newtype wrappers (e.g. `BoundedVec`'s struct, `ChatKey`) add a layer of
/// single-field composite nesting, so unwrap a lone composite child before
/// reading the bytes.
fn value_to_bytes(v: &Value) -> Option<Vec<u8>> {
    let ValueDef::Composite(c) = &v.value else {
        return None;
    };
    let values: Vec<&Value> = c.values().collect();
    // Unwrap a single-field wrapper (e.g. BoundedVec(struct) → inner Vec<u8>).
    if values.len() == 1 && matches!(values[0].value, ValueDef::Composite(_)) {
        return value_to_bytes(values[0]);
    }
    values
        .iter()
        .map(|e| match &e.value {
            ValueDef::Primitive(Primitive::U128(n)) if *n <= u8::MAX as u128 => Some(*n as u8),
            _ => None,
        })
        .collect()
}

/// Read an `Option<BoundedVec<u8, _>>` value: `Some(bytes)` or `None`.
fn option_bytes(v: &Value) -> Option<Vec<u8>> {
    let ValueDef::Variant(option) = &v.value else {
        return None;
    };
    if option.name != "Some" {
        return None;
    }
    value_to_bytes(option.values.values().next()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use subxt::ext::scale_value::Value;

    /// A decoded byte container. scale_value decodes `BoundedVec<u8,_>`/`ChatKey`
    /// as a newtype wrapper: a single-field composite around the inner `u8`
    /// sequence — i.e. one level of nesting (verified against live People-chain
    /// data). `value_to_bytes` must unwrap that layer.
    fn bytes_value(bytes: &[u8]) -> Value {
        Value::unnamed_composite([Value::unnamed_composite(
            bytes.iter().map(|b| Value::u128(*b as u128)),
        )])
    }

    /// Wrap a RuntimeCall value in `Proxy.proxy(real, force_proxy_type, call)`, as
    /// the identity-backend submits attests.
    fn proxy_wrap(inner: Value) -> Value {
        Value::unnamed_variant(
            "Proxy",
            [Value::named_variant(
                "proxy",
                [
                    ("real", bytes_value(b"real")),
                    ("force_proxy_type", none()),
                    ("call", inner),
                ],
            )],
        )
    }

    fn some(inner: Value) -> Value {
        Value::unnamed_variant("Some", [inner])
    }

    fn none() -> Value {
        Value::unnamed_variant("None", [])
    }

    /// Build a `PeopleLite::attest` RuntimeCall value (outer pallet variant wrapping
    /// the inner call variant), with or without a consumer registration.
    fn attest_call(username: &[u8], chat_key: &[u8], reserved: Option<&[u8]>) -> Value {
        let consumer = some(Value::unnamed_composite([
            bytes_value(b"sig"),        // signature (ignored)
            bytes_value(b"account"),    // account (ignored)
            bytes_value(chat_key),      // identifier_key
            bytes_value(username),      // username
            match reserved {            // reserved_username: Option<Username>
                Some(r) => some(bytes_value(r)),
                None => none(),
            },
        ]));
        let attest = Value::named_variant(
            "attest",
            [
                ("candidate", bytes_value(b"cand")),
                ("candidate_signature", bytes_value(b"csig")),
                ("ring_vrf_key", bytes_value(b"rvk")),
                ("proof_of_ownership", bytes_value(b"poo")),
                ("consumer_registration", consumer),
            ],
        );
        Value::unnamed_variant("PeopleLite", [attest])
    }

    fn attest_without_consumer() -> Value {
        let attest = Value::named_variant(
            "attest",
            [
                ("candidate", bytes_value(b"cand")),
                ("candidate_signature", bytes_value(b"csig")),
                ("ring_vrf_key", bytes_value(b"rvk")),
                ("proof_of_ownership", bytes_value(b"poo")),
                ("consumer_registration", none()),
            ],
        );
        Value::unnamed_variant("PeopleLite", [attest])
    }

    fn other_call() -> Value {
        Value::unnamed_variant(
            "Balances",
            [Value::named_variant(
                "transfer_keep_alive",
                [("dest", bytes_value(b"x")), ("value", Value::u128(1))],
            )],
        )
    }

    #[test]
    fn extracts_a_direct_attest_with_username() {
        let chat = [7u8; 65];
        let mut out = Vec::new();
        collect_attest(&attest_call(b"alice.11", &chat, None), &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].lite_label, b"alice.11");
        assert_eq!(out[0].chat_key, chat);
        assert_eq!(out[0].reserved_base_label, None);
    }

    #[test]
    fn carries_the_reserved_base_label_when_present() {
        let chat = [0u8; 65];
        let mut out = Vec::new();
        collect_attest(&attest_call(b"bob.42", &chat, Some(b"bob")), &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].reserved_base_label.as_deref(), Some(&b"bob"[..]));
    }

    #[test]
    fn skips_attest_without_consumer_registration() {
        let mut out = Vec::new();
        collect_attest(&attest_without_consumer(), &mut out);
        assert!(out.is_empty());
    }

    #[test]
    fn unwraps_force_batch_and_ignores_non_attest_calls() {
        let chat = [1u8; 65];
        let batch = Value::unnamed_variant(
            "Utility",
            [Value::named_variant(
                "force_batch",
                [(
                    "calls",
                    Value::unnamed_composite([
                        attest_call(b"alice.11", &chat, None),
                        other_call(),
                        attest_call(b"carol.99", &chat, None),
                    ]),
                )],
            )],
        );
        let mut out = Vec::new();
        collect_attest(&batch, &mut out);
        let labels: Vec<_> = out.iter().map(|r| r.lite_label.clone()).collect();
        assert_eq!(labels, vec![b"alice.11".to_vec(), b"carol.99".to_vec()]);
    }

    #[test]
    fn ignores_unrelated_top_level_calls() {
        let mut out = Vec::new();
        collect_attest(&other_call(), &mut out);
        assert!(out.is_empty());
    }

    #[test]
    fn unwraps_proxy_wrapped_attest() {
        // The real submission shape: Proxy.proxy(Utility.force_batch([attest, ...])).
        let chat = [3u8; 65];
        let batch = Value::unnamed_variant(
            "Utility",
            [Value::named_variant(
                "force_batch",
                [(
                    "calls",
                    Value::unnamed_composite([
                        attest_call(b"tqddhkloaa.01", &chat, None),
                        attest_call(b"guestrics.28", &chat, None),
                    ]),
                )],
            )],
        );
        let mut out = Vec::new();
        collect_attest(&proxy_wrap(batch), &mut out);
        let labels: Vec<_> = out.iter().map(|r| r.lite_label.clone()).collect();
        assert_eq!(
            labels,
            vec![b"tqddhkloaa.01".to_vec(), b"guestrics.28".to_vec()]
        );
        assert_eq!(out[0].chat_key, chat);
    }

    #[test]
    fn value_to_bytes_unwraps_boundedvec_newtype_nesting() {
        // bytes_value double-wraps to mirror the real BoundedVec/ChatKey decode.
        assert_eq!(value_to_bytes(&bytes_value(b"alice.11")).as_deref(), Some(&b"alice.11"[..]));
    }
}
