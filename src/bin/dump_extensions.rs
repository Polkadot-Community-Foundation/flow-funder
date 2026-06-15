//! Dev helper — connect to a chain and print its transaction-extension order
//! together with the SCALE shape of each extension's `extra` type.
//!
//! subxt matches each `TransactionExtension` in a `Config` by NAME against the
//! chain's metadata, in on-wire order, and each extension contributes its
//! `extra` bytes to the extrinsic. To build a correct custom `Config` we must
//! pin both the ordered set AND each extension's "disabled/none" encoding. This
//! dumps both from live metadata so the config is verified, not guessed.
//!
//!   cargo run --bin dump-extensions -- wss://paseo-asset-hub-next-rpc.polkadot.io

use scale_info::{PortableRegistry, TypeDef};
use subxt::{OnlineClient, PolkadotConfig};

fn describe_type(registry: &PortableRegistry, id: u32) -> String {
    let Some(ty) = registry.resolve(id) else {
        return format!("<unresolved #{id}>");
    };
    let path = ty.path.segments.join("::");
    let shape = match &ty.type_def {
        TypeDef::Composite(c) => {
            if c.fields.is_empty() {
                "empty-struct (no bytes)".to_string()
            } else {
                let inner: Vec<String> = c
                    .fields
                    .iter()
                    .map(|f| {
                        format!(
                            "{}: {}",
                            f.name.clone().unwrap_or_else(|| "_".into()),
                            describe_type(registry, f.ty.id)
                        )
                    })
                    .collect();
                format!("struct{{ {} }}", inner.join(", "))
            }
        }
        TypeDef::Variant(v) => {
            let names: Vec<String> = v
                .variants
                .iter()
                .map(|var| format!("{}={}", var.name, var.index))
                .collect();
            format!("enum[ {} ]", names.join(", "))
        }
        TypeDef::Primitive(p) => format!("{p:?}"),
        TypeDef::Tuple(t) => {
            if t.fields.is_empty() {
                "() (no bytes)".to_string()
            } else {
                "tuple".to_string()
            }
        }
        other => format!("{other:?}"),
    };
    if path.is_empty() {
        shape
    } else {
        format!("{path} = {shape}")
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "wss://paseo-asset-hub-next-rpc.polkadot.io".to_string());

    eprintln!("connecting to {url} …");
    let api = OnlineClient::<PolkadotConfig>::from_url(&url).await?;
    let at = api.at_current_block().await?;
    let metadata = at.metadata();
    let registry = metadata.types();
    let extrinsic = metadata.extrinsic();

    println!("# {url}");
    println!("supported extrinsic versions: {:?}", extrinsic.supported_versions());
    println!(
        "tx-extension encoding version: {}",
        extrinsic.transaction_extension_version_to_use_for_encoding()
    );
    for (i, ext) in extrinsic.transaction_extensions_to_use_for_encoding().enumerate() {
        println!(
            "{i:2}: {:<24} extra: {}",
            ext.identifier(),
            describe_type(registry, ext.extra_ty())
        );
    }
    Ok(())
}
