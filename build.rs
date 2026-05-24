use std::io::Result;

fn main() -> Result<()> {
    prost_build::compile_protos(
        &[
            "proto/handshake.proto",
            "proto/headers.proto",
            "proto/hive.proto",
            "proto/pricing.proto",
            "proto/retrieval.proto",
            "proto/pushsync.proto",
            "proto/pseudosettle.proto",
            "proto/swap.proto",
            "proto/status.proto",
            "proto/pullsync.proto",
        ],
        &["proto/"],
    )?;
    Ok(())
}
