//! Compiles ../bench.proto with protox, a pure-Rust protobuf compiler, so no
//! protoc binary is needed, and generates the tonic server stub into OUT_DIR.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let descriptors = protox::compile(["../bench.proto"], [".."])?;
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(false)
        .bytes(".")        .compile_fds(descriptors)?;
    Ok(())
}
