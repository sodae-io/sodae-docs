fn main() -> Result<(), Box<dyn std::error::Error>> {
    let descriptors = protox::compile(["shredstream.proto"], ["../proto"])?;
    tonic_prost_build::configure()
        .build_server(false)
        .compile_fds(descriptors)?;
    println!("cargo:rerun-if-changed=../proto/shredstream.proto");
    Ok(())
}
