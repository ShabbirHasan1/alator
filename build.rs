fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .out_dir("src/test")
        .compile(
            &["proto/exchange.proto"],
            &["proto"],
        )?;
   Ok(())
}
