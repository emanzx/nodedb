fn main() {
    let crate_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();

    let config =
        cbindgen::Config::from_file("cbindgen.toml").expect("cbindgen.toml must be present");

    cbindgen::Builder::new()
        .with_crate(crate_dir)
        .with_config(config)
        .generate()
        .expect("Unable to generate C bindings")
        .write_to_file("include/nodedb_lite.h");
}
