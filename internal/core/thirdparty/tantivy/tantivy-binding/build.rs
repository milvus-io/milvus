use std::{env, path::Path, path::PathBuf};

fn main() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let package_name = env::var("CARGO_PKG_NAME").unwrap();
    let output_file = PathBuf::from(&crate_dir)
        .join("include")
        .join(format!("{}.h", package_name));
    cbindgen::generate(&crate_dir)
        .unwrap()
        .write_to_file(output_file);

    // If TOKENIZER_PROTO is set, generate the grpc_tokenizer protocol.
    let tokenizer_proto_path = env::var("TOKENIZER_PROTO").unwrap_or_default();
    if !tokenizer_proto_path.is_empty() {
        let path = Path::new(&tokenizer_proto_path);
        // Check if the protobuf file exists in the path, and if not, pass.
        if !path.exists() {
            return;
        }
        let include_path = path
            .parent()
            .map(|p| p.to_str().unwrap_or("").to_string())
            .unwrap();
        let iface_files = &[path];
        let output_dir = PathBuf::from(&crate_dir).join("src/analyzer/gen");

        // create if outdir is not exist
        if !output_dir.exists() {
            std::fs::create_dir_all(&output_dir).unwrap();
        }
        if let Err(error) = tonic_build::configure()
            .out_dir(&output_dir)
            .build_client(true)
            .build_server(false)
            .compile_protos(iface_files, &[include_path])
        {
            eprintln!("\nfailed to compile protos: {}", error);
        }
    }
}
