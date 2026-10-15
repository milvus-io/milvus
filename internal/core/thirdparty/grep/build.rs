use std::{env, path::PathBuf};

fn main() {
    let root = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    println!("cargo:rerun-if-changed=src/ffi.rs");
    println!("cargo:rerun-if-changed=cbindgen.toml");
    println!("cargo:rerun-if-changed=include/milvus_grep.h");
    let bindings = cbindgen::Builder::new()
        .with_src(root.join("src/ffi.rs"))
        .with_config(cbindgen::Config::from_file(root.join("cbindgen.toml")).unwrap())
        .generate()
        .expect("generate Grep C header");
    std::fs::create_dir_all(root.join("include")).unwrap();
    bindings.write_to_file(root.join("include/milvus_grep.h"));
    bindings.write_to_file(PathBuf::from(env::var_os("OUT_DIR").unwrap()).join("milvus_grep.h"));
}
