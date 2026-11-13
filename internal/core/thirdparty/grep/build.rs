use std::{env, path::PathBuf};

fn main() {
    let root = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    generate_proto(&root);
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

fn generate_proto(root: &std::path::Path) {
    let repo = root.join("../../../..").canonicalize().unwrap();
    let internal = repo.join("pkg/proto");
    let api = repo.join("cmake_build/thirdparty/milvus-proto/proto");
    println!("cargo:rerun-if-env-changed=PROTOC");
    let protoc = env::var_os("PROTOC")
        .map(PathBuf::from)
        .unwrap_or_else(|| repo.join("cmake_build/bin/protoc"));
    assert!(
        protoc.is_file() && api.join("schema.proto").is_file(),
        "Grep protobuf generation requires the repository protoc and milvus-proto checkout; run make generated-proto-without-cpp first (PROTOC can override the compiler)"
    );
    println!("cargo:rerun-if-changed={}", protoc.display());
    // Track source schemas, including transitive imports, but not generated
    // .pb.go/.pb.cc files in these trees. Include all generated Rust modules
    // so the existing cgo messages' imported types remain resolvable.
    for directory in [&internal, &api] {
        for entry in std::fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            if path
                .extension()
                .is_some_and(|extension| extension == "proto")
            {
                println!("cargo:rerun-if-changed={}", path.display());
            }
        }
    }
    prost_build::Config::new()
        .protoc_executable(protoc)
        .include_file("grep_proto.rs")
        .compile_protos(&[internal.join("cgo_msg.proto")], &[internal, api])
        .expect("generate Rust bindings from pkg/proto/cgo_msg.proto");
}
