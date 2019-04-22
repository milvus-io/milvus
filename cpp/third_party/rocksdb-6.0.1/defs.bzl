load("@fbcode_macros//build_defs:cpp_binary.bzl", "cpp_binary")
load("@fbcode_macros//build_defs:custom_unittest.bzl", "custom_unittest")

def test_binary(
        test_name,
        test_cc,
        parallelism,
        rocksdb_arch_preprocessor_flags,
        rocksdb_compiler_flags,
        rocksdb_preprocessor_flags,
        rocksdb_external_deps):
    TEST_RUNNER = native.package_name() + "/buckifier/rocks_test_runner.sh"

    ttype = "gtest" if parallelism == "parallel" else "simple"
    test_bin = test_name + "_bin"

    cpp_binary(
        name = test_bin,
        srcs = [test_cc],
        arch_preprocessor_flags = rocksdb_arch_preprocessor_flags,
        compiler_flags = rocksdb_compiler_flags,
        preprocessor_flags = rocksdb_preprocessor_flags,
        deps = [":rocksdb_test_lib"],
        external_deps = rocksdb_external_deps,
    )

    custom_unittest(
        name = test_name,
        command = [TEST_RUNNER, "$(location :{})".format(test_bin)],
        type = ttype,
    )
