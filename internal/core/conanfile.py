from conans import ConanFile, CMake


class MilvusConan(ConanFile):

    settings = "os", "compiler", "build_type", "arch"
    requires = (
        "rocksdb/6.29.5",
        "boost/1.80.0",
        "onetbb/2021.7.0",
        "zstd/1.5.2",
        "arrow/8.0.1",
        "openssl/1.1.1q",
        "aws-sdk-cpp/1.9.234",
        "benchmark/1.7.0",
        "gtest/1.8.1",
        "protobuf/3.21.9",
        "rapidxml/1.13",
        "yaml-cpp/0.7.0",
        "marisa/0.2.6",
        "zlib/1.2.13",
        "libcurl/7.86.0",
    )

    generators = "cmake"
    default_options = {
        "rocksdb:shared": True,
        "arrow:parquet": True,
        "arrow:compute": True,
        "arrow:with_zstd": True,
        "arrow:shared": False,
        "arrow:with_jemalloc": True,
        "aws-sdk-cpp:text-to-speech": False,
        "aws-sdk-cpp:transfer": False,
        "gtest:build_gmock": False,
        "boost:without_fiber": True,
        "boost:without_json": True,
        "boost:without_wave": True,
        "boost:without_math": True,
        "boost:without_graph": True,
        "boost:without_graph_parallel": True,
        "boost:without_nowide": True,
    }
    should_build = False

    def configure(self):
        # Macos M1 cannot use jemalloc
        if self.settings.os == "Macos" and self.settings.arch not in ("x86_64", "x86"):
            self.options["arrow"].with_jemalloc = False

    def imports(self):
        self.copy("librocksdb.a", "../lib", "lib")
        self.copy("libarrow.a", "../lib", "lib")
        self.copy("*.dylib", "../lib", "lib")
        self.copy("*.dll", "../lib", "lib")
        self.copy("*.so*", "../lib", "lib")
        self.copy("*", "../bin", "bin")
