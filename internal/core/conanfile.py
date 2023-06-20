from conans import ConanFile, CMake


class MilvusConan(ConanFile):
    settings = "os", "compiler", "build_type", "arch"
    requires = (
        "rocksdb/6.29.5",
        "boost/1.79.0",
        "onetbb/2021.7.0",
        "nlohmann_json/3.11.2",
        "zstd/1.5.4",
        "lz4/1.9.4",
        "snappy/1.1.9",
        "lzo/2.10",
        "arrow/11.0.0",
        "openssl/1.1.1q",
        "s2n/1.3.31@milvus/dev",
        "aws-c-common/0.8.2@milvus/dev",
        "aws-c-compression/0.2.15@milvus/dev",
        "aws-c-sdkutils/0.1.3@milvus/dev",
        "aws-checksums/0.1.13@milvus/dev",
        "aws-c-cal/0.5.20@milvus/dev",
        "aws-c-io/0.10.20@milvus/dev",
        "aws-sdk-cpp/1.9.234",
        "googleapis/cci.20221108",
        "benchmark/1.7.0",
        "gtest/1.8.1",
        "protobuf/3.21.4",
        "rapidxml/1.13",
        "yaml-cpp/0.7.0",
        "marisa/0.2.6",
        "zlib/1.2.13",
        "libcurl/7.86.0",
        "glog/0.6.0",
        "fmt/9.1.0",
        "gflags/2.2.2",
        "double-conversion/3.2.1",
        "libevent/2.1.12",
        "libdwarf/20191104",
        "libiberty/9.1.0",
        "libsodium/cci.20220430",
        "xsimd/9.0.1",
        "xz_utils/5.4.0",
        "prometheus-cpp/1.1.0",
        "re2/20230301",
        "folly/2023.05.22.02@milvus/dev",
        "google-cloud-cpp/2.5.0@milvus/dev",
        "opentelemetry-cpp/1.8.1.1@milvus/dev",
    )
    generators = ("cmake", "cmake_find_package")
    default_options = {
        "rocksdb:shared": True,
        "rocksdb:with_zstd": True,
        "arrow:parquet": True,
        "arrow:compute": True,
        "arrow:with_zstd": True,
        "arrow:shared": False,
        "arrow:with_jemalloc": True,
        "aws-sdk-cpp:text-to-speech": False,
        "aws-sdk-cpp:transfer": False,
        "gtest:build_gmock": False,
        "boost:without_locale": False,
        "glog:with_gflags": False,
        "prometheus-cpp:with_pull": False,
        "fmt:header_only": True,
    }

    def configure(self):
        if self.settings.os == "Macos":
            # Macos M1 cannot use jemalloc
            if self.settings.arch not in ("x86_64", "x86"):
                del self.options["folly"].use_sse4_2

            self.options["arrow"].with_jemalloc = False
            self.options["boost"].without_fiber = True
            self.options["boost"].without_json = True
            self.options["boost"].without_wave = True
            self.options["boost"].without_math = True
            self.options["boost"].without_graph = True
            self.options["boost"].without_graph_parallel = True
            self.options["boost"].without_nowide = True

    def imports(self):
        self.copy("*.dylib", "../lib", "lib")
        self.copy("*.dll", "../lib", "lib")
        self.copy("*.so*", "../lib", "lib")
        self.copy("*", "../bin", "bin")
        self.copy("*.proto", "../include", "include")
