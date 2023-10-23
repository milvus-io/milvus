from conans import ConanFile


class MilvusConan(ConanFile):
    settings = "os", "compiler", "build_type", "arch"
    requires = (
        "rocksdb/6.29.5",
        "boost/1.82.0",
        "onetbb/2021.7.0",
        "nlohmann_json/3.11.2",
        "zstd/1.5.4",
        "lz4/1.9.4",
        "snappy/1.1.9",
        "lzo/2.10",
        "arrow/11.0.0",
        "openssl/3.1.2",
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
        "folly/2023.07.12@milvus/dev",
        "google-cloud-cpp/2.5.0@milvus/dev",
        "opentelemetry-cpp/1.8.1.1@milvus/dev",
        "librdkafka/1.9.1",
    )
    generators = ("cmake", "cmake_find_package")
    default_options = {
        "librdkafka:shared": True,
        "librdkafka:zstd": True,
        "librdkafka:ssl": True,
        "librdkafka:sasl": True,
        "rocksdb:shared": True,
        "rocksdb:with_zstd": True,
        "arrow:parquet": True,
        "arrow:compute": True,
        "arrow:with_re2": True,
        "arrow:with_zstd": True,
        "arrow:with_boost": True,
        "arrow:with_thrift": True,
        "arrow:with_jemalloc": True,
        "arrow:shared": False,
        "aws-sdk-cpp:text-to-speech": False,
        "aws-sdk-cpp:transfer": False,
        "gtest:build_gmock": False,
        "boost:without_locale": False,
        "glog:with_gflags": True,
        "glog:shared": True,
        "prometheus-cpp:with_pull": False,
        "fmt:header_only": True,
        "onetbb:tbbmalloc": False,
        "onetbb:tbbproxy": False,
        "openblas:shared": True,
        "openblas:dynamic_arch": True,
    }

    def configure(self):
        if self.settings.os == "Macos":
            # Macos M1 cannot use jemalloc
            if self.settings.arch not in ("x86_64", "x86"):
                del self.options["folly"].use_sse4_2

            self.options["arrow"].with_jemalloc = False
        if self.settings.arch == "armv8":
            self.options["openblas"].dynamic_arch = False

    def requirements(self):
        if self.settings.os != "Macos":
            # MacOS does not need openblas
            self.requires("openblas/0.3.23@milvus/dev")

    def imports(self):
        self.copy("*.dylib", "../lib", "lib")
        self.copy("*.dll", "../lib", "lib")
        self.copy("*.so*", "../lib", "lib")
        self.copy("*", "../bin", "bin")
        self.copy("*.proto", "../include", "include")
