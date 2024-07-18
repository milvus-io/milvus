from conans import ConanFile


class MilvusConan(ConanFile):
    keep_imports = True
    settings = "os", "compiler", "build_type", "arch"
    requires = (
        "rocksdb/6.29.5@milvus/dev",
        "boost/1.82.0",
        "onetbb/2021.9.0",
        "nlohmann_json/3.11.2",
        "zstd/1.5.4",
        "lz4/1.9.4",
        "snappy/1.1.9",
        "lzo/2.10",
        "arrow/12.0.1",
        "openssl/3.1.2",
        "aws-sdk-cpp/1.9.234",
        "googleapis/cci.20221108",
        "benchmark/1.7.0",
        "gtest/1.13.0",
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
        "folly/2023.10.30.08@milvus/dev",
        "google-cloud-cpp/2.5.0@milvus/dev",
        "opentelemetry-cpp/1.8.1.1@milvus/dev",
        "librdkafka/1.9.1",
        "abseil/20230125.3",
        "roaring/3.0.0",
    )
    generators = ("cmake", "cmake_find_package")
    default_options = {
        "libevent:shared": True,
        "double-conversion:shared": True,
        "folly:shared": True,
        "librdkafka:shared": True,
        "librdkafka:zstd": True,
        "librdkafka:ssl": True,
        "librdkafka:sasl": True,
        "rocksdb:shared": True,
        "rocksdb:with_zstd": True,
        "arrow:filesystem_layer": True,
        "arrow:parquet": True,
        "arrow:compute": True,
        "arrow:with_re2": True,
        "arrow:with_zstd": True,
        "arrow:with_boost": True,
        "arrow:with_thrift": True,
        "arrow:with_jemalloc": True,
        "arrow:shared": False,
        "arrow:with_s3": True,
        "aws-sdk-cpp:config": True,
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
    }

    def configure(self):
        if self.settings.arch not in ("x86_64", "x86"):
            del self.options["folly"].use_sse4_2
        if self.settings.os == "Macos":
            # By default abseil use static link but can not be compatible with macos X86
            self.options["abseil"].shared = True
            self.options["arrow"].with_jemalloc = False

    def requirements(self):
        if self.settings.os != "Macos":
            self.requires("libunwind/1.7.2")

    def imports(self):
        self.copy("*.dylib", "../lib", "lib")
        self.copy("*.dll", "../lib", "lib")
        self.copy("*.so*", "../lib", "lib")
        self.copy("*", "../bin", "bin")
        self.copy("*.proto", "../include", "include")
