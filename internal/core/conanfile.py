from conans import ConanFile


class MilvusConan(ConanFile):
    keep_imports = True
    settings = "os", "compiler", "build_type", "arch"
    requires = (
        "rocksdb/6.29.5@milvus/dev#b1842a53ddff60240c5282a3da498ba1",
        "boost/1.83.0@",
        "onetbb/2021.9.0#4a223ff1b4025d02f31b65aedf5e7f4a",
        "nlohmann_json/3.11.3#ffb9e9236619f1c883e36662f944345d",
        "zstd/1.5.5#34e9debe03bf0964834a09dfbc31a5dd",
        "lz4/1.9.4#c5afb86edd69ac0df30e3a9e192e43db",
        "snappy/1.2.1#dfb7f7e837525210762d74e96eb4a3fc",
        "arrow/17.0.0@milvus/dev-2.6#7af258a853e20887f9969f713110aac8",
        "openssl/3.3.2#9f9f130d58e7c13e76bb8a559f0a6a8b",
        "googleapis/cci.20221108#65604e1b3b9a6b363044da625b201a2a",
        "gtest/1.13.0#f9548be18a41ccc6367efcb8146e92be",
        "benchmark/1.7.0#459f3bb1a64400a886ba43047576df3c",
        "protobuf/5.27.0@milvus/dev#6fff8583e2fe32babef04a9097f1d581",
        "yaml-cpp/0.7.0#9c87b3998de893cf2e5a08ad09a7a6e0",
        "marisa/0.2.6#68446854f5a420672d21f21191f8e5af",
        "zlib/1.3.1#f52e03ae3d251dec704634230cd806a2",
        "libcurl/8.10.1#a3113369c86086b0e84231844e7ed0a9",
        "libevent/2.1.12#b6333a128075d75a3614bd8418bf2099",
        "glog/0.7.1#83889ae482004d816ecac59dfef0d65a",
        "fmt/11.0.2#5c7438ef4d5d69ab106a41e460ce11f3",
        "gflags/2.2.2#b15c28c567c7ade7449cf994168a559f",
        "double-conversion/3.3.0#33321c201741cc32b51169c6d2d05e60",
        "libsodium/cci.20220430#7429a9e5351cc67bea3537229921714d",
        "xsimd/9.0.1#ac9fd02a381698c4e08c5c4ca03b73e1",
        "xz_utils/5.4.5#b885d1d79c9d30cff3803f7f551dbe66",
        "prometheus-cpp/1.2.4#0918d66c13f97acb7809759f9de49b3f",
        "re2/20230301#f8efaf45f98d0193cd0b2ea08b6b4060",
        "folly/2024.08.12.00@milvus/dev#e09fc71826ce6b4568441910665f0889",
        "google-cloud-cpp/2.28.0@milvus/dev#25e69d743269d6c9ae5bf676af2174dc",
        "opentelemetry-cpp/1.23.0@milvus/dev#bcd65b63b8db8447178ed93bbc94dcc0",
        "librdkafka/1.9.1#e24dcbb0a1684dcf5a56d8d0692ceef3",
        "abseil/20250127.0#e6c46096070cc3fd060e95a837cd3fb2",
        "roaring/3.0.0#25a703f80eda0764a31ef939229e202d",
        "grpc/1.67.1@milvus/dev#00eeae5b14c7313dbc91f1a57c0a2554",
        "rapidjson/cci.20230929#624c0094d741e6a3749d2e44d834b96c",
        "crc32c/1.1.1",
        "simde/0.8.2#5e1edfd5cba92f25d79bf6ef4616b972",
        "xxhash/0.8.3#199e63ab9800302c232d030b27accec0",
        "unordered_dense/4.4.0#6a855c992618cc4c63019109a2e47298",
        "geos/3.12.0#0b177c90c25a8ca210578fb9e2899c37",
        "icu/74.2#cd1937b9561b8950a2ae6311284c5813",
        "libavrocpp/1.12.1.1@milvus/dev#a77043b1b435c3abef7b45710d05b300",
        "rdkit/2025.09.1@milvus/dev",
    )

    generators = ("cmake", "cmake_find_package")
    default_options = {
        "openssl:shared": True,
        "openssl:no_apps": True,
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
        "arrow:with_jemalloc": False,
        "arrow:with_openssl": True,
        "arrow:shared": False,
        "arrow:with_azure": True,
        "arrow:with_s3": True,
        "arrow:encryption": True,
        "protobuf:shared": True,
        "grpc:shared": True,
        "grpc:secure": True,
        "aws-sdk-cpp:config": True,
        "aws-sdk-cpp:text-to-speech": False,
        "aws-sdk-cpp:transfer": False,
        "aws-sdk-cpp:s3-crt": True,
        "gtest:build_gmock": True,
        "boost:without_locale": False,
        "boost:without_test": True,
        "glog:with_gflags": True,
        "glog:shared": True,
        "prometheus-cpp:with_pull": False,
        "fmt:header_only": False,
        "onetbb:tbbmalloc": False,
        "onetbb:tbbproxy": False,
        "gflags:shared": True,
        "gdal:shared": False,
        "gdal:fPIC": True,
        "icu:shared": False,
        "icu:data_packaging": "library",
        "xz_utils:shared": True,
        "opentelemetry-cpp:with_stl": True,
    }

    def configure(self):
        if self.settings.arch not in ("x86_64", "x86"):
            del self.options["folly"].use_sse4_2
        if self.settings.os == "Macos":
            # abseil static linking on macOS (previously shared for X86 compat)
            self.options["arrow"].with_jemalloc = False
            # Use OpenSSL for libcurl on macOS
            self.options["libcurl"].with_ssl = "openssl"

    def requirements(self):
        if self.settings.os != "Macos":
            self.requires("libunwind/1.8.1#97965ef7da98cf1662e14219b14134f7")
        self.requires(
            "aws-sdk-cpp/1.11.692@milvus/dev#1e17deac19383217d291a01c23147b33"
        )
        # Override s2n 1.4.1 (from aws-c-io) to 1.6.0 for OpenSSL 3.x FIPS detection
        if self.settings.os in ["Linux", "FreeBSD"]:
            self.requires("s2n/1.6.0")

    def imports(self):
        self.copy("*.dylib", "../lib", "lib")
        self.copy("*.dll", "../lib", "lib")
        self.copy("*.so*", "../lib", "lib")
        self.copy("*", "../bin", "bin")
        self.copy("*.proto", "../include", "include")
