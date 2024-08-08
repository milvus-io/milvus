from conans import ConanFile


class MilvusConan(ConanFile):
    keep_imports = True
    settings = "os", "compiler", "build_type", "arch"
    requires = (
        "rocksdb/6.29.5@milvus/dev#b1842a53ddff60240c5282a3da498ba1",
        "boost/1.82.0#20a883942beee1ebdda43efb5a49b0a1",
        "onetbb/2021.7.0#7129693884c5ebdb459f4dec96831ad9",
        "nlohmann_json/3.11.2#1ded6ae5d200a68ac17c51d528b945e2",
        "zstd/1.5.4#6e56ea56ac1c5d5e9e46b4ab3d222125",
        "lz4/1.9.4#652b313a0444c8b1d60d1bf9e95fb0a1",
        "snappy/1.1.9#5bcaf46bc878b6403844d1be7b674aa2",
        "lzo/2.10#5725914235423c771cb1c6b607109b45",
        "arrow/11.0.0#0418c93e9771905d1345b801a9b7f382",
        "openssl/3.1.2#200478bf15476d67e9dbaf217504b200",
        "aws-sdk-cpp/1.9.234#0ad8828e94ec4025c17631a41f62e629",
        "googleapis/cci.20221108#bbf3991516a8b3ce0923209c3df92665",
        "benchmark/1.7.0#24f5bc0a57af06c7e792aec1f4825092",
        "gtest/1.8.1#4f5a9df3ced2cb4bdbfd2606485593d2",
        "protobuf/3.21.4#8d66bd2fd3080a7f2a64ca9a2ffe53a6",
        "rapidxml/1.13#5f785a84cab7bca01142efb70a618afe",
        "yaml-cpp/0.7.0#474bea868febf8dae4005e35460a51bf",
        "marisa/0.2.6#55d3e3c273a2bfcc5f44adfa2351c78d",
        "zlib/1.2.13#4e74ebf1361fe6fb60326f473f276eb5",
        "libcurl/7.86.0#167caed693de03e80e7182393f921867",
        "glog/0.6.0#f25568477a1100349c4c2959ce946717",
        "fmt/9.1.0#df0c9f310f35025d83544798e6e6543a",
        "gflags/2.2.2#48d1262ffac8d30c3224befb8275a533",
        "double-conversion/3.2.1#0e6c00f6a48471b4b13163f01bd6bbb2",
        "libevent/2.1.12#b6333a128075d75a3614bd8418bf2099",
        "libdwarf/20191104#eae5fa1cb378f3b24919a0cd3d4baa23",
        "libiberty/9.1.0#a6a0b042484db9cefa233a8aa31a33c5",
        "libsodium/cci.20220430#cd827332735a7278432659372a1efb77",
        "xsimd/9.0.1#c74b1d825f467a37c2a2f03c0b022daa",
        "xz_utils/5.4.0#3cae9de8e20aec5621eb1883a46cefa3",
        "prometheus-cpp/1.1.0#2b8cfc6b45222336742b7fbc8ca9b53e",
        "re2/20230301#e8d673155a0494f27c7589b611e8350d",
        "folly/2023.10.30.04@milvus/dev#13826b4344e5c50c597c7d524cdadcab",
        "google-cloud-cpp/2.5.0@milvus/dev#279b39b63c9cdadf31dfcafb06faeed1",
        "opentelemetry-cpp/1.8.1.1@milvus/dev#24c505b6816decf52664823bd641f55d",
        "librdkafka/1.9.1#d7813404d7c9bce58044615afa9b9418",
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
            self.requires("libunwind/1.7.2")

    def imports(self):
        self.copy("*.dylib", "../lib", "lib")
        self.copy("*.dll", "../lib", "lib")
        self.copy("*.so*", "../lib", "lib")
        self.copy("*", "../bin", "bin")
        self.copy("*.proto", "../include", "include")
