from conans import ConanFile


class MilvusConan(ConanFile):
    keep_imports = True
    settings = "os", "compiler", "build_type", "arch"
    requires = (
        "rocksdb/6.29.5@milvus/dev#b1842a53ddff60240c5282a3da498ba1",
        "boost/1.82.0#744a17160ebb5838e9115eab4d6d0c06",
        "onetbb/2021.9.0#4a223ff1b4025d02f31b65aedf5e7f4a",
        "nlohmann_json/3.11.2#ffb9e9236619f1c883e36662f944345d",
        "zstd/1.5.4#308b8b048f9a3823ce248f9c150cc889",
        "lz4/1.9.4#c5afb86edd69ac0df30e3a9e192e43db",
        "snappy/1.1.9#0519333fef284acd04806243de7d3070",
        "lzo/2.10#9517fc1bcc4d4cc229a79806003a1baa",
        "arrow/17.0.0#8cea917a6e06ca17c28411966d6fcdd7",
        "openssl/3.1.2#02594c4c0a6e2b4feb3cd15119993597",
        "aws-sdk-cpp/1.11.352@milvus/dev",
        "googleapis/cci.20221108#65604e1b3b9a6b363044da625b201a2a",
        "benchmark/1.7.0#459f3bb1a64400a886ba43047576df3c",
        "gtest/1.13.0#f9548be18a41ccc6367efcb8146e92be",
        "protobuf/3.21.4#fd372371d994b8585742ca42c12337f9",
        "rapidxml/1.13#10c11a4bfe073e131ed399d5c4f2e075",
        "yaml-cpp/0.7.0#9c87b3998de893cf2e5a08ad09a7a6e0",
        "marisa/0.2.6#68446854f5a420672d21f21191f8e5af",
        "zlib/1.2.13#df233e6bed99052f285331b9f54d9070",
        "libcurl/7.86.0#bbc887fae3341b3cb776c601f814df05",
        "glog/0.6.0#d22ebf9111fed68de86b0fa6bf6f9c3f",
        "fmt/9.1.0#95259249fb7ef8c6b5674a40b00abba3",
        "gflags/2.2.2#b15c28c567c7ade7449cf994168a559f",
        "double-conversion/3.2.1#640e35791a4bac95b0545e2f54b7aceb",
        "libevent/2.1.12#4fd19d10d3bed63b3a8952c923454bc0",
        "libdwarf/20191104#7f56c6c7ccda5fadf5f28351d35d7c01",
        "libiberty/9.1.0#3060045a116b0fff6d4937b0fc9cfc0e",
        "libsodium/cci.20220430#7429a9e5351cc67bea3537229921714d",
        "xsimd/9.0.1#ac9fd02a381698c4e08c5c4ca03b73e1",
        "xz_utils/5.4.0#a6d90890193dc851fa0d470163271c7a",
        "prometheus-cpp/1.1.0#ea9b101cb785943adb40ad82eda7856c",
        "re2/20230301#f8efaf45f98d0193cd0b2ea08b6b4060",
        "folly/2023.10.30.08@milvus/dev#81d7729cd4013a1b708af3340a3b04d9",
        "google-cloud-cpp/2.5.0@milvus/2.4#c5591ab30b26b53ea6068af6f07128d3",
        "opentelemetry-cpp/1.8.1.1@milvus/2.4#7345034855d593047826b0c74d9a0ced",
        "librdkafka/1.9.1#e24dcbb0a1684dcf5a56d8d0692ceef3",
        "abseil/20230125.3#dad7cc4c83bbd44c1f1cc9cc4d97ac88",
        "roaring/3.0.0#25a703f80eda0764a31ef939229e202d",
        "grpc/1.50.1@milvus/dev#75103960d1cac300cf425ccfccceac08",
        "rapidjson/cci.20230929#624c0094d741e6a3749d2e44d834b96c",
        "simde/0.8.2#5e1edfd5cba92f25d79bf6ef4616b972"
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
        "aws-sdk-cpp:s3-crt": True,
        "aws-sdk-cpp:config": True,
        "aws-sdk-cpp:text-to-speech": False,
        "aws-sdk-cpp:transfer": False,
        "gtest:build_gmock": False,
        "boost:without_locale": False,
        "boost:without_test": True,
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
