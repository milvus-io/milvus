required_conan_version = ">=2.0"

from conan import ConanFile
from conan.tools.cmake import CMakeDeps, CMakeToolchain
from conan.tools.files import copy
import os


class MilvusConan(ConanFile):
    settings = "os", "compiler", "build_type", "arch"
    requires = (
        "rocksdb/6.29.5@milvus/dev#67b8ae76ad7be5f779082f67416f89bf",
        "onetbb/2021.9.0#f9d7a3aa294ac4a594a93f9b4c7f272d",
        "zstd/1.5.5#70dc5eb8ea16708fc946fbac884c507e",
        "arrow/17.0.0@milvus/dev-2.6#c743ea7a6f2420ba5811b2be3df59892",
        "libevent/2.1.12#95065aaefcd58d3956d6dfbfc5631d97",
        "googleapis/cci.20221108#4553d68a2429cc0fff7d2bab4e5b3ea9",
        "gtest/1.13.0#2cf98fac7337eb73fc4ee839dbcd4468",
        "benchmark/1.7.0#459f3bb1a64400a886ba43047576df3c",
        "yaml-cpp/0.7.0#355a88fb838abacfeb022dd52b91e248",
        "marisa/0.2.6#a1352b20c0c6c48fee4968584ffef631",
        "glog/0.7.1#a306e61d7b8311db8cb148ad62c48030",
        "gflags/2.2.2#7671803f1dc19354cc90bd32874dcfda",
        "double-conversion/3.3.0#640e35791a4bac95b0545e2f54b7aceb",
        "libsodium/cci.20220430#7429a9e5351cc67bea3537229921714d",
        "xsimd/9.0.1#51df19a2d512f70597105ee2a2d21916",
        "xz_utils/5.4.5#fc4e36861e0a47ecd4a40a00e6d29ac8",
        "prometheus-cpp/1.2.4#0918d66c13f97acb7809759f9de49b3f",
        "re2/20230301#f8efaf45f98d0193cd0b2ea08b6b4060",
        "folly/2024.08.12.00@milvus/dev#f9b2bdf162c0ec47cb4e5404097b340d",
        "google-cloud-cpp/2.28.0@milvus/dev#468918b43cec43624531a0340398cf43",
        "opentelemetry-cpp/1.23.0@milvus/dev#11bc565ec6e82910ae8f7471da756720",
        "librdkafka/1.9.1#ec1a00d5414f618555799be9566adfb7",
        "roaring/3.0.0#25a703f80eda0764a31ef939229e202d",
        "crc32c/1.1.2",
        "simde/0.8.2#5e1edfd5cba92f25d79bf6ef4616b972",
        "xxhash/0.8.3#caa6d0af1b951c247922e38fbcebdbe6",
        "unordered_dense/4.4.0#6a855c992618cc4c63019109a2e47298",
        "geos/3.12.0#a923af6dc4c18f87a7dfa960118f3166",
        "icu/74.2#cd1937b9561b8950a2ae6311284c5813",
        "libavrocpp/1.12.1.1@milvus/dev#cde7bb587a29f6f233bae7e18b71815d",
    )

    default_options = {
        "openssl/*:shared": True,
        "openssl/*:no_apps": True,
        "libevent/*:shared": True,
        "double-conversion/*:shared": True,
        "folly/*:shared": True,
        "librdkafka/*:shared": True,
        "librdkafka/*:zstd": True,
        "librdkafka/*:ssl": True,
        "librdkafka/*:sasl": True,
        "rocksdb/*:shared": True,
        "rocksdb/*:with_zstd": True,
        "arrow/*:filesystem_layer": True,
        "arrow/*:parquet": True,
        "arrow/*:compute": True,
        "arrow/*:with_re2": True,
        "arrow/*:with_zstd": True,
        "arrow/*:with_snappy": True,
        "arrow/*:with_lz4": True,
        "arrow/*:with_boost": True,
        "arrow/*:with_thrift": True,
        "arrow/*:with_jemalloc": False,
        "arrow/*:with_openssl": True,
        "arrow/*:shared": False,
        "arrow/*:with_azure": True,
        "arrow/*:with_s3": True,
        "arrow/*:encryption": True,
        "protobuf/*:shared": True,
        "grpc/*:shared": True,
        "grpc/*:secure": True,
        "aws-sdk-cpp/*:config": True,
        "aws-sdk-cpp/*:text-to-speech": False,
        "aws-sdk-cpp/*:transfer": False,
        "aws-sdk-cpp/*:s3-crt": True,
        "gtest/*:build_gmock": True,
        "boost/*:without_locale": False,
        "boost/*:without_test": True,
        "glog/*:with_gflags": True,
        "glog/*:shared": True,
        "prometheus-cpp/*:with_pull": False,
        "fmt/*:header_only": False,
        "onetbb/*:tbbmalloc": False,
        "onetbb/*:tbbproxy": False,
        "gflags/*:shared": True,
        "gdal/*:shared": False,
        "gdal/*:fPIC": True,
        "icu/*:shared": False,
        "icu/*:data_packaging": "library",
        "xz_utils/*:shared": True,
        "opentelemetry-cpp/*:with_stl": True,
    }

    def configure(self):
        if self.settings.arch not in ("x86_64", "x86"):
            try:
                del self.options["folly"].use_sse4_2
            except Exception:
                pass
        if self.settings.os == "Macos":
            # abseil static linking on macOS (previously shared for X86 compat)
            self.options["arrow"].with_jemalloc = False
            # Use OpenSSL for libcurl on macOS
            self.options["libcurl"].with_ssl = "openssl"

    def requirements(self):
        # force=True: override transitive dependency versions (same behavior as Conan 1)
        # In Conan 1.x, the consumer's requires implicitly override transitive versions.
        # In Conan 2.x, force=True must be explicit.
        self.requires("boost/1.83.0#4e8a94ac1b88312af95eded83cd81ca8", force=True)
        self.requires("openssl/3.3.2#9f9f130d58e7c13e76bb8a559f0a6a8b", force=True)
        self.requires("protobuf/5.27.0@milvus/dev#42f031a96d21c230a6e05bcac4bdd633", force=True)
        self.requires("grpc/1.67.1@milvus/dev#efeaa484b59bffaa579004d5e82ec4fd", force=True)
        self.requires("zlib/1.3.1#8045430172a5f8d56ba001b14561b4ea", force=True)
        self.requires("libcurl/8.10.1#a3113369c86086b0e84231844e7ed0a9", force=True)
        self.requires("nlohmann_json/3.11.3#ffb9e9236619f1c883e36662f944345d", force=True)
        self.requires("abseil/20250127.0#481edcc75deb0efb16500f511f0f0a1c", force=True)
        self.requires("fmt/11.0.2#eb98daa559c7c59d591f4720dde4cd5c", force=True)
        self.requires("rapidjson/cci.20230929#0a3982e5f4fa453a9b9cd0dd5b1dcb3a", force=True)
        # azure-sdk-for-cpp is a transitive dep of Arrow, but must be declared
        # as a direct dep so CMakeDeps generates standalone cmake config files.
        # Without this, find_package(Azure) can't find include directories.
        self.requires("azure-sdk-for-cpp/1.11.3@milvus/dev#395e8e7a0c29644d41ef160088128f14")
        self.requires("aws-sdk-cpp/1.11.692@milvus/dev#c309ce91fa572fff68f9f4e36d477a04")
        # Force snappy/lz4 versions to override Arrow's older transitive deps
        # (arrow/*:with_snappy and arrow/*:with_lz4 are enabled for Parquet decoding)
        self.requires("snappy/1.2.1#b940695c64ccbff63c1aabd4b1eee3f3", force=True)
        self.requires("lz4/1.9.4#7f0b5851453198536c14354ee30ca9ae", force=True)
        if self.settings.os != "Macos":
            self.requires("libunwind/1.8.1#748a981ace010b80163a08867b732e71")
        # Override s2n 1.4.1 (from aws-c-io) to 1.6.0 for OpenSSL 3.x FIPS detection
        if self.settings.os in ["Linux", "FreeBSD"]:
            self.requires("s2n/1.6.0#4fa3b751b92e126a55e45dce723f0384", force=True)

    def generate(self):
        deps = CMakeDeps(self)
        # Set cmake file names to match what downstream projects expect
        deps.set_property("libavrocpp", "cmake_file_name", "libavrocpp")
        deps.set_property("libavrocpp", "cmake_target_name", "libavrocpp::libavrocpp")
        deps.generate()
        tc = CMakeToolchain(self)
        tc.generate()
        # Copy shared libraries (replaces imports() from Conan 1)
        # In Conan 1, imports() dst="lib" was relative to the build dir (cmake_build/).
        # In Conan 2, we use generators_folder (cmake_build/conan/) parent to get cmake_build/lib.
        build_dir = os.path.join(self.generators_folder, "..")
        for dep in self.dependencies.values():
            if dep.package_folder:
                copy(self, "*.so*", src=os.path.join(dep.package_folder, "lib"),
                     dst=os.path.join(build_dir, "lib"))
                copy(self, "*.dylib", src=os.path.join(dep.package_folder, "lib"),
                     dst=os.path.join(build_dir, "lib"))
                copy(self, "*.dll", src=os.path.join(dep.package_folder, "lib"),
                     dst=os.path.join(build_dir, "lib"))
                copy(self, "*", src=os.path.join(dep.package_folder, "bin"),
                     dst=os.path.join(build_dir, "bin"))
                copy(self, "*.proto", src=os.path.join(dep.package_folder, "include"),
                     dst=os.path.join(build_dir, "include"))
