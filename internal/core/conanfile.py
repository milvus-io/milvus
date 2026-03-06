from conan import ConanFile
from conan.tools.cmake import CMakeDeps, CMakeToolchain
from conan.tools.files import copy
import os


class MilvusConan(ConanFile):
    settings = "os", "compiler", "build_type", "arch"
    requires = (
        "rocksdb/6.29.5@milvus/dev",
        "onetbb/2021.9.0",
        "zstd/1.5.5",
        "lz4/1.9.4",
        "snappy/1.1.9",
        "arrow/17.0.0@milvus/dev-2.6",
        "googleapis/cci.20221108",
        "gtest/1.13.0",
        "benchmark/1.7.0",
        "yaml-cpp/0.7.0",
        "marisa/0.2.6",
        "glog/0.6.0",
        "gflags/2.2.2",
        "double-conversion/3.2.1",
        "libsodium/cci.20220430",
        "xsimd/9.0.1",
        "xz_utils/5.4.0",
        "prometheus-cpp/1.1.0",
        "re2/20230301",
        "folly/2023.10.30.09@milvus/dev",
        "google-cloud-cpp/2.28.0@milvus/dev",
        "opentelemetry-cpp/1.8.1.1@milvus/2.4",
        "librdkafka/1.9.1",
        "roaring/3.0.0",
        "simde/0.8.2",
        "xxhash/0.8.3",
        "unordered_dense/4.4.0",
        "geos/3.12.0",
        "icu/74.2",
        "libavrocpp/1.12.1@milvus/dev",
        "rapidjson/1.1.0",
    )

    default_options = {
        "openssl/*:shared": True,
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
        "arrow/*:with_boost": True,
        "arrow/*:with_thrift": True,
        "arrow/*:with_jemalloc": True,
        "arrow/*:with_openssl": True,
        "arrow/*:shared": False,
        "arrow/*:with_azure": True,
        "arrow/*:with_s3": True,
        "arrow/*:encryption": True,
        "aws-sdk-cpp/*:config": True,
        "aws-sdk-cpp/*:text-to-speech": False,
        "aws-sdk-cpp/*:transfer": False,
        "aws-sdk-cpp/*:s3-crt": True,
        "gtest/*:build_gmock": True,
        "boost/*:without_locale": False,
        "boost/*:without_test": True,
        "folly/*:use_sse4_2": True,
        "glog/*:with_gflags": True,
        "glog/*:shared": True,
        "prometheus-cpp/*:with_pull": False,
        "fmt/*:header_only": True,
        "onetbb/*:tbbmalloc": False,
        "onetbb/*:tbbproxy": False,
        "gdal/*:shared": True,
        "gdal/*:fPIC": True,
        "icu/*:shared": False,
        "icu/*:data_packaging": "library",
        "opentelemetry-cpp/*:with_stl": True,
    }

    def configure(self):
        if self.settings.arch not in ("x86_64", "x86"):
            try:
                del self.options["folly"].use_sse4_2
            except Exception:
                pass
        if self.settings.os == "Macos":
            # By default abseil use static link but can not be compatible with macos X86
            self.options["abseil"].shared = True
            self.options["arrow"].with_jemalloc = False
            # Disable Arrow's S3 support on macOS - the Conan aws-c-* packages
            # (aws-c-cal, aws-c-io) use deprecated macOS Security framework APIs
            # that were removed in macOS 15+. Instead, milvus-storage provides
            # S3 support using Homebrew's aws-sdk-cpp (1.11.735+) which has
            # the compatibility fixes.
            self.options["arrow"].with_s3 = False
            # Use OpenSSL for libcurl on macOS
            self.options["libcurl"].with_ssl = "openssl"

    def requirements(self):
        # force=True: override transitive dependency versions (same behavior as Conan 1)
        self.requires("boost/1.83.0", force=True)
        self.requires("openssl/3.1.2", force=True)
        self.requires("protobuf/3.21.4", force=True)
        self.requires("grpc/1.50.1@milvus/dev", force=True)
        self.requires("zlib/1.2.13", force=True)
        self.requires("libcurl/7.86.0", force=True)
        self.requires("nlohmann_json/3.11.3", force=True)
        self.requires("abseil/20230125.3", force=True)
        self.requires("fmt/9.1.0", force=True)
        if self.settings.os != "Macos":
            self.requires("libunwind/1.7.2")
            # On Linux, use Conan's aws-sdk-cpp (Arrow and milvus-storage both use it)
            self.requires("aws-sdk-cpp/1.11.692@milvus/dev")
        # On macOS, S3 support is provided by milvus-storage using Homebrew's
        # aws-sdk-cpp (brew install aws-sdk-cpp). Arrow's S3 is disabled to avoid
        # pulling in incompatible aws-c-* packages from Conan.

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
