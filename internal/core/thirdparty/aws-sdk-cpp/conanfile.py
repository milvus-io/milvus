import os

from conan import ConanFile
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout
from conan.tools.files import get, replace_in_file, rmdir


class AwsSdkCppConan(ConanFile):
    name = "aws-sdk-cpp"
    version = "1.11.921+rdma"
    user = "milvus"
    channel = "dev"
    package_type = "library"
    settings = "os", "arch", "compiler", "build_type"
    options = {
        "shared": [True, False],
        "fPIC": [True, False],
        "config": [True, False],
        "cognito-identity": [True, False],
        "identity-management": [True, False],
        "s3": [True, False],
        "s3-crt": [True, False],
        "sts": [True, False],
    }
    default_options = {
        "shared": True,
        "fPIC": True,
        "config": True,
        "cognito-identity": True,
        "identity-management": True,
        "s3": True,
        "s3-crt": True,
        "sts": True,
    }

    def config_options(self):
        if self.settings.os == "Windows":
            self.options.rm_safe("fPIC")

    def configure(self):
        if self.options.shared:
            self.options.rm_safe("fPIC")

    def layout(self):
        cmake_layout(self, src_folder="src")

    def requirements(self):
        self.requires("aws-c-common/1.0.5")
        self.requires("aws-c-event-stream/1.0.0")
        self.requires("aws-checksums/1.0.1")
        self.requires("aws-c-cal/1.0.0")
        self.requires("aws-c-http/1.1.1")
        self.requires("aws-c-io/1.1.1")
        self.requires("aws-c-auth/1.0.2")
        self.requires("aws-c-compression/1.0.0")
        self.requires("aws-c-mqtt/1.1.2")
        self.requires("aws-c-sdkutils/1.0.1")
        self.requires("aws-crt-cpp/0.44.3", transitive_headers=True)
        if self.options.get_safe("s3-crt"):
            self.requires("aws-c-s3/1.5.1")
        if self.settings.os != "Windows":
            self.requires("openssl/3.3.2")
            self.requires("libcurl/8.10.1")

    def source(self):
        get(
            self,
            url="https://github.com/cloudian/aws-sdk-cpp/archive/refs/tags/1.11.921+rdma.tar.gz",
            sha256="fa3302f68fab3d7d09d703c32432b02bffb7d1692bc0696444c01ccc522e4e1e",
            strip_root=True,
        )
        replace_in_file(
            self,
            os.path.join(self.source_folder, "generated/src/aws-cpp-sdk-s3/CMakeLists.txt"),
            "find_package(CUDAToolkit 13.1 REQUIRED)\n",
            "",
            strict=True,
        )

    def generate(self):
        toolchain = CMakeToolchain(self)
        cuobj_include_dir = self.conf.get("user.aws-sdk-cpp:cuobj_include_dir", default=None)
        if not cuobj_include_dir or not os.path.isfile(os.path.join(cuobj_include_dir, "cuobjclient.h")):
            raise ValueError(
                "Set user.aws-sdk-cpp:cuobj_include_dir to the directory containing cuobjclient.h"
            )
        build_only = ["core"]
        if self.options.config:
            build_only.append("config")
        if self.options.get_safe("cognito-identity"):
            build_only.append("cognito-identity")
        if self.options.get_safe("identity-management"):
            build_only.append("identity-management")
        if self.options.s3:
            build_only.append("s3")
        if self.options.get_safe("s3-crt"):
            build_only.append("s3-crt")
        if self.options.get_safe("sts"):
            build_only.append("sts")
        toolchain.cache_variables["BUILD_ONLY"] = ";".join(build_only)
        toolchain.cache_variables["BUILD_DEPS"] = False
        toolchain.cache_variables["ENABLE_TESTING"] = False
        toolchain.cache_variables["AUTORUN_UNIT_TESTS"] = False
        toolchain.cache_variables["ENABLE_OPENSSL_ENCRYPTION"] = True
        toolchain.cache_variables["BUILD_SHARED_LIBS"] = self.options.shared
        toolchain.cache_variables["NVIDIA_CUOBJ_INCLUDE_DIR"] = cuobj_include_dir
        toolchain.generate()
        CMakeDeps(self).generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        cmake = CMake(self)
        cmake.install()
        rmdir(self, f"{self.package_folder}/lib/cmake")
        rmdir(self, f"{self.package_folder}/lib/pkgconfig")

    def package_info(self):
        self.cpp_info.set_property("cmake_file_name", "AWSSDK")
        self.cpp_info.components["core"].set_property("cmake_target_name", "AWS::aws-sdk-cpp-core")
        self.cpp_info.components["core"].libs = ["aws-cpp-sdk-core"]
        self.cpp_info.components["core"].requires = [
            "aws-c-common::aws-c-common",
            "aws-c-event-stream::aws-c-event-stream",
            "aws-checksums::aws-checksums",
            "aws-c-cal::aws-c-cal",
            "aws-c-http::aws-c-http",
            "aws-c-io::aws-c-io",
            "aws-c-auth::aws-c-auth",
            "aws-c-compression::aws-c-compression",
            "aws-c-mqtt::aws-c-mqtt",
            "aws-c-sdkutils::aws-c-sdkutils",
            "aws-crt-cpp::aws-crt-cpp",
            "libcurl::curl",
            "openssl::openssl",
        ]
        for component in ("config", "cognito-identity", "identity-management", "s3", "s3-crt", "sts"):
            if self.options.get_safe(component):
                self.cpp_info.components[component].set_property(
                    "cmake_target_name", f"AWS::aws-sdk-cpp-{component}"
                )
                library_name = f"aws-cpp-sdk-{component}"
                if component == "s3":
                    library_name = "cloudian-aws-cpp-sdk-s3"
                self.cpp_info.components[component].libs = [library_name]
                self.cpp_info.components[component].requires = ["core"]
        if self.options.get_safe("identity-management"):
            self.cpp_info.components["identity-management"].requires.extend(
                ["cognito-identity", "sts"]
            )
        if self.options.get_safe("s3-crt"):
            self.cpp_info.components["s3-crt"].requires.append("aws-c-s3::aws-c-s3")
