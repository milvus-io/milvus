"""
Setup script for milvus_minhash Python binding.

Build:
    pip install pybind11
    python setup.py build_ext --inplace

Or with pip:
    pip install .
"""

import os
import sys
from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext


class get_pybind_include:
    """Helper class to determine the pybind11 include path."""

    def __str__(self):
        import pybind11
        return pybind11.get_include()


# Detect platform-specific settings
if sys.platform == "darwin":
    # macOS: use Homebrew paths
    extra_compile_args = ["-std=c++17", "-O3"]
    extra_link_args = []

    # Try to find xxhash and openssl via Homebrew
    homebrew_prefix = "/opt/homebrew" if os.path.exists("/opt/homebrew") else "/usr/local"

    include_dirs = [
        get_pybind_include(),
        f"{homebrew_prefix}/include",
        f"{homebrew_prefix}/opt/openssl/include",
    ]
    library_dirs = [
        f"{homebrew_prefix}/lib",
        f"{homebrew_prefix}/opt/openssl/lib",
    ]
    libraries = ["xxhash", "crypto"]

elif sys.platform == "linux":
    extra_compile_args = ["-std=c++17", "-O3"]
    extra_link_args = []
    include_dirs = [get_pybind_include()]
    library_dirs = []
    libraries = ["xxhash", "crypto"]

else:
    raise RuntimeError(f"Unsupported platform: {sys.platform}")


ext_modules = [
    Extension(
        "milvus_minhash",
        sources=["minhash_binding.cpp"],
        include_dirs=include_dirs,
        library_dirs=library_dirs,
        libraries=libraries,
        extra_compile_args=extra_compile_args,
        extra_link_args=extra_link_args,
        language="c++",
    ),
]


class BuildExt(build_ext):
    """Custom build_ext to add C++17 support."""

    def build_extensions(self):
        # Remove -Wstrict-prototypes for C++ compilation
        if "-Wstrict-prototypes" in self.compiler.compiler_so:
            self.compiler.compiler_so.remove("-Wstrict-prototypes")
        super().build_extensions()


setup(
    name="milvus_minhash",
    version="0.1.0",
    author="Milvus Team",
    description="Milvus MinHash C++ binding for Python testing",
    long_description="",
    ext_modules=ext_modules,
    cmdclass={"build_ext": BuildExt},
    python_requires=">=3.8",
    install_requires=["pybind11>=2.6.0"],
)
