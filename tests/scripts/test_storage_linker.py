"""Dependency-free CMake configuration checks; no full Milvus build required."""

import pathlib
import shutil
import subprocess
import tempfile
import unittest

MODULE = pathlib.Path(__file__).resolve().parents[2] / "internal/core/cmake/StorageLinker.cmake"


class StorageLinkerTest(unittest.TestCase):
    def configure(self, enabled, system="Linux", compiler="GNU", missing=False):
        with tempfile.TemporaryDirectory() as tmp:
            source = pathlib.Path(tmp)
            (source / "test.cpp").write_text("int example() { return 1; }\n")
            (source / "CMakeLists.txt").write_text(
                '''cmake_minimum_required(VERSION 3.18)
project(StorageLinkerTest LANGUAGES CXX)
set(CMAKE_SYSTEM_NAME "'''
                + system
                + '''")
set(CMAKE_SYSTEM_PROCESSOR x86_64)
set(CMAKE_CXX_COMPILER_ID "'''
                + compiler
                + '''")
add_library(storage SHARED test.cpp)
add_library(control SHARED test.cpp)
include("'''
                + MODULE.as_posix()
                + """")
milvus_configure_storage_linker(storage)
get_target_property(control_options control LINK_OPTIONS)
if(control_options)
  message(FATAL_ERROR "Linker options leaked to unrelated target")
endif()
"""
            )
            args = ["cmake", "-S", tmp, "-B", str(source / "build"), "-DMILVUS_STORAGE_USE_LLD=" + enabled]
            if missing:
                args += ["-DMILVUS_STORAGE_LLD_PROGRAM=/nonexistent/ld.lld"]
            return subprocess.run(args, text=True, capture_output=True)

    def test_default_off(self):
        result = self.configure("OFF")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_unsupported_platform_fails_when_enabled(self):
        result = self.configure("ON", system="Darwin")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("requires Linux", result.stdout + result.stderr)

    def test_invalid_lld_fails(self):
        result = self.configure("ON", missing=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("does not identify", result.stdout + result.stderr)

    @unittest.skipUnless(shutil.which("ld.lld"), "Native ld.lld is not installed")
    def test_available_lld(self):
        result = self.configure("ON")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
