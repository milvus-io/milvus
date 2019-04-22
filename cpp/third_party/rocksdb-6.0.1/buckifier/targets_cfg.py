from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
rocksdb_target_header = """load("@fbcode_macros//build_defs:auto_headers.bzl", "AutoHeaders")
load("@fbcode_macros//build_defs:cpp_library.bzl", "cpp_library")
load(":defs.bzl", "test_binary")

REPO_PATH = package_name() + "/"

ROCKSDB_COMPILER_FLAGS = [
    "-fno-builtin-memcmp",
    "-DROCKSDB_PLATFORM_POSIX",
    "-DROCKSDB_LIB_IO_POSIX",
    "-DROCKSDB_FALLOCATE_PRESENT",
    "-DROCKSDB_MALLOC_USABLE_SIZE",
    "-DROCKSDB_RANGESYNC_PRESENT",
    "-DROCKSDB_SCHED_GETCPU_PRESENT",
    "-DROCKSDB_SUPPORT_THREAD_LOCAL",
    "-DOS_LINUX",
    # Flags to enable libs we include
    "-DSNAPPY",
    "-DZLIB",
    "-DBZIP2",
    "-DLZ4",
    "-DZSTD",
    "-DZSTD_STATIC_LINKING_ONLY",
    "-DGFLAGS=gflags",
    "-DNUMA",
    "-DTBB",
    # Needed to compile in fbcode
    "-Wno-expansion-to-defined",
    # Added missing flags from output of build_detect_platform
    "-DROCKSDB_PTHREAD_ADAPTIVE_MUTEX",
    "-DROCKSDB_BACKTRACE",
    "-Wnarrowing",
]

ROCKSDB_EXTERNAL_DEPS = [
    ("bzip2", None, "bz2"),
    ("snappy", None, "snappy"),
    ("zlib", None, "z"),
    ("gflags", None, "gflags"),
    ("lz4", None, "lz4"),
    ("zstd", None),
    ("tbb", None),
    ("numa", None, "numa"),
    ("googletest", None, "gtest"),
]

ROCKSDB_PREPROCESSOR_FLAGS = [
    # Directories with files for #include
    "-I" + REPO_PATH + "include/",
    "-I" + REPO_PATH,
]

ROCKSDB_ARCH_PREPROCESSOR_FLAGS = {
    "x86_64": [
        "-DHAVE_SSE42",
        "-DHAVE_PCLMUL",
    ],
}

build_mode = read_config("fbcode", "build_mode")

is_opt_mode = build_mode.startswith("opt")

# -DNDEBUG is added by default in opt mode in fbcode. But adding it twice
# doesn't harm and avoid forgetting to add it.
ROCKSDB_COMPILER_FLAGS += (["-DNDEBUG"] if is_opt_mode else [])

sanitizer = read_config("fbcode", "sanitizer")

# Do not enable jemalloc if sanitizer presents. RocksDB will further detect
# whether the binary is linked with jemalloc at runtime.
ROCKSDB_COMPILER_FLAGS += (["-DROCKSDB_JEMALLOC"] if sanitizer == "" else [])

ROCKSDB_EXTERNAL_DEPS += ([("jemalloc", None, "headers")] if sanitizer == "" else [])
"""


library_template = """
cpp_library(
    name = "{name}",
    srcs = [{srcs}],
    {headers_attr_prefix}headers = {headers},
    arch_preprocessor_flags = ROCKSDB_ARCH_PREPROCESSOR_FLAGS,
    compiler_flags = ROCKSDB_COMPILER_FLAGS,
    preprocessor_flags = ROCKSDB_PREPROCESSOR_FLAGS,
    deps = [{deps}],
    external_deps = ROCKSDB_EXTERNAL_DEPS,
)
"""

binary_template = """
cpp_binary(
    name = "%s",
    srcs = [%s],
    arch_preprocessor_flags = ROCKSDB_ARCH_PREPROCESSOR_FLAGS,
    compiler_flags = ROCKSDB_COMPILER_FLAGS,
    preprocessor_flags = ROCKSDB_PREPROCESSOR_FLAGS,
    deps = [%s],
    external_deps = ROCKSDB_EXTERNAL_DEPS,
)
"""

test_cfg_template = """    [
        "%s",
        "%s",
        "%s",
    ],
"""

unittests_template = """
# [test_name, test_src, test_type]
ROCKS_TESTS = [
%s]

# Generate a test rule for each entry in ROCKS_TESTS
# Do not build the tests in opt mode, since SyncPoint and other test code
# will not be included.
[
    test_binary(
        parallelism = parallelism,
        rocksdb_arch_preprocessor_flags = ROCKSDB_ARCH_PREPROCESSOR_FLAGS,
        rocksdb_compiler_flags = ROCKSDB_COMPILER_FLAGS,
        rocksdb_external_deps = ROCKSDB_EXTERNAL_DEPS,
        rocksdb_preprocessor_flags = ROCKSDB_PREPROCESSOR_FLAGS,
        test_cc = test_cc,
        test_name = test_name,
    )
    for test_name, test_cc, parallelism in ROCKS_TESTS
    if not is_opt_mode
]
"""
