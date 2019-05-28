# copied from: https://github.com/bazelbuild/bazel/blob/master/third_party/zlib/BUILD

licenses(["notice"])  # BSD/MIT-like license (for zlib)

cc_library(
    name = "z",
    srcs = glob(["*.c"]),
    hdrs = glob(["*.h"]),
    # Use -Dverbose=-1 to turn off zlib's trace logging. (bazelbuild/bazel#3280)
    copts = ["-w", "-Dverbose=-1"],
    includes = ["."],
    visibility = ["//visibility:public"],
)