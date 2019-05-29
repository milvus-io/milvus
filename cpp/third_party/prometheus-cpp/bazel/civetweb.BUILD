licenses(["notice"])  # MIT license

config_setting(
    name = "darwin",
    values = {"cpu": "darwin"},)

config_setting(
    name = "darwin_x86_64",
    values = {"cpu": "darwin_x86_64"},
)

config_setting(
    name = "windows",
    values = { "cpu": "x64_windows" },
)

config_setting(
    name = "windows_msvc",
    values = {"cpu": "x64_windows_msvc"},
)

cc_library(
    name = "libcivetweb",
    srcs = [
        "src/civetweb.c",
    ],
    hdrs = [
        "include/civetweb.h",
    ],
    copts = [
        "-DUSE_IPV6",
        "-DNDEBUG",
        "-DNO_CGI",
        "-DNO_CACHING",
        "-DNO_SSL",
        "-DNO_FILES",
        "-UDEBUG",
    ],
    includes = [
        "include",
    ],
    linkopts = select({
        ":windows": [],
        ":windows_msvc": [],
        "//conditions:default": ["-lpthread"],
    }) + select({
        ":darwin": [],
        ":darwin_x86_64": [],
        ":windows": [],
        ":windows_msvc": [],
        "//conditions:default": ["-lrt"],
    }),
    textual_hdrs = [
        "src/md5.inl",
        "src/handle_form.inl",
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "civetweb",
    srcs = [
        "src/CivetServer.cpp",
    ],
    hdrs = [
        "include/CivetServer.h",
    ],
    deps = [
        ":libcivetweb",
    ],
    copts = [
        "-DUSE_IPV6",
        "-DNDEBUG",
        "-DNO_CGI",
        "-DNO_CACHING",
        "-DNO_SSL",
        "-DNO_FILES",
    ],
    includes = [
        "include",
    ],
    linkopts = select({
        ":windows": [],
        ":windows_msvc": [],
        "//conditions:default": ["-lpthread"],
    }) + select({
        ":darwin": [],
        ":darwin_x86_64": [],
        ":windows": [],
        ":windows_msvc": [],
        "//conditions:default": ["-lrt"],
    }),
    visibility = ["//visibility:public"],
)