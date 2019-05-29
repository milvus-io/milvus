load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def load_civetweb():
    http_archive(
        name = "civetweb",
        strip_prefix = "civetweb-1.11",
        sha256 = "de7d5e7a2d9551d325898c71e41d437d5f7b51e754b242af897f7be96e713a42",
        urls = [
            "https://github.com/civetweb/civetweb/archive/v1.11.tar.gz",
        ],
        build_file = "@com_github_jupp0r_prometheus_cpp//bazel:civetweb.BUILD",
    )

def load_com_google_googletest():
    http_archive(
        name = "com_google_googletest",
        sha256 = "9bf1fe5182a604b4135edc1a425ae356c9ad15e9b23f9f12a02e80184c3a249c",
        strip_prefix = "googletest-release-1.8.1",
        urls = [
            "https://github.com/google/googletest/archive/release-1.8.1.tar.gz",
        ],
    )

def load_com_github_curl():
    http_archive(
        name = "com_github_curl",
        sha256 = "e9c37986337743f37fd14fe8737f246e97aec94b39d1b71e8a5973f72a9fc4f5",
        strip_prefix = "curl-7.60.0",
        urls = [
            "https://mirror.bazel.build/curl.haxx.se/download/curl-7.60.0.tar.gz",
            "https://curl.haxx.se/download/curl-7.60.0.tar.gz",
        ],
        build_file = "@com_github_jupp0r_prometheus_cpp//bazel:curl.BUILD",
    )

def load_com_github_google_benchmark():
    http_archive(
        name = "com_github_google_benchmark",
        sha256 = "f8e525db3c42efc9c7f3bc5176a8fa893a9a9920bbd08cef30fb56a51854d60d",
        strip_prefix = "benchmark-1.4.1",
        urls = [
            "https://github.com/google/benchmark/archive/v1.4.1.tar.gz",
        ],
    )

def load_net_zlib_zlib():
    http_archive(
        name = "net_zlib_zlib",
        sha256 = "c3e5e9fdd5004dcb542feda5ee4f0ff0744628baf8ed2dd5d66f8ca1197cb1a1",
        strip_prefix = "zlib-1.2.11",
        urls = [
            "https://mirror.bazel.build/zlib.net/zlib-1.2.11.tar.gz",
            "https://zlib.net/zlib-1.2.11.tar.gz",
        ],
        build_file = "@com_github_jupp0r_prometheus_cpp//bazel:zlib.BUILD",
    )

def prometheus_cpp_repositories():
    if "civetweb" not in native.existing_rules(): load_civetweb()
    if "com_google_googletest" not in native.existing_rules(): load_com_google_googletest()
    if "com_github_google_benchmark" not in native.existing_rules(): load_com_github_google_benchmark()
    if "com_github_curl" not in native.existing_rules(): load_com_github_curl()
    if "net_zlib_zlib" not in native.existing_rules(): load_net_zlib_zlib()
