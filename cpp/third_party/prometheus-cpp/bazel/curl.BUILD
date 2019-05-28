# Copyright 2017, OpenCensus Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# copied from: https://github.com/census-instrumentation/opencensus-cpp/blob/master/WORKSPACE

licenses(["notice"])  # MIT/X derivative license

load("@com_github_jupp0r_prometheus_cpp//bazel:curl.bzl", "CURL_COPTS")

package(features = ['no_copts_tokenization'])

config_setting(
    name = "windows",
    values = {"cpu": "x64_windows"},
    visibility = [ "//visibility:private" ],
)

config_setting(
    name = "osx",
    values = {"cpu": "darwin"},
    visibility = [ "//visibility:private" ],
)

cc_library(
    name = "curl",
    srcs = glob([
        "lib/**/*.c",
    ]),
    hdrs = glob([
        "include/curl/*.h",
        "lib/**/*.h",
    ]),
    defines = ["CURL_STATICLIB"],
    includes = ["include/", "lib/"],
    linkopts =  select({
        "//:windows": [
            "-DEFAULTLIB:ws2_32.lib",
            "-DEFAULTLIB:advapi32.lib",
            "-DEFAULTLIB:crypt32.lib",
            "-DEFAULTLIB:Normaliz.lib",
        ],
        "//conditions:default": [
            "-lpthread",
        ],
    }),
    copts = CURL_COPTS + [
        '-DOS="os"',
    ],
    visibility = ["//visibility:public"],
)
