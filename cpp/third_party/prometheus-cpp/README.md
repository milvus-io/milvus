# Prometheus Client Library for Modern C++ [![Build Status](https://travis-ci.org/jupp0r/prometheus-cpp.svg?branch=master)](https://travis-ci.org/jupp0r/prometheus-cpp)[![Coverage Status](https://coveralls.io/repos/github/jupp0r/prometheus-cpp/badge.svg?branch=master)](https://coveralls.io/github/jupp0r/prometheus-cpp?branch=master)[![Coverity Scan](https://scan.coverity.com/projects/10567/badge.svg)](https://scan.coverity.com/projects/jupp0r-prometheus-cpp)

This library aims to enable
[Metrics-Driven Development](https://sookocheff.com/post/mdd/mdd/) for
C++ services. It implements the
[Prometheus Data Model](https://prometheus.io/docs/concepts/data_model/),
a powerful abstraction on which to collect and expose metrics. We
offer the possibility for metrics to be collected by Prometheus, but
other push/pull collections can be added as plugins.

## Usage

``` c++
#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <thread>

#include <prometheus/exposer.h>
#include <prometheus/registry.h>

int main(int argc, char** argv) {
  using namespace prometheus;

  // create an http server running on port 8080
  Exposer exposer{"127.0.0.1:8080"};

  // create a metrics registry with component=main labels applied to all its
  // metrics
  auto registry = std::make_shared<Registry>();

  // add a new counter family to the registry (families combine values with the
  // same name, but distinct label dimensions)
  auto& counter_family = BuildCounter()
                             .Name("time_running_seconds_total")
                             .Help("How many seconds is this server running?")
                             .Labels({{"label", "value"}})
                             .Register(*registry);

  // add a counter to the metric family
  auto& second_counter = counter_family.Add(
      {{"another_label", "value"}, {"yet_another_label", "value"}});

  // ask the exposer to scrape the registry on incoming scrapes
  exposer.RegisterCollectable(registry);

  for (;;) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    // increment the counter by one (second)
    second_counter.Increment();
  }
  return 0;
}
```

## Requirements

Using `prometheus-cpp` requires a C++11 compliant compiler. It has been successfully tested with GNU GCC 4.8 on Ubuntu Trusty and Visual Studio 2017 (but Visual Studio 2015 should work, too).

## Building

There are two supported ways to build
`prometheus-cpp` - [CMake](https://cmake.org)
and [bazel](https://bazel.io). Both are tested in CI and should work
on master and for all releases.

In case these instructions don't work for you, looking at
the [travis build script](.travis.yml) might help.

### via CMake

For CMake builds don't forget to fetch the submodules first. Then build as usual.

``` shell
# fetch third-party dependencies
git submodule init
git submodule update

mkdir _build
cd _build

# run cmake
cmake .. -DBUILD_SHARED_LIBS=ON # or OFF for static libraries

# build
make -j 4

# run tests
ctest -V

# install the libraries and headers
mkdir -p deploy
make DESTDIR=`pwd`/deploy install
```

### via Bazel

Install [bazel](https://www.bazel.io).  Bazel makes it easy to add
this repo to your project as a dependency. Just add the following
to your `WORKSPACE`:

```python
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")
http_archive(
    name = "com_github_jupp0r_prometheus_cpp",
    strip_prefix = "prometheus-cpp-master",
    urls = ["https://github.com/jupp0r/prometheus-cpp/archive/master.zip"],
)

load("@com_github_jupp0r_prometheus_cpp//:repositories.bzl", "prometheus_cpp_repositories")

prometheus_cpp_repositories()
```

Then, you can reference this library in your own `BUILD` file, as
demonstrated with the sample server included in this repository:

```python
cc_binary(
    name = "sample_server",
    srcs = ["sample_server.cc"],
    deps = ["@com_github_jupp0r_prometheus_cpp//pull"],
)
```

When you call `prometheus_cpp_repositories()` in your `WORKSPACE` file,
you introduce the following dependencies, if they do not exist yet, to your project:

* `load_civetweb()` to load `civetweb` rules for Civetweb
* `load_com_google_googletest()` to load `com_google_googletest` rules for Google gtest
* `load_com_google_googlebenchmark()` to load `com_github_google_benchmark` rules for Googlebenchmark
* `load_com_github_curl()` to load `com_github_curl` rules for curl
* `load_net_zlib_zlib()` to load `net_zlib_zlib` rules for zlib

The list of dependencies is also available from file `repositories.bzl`.


## Contributing

Please adhere to the [Google C++ Style
Guide](https://google.github.io/styleguide/cppguide.html). Make sure
to clang-format your patches before opening a PR. Also make sure to
adhere to [these commit message
guidelines](https://chris.beams.io/posts/git-commit/).

You can check out this repo and build the library using
``` bash
bazel build //...           # build everything
bazel build //core //pull   # build just the libraries
```

Run the unit tests using
```
bazel test //...
```

There is also an integration test that
uses [telegraf](https://github.com/influxdata/telegraf) to scrape a
sample server. With telegraf installed, it can be run using
```
bazel test //pull/tests/integration:scrape-test
```

## Benchmarks

There's a benchmark suite you can run:

```
bazel run -c opt //core/benchmarks

INFO: Analysed target //core/benchmarks:benchmarks (0 packages loaded, 0 targets configured).
INFO: Found 1 target...
Target //core/benchmarks:benchmarks up-to-date:
  bazel-bin/core/benchmarks/benchmarks
INFO: Elapsed time: 0.356s, Critical Path: 0.01s, Remote (0.00% of the time): [queue: 0.00%, setup: 0.00%, process: 0.00%]
INFO: 0 processes.
INFO: Build completed successfully, 1 total action
INFO: Build completed successfully, 1 total action
2018-11-30 15:13:14
Run on (4 X 2200 MHz CPU s)
CPU Caches:
  L1 Data 32K (x2)
  L1 Instruction 32K (x2)
  L2 Unified 262K (x2)
  L3 Unified 4194K (x1)
-----------------------------------------------------------------------------------
Benchmark                                            Time           CPU Iterations
-----------------------------------------------------------------------------------
BM_Counter_Increment                                13 ns         12 ns   55616469
BM_Counter_Collect                                   7 ns          7 ns   99823170
BM_Gauge_Increment                                  12 ns         12 ns   51511873
BM_Gauge_Decrement                                  12 ns         12 ns   56831098
BM_Gauge_SetToCurrentTime                          184 ns        183 ns    3928964
BM_Gauge_Collect                                     6 ns          6 ns  117223478
BM_Histogram_Observe/0                             134 ns        124 ns    5665310
BM_Histogram_Observe/1                             122 ns        120 ns    5937185
BM_Histogram_Observe/8                             137 ns        135 ns    4652863
BM_Histogram_Observe/64                            143 ns        143 ns    4835957
BM_Histogram_Observe/512                           259 ns        257 ns    2334750
BM_Histogram_Observe/4096                         1545 ns       1393 ns     620754
BM_Histogram_Collect/0                             103 ns        102 ns    5654829
BM_Histogram_Collect/1                             100 ns        100 ns    7015153
BM_Histogram_Collect/8                             608 ns        601 ns    1149652
BM_Histogram_Collect/64                           1438 ns       1427 ns     515236
BM_Histogram_Collect/512                          5178 ns       5159 ns     114619
BM_Histogram_Collect/4096                        33527 ns      33280 ns      20785
BM_Registry_CreateFamily                           320 ns        316 ns    2021567
BM_Registry_CreateCounter/0                        128 ns        128 ns    5487140
BM_Registry_CreateCounter/1                       2066 ns       2058 ns     386002
BM_Registry_CreateCounter/8                       7672 ns       7634 ns      91328
BM_Registry_CreateCounter/64                     63270 ns      62761 ns      10780
BM_Registry_CreateCounter/512                   560714 ns     558328 ns       1176
BM_Registry_CreateCounter/4096                18672798 ns   18383000 ns         35
BM_Summary_Observe/0/iterations:262144            9351 ns       9305 ns     262144
BM_Summary_Observe/1/iterations:262144            9242 ns       9169 ns     262144
BM_Summary_Observe/8/iterations:262144           14344 ns      14195 ns     262144
BM_Summary_Observe/64/iterations:262144          19176 ns      18950 ns     262144
BM_Summary_Collect/0/0                              31 ns         30 ns   24873766
BM_Summary_Collect/1/0                             166 ns        166 ns    4266706
BM_Summary_Collect/8/0                            1040 ns       1036 ns     660527
BM_Summary_Collect/64/0                           4529 ns       4489 ns     155600
BM_Summary_Collect/0/1                              28 ns         28 ns   24866697
BM_Summary_Collect/1/1                             190 ns        188 ns    3930354
BM_Summary_Collect/8/1                            1372 ns       1355 ns     535779
BM_Summary_Collect/64/1                           9901 ns       9822 ns      64632
BM_Summary_Collect/0/8                              29 ns         29 ns   24922651
BM_Summary_Collect/1/8                             217 ns        215 ns    3278381
BM_Summary_Collect/8/8                            2275 ns       2256 ns     282503
BM_Summary_Collect/64/8                          56790 ns      55804 ns      13878
BM_Summary_Collect/0/64                             32 ns         31 ns   22548350
BM_Summary_Collect/1/64                            395 ns        389 ns    1817073
BM_Summary_Collect/8/64                          10187 ns      10064 ns      71928
BM_Summary_Collect/64/64                        374835 ns     373560 ns       1812
BM_Summary_Collect/0/512                            28 ns         28 ns   25234228
BM_Summary_Collect/1/512                          1710 ns       1639 ns     802285
BM_Summary_Collect/8/512                         50355 ns      49335 ns      15975
BM_Summary_Collect/64/512                      2520972 ns    2493417 ns        295
BM_Summary_Collect/0/4096                           31 ns         31 ns   24059034
BM_Summary_Collect/1/4096                         2719 ns       2698 ns     286186
BM_Summary_Collect/8/4096                       121689 ns     119995 ns       5647
BM_Summary_Collect/64/4096                     5660131 ns    5587634 ns        134
BM_Summary_Collect/0/32768                          29 ns         29 ns   22217567
BM_Summary_Collect/1/32768                        4344 ns       4294 ns     138135
BM_Summary_Collect/8/32768                      331563 ns     326403 ns       2017
BM_Summary_Collect/64/32768                   16363553 ns   16038182 ns         44
BM_Summary_Collect/0/262144                         27 ns         27 ns   23923036
BM_Summary_Collect/1/262144                      10457 ns      10332 ns      67690
BM_Summary_Collect/8/262144                     930434 ns     869234 ns        792
BM_Summary_Collect/64/262144                  39217069 ns   39054846 ns         13
BM_Summary_Observe_Common/iterations:262144       5587 ns       5557 ns     262144
BM_Summary_Collect_Common/0                        676 ns        673 ns    1054630
BM_Summary_Collect_Common/1                        709 ns        705 ns     990659
BM_Summary_Collect_Common/8                       1030 ns       1025 ns     685649
BM_Summary_Collect_Common/64                      2066 ns       2055 ns     339969
BM_Summary_Collect_Common/512                     5754 ns       5248 ns     156895
BM_Summary_Collect_Common/4096                   23894 ns      23292 ns      31096
BM_Summary_Collect_Common/32768                  49831 ns      49292 ns      13492
BM_Summary_Collect_Common/262144                128723 ns     126987 ns       5579
```

## Project Status
Beta, getting ready for 1.0. The library is pretty stable and used in
production. There are some small breaking API changes that might
happen before 1.0 Parts of the library are instrumented by itself
(bytes scraped, number of scrapes, scrape request latencies).  There
is a working [example](pull/tests/integration/sample_server.cc) that's
scraped by telegraf as part of integration tests.

## FAQ

### What scrape formats do you support

Only the [Prometheus Text Exposition
Format](https://github.com/prometheus/docs/blob/master/content/docs/instrumenting/exposition_formats.md#text-format-details).
Support for the protobuf format was removed because it's been removed
from Prometheus 2.0.

## License

MIT
