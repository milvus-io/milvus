// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <vector>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/cpu-info.h"

namespace arrow {
namespace compute {

using internal::CpuInfo;
static CpuInfo* cpu_info = CpuInfo::GetInstance();

static const int64_t kL1Size = cpu_info->CacheSize(CpuInfo::L1_CACHE);
static const int64_t kL2Size = cpu_info->CacheSize(CpuInfo::L2_CACHE);
static const int64_t kL3Size = cpu_info->CacheSize(CpuInfo::L3_CACHE);
static const int64_t kCantFitInL3Size = kL3Size * 4;
static const std::vector<int64_t> kMemorySizes = {kL1Size, kL2Size, kL3Size,
                                                  kCantFitInL3Size};

template <typename Func>
struct BenchmarkArgsType;

// Pattern matching that extracts the vector element type of Benchmark::Args()
template <typename Values>
struct BenchmarkArgsType<benchmark::internal::Benchmark* (
    benchmark::internal::Benchmark::*)(const std::vector<Values>&)> {
  using type = Values;
};

// Benchmark changed its parameter type between releases from
// int to int64_t. As it doesn't have version macros, we need
// to apply C++ template magic.
using ArgsType =
    typename BenchmarkArgsType<decltype(&benchmark::internal::Benchmark::Args)>::type;

void BenchmarkSetArgsWithSizes(benchmark::internal::Benchmark* bench,
                               const std::vector<int64_t>& sizes = kMemorySizes) {
  bench->Unit(benchmark::kMicrosecond);

  for (auto size : sizes)
    for (auto nulls : std::vector<ArgsType>({0, 1, 10, 50}))
      bench->Args({static_cast<ArgsType>(size), nulls});
}

void BenchmarkSetArgs(benchmark::internal::Benchmark* bench) {
  BenchmarkSetArgsWithSizes(bench, kMemorySizes);
}

void RegressionSetArgs(benchmark::internal::Benchmark* bench) {
  // Regression do not need to account for cache hierarchy, thus optimize for
  // the best case.
  BenchmarkSetArgsWithSizes(bench, {kL1Size});
}

// RAII struct to handle some of the boilerplate in regression benchmarks
struct RegressionArgs {
  // size of memory tested (per iteration) in bytes
  const int64_t size;

  // proportion of nulls in generated arrays
  const double null_proportion;

  explicit RegressionArgs(benchmark::State& state)
      : size(state.range(0)),
        null_proportion(static_cast<double>(state.range(1)) / 100.0),
        state_(state) {}

  ~RegressionArgs() {
    state_.counters["size"] = static_cast<double>(size);
    state_.counters["null_percent"] = static_cast<double>(state_.range(1));
    state_.SetBytesProcessed(state_.iterations() * size);
  }

 private:
  benchmark::State& state_;
};

}  // namespace compute
}  // namespace arrow
