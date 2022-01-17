// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <benchmark/benchmark.h>
#include <string>

static void
BN_Naive_StringCreation(benchmark::State& state) {
    for (auto _ : state) std::string empty_string;
}
// Register the function as a benchmark
BENCHMARK(BN_Naive_StringCreation);

// Define another benchmark
static void
BN_Naive_StringCopy(benchmark::State& state) {
    std::string x = "hello";
    for (auto _ : state) std::string copy(x);
}
BENCHMARK(BN_Naive_StringCopy);
