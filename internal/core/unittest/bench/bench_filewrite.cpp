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
#include <filesystem>
#include "common/File.h"

static void
BN_FILE_Write_Syscall(benchmark::State& stats, int n) {
    std::string s(n, '*');
    auto file_path = std::filesystem::current_path() / "bn_write_syscall";
    milvus::File f =
        milvus::File::Open(file_path.string(), O_CREAT | O_TRUNC | O_RDWR);

    for (auto _ : stats) {
        f.WriteInt(n);
        f.Write(s.c_str(), n);
    }
}

static void
BN_FILE_Write_Stream(benchmark::State& stats, size_t buf_size, int n) {
    std::string s(n, '*');
    auto file_path = std::filesystem::current_path() / "bn_write_stream";
    milvus::File f = milvus::File::Open(
        file_path.string(), O_CREAT | O_TRUNC | O_RDWR, buf_size);

    for (auto _ : stats) {
        f.FWriteInt(n);
        f.FWrite(s.c_str(), n);
    }
    f.FFlush();
}

static void
BN_FILE_Write_Syscall_2(benchmark::State& stats) {
    BN_FILE_Write_Syscall(stats, 2);
}
BENCHMARK(BN_FILE_Write_Syscall_2);

static void
BN_FILE_Write_Syscall_65535(benchmark::State& stats) {
    BN_FILE_Write_Syscall(stats, 65535);
}
BENCHMARK(BN_FILE_Write_Syscall_65535);

static void
BN_FILE_Write_Stream_4096_2(benchmark::State& stats) {
    BN_FILE_Write_Stream(stats, 4096, 2);
}
BENCHMARK(BN_FILE_Write_Stream_4096_2);

static void
BN_FILE_Write_Stream_16384_2(benchmark::State& stats) {
    BN_FILE_Write_Stream(stats, 16384, 2);
}
BENCHMARK(BN_FILE_Write_Stream_16384_2);

static void
BN_FILE_Write_Stream_163840_2(benchmark::State& stats) {
    BN_FILE_Write_Stream(stats, 163840, 2);
}
BENCHMARK(BN_FILE_Write_Stream_163840_2);

static void
BN_FILE_Write_Stream_4096_65535(benchmark::State& stats) {
    BN_FILE_Write_Stream(stats, 4096, 65535);
}
BENCHMARK(BN_FILE_Write_Stream_4096_65535);

static void
BN_FILE_Write_Stream_16384_65535(benchmark::State& stats) {
    BN_FILE_Write_Stream(stats, 16384, 65535);
}
BENCHMARK(BN_FILE_Write_Stream_16384_65535);

static void
BN_FILE_Write_Stream_163840_65535(benchmark::State& stats) {
    BN_FILE_Write_Stream(stats, 163840, 65535);
}
BENCHMARK(BN_FILE_Write_Stream_163840_65535);