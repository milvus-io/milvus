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

#pragma once

#include <cstdint>
#include <string>

constexpr int64_t TestChunkSize = 32 * 1024;
// Root path for LocalChunkManager and ArrowFileSystem in tests.
// Initialized in init_gtest.cpp with a shard-aware random path to
// avoid conflicts during parallel test execution.
extern std::string TestLocalPath;
// Root path for RemoteChunkManager in tests (simulated as local storage).
// Shares the same random prefix as TestLocalPath.
extern std::string TestRemotePath;
// Root path for MmapManager tests. Separate from TestLocalPath to avoid
// being deleted when tests clean up LocalChunkManager's root path.
extern std::string TestMmapPath;

constexpr int64_t kTestSparseDim = 1000;
constexpr float kTestSparseVectorDensity = 0.003;
