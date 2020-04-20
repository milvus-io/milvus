// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

namespace milvus {
namespace engine {
namespace meta {

const size_t S_PS = 1UL;
const size_t MS_PS = 1000 * S_PS;
const size_t US_PS = 1000 * MS_PS;
const size_t NS_PS = 1000 * US_PS;

const size_t SECOND = 1UL;
const size_t MINUTE = 60 * SECOND;
const size_t HOUR = 60 * MINUTE;
const size_t DAY = 24 * HOUR;
const size_t WEEK = 7 * DAY;

// This value is to ignore small raw files when building index.
// The reason is:
// 1. The performance of brute-search for small raw files could be better than small index file.
// 2. And small raw files can be merged to larger files, thus reduce fragmented files count.
// We decide the value based on a testing for small size raw/index files.
const size_t BUILD_INDEX_THRESHOLD = 5000;

}  // namespace meta
}  // namespace engine
}  // namespace milvus
