// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstdint>
#include <string>

#include "nlohmann/json.hpp"

namespace milvus {
struct OpContext;
}  // namespace milvus

// Per-open loading options. Loader implementations interpret family-specific
// parameters; the storage boundary does not persist or reinterpret them.
namespace milvus::storage {

// Native warmup policy. Segment loading translates cache configuration or
// strings to this enum without exposing cache policy types in the interface.
enum class WarmupPolicy {
    Disable,
    Sync,
    Async,
};

struct LoadOptions {
    // Open with file-backed bulk ownership. A family may stream format
    // conversion or build heap auxiliary metadata within its declared load
    // budget; mmap does not mean zero parsing and legacy families may still
    // have known full-materialization gaps.
    bool enable_mmap{false};

    // Where mmap-able / streamed entries are materialized locally.
    std::string mmap_dir_path;

    WarmupPolicy warmup{WarmupPolicy::Sync};

    // Remote artifact payload bytes supplied by load metadata. This is a
    // pre-load admission estimate only; opened readers report owned heap and
    // file-backed bytes through IIndexReaderBase::CellByteSize(). -1 means the
    // caller did not provide an estimate; zero is a valid empty artifact.
    int64_t estimated_bytes{-1};

    // Runtime-only family parameters. Storage transports neither interpret nor
    // persist this bag; the selected family's static open function immediately
    // parses its own typed settings from it.
    nlohmann::json params = nlohmann::json::object();

    // Per-operation context: cancellation token, runtime load priority, cold-byte
    // accounting. Borrowed, never owned; may be null.
    //
    // `milvus::OpContext` (milvus-common `common/OpContext.h`) is L0 and is
    // already the repo-wide currency for exactly these three things, so it does
    // not widen the contract. Forward-declared rather than included so this
    // header stays free of folly/tracer includes.
    milvus::OpContext* op_ctx{nullptr};
};

}  // namespace milvus::storage
