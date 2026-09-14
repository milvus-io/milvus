// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstdint>
#include <memory>
#include <string_view>
#include "arrow/io/interfaces.h"
#include "filemanager/InputStream.h"
#include "folly/CancellationToken.h"
#include "folly/coro/Task.h"
#include "pb/common.pb.h"
#include "storage/IndexEntryCatalog.h"
#include "storage/plugin/PluginInterface.h"

namespace milvus::storage {

// The V3 async path owns its reader. No method delegates loading to IndexEntryReader.
// All calls run on the selected shared load executor; issued reads drain even
// when cancellation is requested, keeping caller-owned destinations alive.
class AsyncIndexEntryReader {
 public:
    static folly::coro::Task<std::unique_ptr<AsyncIndexEntryReader>>
    Open(std::shared_ptr<milvus::InputStream> input,
         int64_t file_size,
         int64_t collection_id,
         proto::common::LoadPriority priority,
         folly::CancellationToken token = {});

    const IndexEntryCatalog&
    Catalog() const noexcept {
        return catalog_;
    }

    // Plain ranges may be split arbitrarily; encrypted ranges must coincide
    // with one persisted encryption slice. No budget is acquired here: the
    // materializer owns the lease through CRC and destination placement.
    folly::coro::Task<void>
    ReadSliceIntoAsync(std::string_view entry,
                       uint64_t offset,
                       uint8_t* destination,
                       size_t bytes,
                       folly::CancellationToken token = {});

 private:
    AsyncIndexEntryReader() = default;

    // Await the actual IO completion, including after caller cancellation.
    folly::coro::Task<void>
    ReadRangeAsync(uint64_t offset,
                   uint8_t* destination,
                   size_t bytes,
                   folly::CancellationToken token = {});

    std::shared_ptr<milvus::InputStream> input_;
    std::shared_ptr<arrow::io::RandomAccessFile> remote_file_;
    int64_t file_size_{0};
    int64_t collection_id_{0};
    std::string edek_;
    int64_t ez_id_{0};
    std::shared_ptr<plugin::ICipherPlugin> cipher_plugin_;
    IndexEntryCatalog catalog_;
};

}  // namespace milvus::storage
