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
#include <memory>
#include <vector>

#include <marisa.h>

#include "common/EasyAssert.h"
#include "common/Types.h"
#include "storage/artifact/Artifact.h"
#include "storage/artifact/FileSink.h"

// The ARTIFACT of the marisa family. The trie itself can only be written
// through marisa's file API. V1/V2 maps that temporary file into the legacy
// named-buffer path so historical slicing metadata is preserved; V3 streams
// the temporary file directly into its packed entry.

namespace milvus::index {

// Classify a marisa::Exception into a segcore ErrorCode so a trie IO /
// corruption failure does not collapse to the generic UnexpectedError(2001).
// marisa is a vendored third-party library with no dependency on
// milvus-common, so every call into it owes this mapping at its boundary.
//
// io_code is used for MARISA_IO_ERROR (FileReadFailed on load,
// FileWriteFailed on save); data_code for MARISA_FORMAT_ERROR /
// MARISA_SIZE_ERROR (DataFormatBroken when reading back a persisted trie means
// the bytes are corrupt; InvalidParameter when building means the input key set
// exceeds marisa's limits). MARISA_MEMORY_ERROR is a retriable OOM; anything
// else is an internal invariant.
inline ErrorCode
ClassifyMarisaError(const marisa::Exception& error,
                    ErrorCode io_code,
                    ErrorCode data_code) {
    switch (error.error_code()) {
        case MARISA_IO_ERROR:
            return io_code;
        case MARISA_FORMAT_ERROR:
        case MARISA_SIZE_ERROR:
            return data_code;
        case MARISA_MEMORY_ERROR:
            return ErrorCode::MemAllocateFailed;
        default:
            return ErrorCode::UnexpectedError;
    }
}

struct MarisaIndexStorage;

class MarisaIndexArtifact final : public storage::Artifact {
 public:
    MarisaIndexArtifact(std::shared_ptr<marisa::Trie> trie,
                        std::vector<int64_t> str_ids,
                        std::vector<uint32_t> csr_index,
                        std::vector<uint32_t> csr_offsets,
                        DataType value_type);

    ~MarisaIndexArtifact() override;

    void
    Serialize(storage::FileSink& sink) const override;
    void
    Serialize(storage::IndexEntryWriter& writer) const override;

 private:
    // Share encoding while selecting the existing format at compile time.
    template <typename Output>
    void
    SerializeImpl(Output& sink) const;

    std::shared_ptr<const MarisaIndexStorage> storage_;
};

}  // namespace milvus::index
