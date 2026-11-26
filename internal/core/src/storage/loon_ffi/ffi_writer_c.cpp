// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <string.h>
#include <cstdlib>
#include <exception>
#include <memory>
#include <string>
#include <utility>

#include <arrow/util/base64.h>

#include "common/CGoCatch.h"
#include "common/EasyAssert.h"
#include "common/common_type_c.h"
#include "storage/KeyRetriever.h"
#include "storage/loon_ffi/ffi_writer_c.h"

CStatus
GetEncParams(CPluginContext* context, char** out_key, char** out_meta) {
    try {
        AssertInfo(out_key != nullptr && out_meta != nullptr,
                   "encryption parameter outputs must not be null");
        *out_key = nullptr;
        *out_meta = nullptr;
        auto [raw_key, metadata] =
            milvus::storage::GetWriterEncryptionParams(context);
        // Both cgo and Loon properties use NUL-terminated strings. Encode the
        // binary DEK before crossing either boundary. The Parquet writer
        // decodes it when constructing its encryption configuration.
        auto key = arrow::util::base64_encode(raw_key);
        auto key_buffer =
            std::unique_ptr<char, decltype(&free)>(strdup(key.c_str()), free);
        auto metadata_buffer = std::unique_ptr<char, decltype(&free)>(
            strdup(metadata.c_str()), free);
        if (!key_buffer || !metadata_buffer) {
            throw std::bad_alloc();
        }
        *out_key = key_buffer.release();
        *out_meta = metadata_buffer.release();
        return milvus::SuccessCStatus();
    }
    CGO_CATCH_AND_RETURN_CSTATUS
}
