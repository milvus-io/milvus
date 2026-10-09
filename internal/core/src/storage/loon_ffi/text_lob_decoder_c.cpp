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

#include "storage/loon_ffi/text_lob_decoder_c.h"

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/c/bridge.h>

#include <cstring>
#include <memory>
#include <string>

#include "common/CGoCatch.h"
#include "common/EasyAssert.h"
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/lob_column/lob_column_reader.h"
#include "milvus-storage/lob_column/lob_reference.h"
#include "storage/StatusToErrorCode.h"
#include "storage/loon_ffi/util.h"

namespace {

struct TextLOBDecoder {
    std::unique_ptr<milvus_storage::lob_column::LobColumnReader> reader;
};

arrow::Status
ValidateReferences(const arrow::BinaryArray& references) {
    using namespace milvus_storage::lob_column;
    for (int64_t i = 0; i < references.length(); ++i) {
        if (references.IsNull(i)) {
            continue;
        }
        auto ref = references.GetView(i);
        if (ref.empty()) {
            return arrow::Status::Invalid("empty TEXT LOB reference at row ",
                                          i);
        }
        const auto tag = static_cast<uint8_t>(ref[0]);
        if (tag == FLAG_INLINE_DATA) {
            continue;
        }
        if (tag != FLAG_LOB_REFERENCE || ref.size() != LOB_REFERENCE_SIZE) {
            return arrow::Status::Invalid(
                "malformed TEXT LOB reference at row ", i);
        }
        auto decoded =
            DecodeLOBReference(reinterpret_cast<const uint8_t*>(ref.data()));
        if (decoded.row_offset < 0) {
            return arrow::Status::Invalid(
                "negative TEXT LOB row offset at row ", i);
        }
    }
    return arrow::Status::OK();
}

CStatus
DataError(const std::string& message) {
    return milvus::FailureCStatus(static_cast<int>(milvus::DataFormatBroken),
                                  message);
}

CStatus
ArrowError(const arrow::Status& status, const std::string& context) {
    auto code = status.IsIndexError()
                    ? milvus::DataFormatBroken
                    : milvus::storage::ArrowStatusToErrorCode(status);
    return milvus::FailureCStatus(static_cast<int>(code),
                                  context + ": " + status.ToString());
}

}  // namespace

CStatus
NewTextLOBDecoder(int64_t field_id,
                  const char* lob_base_path,
                  CStorageConfig storage_config,
                  CTextLOBDecoder* out_decoder) {
    try {
        if (out_decoder == nullptr || lob_base_path == nullptr) {
            return milvus::FailureCStatus(
                static_cast<int>(milvus::UnexpectedError),
                "invalid TEXT LOB decoder arguments");
        }
        *out_decoder = nullptr;
        auto properties =
            MakeInternalPropertiesFromStorageConfig(storage_config);
        auto fs_result = milvus_storage::FilesystemCache::getInstance().get(
            *properties, lob_base_path);
        if (!fs_result.ok()) {
            return ArrowError(fs_result.status(), "open TEXT LOB filesystem");
        }
        milvus_storage::lob_column::LobColumnConfig config;
        config.field_id = field_id;
        config.lob_base_path = lob_base_path;
        config.data_type = milvus_storage::lob_column::LobDataType::kText;
        config.properties = *properties;
        auto reader_result = milvus_storage::lob_column::CreateLobColumnReader(
            fs_result.ValueOrDie(), config);
        if (!reader_result.ok()) {
            return ArrowError(reader_result.status(), "open TEXT LOB reader");
        }
        auto decoder = std::make_unique<TextLOBDecoder>();
        decoder->reader = std::move(reader_result.ValueOrDie());
        *out_decoder = decoder.release();
        return milvus::SuccessCStatus();
    }
    CGO_CATCH_AND_RETURN_CSTATUS
}

CStatus
DecodeTextLOB(CTextLOBDecoder decoder,
              struct ArrowArray* references,
              struct ArrowArray* out_strings) {
    try {
        if (decoder == nullptr || references == nullptr ||
            out_strings == nullptr) {
            return milvus::FailureCStatus(
                static_cast<int>(milvus::UnexpectedError),
                "invalid TEXT LOB decode arguments");
        }
        auto imported = arrow::ImportArray(references, arrow::binary());
        if (!imported.ok()) {
            return ArrowError(imported.status(), "import TEXT LOB references");
        }
        auto refs =
            std::static_pointer_cast<arrow::BinaryArray>(imported.ValueOrDie());
        auto validation = ValidateReferences(*refs);
        if (!validation.ok()) {
            return DataError(validation.ToString());
        }
        auto* handle = static_cast<TextLOBDecoder*>(decoder);
        auto decoded = handle->reader->ReadArrowArray(refs);
        if (!decoded.ok()) {
            return ArrowError(decoded.status(), "read TEXT LOB references");
        }
        if (decoded.ValueOrDie()->length() != refs->length()) {
            return DataError("decoded TEXT LOB row count mismatch: expected " +
                             std::to_string(refs->length()) + ", got " +
                             std::to_string(decoded.ValueOrDie()->length()));
        }
        arrow::StringBuilder builder;
        auto values = decoded.ValueOrDie();
        // The pinned ReadArrowArray batches Vortex take() results but does not
        // check each file group's result count. A dropped out-of-range row can
        // leave an empty final view and shift earlier values. On that signal,
        // verify every out-of-line value through ReadData, whose single-row
        // path reports an IndexError for a missing row. Legitimate empty LOBs
        // remain supported; they only take this slower verification path.
        bool verify_out_of_line = false;
        for (int64_t i = 0; i < refs->length(); ++i) {
            if (refs->IsNull(i) != values->IsNull(i)) {
                return DataError("decoded TEXT LOB nullity mismatch at row " +
                                 std::to_string(i));
            }
            if (!refs->IsNull(i) &&
                static_cast<uint8_t>(refs->GetView(i)[0]) ==
                    milvus_storage::lob_column::FLAG_LOB_REFERENCE &&
                values->GetView(i).empty()) {
                verify_out_of_line = true;
                break;
            }
        }
        if (verify_out_of_line) {
            for (int64_t i = 0; i < refs->length(); ++i) {
                if (refs->IsNull(i)) {
                    continue;
                }
                auto ref = refs->GetView(i);
                if (static_cast<uint8_t>(ref[0]) !=
                    milvus_storage::lob_column::FLAG_LOB_REFERENCE) {
                    continue;
                }
                auto direct = handle->reader->ReadData(
                    reinterpret_cast<const uint8_t*>(ref.data()), ref.size());
                if (!direct.ok()) {
                    auto code =
                        direct.status().IsIndexError()
                            ? milvus::DataFormatBroken
                            : milvus::storage::ArrowStatusToErrorCode(direct);
                    return milvus::FailureCStatus(
                        static_cast<int>(code),
                        "verify TEXT LOB reference at row " +
                            std::to_string(i) + ": " +
                            direct.status().ToString());
                }
                auto actual = values->GetView(i);
                const auto& expected = direct.ValueOrDie();
                if (actual.size() != expected.size() ||
                    (!expected.empty() && std::memcmp(actual.data(),
                                                      expected.data(),
                                                      expected.size()) != 0)) {
                    return DataError("TEXT LOB batch decode mismatch at row " +
                                     std::to_string(i));
                }
            }
        }
        for (int64_t i = 0; i < values->length(); ++i) {
            arrow::Status status;
            if (values->IsNull(i)) {
                status = builder.AppendNull();
            } else {
                const auto value = values->GetView(i);
                status = builder.Append(value.data(), value.size());
            }
            if (!status.ok()) {
                return ArrowError(status, "build decoded TEXT array");
            }
        }
        std::shared_ptr<arrow::Array> strings;
        auto status = builder.Finish(&strings);
        if (!status.ok()) {
            return ArrowError(status, "finish decoded TEXT array");
        }
        status = std::static_pointer_cast<arrow::StringArray>(strings)
                     ->ValidateUTF8();
        if (!status.ok()) {
            return DataError("invalid UTF-8 in decoded TEXT LOB: " +
                             status.ToString());
        }
        status = arrow::ExportArray(*strings, out_strings);
        if (!status.ok()) {
            return ArrowError(status, "export decoded TEXT array");
        }
        return milvus::SuccessCStatus();
    }
    CGO_CATCH_AND_RETURN_CSTATUS
}

CStatus
CloseTextLOBDecoder(CTextLOBDecoder decoder) {
    try {
        if (decoder == nullptr) {
            return milvus::SuccessCStatus();
        }
        std::unique_ptr<TextLOBDecoder> handle(
            static_cast<TextLOBDecoder*>(decoder));
        auto status = handle->reader->Close();
        if (!status.ok()) {
            return ArrowError(status, "close TEXT LOB reader");
        }
        return milvus::SuccessCStatus();
    }
    CGO_CATCH_AND_RETURN_CSTATUS
}
