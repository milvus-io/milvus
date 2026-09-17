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

#include "index/scalar/fmindex/FmIndexArtifact.h"

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <unistd.h>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "index/fmindex/FMIndex.h"
#include "index/scalar/fmindex/FmIndexReader.h"
#include "nlohmann/json.hpp"

namespace milvus::index {
namespace {

constexpr std::string_view kBlobEntry = "fm_index.bin";
constexpr std::string_view kNullBitmapEntry = "fm_index_null_bitmap";
constexpr std::string_view kTotalRowsMeta = "total_rows";
constexpr std::string_view kNullableMeta = "nullable";

class TempFile final {
 public:
    explicit TempFile(const std::string& configured_parent) {
        std::error_code error;
        const auto directory = configured_parent.empty()
                                   ? std::filesystem::temp_directory_path(error)
                                   : std::filesystem::path(configured_parent);
        if (error) {
            ThrowInfo(FileCreateFailed,
                      "failed to locate FM-index temporary directory: {}",
                      error.message());
        }
        std::filesystem::create_directories(directory, error);
        if (error) {
            ThrowInfo(FileCreateFailed,
                      "failed to create FM-index staging parent {}: {}",
                      directory.string(),
                      error.message());
        }
        auto pattern = (directory / "milvus-fmindex-XXXXXX").string();
        std::vector<char> mutable_pattern(pattern.begin(), pattern.end());
        mutable_pattern.push_back('\0');
        const auto fd = ::mkstemp(mutable_pattern.data());
        if (fd < 0) {
            ThrowInfo(FileCreateFailed,
                      "failed to create FM-index temporary file: {}",
                      std::strerror(errno));
        }
        if (::close(fd) != 0) {
            const auto saved_errno = errno;
            ::unlink(mutable_pattern.data());
            ThrowInfo(FileWriteFailed,
                      "failed to close FM-index temporary file {}: {}",
                      mutable_pattern.data(),
                      std::strerror(saved_errno));
        }
        try {
            path_ = mutable_pattern.data();
        } catch (...) {
            ::unlink(mutable_pattern.data());
            throw;
        }
    }

    TempFile(const TempFile&) = delete;
    TempFile&
    operator=(const TempFile&) = delete;

    ~TempFile() {
        if (!path_.empty()) {
            ::unlink(path_.c_str());
        }
    }

    const std::string&
    Path() const {
        return path_;
    }

 private:
    std::string path_;
};

std::vector<uint8_t>
PackNullBitmap(const TargetBitmap& null_bitmap, int64_t total_rows) {
    const auto packed_size = static_cast<size_t>(total_rows / 8) +
                             static_cast<size_t>(total_rows % 8 != 0);
    std::vector<uint8_t> packed(packed_size, 0);
    for (int64_t row = 0; row < total_rows; ++row) {
        if (null_bitmap[static_cast<size_t>(row)]) {
            packed[static_cast<size_t>(row) >> 3] |=
                static_cast<uint8_t>(1u << (row & 0x07));
        }
    }
    return packed;
}

}  // namespace

FmIndexArtifact::FmIndexArtifact(fmindex::FMIndex engine,
                                 TargetBitmap null_bitmap,
                                 int64_t total_rows,
                                 DataType value_type,
                                 bool nullable,
                                 std::string local_dir)
    : storage_(FmIndexStorage::Create(
          {},
          std::make_shared<const fmindex::FMIndex>(std::move(engine)),
          std::move(null_bitmap),
          total_rows,
          value_type,
          nullable,
          FmIndexStateOrigin::Builder)),
      local_dir_(std::move(local_dir)) {
    AssertInfo(IsStringDataType(storage_->ValueType()),
               "FM-index artifact requires a string value type");
}

FmIndexArtifact::~FmIndexArtifact() = default;

void
FmIndexArtifact::Serialize(storage::FileSink& sink) const {
    if (sink.Gen() != storage::Generation::V3) {
        ThrowInfo(UnexpectedError,
                  "FM-index has no V1/V2 artifact representation");
    }
    AssertInfo(storage_ != nullptr,
               "cannot serialize an FM-index artifact without storage");

    TempFile blob_file(local_dir_);
    errno = 0;
    const auto status = storage_->Engine().SerializeToFile(blob_file.Path());
    const auto saved_errno = errno;
    if (status == fmindex::FMIndex::SerializeFileStatus::OpenFailed) {
        ThrowInfo(FileOpenFailed,
                  "failed to open FM-index temporary blob {}: {}",
                  blob_file.Path(),
                  std::strerror(saved_errno));
    }
    if (status == fmindex::FMIndex::SerializeFileStatus::WriteFailed) {
        ThrowInfo(FileWriteFailed,
                  "failed to write FM-index temporary blob {}: {}",
                  blob_file.Path(),
                  std::strerror(saved_errno));
    }
    sink.WriteEntryFromLocalFile(kBlobEntry, blob_file.Path());

    if (storage_->Nullable()) {
        const auto packed =
            PackNullBitmap(storage_->NullBitmap(), storage_->Count());
        sink.WriteEntry(kNullBitmapEntry, packed.data(), packed.size());
    }
    sink.PutMeta(kTotalRowsMeta, nlohmann::json(storage_->Count()));
    sink.PutMeta(kNullableMeta, nlohmann::json(storage_->Nullable()));
}

}  // namespace milvus::index
