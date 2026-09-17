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

#include "index/vector/VectorDiskBuilder.h"

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "index/vector/VectorDiskArtifact.h"
#include "index/vector/VectorDiskBuildFileManager.h"
#include "index/vector/VectorIndexValidDataUtils.h"
#include "index/vector/VectorParamUtils.h"
#include "storage/artifact/LocalDirectory.h"
#include "storage/artifact/LocalFileUtils.h"
#include "knowhere/binaryset.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/segcore_error_code.h"

namespace milvus::index {
namespace {

constexpr std::string_view kEmptyEmbListOffsets = "empty_emb_list_offsets";

struct RawHeader {
    uint32_t rows{0};
    uint32_t dim{0};
};

struct DiskValidity {
    bool present{false};
    size_t total_count{0};
    size_t valid_count{0};
    std::vector<uint8_t> bytes;
};

void
ReadExact(
    int fd, void* data, size_t size, size_t offset, const std::string& path) {
    size_t read_bytes = 0;
    while (read_bytes < size) {
        const auto result = ::pread(fd,
                                    static_cast<uint8_t*>(data) + read_bytes,
                                    size - read_bytes,
                                    static_cast<off_t>(offset + read_bytes));
        if (result < 0 && errno == EINTR) {
            continue;
        }
        if (result <= 0) {
            ThrowInfo(FileReadFailed,
                      "failed to read vector build input {}: {}",
                      path,
                      result == 0 ? "unexpected EOF" : std::strerror(errno));
        }
        read_bytes += static_cast<size_t>(result);
    }
}

std::vector<uint8_t>
ReadFile(const std::string& path) {
    const auto fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        ThrowInfo(FileOpenFailed,
                  "failed to open vector build input {}: {}",
                  path,
                  std::strerror(errno));
    }
    storage::FileDescriptorGuard descriptor(fd);
    struct stat stat {};
    if (::fstat(descriptor.Get(), &stat) != 0 || stat.st_size < 0 ||
        static_cast<uint64_t>(stat.st_size) >
            static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
        ThrowInfo(FileReadFailed,
                  "failed to stat vector build input {}: {}",
                  path,
                  std::strerror(errno));
    }
    std::vector<uint8_t> bytes(static_cast<size_t>(stat.st_size));
    if (!bytes.empty()) {
        ReadExact(descriptor.Get(), bytes.data(), bytes.size(), 0, path);
    }
    return bytes;
}

RawHeader
ReadRawHeader(const std::string& path) {
    const auto fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        ThrowInfo(FileOpenFailed,
                  "failed to open vector raw data {}: {}",
                  path,
                  std::strerror(errno));
    }
    storage::FileDescriptorGuard descriptor(fd);
    RawHeader header;
    ReadExact(descriptor.Get(), &header, sizeof(header), 0, path);
    return header;
}

void
WriteFile(const std::string& path, const void* data, size_t size) {
    auto fd =
        ::open(path.c_str(), O_CREAT | O_EXCL | O_WRONLY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        ThrowInfo(FileCreateFailed,
                  "failed to create vector staging file {}: {}",
                  path,
                  std::strerror(errno));
    }
    try {
        storage::WriteAll(
            fd, data, size, path, "failed to write vector staging file");
        if (::fsync(fd) != 0) {
            ThrowInfo(FileWriteFailed,
                      "failed to flush vector staging file {}: {}",
                      path,
                      std::strerror(errno));
        }
        const auto close_result = ::close(fd);
        const auto close_error = errno;
        fd = -1;
        if (close_result != 0) {
            ThrowInfo(FileWriteFailed,
                      "failed to close vector staging file {}: {}",
                      path,
                      std::strerror(close_error));
        }
    } catch (...) {
        if (fd >= 0) {
            ::close(fd);
        }
        ::unlink(path.c_str());
        throw;
    }
}

DiskValidity
ReadValidity(const std::string& path) {
    DiskValidity result;
    if (path.empty()) {
        return result;
    }
    result.bytes = ReadFile(path);
    if (result.bytes.size() < sizeof(uint64_t)) {
        ThrowInfo(DataFormatBroken,
                  "nullable vector disk valid_data file is too small");
    }
    uint64_t wire_count = 0;
    std::memcpy(&wire_count, result.bytes.data(), sizeof(wire_count));
    result.total_count = FromValidDataCount(wire_count);
    const auto bitmap_size = GetValidDataBitmapSize(result.total_count);
    if (result.bytes.size() < sizeof(uint64_t) + bitmap_size) {
        ThrowInfo(DataFormatBroken,
                  "nullable vector disk valid_data bitmap is truncated");
    }
    result.valid_count = CountValidDataBitmap(
        result.total_count, result.bytes.data() + sizeof(uint64_t));
    result.present = true;
    return result;
}

std::vector<size_t>
ReadEmbeddingOffsets(const std::string& path) {
    auto bytes = ReadFile(path);
    if (bytes.size() < sizeof(size_t)) {
        ThrowInfo(DataFormatBroken, "embedding-list offset file is too small");
    }
    size_t count = 0;
    std::memcpy(&count, bytes.data(), sizeof(count));
    if (count == 0 ||
        count > (std::numeric_limits<size_t>::max() - sizeof(size_t)) /
                    sizeof(size_t)) {
        ThrowInfo(DataFormatBroken, "embedding-list offset count is invalid");
    }
    const auto required = sizeof(size_t) + count * sizeof(size_t);
    if (bytes.size() < required) {
        ThrowInfo(DataFormatBroken, "embedding-list offset file is truncated");
    }
    std::vector<size_t> offsets(count);
    std::memcpy(
        offsets.data(), bytes.data() + sizeof(size_t), count * sizeof(size_t));
    if (offsets.front() != 0 ||
        !std::is_sorted(offsets.begin(), offsets.end())) {
        ThrowInfo(DataFormatBroken,
                  "embedding-list offsets are not a monotonic prefix sum");
    }
    return offsets;
}

std::vector<uint8_t>
EncodeEmptyEmbeddingOffsets(int64_t dim, const std::vector<size_t>& offsets) {
    AssertInfo(dim > 0, "empty embedding-list dimension must be positive");
    AssertInfo(!offsets.empty() && offsets.front() == 0 && offsets.back() == 0,
               "empty embedding-list offsets are invalid");
    constexpr size_t header_size = sizeof(int64_t) + sizeof(uint64_t);
    if (offsets.size() >
        (std::numeric_limits<size_t>::max() - header_size) / sizeof(size_t)) {
        ThrowInfo(UnexpectedError,
                  "empty embedding-list offset payload size overflows");
    }
    std::vector<uint8_t> bytes(header_size + offsets.size() * sizeof(size_t));
    auto* cursor = bytes.data();
    const auto wire_count = ToValidDataCount(offsets.size());
    std::memcpy(cursor, &dim, sizeof(dim));
    cursor += sizeof(dim);
    std::memcpy(cursor, &wire_count, sizeof(wire_count));
    cursor += sizeof(wire_count);
    std::memcpy(cursor, offsets.data(), offsets.size() * sizeof(size_t));
    return bytes;
}

void
NormalizeDiskAnnBuildThreads(Config& config, const IndexType& index_type) {
    if (index_type != knowhere::IndexEnum::INDEX_DISKANN) {
        return;
    }
    if (!config.contains(DISK_ANN_BUILD_THREAD_NUM)) {
        ThrowInfo(ConfigInvalid,
                  "DiskANN build requires {}",
                  DISK_ANN_BUILD_THREAD_NUM);
    }
    config[DISK_ANN_THREADS_NUM] = vector_params::ParsePositiveInt32(
        config.at(DISK_ANN_BUILD_THREAD_NUM), DISK_ANN_BUILD_THREAD_NUM);
}

}  // namespace

template <typename T>
VectorDiskBuilder<T>::VectorDiskBuilder(DataType elem_type,
                                        IndexType index_type,
                                        MetricType metric_type,
                                        IndexVersion version,
                                        int64_t dim,
                                        knowhere::Json build_params,
                                        std::string local_dir)
    : local_files_(storage::LocalDirectory::CreateOwned(
          local_dir, "vector_disk_XXXXXX", "vector disk")),
      file_manager_(std::make_shared<VectorDiskBuildFileManager>(local_files_)),
      build_params_(std::move(build_params)) {
    auto manager = std::static_pointer_cast<milvus::FileManager>(file_manager_);
    auto pack = knowhere::Pack(std::move(manager));
    engine_ = std::make_unique<KnowhereEngine>(PhysicalVectorDataType<T>(),
                                               elem_type,
                                               std::move(index_type),
                                               std::move(metric_type),
                                               version,
                                               file_manager_,
                                               pack,
                                               true);
    if (dim < 0 || (dim == 0 && PhysicalVectorDataType<T>() !=
                                    DataType::VECTOR_SPARSE_U32_F32)) {
        ThrowInfo(ConfigInvalid,
                  "disk vector dimension {} is invalid for type {}",
                  dim,
                  PhysicalVectorDataType<T>());
    }
    engine_->SetDim(dim);
}

template <typename T>
void
VectorDiskBuilder<T>::EnsureOpen(const char* operation) const {
    AssertInfo(!failed_, "cannot {} a failed vector disk builder", operation);
    AssertInfo(!sealed_, "cannot {} a sealed vector disk builder", operation);
    AssertInfo(engine_ != nullptr,
               "vector disk builder engine is missing during {}",
               operation);
}

template <typename T>
BuilderInputSpec
VectorDiskBuilder<T>::InputSpec() const {
    BuilderInputSpec spec;
    const auto opt_fields =
        GetValueFromConfig<OptFieldT>(build_params_, VEC_OPT_FIELDS);
    const auto partition_isolation =
        GetValueFromConfig<bool>(build_params_, PARTITION_KEY_ISOLATION_KEY)
            .value_or(false);
    if (opt_fields.has_value() &&
        engine_->native_index.IsAdditionalScalarSupported(
            partition_isolation)) {
        spec.side_inputs.reserve(opt_fields->size());
        for (const auto& [field_id, _] : *opt_fields) {
            spec.side_inputs.emplace_back(field_id);
        }
        std::sort(spec.side_inputs.begin(), spec.side_inputs.end());
    }
    return spec;
}

template <typename T>
storage::ArtifactPtr
VectorDiskBuilder<T>::Build(const PreparedVectorBuildFiles<T>& input) && {
    EnsureOpen("build");
    sealed_ = true;
    try {
        AssertInfo(!input.raw_path.empty(),
                   "vector disk build raw input path is empty");
        AssertInfo(
            !input.validity_path.has_value() || !input.validity_path->empty(),
            "vector disk validity input path is empty");
        AssertInfo(!input.embedding_offsets_path.has_value() ||
                       !input.embedding_offsets_path->empty(),
                   "vector disk embedding offsets input path is empty");
        const auto raw = ReadRawHeader(input.raw_path);
        if constexpr (std::is_same_v<T, sparse_u32_f32>) {
            AssertInfo(engine_->Dim() == 0 ||
                           raw.dim <= static_cast<uint64_t>(engine_->Dim()),
                       "sparse vector raw-data dimension {} exceeds build "
                       "dimension {}",
                       raw.dim,
                       engine_->Dim());
            if (raw.dim != 0) {
                engine_->SetDim(static_cast<int64_t>(raw.dim));
            }
            build_params_[DIM_KEY] = engine_->Dim();
        } else {
            AssertInfo(raw.dim == static_cast<uint64_t>(engine_->Dim()),
                       "vector raw-data dimension {} disagrees with build "
                       "dimension {}",
                       raw.dim,
                       engine_->Dim());
        }

        auto validity = ReadValidity(input.validity_path.value_or(""));

        const bool all_null = validity.present && validity.total_count > 0 &&
                              validity.valid_count == 0;
        const bool embedding_list = engine_->IsEmbeddingList();
        std::vector<size_t> offsets;
        if (embedding_list && !all_null) {
            AssertInfo(input.embedding_offsets_path.has_value(),
                       "embedding-list disk build is missing offsets");
            offsets = ReadEmbeddingOffsets(*input.embedding_offsets_path);
            if (offsets.back() != raw.rows) {
                ThrowInfo(DataFormatBroken,
                          "embedding-list terminal offset {} disagrees with "
                          "raw vector count {}",
                          offsets.back(),
                          raw.rows);
            }
            if (validity.present &&
                validity.valid_count + 1 != offsets.size()) {
                ThrowInfo(DataFormatBroken,
                          "embedding-list offset count disagrees with valid "
                          "parent row count");
            }
        } else if (!embedding_list) {
            AssertInfo(!input.embedding_offsets_path.has_value(),
                       "ordinary vector disk build has embedding-list "
                       "offsets");
            if (validity.present && validity.valid_count != raw.rows) {
                ThrowInfo(DataFormatBroken,
                          "nullable valid vector count {} disagrees with raw "
                          "vector count {}",
                          validity.valid_count,
                          raw.rows);
            }
        }

        if (validity.present) {
            const auto path =
                (std::filesystem::path(file_manager_->Directory()) /
                 VALID_DATA_KEY)
                    .string();
            WriteFile(path, validity.bytes.data(), validity.bytes.size());
            file_manager_->RegisterOwnedFile(path);
        }

        const bool empty_embedding_list =
            embedding_list && !offsets.empty() && offsets.back() == 0;
        const bool publish_empty_embedding_list =
            !all_null && empty_embedding_list;

        if (all_null) {
            AssertInfo(raw.rows == 0,
                       "all-null vector build contains {} raw vectors",
                       raw.rows);
        } else if (publish_empty_embedding_list) {
            auto bytes = EncodeEmptyEmbeddingOffsets(engine_->Dim(), offsets);
            const auto path =
                (std::filesystem::path(file_manager_->Directory()) /
                 kEmptyEmbListOffsets)
                    .string();
            WriteFile(path, bytes.data(), bytes.size());
            file_manager_->RegisterOwnedFile(path);
            engine_->SetEmptyEmbListOffsets(offsets);
        } else {
            const auto required_side_inputs = InputSpec().side_inputs;
            AssertInfo(required_side_inputs.empty() ||
                           input.scalar_info_path.has_value(),
                       "vector disk build requires declared scalar input "
                       "delivery");
            auto config = build_params_;
            config[EMB_LIST] = embedding_list;
            config[DISK_ANN_RAW_DATA_PATH] = input.raw_path;
            config[DISK_ANN_PREFIX_PATH] = file_manager_->IndexPrefix();
            config.erase(VALID_DATA_PATH_KEY);
            config.erase(INSERT_FILES_KEY);
            config.erase(VEC_OPT_FIELDS);
            config.erase(VEC_OPT_FIELDS_PATH);
            if (input.scalar_info_path.has_value() &&
                !input.scalar_info_path->empty()) {
                config[VEC_OPT_FIELDS_PATH] = *input.scalar_info_path;
            }
            if (embedding_list) {
                config[EMB_LIST_OFFSETS_PATH] = *input.embedding_offsets_path;
            } else {
                config.erase(EMB_LIST_OFFSETS_PATH);
            }
            NormalizeDiskAnnBuildThreads(config, engine_->KnowhereIndexType());

            const auto status = engine_->native_index.Build({}, config);
            file_manager_->RethrowFirstFailure();
            if (status != knowhere::Status::success) {
                ThrowInfo(knowhere::ToSegcoreErrorCode(status),
                          "failed to build disk vector index: status {} ({})",
                          static_cast<int>(status),
                          knowhere::Status2String(status));
            }
            knowhere::BinarySet entries;
            const auto serialize_status =
                engine_->native_index.Serialize(entries);
            file_manager_->RethrowFirstFailure();
            if (serialize_status != knowhere::Status::success) {
                ThrowInfo(knowhere::ToSegcoreErrorCode(serialize_status),
                          "failed to serialize disk vector index: status {} "
                          "({})",
                          static_cast<int>(serialize_status),
                          knowhere::Status2String(serialize_status));
            }
            AssertInfo(entries.binary_map_.empty(),
                       "disk vector index {} unexpectedly returned in-memory "
                       "serialized entries",
                       engine_->KnowhereIndexType());
        }

        file_manager_->RethrowFirstFailure();
        auto files = file_manager_->Files();
        AssertInfo(!files.empty(),
                   "vector disk build produced no artifact files");

        auto artifact = std::make_unique<VectorDiskArtifact>(
            std::move(local_files_), std::move(files));
        engine_.reset();
        file_manager_.reset();
        build_params_.clear();
        return artifact;
    } catch (...) {
        failed_ = true;
        // Knowhere may have retained paths in its build configuration while
        // unwinding. Destroy every engine-side borrower before the caller is
        // allowed to release the prepared input generation.
        engine_.reset();
        file_manager_.reset();
        local_files_.reset();
        throw;
    }
}

template class VectorDiskBuilder<float>;
template class VectorDiskBuilder<float16>;
template class VectorDiskBuilder<bfloat16>;
template class VectorDiskBuilder<bin1>;
template class VectorDiskBuilder<sparse_u32_f32>;
template class VectorDiskBuilder<int8>;

}  // namespace milvus::index
