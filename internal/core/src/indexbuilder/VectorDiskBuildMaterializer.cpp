// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License
// at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "indexbuilder/VectorDiskBuildMaterializer.h"

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common/EasyAssert.h"
#include "common/VectorArray.h"
#include "index/Utils.h"
#include "index/contracts/Registry.h"
#include "index/contracts/build/VectorBuildInput.h"
#include "index/vector/VectorTypeUtils.h"
#include "knowhere/sparse_utils.h"
#include "storage/artifact/LocalDirectory.h"

namespace milvus::indexbuilder {
namespace {

constexpr size_t kRawHeaderSize = sizeof(uint32_t) * 2;

size_t
CheckedSize(int64_t value, const char* label) {
    if (value < 0 ||
        static_cast<uint64_t>(value) >
            static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
        ThrowInfo(
            DataFormatBroken, "{} is out of size_t range: {}", label, value);
    }
    return static_cast<size_t>(value);
}

size_t
CheckedAdd(size_t left, size_t right, const char* label) {
    if (right > std::numeric_limits<size_t>::max() - left) {
        ThrowInfo(DataFormatBroken, "{} count overflows size_t", label);
    }
    return left + right;
}

}  // namespace

class VectorDiskBuildMaterializer::OutputFile final {
 public:
    explicit OutputFile(std::string path) : path_(std::move(path)) {
        fd_ = ::open(
            path_.c_str(), O_CREAT | O_EXCL | O_WRONLY, S_IRUSR | S_IWUSR);
        if (fd_ < 0) {
            ThrowInfo(FileCreateFailed,
                      "failed to create vector build input {}: {}",
                      path_,
                      std::strerror(errno));
        }
    }

    ~OutputFile() {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

    OutputFile(const OutputFile&) = delete;
    OutputFile&
    operator=(const OutputFile&) = delete;

    void
    Write(const void* data, size_t size) {
        AssertInfo(fd_ >= 0, "vector build input {} is closed", path_);
        AssertInfo(data != nullptr || size == 0,
                   "vector build input {} has null write data",
                   path_);
        const auto* bytes = static_cast<const uint8_t*>(data);
        size_t written = 0;
        while (written < size) {
            const auto chunk = std::min(
                size - written,
                static_cast<size_t>(std::numeric_limits<ssize_t>::max()));
            const auto result = ::write(fd_, bytes + written, chunk);
            if (result < 0 && errno == EINTR) {
                continue;
            }
            if (result <= 0) {
                ThrowInfo(
                    FileWriteFailed,
                    "failed to write vector build input {}: {}",
                    path_,
                    result == 0 ? "zero-byte write" : std::strerror(errno));
            }
            written += static_cast<size_t>(result);
        }
    }

    void
    WriteAt(const void* data, size_t size, size_t offset) {
        AssertInfo(fd_ >= 0, "vector build input {} is closed", path_);
        AssertInfo(data != nullptr || size == 0,
                   "vector build input {} has null positioned-write data",
                   path_);
        if (offset > static_cast<size_t>(std::numeric_limits<off_t>::max()) ||
            size > static_cast<size_t>(std::numeric_limits<off_t>::max()) -
                       offset) {
            ThrowInfo(FileWriteFailed,
                      "vector build input {} exceeds file offset range",
                      path_);
        }
        const auto* bytes = static_cast<const uint8_t*>(data);
        size_t written = 0;
        while (written < size) {
            const auto chunk = std::min(
                size - written,
                static_cast<size_t>(std::numeric_limits<ssize_t>::max()));
            const auto result = ::pwrite(fd_,
                                         bytes + written,
                                         chunk,
                                         static_cast<off_t>(offset + written));
            if (result < 0 && errno == EINTR) {
                continue;
            }
            if (result <= 0) {
                ThrowInfo(
                    FileWriteFailed,
                    "failed to write vector build input {}: {}",
                    path_,
                    result == 0 ? "zero-byte write" : std::strerror(errno));
            }
            written += static_cast<size_t>(result);
        }
    }

    void
    Close() {
        AssertInfo(fd_ >= 0, "vector build input {} is already closed", path_);
        if (::fsync(fd_) != 0) {
            ThrowInfo(FileWriteFailed,
                      "failed to flush vector build input {}: {}",
                      path_,
                      std::strerror(errno));
        }
        const auto result = ::close(fd_);
        const auto close_error = errno;
        fd_ = -1;
        if (result != 0) {
            ThrowInfo(FileWriteFailed,
                      "failed to close vector build input {}: {}",
                      path_,
                      std::strerror(close_error));
        }
    }

 private:
    std::string path_;
    int fd_{-1};
};

void
VectorDiskBuildMaterializer::WriteWholeFile(const std::string& path,
                                            const void* header,
                                            size_t header_size,
                                            const void* payload,
                                            size_t payload_size) {
    OutputFile file(path);
    file.Write(header, header_size);
    file.Write(payload, payload_size);
    file.Close();
}

namespace {

bool
CompatibleVectorType(DataType actual, DataType expected) {
    return actual == expected;
}

}  // namespace

namespace {

template <typename Builder>
struct DiskBuilderValueType;

template <typename T>
struct DiskBuilderValueType<std::unique_ptr<
    index::IArtifactBuilder<index::PreparedVectorBuildFiles<T>>>> {
    using type = T;
};

}  // namespace

void
VectorDiskBuildMaterializer::InitializeStaging(std::string staging_parent,
                                               DataType field_type,
                                               DataType value_type,
                                               int64_t dim,
                                               bool nullable,
                                               int64_t expected_rows,
                                               bool side_input_declared) {
        owner_ = storage::LocalDirectory::CreateOwned(
            staging_parent, "vector_disk_XXXXXX", "vector disk");
        field_type_ = field_type;
        value_type_ = value_type;
        dim_ = dim;
        raw_dim_ = value_type == DataType::VECTOR_SPARSE_U32_F32 ? 0 : dim;
        nullable_ = nullable;
        expected_rows_ = expected_rows;
        side_input_declared_ = side_input_declared;
        state_ = State::Feeding;
        AssertInfo(IsVectorDataType(field_type_),
                   "disk vector materializer received non-vector field type {}",
                   field_type_);
        AssertInfo(expected_rows_ >= 0,
                   "disk vector expected row count is negative: {}",
                   expected_rows_);
        AssertInfo(
            static_cast<uint64_t>(expected_rows_) <=
                static_cast<uint64_t>(std::numeric_limits<size_t>::max()),
            "disk vector expected row count exceeds size_t: {}",
            expected_rows_);
        embedding_list_ = field_type_ == DataType::VECTOR_ARRAY;
        if (embedding_list_) {
            AssertInfo(value_type_ != DataType::VECTOR_ARRAY &&
                           value_type_ != DataType::VECTOR_SPARSE_U32_F32 &&
                           IsVectorDataType(value_type_),
                       "disk VECTOR_ARRAY has invalid element type {}",
                       value_type_);
        } else {
            AssertInfo(field_type_ == value_type_,
                       "disk vector field type {} disagrees with value type {}",
                       field_type_,
                       value_type_);
        }
        AssertInfo(
            dim_ >= 0 &&
                (dim_ > 0 || value_type_ == DataType::VECTOR_SPARSE_U32_F32),
            "disk vector dimension {} is invalid for type {}",
            dim_,
            value_type_);
        if (side_input_declared_) {
            AssertInfo(!embedding_list_,
                       "VECTOR_ARRAY optional scalar input is not migrated");
            primary_layout_.emplace();
        }

        raw_path_ = (std::filesystem::path(owner_->Path()) /
                     (value_type_ == DataType::VECTOR_SPARSE_U32_F32
                          ? "raw_data.sparse_u32_f32"
                          : "raw_data"))
                        .string();
        raw_file_ = std::make_unique<OutputFile>(raw_path_);
        const uint32_t empty_header[2] = {0, 0};
        raw_file_->Write(empty_header, sizeof(empty_header));
        if (embedding_list_) {
            offsets_.push_back(0);
        }
    }

void
VectorDiskBuildMaterializer::Add(const FieldDataPtr& batch) {
        AssertState(State::Feeding, "add primary input to");
        try {
            if (batch == nullptr) {
                ThrowInfo(
                    DataFormatBroken,
                    "disk vector source produced a null field-data batch");
            }
            if (!CompatibleVectorType(batch->get_data_type(), field_type_)) {
                ThrowInfo(DataFormatBroken,
                          "disk vector source type {} disagrees with {}",
                          batch->get_data_type(),
                          field_type_);
            }
            if (!nullable_ && batch->IsNullable()) {
                ThrowInfo(DataFormatBroken,
                          "non-nullable disk vector source produced nullable "
                          "field data");
            }
            const auto logical_rows = batch->Length();
            const auto next_logical = CheckedAdd(
                logical_rows_, logical_rows, "disk vector logical row");
            if (static_cast<uint64_t>(next_logical) >
                static_cast<uint64_t>(expected_rows_)) {
                ThrowInfo(DataFormatBroken,
                          "disk vector source exceeds expected row count {}",
                          expected_rows_);
            }

            const auto physical_rows =
                batch->IsNullable() ? CheckedSize(batch->get_valid_rows(),
                                                  "valid vector row count")
                                    : logical_rows;
            if (physical_rows > logical_rows) {
                ThrowInfo(DataFormatBroken,
                          "disk vector batch has {} physical rows for {} "
                          "logical rows",
                          physical_rows,
                          logical_rows);
            }
            const auto* valid = UnpackValidity(*batch, logical_rows);
            size_t counted_valid = logical_rows;
            if (valid != nullptr) {
                counted_valid = 0;
                for (size_t row = 0; row < logical_rows; ++row) {
                    counted_valid += valid[row] ? 1 : 0;
                }
            }
            if (counted_valid != physical_rows) {
                ThrowInfo(DataFormatBroken,
                          "disk vector validity has {} rows, field data has {} "
                          "physical rows",
                          counted_valid,
                          physical_rows);
            }

            size_t batch_vectors = physical_rows;
            if (embedding_list_) {
                batch_vectors = ValidateEmbeddingList(*batch, physical_rows);
            } else if (value_type_ == DataType::VECTOR_SPARSE_U32_F32) {
                ValidateSparse(*batch, physical_rows);
                const auto* sparse =
                    dynamic_cast<const FieldData<SparseFloatVector>*>(
                        batch.get());
                raw_dim_ = std::max(raw_dim_, sparse->Dim());
            } else {
                ValidateDense(*batch, physical_rows);
            }
            const auto next_physical = CheckedAdd(
                physical_rows_, batch_vectors, "disk vector physical row");
            if (next_physical > std::numeric_limits<uint32_t>::max()) {
                ThrowInfo(Unsupported,
                          "disk vector physical row count {} exceeds uint32 "
                          "wire format",
                          next_physical);
            }

            AppendValidity(logical_rows, valid);
            if (primary_layout_.has_value()) {
                primary_layout_->Append(logical_rows,
                                        ValidityView::FromExpanded(valid));
            }
            if (embedding_list_) {
                WriteEmbeddingList(*batch, physical_rows);
            } else if (value_type_ == DataType::VECTOR_SPARSE_U32_F32) {
                WriteSparse(*batch, physical_rows);
            } else {
                raw_file_->Write(batch->Data(),
                                 static_cast<size_t>(batch->DataSize()));
            }
            logical_rows_ = next_logical;
            physical_rows_ = next_physical;
        } catch (...) {
            state_ = State::Failed;
            throw;
        }
    }

void
VectorDiskBuildMaterializer::FinishPrimary() {
        AssertState(State::Feeding, "finish primary input on");
        try {
            if (logical_rows_ != static_cast<size_t>(expected_rows_)) {
                ThrowInfo(DataFormatBroken,
                          "disk vector source produced {} rows, expected {}",
                          logical_rows_,
                          expected_rows_);
            }
            if (logical_rows_ == 0) {
                ThrowInfo(DataIsEmpty,
                          "cannot build an empty disk vector index");
            }
            AssertInfo(raw_dim_ <= std::numeric_limits<uint32_t>::max(),
                       "disk vector dimension {} exceeds uint32 wire format",
                       raw_dim_);
            const uint32_t header[2] = {static_cast<uint32_t>(physical_rows_),
                                        static_cast<uint32_t>(raw_dim_)};
            raw_file_->WriteAt(header, sizeof(header), 0);
            raw_file_->Close();
            raw_file_.reset();

            if (nullable_) {
                valid_path_ = (std::filesystem::path(owner_->Path()) /
                               "valid_data_input")
                                  .string();
                const auto logical = static_cast<uint64_t>(logical_rows_);
                WriteWholeFile(valid_path_,
                               &logical,
                               sizeof(logical),
                               validity_.data(),
                               validity_.size());
            }

            all_null_ = nullable_ && physical_valid_parents_ == 0;
            empty_embedding_list_ =
                embedding_list_ && !all_null_ && physical_rows_ == 0;
            if (embedding_list_ && !all_null_) {
                AssertInfo(offsets_.size() == physical_valid_parents_ + 1 &&
                               offsets_.front() == 0 &&
                               offsets_.back() == physical_rows_,
                           "disk VECTOR_ARRAY offsets disagree with primary "
                           "row counts");
                offsets_path_ = (std::filesystem::path(owner_->Path()) /
                                 "emb_list_offsets_input")
                                    .string();
                const auto count = offsets_.size();
                AssertInfo(count <= std::numeric_limits<size_t>::max() /
                                        sizeof(size_t),
                           "disk VECTOR_ARRAY offset bytes overflow size_t");
                WriteWholeFile(offsets_path_,
                               &count,
                               sizeof(count),
                               offsets_.data(),
                               count * sizeof(size_t));
            }
            state_ = State::PrimaryFinished;
        } catch (...) {
            state_ = State::Failed;
            throw;
        }
    }

bool
VectorDiskBuildMaterializer::RequiresEngineBuild() const {
        AssertState(State::PrimaryFinished, "inspect primary input on");
        return !all_null_ && !empty_embedding_list_;
    }

const VectorPrimaryLayout&
VectorDiskBuildMaterializer::PrimaryLayout() const {
        AssertState(State::PrimaryFinished, "read primary layout from");
        AssertInfo(primary_layout_.has_value(),
                   "disk vector materializer did not declare side input");
        return *primary_layout_;
    }

void
VectorDiskBuildMaterializer::SetScalarInfo(VectorScalarInfo scalar_info) {
        AssertState(State::PrimaryFinished, "set scalar info on");
        try {
            AssertInfo(side_input_declared_,
                       "disk vector materializer did not declare side input");
            AssertInfo(!scalar_info_path_.has_value(),
                       "disk vector scalar info was already delivered");
            AssertInfo(RequiresEngineBuild(),
                       "disk vector scalar info is unnecessary for an empty "
                       "engine build");
            if (scalar_info.empty()) {
                scalar_info_path_ = std::string();
                return;
            }
            if (scalar_info.size() != 1) {
                ThrowInfo(Unsupported,
                          "disk vector optional format supports one field");
            }
            if (scalar_info.begin()->second.empty()) {
                scalar_info_path_ = std::string();
                return;
            }
            const auto& [field_id, groups] = *scalar_info.begin();
            if (groups.size() > std::numeric_limits<uint32_t>::max()) {
                ThrowInfo(Unsupported,
                          "disk vector optional category count exceeds uint32");
            }
            for (const auto& group : groups) {
                if (group.size() > std::numeric_limits<uint32_t>::max()) {
                    ThrowInfo(Unsupported,
                              "disk vector optional category size exceeds "
                              "uint32");
                }
            }

            auto path = (std::filesystem::path(owner_->Path()) /
                         "opt_fields_input")
                            .string();
            OutputFile file(path);
            const uint8_t version = 0;
            const uint32_t field_count = 1;
            const auto category_count = static_cast<uint32_t>(groups.size());
            file.Write(&version, sizeof(version));
            file.Write(&field_count, sizeof(field_count));
            file.Write(&field_id, sizeof(field_id));
            file.Write(&category_count, sizeof(category_count));
            for (const auto& group : groups) {
                const auto count = static_cast<uint32_t>(group.size());
                file.Write(&count, sizeof(count));
                file.Write(group.data(), group.size() * sizeof(uint32_t));
            }
            file.Close();
            scalar_info_path_ = std::move(path);
        } catch (...) {
            state_ = State::Failed;
            throw;
        }
    }

VectorDiskBuildInputs
VectorDiskBuildMaterializer::TakeInputs() {
        AssertState(State::PrimaryFinished, "take inputs from");
        if (RequiresEngineBuild() && side_input_declared_) {
            AssertInfo(scalar_info_path_.has_value(),
                       "disk vector scalar info was not delivered");
        }
        state_ = State::Consumed;
        VectorDiskBuildInputs result{
            .owner = std::move(owner_),
            .raw_path = std::move(raw_path_),
            .valid_path = valid_path_.empty() ? std::nullopt
                                              : std::optional<std::string>(
                                                    std::move(valid_path_)),
            .offsets_path =
                offsets_path_.empty()
                    ? std::nullopt
                    : std::optional<std::string>(std::move(offsets_path_)),
            .scalar_info_path = std::move(scalar_info_path_)};
        std::vector<uint8_t>().swap(validity_);
        std::vector<size_t>().swap(offsets_);
        primary_layout_.reset();
        validity_scratch_.reset();
        validity_scratch_capacity_ = 0;
        return result;
    }

void
VectorDiskBuildMaterializer::AssertState(State expected,
                                         const char* operation) const {
        AssertInfo(state_ != State::MovedFrom,
                   "disk vector materializer was moved from");
        AssertInfo(state_ == expected,
                   "cannot {} a disk vector materializer in state {}",
                   operation,
                   static_cast<int>(state_));
    }

const bool*
VectorDiskBuildMaterializer::UnpackValidity(FieldDataBase& batch,
                                            size_t logical_rows) {
        const auto* packed = batch.IsNullable() ? batch.ValidData() : nullptr;
        AssertInfo(
            !batch.IsNullable() || logical_rows == 0 || packed != nullptr,
            "nullable disk vector batch has no validity bitmap");
        if (!batch.IsNullable()) {
            return nullptr;
        }
        if (logical_rows > validity_scratch_capacity_) {
            validity_scratch_ = std::make_unique<bool[]>(logical_rows);
            validity_scratch_capacity_ = logical_rows;
        }
        for (size_t row = 0; row < logical_rows; ++row) {
            validity_scratch_[row] =
                ((packed[row >> 3] >> static_cast<unsigned>(row & 7)) & 1U) !=
                0;
        }
        return validity_scratch_.get();
    }

void
VectorDiskBuildMaterializer::AppendValidity(size_t logical_rows,
                                            const bool* valid) {
        if (!nullable_) {
            physical_valid_parents_ = CheckedAdd(
                physical_valid_parents_, logical_rows, "valid parent row");
            return;
        }
        const auto next_rows =
            CheckedAdd(logical_rows_, logical_rows, "disk vector validity row");
        if (next_rows > std::numeric_limits<size_t>::max() - 7) {
            ThrowInfo(DataFormatBroken,
                      "disk vector validity byte count overflows size_t");
        }
        validity_.resize((next_rows + 7) / 8, 0);
        size_t added_valid = 0;
        for (size_t row = 0; row < logical_rows; ++row) {
            if (valid == nullptr || valid[row]) {
                const auto bit = logical_rows_ + row;
                validity_[bit >> 3] |=
                    static_cast<uint8_t>(1U << static_cast<unsigned>(bit & 7));
                ++added_valid;
            }
        }
        physical_valid_parents_ = CheckedAdd(
            physical_valid_parents_, added_valid, "valid parent row");
    }

void
VectorDiskBuildMaterializer::ValidateDense(FieldDataBase& batch,
                                           size_t physical_rows) const {
        if (batch.get_dim() != dim_) {
            ThrowInfo(DataFormatBroken,
                      "disk vector dimension {} disagrees with {}",
                      batch.get_dim(),
                      dim_);
        }
        const auto row_bytes = vector_bytes_per_element(value_type_, dim_);
        if (physical_rows != 0 &&
            row_bytes > std::numeric_limits<size_t>::max() / physical_rows) {
            ThrowInfo(DataFormatBroken,
                      "disk vector batch byte count overflows size_t");
        }
        const auto expected = physical_rows * row_bytes;
        if (batch.DataSize() < 0 ||
            static_cast<uint64_t>(batch.DataSize()) != expected) {
            ThrowInfo(DataFormatBroken,
                      "disk vector batch has {} bytes, expected {}",
                      batch.DataSize(),
                      expected);
        }
        AssertInfo(batch.Data() != nullptr || expected == 0,
                   "disk vector batch has null data for {} bytes",
                   expected);
    }

void
VectorDiskBuildMaterializer::ValidateSparse(FieldDataBase& batch,
                                            size_t physical_rows) const {
        auto* sparse = dynamic_cast<FieldData<SparseFloatVector>*>(&batch);
        AssertInfo(sparse != nullptr,
                   "sparse disk vector field-data has the wrong layout");
        if (sparse->Dim() < 0 || (dim_ > 0 && sparse->Dim() > dim_)) {
            ThrowInfo(DataFormatBroken,
                      "sparse disk vector dimension {} exceeds {}",
                      sparse->Dim(),
                      dim_);
        }
        const auto* rows = static_cast<
            const knowhere::sparse::SparseRow<sparse_u32_f32::ValueType>*>(
            batch.Data());
        AssertInfo(rows != nullptr || physical_rows == 0,
                   "sparse disk vector batch has null physical data");
        size_t data_bytes = 0;
        for (size_t row = 0; row < physical_rows; ++row) {
            if (rows[row].size() > std::numeric_limits<uint32_t>::max()) {
                ThrowInfo(Unsupported,
                          "sparse disk vector row {} nnz exceeds uint32",
                          row);
            }
            data_bytes = CheckedAdd(
                data_bytes, rows[row].data_byte_size(), "sparse vector byte");
        }
        if (batch.DataSize() < 0 ||
            static_cast<uint64_t>(batch.DataSize()) != data_bytes) {
            ThrowInfo(DataFormatBroken,
                      "sparse disk vector batch has {} bytes, expected {}",
                      batch.DataSize(),
                      data_bytes);
        }
    }

size_t
VectorDiskBuildMaterializer::ValidateEmbeddingList(
    FieldDataBase& batch, size_t physical_parents) const {
        auto* arrays = dynamic_cast<FieldData<VectorArray>*>(&batch);
        AssertInfo(arrays != nullptr,
                   "disk VECTOR_ARRAY field-data has the wrong layout");
        if (arrays->get_element_type() != value_type_ ||
            arrays->get_dim() != dim_) {
            ThrowInfo(DataFormatBroken,
                      "disk VECTOR_ARRAY element type/dimension disagrees "
                      "with the request");
        }
        const auto* values = static_cast<const VectorArray*>(batch.Data());
        AssertInfo(values != nullptr || physical_parents == 0,
                   "disk VECTOR_ARRAY batch has null compact values");
        size_t vectors = 0;
        size_t bytes = 0;
        const auto row_bytes = vector_bytes_per_element(value_type_, dim_);
        for (size_t row = 0; row < physical_parents; ++row) {
            const auto& value = values[row];
            if (value.get_element_type() != value_type_ ||
                value.dim() != dim_ || value.physical_length() < 0) {
                ThrowInfo(DataFormatBroken,
                          "disk VECTOR_ARRAY value {} has invalid shape",
                          row);
            }
            const auto count = static_cast<size_t>(value.physical_length());
            if (count != 0 &&
                row_bytes > std::numeric_limits<size_t>::max() / count) {
                ThrowInfo(DataFormatBroken,
                          "disk VECTOR_ARRAY value {} byte count overflows",
                          row);
            }
            const auto expected = count * row_bytes;
            if (value.byte_size() != expected ||
                (value.data() == nullptr && expected != 0)) {
                ThrowInfo(DataFormatBroken,
                          "disk VECTOR_ARRAY value {} has {} bytes, expected "
                          "{}",
                          row,
                          value.byte_size(),
                          expected);
            }
            vectors = CheckedAdd(vectors, count, "VECTOR_ARRAY vector");
            bytes = CheckedAdd(bytes, expected, "VECTOR_ARRAY byte");
        }
        if (batch.DataSize() < 0 ||
            static_cast<uint64_t>(batch.DataSize()) != bytes) {
            ThrowInfo(DataFormatBroken,
                      "disk VECTOR_ARRAY batch has {} bytes, expected {}",
                      batch.DataSize(),
                      bytes);
        }
        return vectors;
    }

void
VectorDiskBuildMaterializer::WriteSparse(FieldDataBase& batch,
                                         size_t physical_rows) {
        const auto* rows = static_cast<
            const knowhere::sparse::SparseRow<sparse_u32_f32::ValueType>*>(
            batch.Data());
        for (size_t row = 0; row < physical_rows; ++row) {
            const auto nnz = static_cast<uint32_t>(rows[row].size());
            raw_file_->Write(&nnz, sizeof(nnz));
            raw_file_->Write(rows[row].data(), rows[row].data_byte_size());
        }
    }

void
VectorDiskBuildMaterializer::WriteEmbeddingList(FieldDataBase& batch,
                                                size_t physical_parents) {
        const auto* values = static_cast<const VectorArray*>(batch.Data());
        for (size_t row = 0; row < physical_parents; ++row) {
            raw_file_->Write(values[row].data(), values[row].byte_size());
            offsets_.push_back(
                CheckedAdd(offsets_.back(),
                           static_cast<size_t>(values[row].physical_length()),
                           "VECTOR_ARRAY offset"));
        }
    }

VectorDiskBuildMaterializer::VectorDiskBuildMaterializer(
    std::string staging_parent,
    DataType field_type,
    DataType value_type,
    int64_t dim,
    bool nullable,
    int64_t expected_rows,
    const index::IndexFamily& family,
    const index::BuildParams& params) {
    auto create_builder = [&]<typename T>() {
        auto builder = index::BuilderRegistry<
                           index::PreparedVectorBuildFiles<T>>::Instance()
                           .Create(family, params);
        AssertInfo(builder != nullptr,
                   "index family {} has no disk vector builder for type {}",
                   family,
                   value_type);
        input_spec_ = builder->InputSpec();
        builder_ = std::move(builder);
    };
    index::DispatchPhysicalVectorDataType(
        value_type, create_builder, [&] {
            ThrowInfo(DataTypeInvalid,
                      "unsupported disk vector build value type {}",
                      value_type);
        });
    InitializeStaging(std::move(staging_parent),
                      field_type,
                      value_type,
                      dim,
                      nullable,
                      expected_rows,
                      !input_spec_.side_inputs.empty());
}

VectorDiskBuildMaterializer::~VectorDiskBuildMaterializer() = default;
VectorDiskBuildMaterializer::VectorDiskBuildMaterializer(
    VectorDiskBuildMaterializer&& other) noexcept {
    Swap(other);
}

VectorDiskBuildMaterializer&
VectorDiskBuildMaterializer::operator=(
    VectorDiskBuildMaterializer&& other) noexcept {
    if (this != &other) {
        VectorDiskBuildMaterializer replacement(std::move(other));
        Swap(replacement);
    }
    return *this;
}

void
VectorDiskBuildMaterializer::Swap(
    VectorDiskBuildMaterializer& other) noexcept {
    using std::swap;
    swap(owner_, other.owner_);
    swap(raw_file_, other.raw_file_);
    swap(field_type_, other.field_type_);
    swap(value_type_, other.value_type_);
    swap(dim_, other.dim_);
    swap(raw_dim_, other.raw_dim_);
    swap(nullable_, other.nullable_);
    swap(expected_rows_, other.expected_rows_);
    swap(side_input_declared_, other.side_input_declared_);
    swap(embedding_list_, other.embedding_list_);
    swap(all_null_, other.all_null_);
    swap(empty_embedding_list_, other.empty_embedding_list_);
    swap(state_, other.state_);
    swap(logical_rows_, other.logical_rows_);
    swap(physical_valid_parents_, other.physical_valid_parents_);
    swap(physical_rows_, other.physical_rows_);
    swap(primary_layout_, other.primary_layout_);
    swap(validity_scratch_, other.validity_scratch_);
    swap(validity_scratch_capacity_, other.validity_scratch_capacity_);
    swap(validity_, other.validity_);
    swap(offsets_, other.offsets_);
    swap(raw_path_, other.raw_path_);
    swap(valid_path_, other.valid_path_);
    swap(offsets_path_, other.offsets_path_);
    swap(scalar_info_path_, other.scalar_info_path_);
    swap(input_spec_, other.input_spec_);
    swap(builder_, other.builder_);
}

const index::BuilderInputSpec&
VectorDiskBuildMaterializer::InputSpec() const {
    AssertInfo(!std::holds_alternative<std::monostate>(builder_),
               "disk vector builder was moved from");
    return input_spec_;
}

storage::ArtifactPtr
VectorDiskBuildMaterializer::Build() && {
    AssertInfo(state_ != State::MovedFrom,
               "disk vector materializer was moved from");
    AssertInfo(!std::holds_alternative<std::monostate>(builder_),
               "disk vector builder was moved from");
    auto inputs = TakeInputs();
    auto builders = std::move(builder_);
    builder_ = std::monostate{};
    return std::visit(
        [&](auto& typed_builder) -> storage::ArtifactPtr {
            using BuilderType = std::decay_t<decltype(typed_builder)>;
            if constexpr (std::is_same_v<BuilderType, std::monostate>) {
                ThrowInfo(UnexpectedError,
                          "disk vector builder variant is empty");
            } else {
                using T = typename DiskBuilderValueType<BuilderType>::type;
                index::PreparedVectorBuildFiles<T> input{
                    .raw_path = inputs.raw_path,
                    .validity_path = inputs.valid_path,
                    .embedding_offsets_path = inputs.offsets_path,
                    .scalar_info_path = inputs.scalar_info_path,
                };
                auto builder = std::move(typed_builder);
                AssertInfo(builder != nullptr,
                           "disk vector typed builder was moved from");
                try {
                    return std::move(*builder).Build(input);
                } catch (...) {
                    builder.reset();
                    throw;
                }
            }
        },
        builders);
}

}  // namespace milvus::indexbuilder
