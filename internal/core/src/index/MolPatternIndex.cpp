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

#include "index/MolPatternIndex.h"

#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <memory>
#include <sys/mman.h>
#include <unistd.h>

#include "common/EasyAssert.h"
#include "common/Slice.h"
#include "common/FieldDataInterface.h"
#include "common/mol_c.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "log/Log.h"
#include "storage/FileWriter.h"
#include "storage/ThreadPools.h"

namespace milvus::index {

static constexpr const char* MOL_FP_DATA_KEY = "mol_pattern_fp_data";
static constexpr const char* MOL_FP_META_KEY = "mol_pattern_fp_meta";
static constexpr int32_t DEFAULT_MOL_FP_DIM = 2048;

template <typename T>
MolPatternIndex<T>::MolPatternIndex(
    const storage::FileManagerContext& ctx)
    : ScalarIndex<T>(MOL_PATTERN_INDEX_TYPE) {
    mem_file_manager_ = std::make_shared<MemFileManager>(ctx);
    dim_ = DEFAULT_MOL_FP_DIM;
    bytes_per_row_ = (dim_ + 7) / 8;
}

template <typename T>
MolPatternIndex<T>::~MolPatternIndex() {
    if (fp_data_mmap_ != nullptr && fp_data_mmap_size_ > 0) {
        munmap(const_cast<uint8_t*>(fp_data_mmap_), fp_data_mmap_size_);
        fp_data_mmap_ = nullptr;
        fp_data_mmap_size_ = 0;
    }
}

template <typename T>
void
MolPatternIndex<T>::Build(const Config& config) {
    // Read fingerprint bit size from index params (user-configurable)
    auto n_bit = GetValueFromConfig<std::string>(config, "n_bit");
    if (n_bit.has_value()) {
        auto val = std::stoi(n_bit.value());
        if (val > 0) {
            dim_ = static_cast<int32_t>(val);
            bytes_per_row_ = (dim_ + 7) / 8;
        }
    }

    // Download raw field data from object storage
    auto field_datas = mem_file_manager_->CacheRawDataToMemory(config);
    BuildWithFieldData(field_datas);
    ComputeByteSize();
}

template <typename T>
void
MolPatternIndex<T>::Build(size_t n,
                          const T* values,
                          const bool* valid_data) {
    if constexpr (std::is_same_v<T, std::string>) {
        bytes_per_row_ = (dim_ + 7) / 8;
        fp_data_owned_.clear();
        fp_data_owned_.reserve(n * bytes_per_row_);
        row_count_ = 0;

        for (size_t i = 0; i < n; ++i) {
            bool is_valid = (valid_data == nullptr) || valid_data[i];
            if (!is_valid || values[i].empty()) {
                // Null/empty: zero FP placeholder (nullable rows are
                // handled by valid_data in expression evaluation)
                fp_data_owned_.resize(fp_data_owned_.size() + bytes_per_row_, 0);
                row_count_++;
                continue;
            }

            const auto& pickle = values[i];
            auto result = GeneratePatternFingerprintFromPickle(
                reinterpret_cast<const uint8_t*>(pickle.data()),
                pickle.size(),
                dim_);

            AssertInfo(result.error_code == MOL_SUCCESS &&
                       result.data != nullptr &&
                       result.size == static_cast<size_t>(bytes_per_row_),
                       "MolPatternIndex: FP generation should not fail for "
                       "validated pickle data (row {})", i);
            fp_data_owned_.insert(fp_data_owned_.end(),
                                  result.data,
                                  result.data + bytes_per_row_);
            FreeMolDataResult(&result);
            row_count_++;
        }
        published_row_count_.store(row_count_, std::memory_order_release);
    } else {
        ThrowInfo(ErrorCode::Unsupported,
                  "MolPatternIndex only supports string type");
    }
}

template <typename T>
void
MolPatternIndex<T>::BuildWithRawDataForUT(size_t n,
                                          const void* values,
                                          const Config& config) {
    if constexpr (std::is_same_v<T, std::string>) {
        Build(n, static_cast<const std::string*>(values), nullptr);
    } else {
        ThrowInfo(ErrorCode::Unsupported,
                  "MolPatternIndex only supports string type");
    }
}

template <typename T>
void
MolPatternIndex<T>::BuildWithFieldData(
    const std::vector<FieldDataPtr>& field_datas) {
    bytes_per_row_ = (dim_ + 7) / 8;
    fp_data_owned_.clear();
    row_count_ = 0;

    for (const auto& field_data : field_datas) {
        auto num_rows = field_data->get_num_rows();
        fp_data_owned_.reserve(fp_data_owned_.size() +
                               num_rows * bytes_per_row_);

        for (int64_t i = 0; i < num_rows; ++i) {
            bool is_valid = field_data->is_valid(i);
            if (!is_valid) {
                // Null/empty: zero FP placeholder
                fp_data_owned_.resize(
                    fp_data_owned_.size() + bytes_per_row_, 0);
                row_count_++;
                continue;
            }

            // Mol field data is stored as std::string (pickle bytes)
            const auto* str_data =
                static_cast<const std::string*>(field_data->RawValue(i));
            if (!str_data || str_data->empty()) {
                fp_data_owned_.resize(
                    fp_data_owned_.size() + bytes_per_row_, 0);
                row_count_++;
                continue;
            }

            auto result = GeneratePatternFingerprintFromPickle(
                reinterpret_cast<const uint8_t*>(str_data->data()),
                str_data->size(),
                dim_);

            AssertInfo(result.error_code == MOL_SUCCESS &&
                       result.data != nullptr &&
                       result.size == static_cast<size_t>(bytes_per_row_),
                       "MolPatternIndex: FP generation should not fail for "
                       "validated pickle data (row {})", row_count_);
            fp_data_owned_.insert(fp_data_owned_.end(),
                                  result.data,
                                  result.data + bytes_per_row_);
            FreeMolDataResult(&result);
            row_count_++;
        }
    }

    published_row_count_.store(row_count_, std::memory_order_release);

    LOG_INFO("MolPatternIndex: built {} rows, dim={}, total_bytes={}",
             row_count_,
             dim_,
             fp_data_owned_.size());
}

template <typename T>
BinarySet
MolPatternIndex<T>::Serialize(const Config& config) {
    BinarySet res_set;

    // Use published row count — only serialize rows that are fully written.
    auto pub_count = published_row_count_.load(std::memory_order_acquire);

    // Serialize meta: dim (4 bytes) + row_count (8 bytes)
    constexpr size_t meta_size = sizeof(int32_t) + sizeof(int64_t);
    std::shared_ptr<uint8_t[]> meta_buf(new uint8_t[meta_size]);
    std::memcpy(meta_buf.get(), &dim_, sizeof(int32_t));
    std::memcpy(meta_buf.get() + sizeof(int32_t), &pub_count,
                sizeof(int64_t));
    res_set.Append(MOL_FP_META_KEY, meta_buf, meta_size);

    // Serialize only the published portion of fingerprint data
    auto data_size = static_cast<size_t>(pub_count) * bytes_per_row_;
    if (data_size > 0) {
        const uint8_t* src =
            fp_data_mmap_ ? fp_data_mmap_ : fp_data_owned_.data();
        std::shared_ptr<uint8_t[]> data_buf(new uint8_t[data_size]);
        std::memcpy(data_buf.get(), src, data_size);
        res_set.Append(MOL_FP_DATA_KEY, data_buf, data_size);
    }

    return res_set;
}

template <typename T>
void
MolPatternIndex<T>::Load(const BinarySet& binary_set, const Config& config) {
    milvus::Assemble(const_cast<BinarySet&>(binary_set));
    LoadWithoutAssemble(binary_set, config);
}

template <typename T>
void
MolPatternIndex<T>::LoadWithoutAssemble(const BinarySet& binary_set,
                                        const Config& config) {
    // Load meta
    auto meta_buf = binary_set.GetByName(MOL_FP_META_KEY);
    AssertInfo(meta_buf != nullptr,
               "MolPatternIndex: missing meta in BinarySet");
    std::memcpy(&dim_, meta_buf->data.get(), sizeof(int32_t));
    std::memcpy(&row_count_, meta_buf->data.get() + sizeof(int32_t),
                sizeof(int64_t));
    bytes_per_row_ = (dim_ + 7) / 8;

    auto data_buf = binary_set.GetByName(MOL_FP_DATA_KEY);

    if (config.contains(MMAP_FILE_PATH) && data_buf != nullptr &&
        data_buf->size > 0) {
        // mmap path: write fp data to file, then mmap it
        auto mmap_filepath =
            GetValueFromConfig<std::string>(config, MMAP_FILE_PATH);
        AssertInfo(mmap_filepath.has_value(),
                   "mmap filepath is empty when load MolPatternIndex");
        auto load_priority =
            GetValueFromConfig<milvus::proto::common::LoadPriority>(
                config, milvus::LOAD_PRIORITY)
                .value_or(milvus::proto::common::LoadPriority::HIGH);

        auto file_name = mmap_filepath.value();
        std::filesystem::create_directories(
            std::filesystem::path(file_name).parent_path());
        {
            auto file_writer = storage::FileWriter(
                file_name,
                storage::io::GetPriorityFromLoadPriority(load_priority));
            file_writer.Write(data_buf->data.get(), data_buf->size);
            file_writer.Finish();
        }

        // mmap the file read-only
        auto fd = open(file_name.c_str(), O_RDONLY);
        AssertInfo(fd >= 0, "Failed to open mmap file: {}", file_name);
        fp_data_mmap_size_ = data_buf->size;
        auto* ptr = mmap(nullptr, fp_data_mmap_size_, PROT_READ,
                         MAP_PRIVATE, fd, 0);
        close(fd);
        AssertInfo(ptr != MAP_FAILED,
                   "Failed to mmap MolPatternIndex fp data");
        fp_data_mmap_ = static_cast<const uint8_t*>(ptr);
        mmap_file_raii_ = std::make_unique<MmapFileRAII>(file_name);

        LOG_INFO(
            "MolPatternIndex: mmap loaded {} rows, dim={}, size={}",
            row_count_, dim_, fp_data_mmap_size_);
    } else {
        // Memory path
        if (data_buf != nullptr && data_buf->size > 0) {
            fp_data_owned_.assign(data_buf->data.get(),
                                  data_buf->data.get() + data_buf->size);
        } else {
            fp_data_owned_.clear();
        }
        LOG_INFO("MolPatternIndex: loaded {} rows, dim={}", row_count_, dim_);
    }

    published_row_count_.store(row_count_, std::memory_order_release);
    ComputeByteSize();
}

template <typename T>
void
MolPatternIndex<T>::Load(tracer::TraceContext ctx, const Config& config) {
    auto index_files =
        GetValueFromConfig<std::vector<std::string>>(config, "index_files");
    AssertInfo(index_files.has_value(),
               "MolPatternIndex: index_files not found in config");

    auto load_priority =
        GetValueFromConfig<milvus::proto::common::LoadPriority>(
            config, milvus::LOAD_PRIORITY)
            .value_or(milvus::proto::common::LoadPriority::HIGH);

    auto index_datas = mem_file_manager_->LoadIndexToMemory(
        index_files.value(), load_priority);
    BinarySet binary_set;
    AssembleIndexDatas(index_datas, binary_set);
    index_datas.clear();
    LoadWithoutAssemble(binary_set, config);
}

template <typename T>
IndexStatsPtr
MolPatternIndex<T>::Upload(const Config& config) {
    auto binary_set = Serialize(config);
    mem_file_manager_->AddFile(binary_set);

    auto remote_paths_to_size =
        mem_file_manager_->GetRemotePathsToFileSize();

    return IndexStats::NewFromSizeMap(
        mem_file_manager_->GetAddedTotalMemSize(), remote_paths_to_size);
}

template <typename T>
void
MolPatternIndex<T>::AppendMolRow(int64_t row_offset,
                                 const std::string& pickle_data,
                                 bool is_valid) {
    AssertInfo(bytes_per_row_ > 0,
               "MolPatternIndex: bytes_per_row_ not initialized");
    AssertInfo(row_offset >= 0 && row_offset < max_row_count_,
               "MolPatternIndex: row_offset {} out of range [0, {})",
               row_offset, max_row_count_);

    // Write fp data at the exact offset (buffer is pre-allocated with 0x00).
    if (is_valid && !pickle_data.empty()) {
        auto byte_offset = row_offset * bytes_per_row_;
        auto result = GeneratePatternFingerprintFromPickle(
            reinterpret_cast<const uint8_t*>(pickle_data.data()),
            pickle_data.size(),
            dim_);
        auto fp_ok = result.error_code == MOL_SUCCESS &&
                     result.data != nullptr &&
                     result.size == static_cast<size_t>(bytes_per_row_);
        auto error_code = result.error_code;
        if (fp_ok) {
            std::memcpy(fp_data_owned_.data() + byte_offset,
                        result.data, bytes_per_row_);
        }
        FreeMolDataResult(&result);
        AssertInfo(fp_ok,
                   "MolPatternIndex: FP generation should not fail for "
                   "validated pickle data (row {}) with error code {}",
                   row_offset,
                   error_code);
    }

    // Advance published_row_count_ to max(current, row_offset + 1).
    // Concurrent callers with disjoint offsets race here; the highest
    // offset wins.  Gaps contain zero FPs, and query visibility is
    // ultimately bounded by AckResponder::GetAck().
    auto desired = row_offset + 1;
    auto cur = published_row_count_.load(std::memory_order_relaxed);
    while (desired > cur) {
        if (published_row_count_.compare_exchange_weak(
                cur, desired,
                std::memory_order_release,
                std::memory_order_relaxed)) {
            break;
        }
    }
}

// Explicit template instantiation
template class MolPatternIndex<std::string>;

}  // namespace milvus::index
