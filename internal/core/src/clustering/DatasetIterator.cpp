// Copyright (c) KIOXIA Corporation. All rights reserved.
// Licensed under the MIT license.

#include "DatasetIterator.h"

#include <algorithm>
#include <random>
#include <stdexcept>

namespace milvus::clustering {

template <typename T>
DatasetIterator<T>::DatasetIterator(
    DatasetPurpose purpose,
    const std::vector<int64_t>& segment_ids,
    const std::map<int64_t, std::vector<std::string>>& segment_files,
    const std::map<int64_t, int64_t>& num_rows,
    const std::map<std::string, int64_t>& file_sizes_map,
    int64_t dim,
    int64_t max_batch_bytes,
    int64_t total_sample_bytes,
    int storage_version,
    bool random_sample)
    : purpose_(purpose),
      segment_ids_(segment_ids),
      segment_files_(segment_files),
      num_rows_(num_rows),
      file_sizes_map_(file_sizes_map),
      dim_(dim),
      max_batch_bytes_(max_batch_bytes),
      remaining_sample_bytes_(total_sample_bytes),
      storage_version_(storage_version),
      random_sample_(random_sample) {
    if (purpose_ == DatasetPurpose::TRAIN && random_sample_) {
        std::random_device rd;
        std::mt19937 rng(rd());
        std::shuffle(segment_ids_.begin(), segment_ids_.end(), rng);
    }
}

template <typename T>
bool
DatasetIterator<T>::HasNext() const {
    return remaining_sample_bytes_ > 0 &&
           cur_segment_idx_ < segment_ids_.size();
}

template <typename T>
DatasetPart
DatasetIterator<T>::Next() {
    if (!HasNext()) {
        return DatasetPart{};
    }

    int64_t segment_id = segment_ids_[cur_segment_idx_];
    const auto& files = segment_files_.at(segment_id);
    file_has_progressed_ = false;
    DatasetPart part;
    bool full_sample = purpose_ != DatasetPurpose::TRAIN || !random_sample_;
    int64_t bytes_per_row = dim_ * sizeof(T);
    if (storage_version_ != STORAGE_V1) {
        const std::string& file = files[cur_file_idx_];
        int64_t available_rows;
        if (!full_sample) {
            available_rows = file_sizes_map_.at(file) / bytes_per_row;
        } else {
            available_rows = num_rows_.at(segment_id);
        }

        int64_t rows_by_batch = max_batch_bytes_ / bytes_per_row;
        int64_t rows_by_sample = remaining_sample_bytes_ / bytes_per_row;

        int64_t rows_left_in_file = available_rows - cur_file_offset_rows_;

        int64_t rows_to_fetch =
            std::min({rows_left_in_file, rows_by_batch, rows_by_sample});

        part.files = {file};
        part.offset_rows = cur_file_offset_rows_;
        part.max_bytes = rows_to_fetch * bytes_per_row;

        part.storage_config[SEGMENT_INSERT_FILES_KEY] =
            std::vector<std::vector<std::string>>{{file}};
        part.storage_config[DATA_TYPE_KEY] = DataType::VECTOR_FLOAT;
        part.storage_config[ELEMENT_TYPE_KEY] = DataType::VECTOR_FLOAT;
        part.storage_config[DIM_KEY] = dim_;
        part.storage_config[NUM_ROWS_KEY] = rows_to_fetch;
        part.storage_config[OFFSET_KEY] = cur_file_offset_rows_;
        part.storage_config[STORAGE_VERSION_KEY] = storage_version_;

        cur_file_offset_rows_ += rows_to_fetch;
        if (cur_file_offset_rows_ >= available_rows) {
            cur_file_offset_rows_ = 0;
            cur_file_idx_++;
            file_has_progressed_ = true;
        }
        if (!full_sample) {
            remaining_sample_bytes_ -= part.max_bytes;
        }
    } else {
        int64_t total_bytes = 0;
        bool done = false;
        while (cur_file_idx_ < files.size() && !done) {
            const std::string& f = files[cur_file_idx_];
            int64_t file_size = file_sizes_map_.at(f);
            AssertInfo(max_batch_bytes_ >= file_size,
                       fmt::format("File {} size {} exceeds max batch bytes {}",
                                   f,
                                   max_batch_bytes_,
                                   file_size));

            if (total_bytes + file_size > max_batch_bytes_) {
                total_bytes = max_batch_bytes_;
                done = true;
            }
            if (total_bytes + file_size > remaining_sample_bytes_) {
                total_bytes = remaining_sample_bytes_;
                done = true;
            } else {
                total_bytes += file_size;
            }

            part.files.push_back(f);
            cur_file_idx_++;
            file_has_progressed_ = true;
        }
        if (!full_sample) {
            remaining_sample_bytes_ -= total_bytes;
        }
        part.max_bytes = max_batch_bytes_;
        part.offset_rows = 0;
        part.storage_config[INSERT_FILES_KEY] = part.files;
        part.storage_config[STORAGE_VERSION_KEY] = storage_version_;
    }
    if (cur_file_idx_ >= files.size()) {
        if (full_sample) {
            //we can only update remaining bytes here in this case, since we dont know the real size of each file
            remaining_sample_bytes_ -= num_rows_.at(segment_id) * bytes_per_row;
        }
        cur_segment_idx_++;
        cur_file_idx_ = 0;
        cur_file_offset_rows_ = 0;
    }

    return part;
}

template <typename T>
void
DatasetIterator<T>::notifyEOF() {
    bool full_sample = purpose_ != DatasetPurpose::TRAIN || !random_sample_;
    if (!full_sample || file_has_progressed_) {
        return;
    }
    int64_t segment_id = segment_ids_[cur_segment_idx_];
    const auto& files = segment_files_.at(segment_id);
    if (cur_file_idx_ >= files.size()) {
        return;
    }
    LOG_DEBUG("notifyEOF to segment: {} at file {} offset rows {}",
              segment_id,
              files.at(cur_file_idx_),
              cur_file_offset_rows_);
    cur_file_idx_++;
    cur_file_offset_rows_ = 0;
    if (cur_file_idx_ >= files.size()) {
        int64_t bytes_per_row = dim_ * sizeof(T);
        remaining_sample_bytes_ -= num_rows_.at(segment_id) * bytes_per_row;
        cur_segment_idx_++;  // move to next segment
    }
}

template class DatasetIterator<float>;
template class DatasetIterator<double>;

}  // namespace milvus::clustering
