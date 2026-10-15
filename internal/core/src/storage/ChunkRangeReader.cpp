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

#include "storage/ChunkRangeReader.h"

#include <algorithm>

namespace milvus::storage {

arrow::Result<std::unique_ptr<ChunkRangeReader>>
ChunkRangeReader::Create(milvus_storage::api::ChunkReader& reader,
                         ChunkRangeReadOptions options) {
    if (options.storage.concurrency == 0 || options.storage.request_size == 0) {
        return arrow::Status::Invalid(
            "Read concurrency and request size must be positive");
    }
    auto result = std::unique_ptr<ChunkRangeReader>(
        new ChunkRangeReader(reader, std::move(options)));
    ARROW_RETURN_NOT_OK(result->Plan());
    result->results_.resize(result->ranges_.size());
    result->completed_.reserve(result->ranges_.size());
    result->workers_.reserve(
        std::min(result->options_.storage.concurrency, result->ranges_.size()));
    // Without a shared pool, keep the temporary executor alive until the
    // consumer has drained the ranges and the reader has joined its workers.
    result->pool_ = milvus_storage::ThreadPoolHolder::GetThreadPool(
        milvus_storage::ThreadPoolHolder::GetParallelism());
    for (size_t i = 0; i < std::min(result->options_.storage.concurrency,
                                    result->ranges_.size());
         ++i) {
        auto task = std::make_shared<std::packaged_task<void()>>(
            [ptr = result.get()] { ptr->Read(); });
        auto future = task->get_future();
        result->pool_->add([task] { (*task)(); });
        result->workers_.push_back(std::move(future));
    }
    return result;
}

ChunkRangeReader::ChunkRangeReader(milvus_storage::api::ChunkReader& reader,
                                   ChunkRangeReadOptions options)
    : reader_(reader), options_(std::move(options)) {
}

ChunkRangeReader::~ChunkRangeReader() {
    {
        std::lock_guard lock(mutex_);
        stopped_ = true;
    }
    changed_.notify_all();
    for (auto& worker : workers_) {
        worker.wait();
    }
}

arrow::Status
ChunkRangeReader::Plan() {
    ARROW_ASSIGN_OR_RAISE(auto chunks, reader_.get_chunk_infos());
    for (size_t begin = 0; begin < chunks.size();) {
        const auto& first = chunks[begin];
        // Formats without stored-byte metadata keep each chunk intact.
        if (!first.file_range) {
            ranges_.push_back({{static_cast<int64_t>(begin)},
                               first.global_row_end - first.number_of_rows,
                               first.number_of_rows});
            ++begin;
            continue;
        }
        Range range;
        range.row_offset = first.global_row_end - first.number_of_rows;
        const auto start = first.file_range->offset;
        size_t end = begin;
        do {
            const auto& chunk = chunks[end];
            // Stored offsets already account for compression and page headers.
            // Keep complete chunks within the target so transport does not
            // split a slightly larger range into a full request and a tiny tail.
            // A single oversized chunk still stays whole.
            const auto bytes =
                chunk.file_range->offset + chunk.file_range->length - start;
            if (end > begin && bytes > options_.storage.request_size) {
                break;
            }
            range.chunks.push_back(end++);
            range.row_count += chunk.number_of_rows;
            if (bytes >= options_.storage.request_size) {
                break;
            }
        } while (end < chunks.size() &&
                 chunks[end].file_index == first.file_index &&
                 chunks[end].file_range.has_value());
        ranges_.push_back(std::move(range));
        begin = end;
    }
    return arrow::Status::OK();
}

void
ChunkRangeReader::Read() {
    for (;;) {
        size_t index;
        {
            std::unique_lock lock(mutex_);
            changed_.wait(lock, [&] {
                return stopped_ || next_ == ranges_.size() ||
                       options_.destination ||
                       next_ < consumed_ + options_.storage.concurrency;
            });
            if (stopped_ || next_ == ranges_.size()) {
                return;
            }
            index = next_++;
        }
        const auto& range = ranges_[index];
        auto result = [&]() -> arrow::Result<ChunkRange> {
            try {
                ChunkRange output;
                output.row_offset = range.row_offset;
                output.row_count = range.row_count;
                if (options_.destination) {
                    ARROW_RETURN_NOT_OK(reader_.get_chunks_into(
                        range.chunks, *options_.destination));
                } else {
                    ARROW_ASSIGN_OR_RAISE(output.batches,
                                          reader_.get_chunks(range.chunks, 1));
                }
                return output;
            } catch (const std::bad_alloc&) {
                return arrow::Status::OutOfMemory("Reading chunk range");
            } catch (const std::exception& error) {
                return arrow::Status::UnknownError(error.what());
            } catch (...) {
                return arrow::Status::UnknownError("Reading chunk range");
            }
        }();
        {
            std::lock_guard lock(mutex_);
            if (!result.ok()) {
                if (error_.ok()) {
                    error_ = result.status();
                }
                stopped_ = true;
            } else {
                results_[index] = std::move(result).ValueOrDie();
                completed_.push_back(index);
            }
        }
        changed_.notify_all();
        // Each worker immediately picks another range; there is no batch barrier.
    }
}

arrow::Result<std::optional<ChunkRange>>
ChunkRangeReader::Next() {
    std::unique_lock lock(mutex_);
    changed_.wait(lock, [&] {
        return !error_.ok() || consumed_ == ranges_.size() ||
               (options_.preserve_order ? results_[consumed_].has_value()
                                        : completed_.size() > consumed_);
    });
    ARROW_RETURN_NOT_OK(error_);
    if (consumed_ == ranges_.size()) {
        return std::optional<ChunkRange>{};
    }
    const auto index =
        options_.preserve_order ? consumed_ : completed_[consumed_];
    auto result = std::move(results_[index]);
    results_[index].reset();
    ++consumed_;
    lock.unlock();
    changed_.notify_all();
    return result;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>>
ChunkRangeReader::NextBatch() {
    while (batch_index_ == batches_.size()) {
        batches_.clear();
        batch_index_ = 0;
        ARROW_ASSIGN_OR_RAISE(auto range, Next());
        if (!range) {
            return nullptr;
        }
        batches_ = std::move(range->batches);
    }
    return std::move(batches_[batch_index_++]);
}

}  // namespace milvus::storage
