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

#include "segcore/TextColumnCache.h"

#include "common/EasyAssert.h"
#include "log/Log.h"
#include "milvus-storage/text_column/lob_reference.h"

namespace milvus::segcore {

using namespace milvus_storage::text_column;

TextColumnCache::TextColumnCache(const TextColumnCacheConfig& config)
    : config_(config), reader_cache_(config.max_file_readers) {
}

TextColumnCache::~TextColumnCache() {
    Clear();
}

std::shared_ptr<TextColumnReader>
TextColumnCache::GetOrCreateReader(
    const std::string& lob_base_path,
    std::shared_ptr<arrow::fs::FileSystem> fs,
    const milvus_storage::api::Properties& properties) {
    {
        std::lock_guard<std::mutex> lock(reader_cache_mutex_);
        auto cached = reader_cache_.get(lob_base_path);
        if (cached.has_value()) {
            file_cache_hits_.fetch_add(1, std::memory_order_relaxed);
            return cached.value();
        }
    }

    file_cache_misses_.fetch_add(1, std::memory_order_relaxed);

    TextColumnConfig config;
    config.lob_base_path = lob_base_path;
    config.properties = properties;

    auto reader_result = CreateTextColumnReader(fs, config);
    if (!reader_result.ok()) {
        throw SegcoreError(
            ErrorCode::UnexpectedError,
            fmt::format("Failed to create TextColumnReader for {}: {}",
                        lob_base_path,
                        reader_result.status().message()));
    }

    auto reader = std::shared_ptr<TextColumnReader>(
        std::move(reader_result).ValueOrDie().release());

    {
        std::lock_guard<std::mutex> lock(reader_cache_mutex_);
        reader_cache_.put(lob_base_path, reader);
    }

    return reader;
}

std::string
TextColumnCache::ReadText(const std::string& lob_base_path,
                          std::shared_ptr<arrow::fs::FileSystem> fs,
                          const milvus_storage::api::Properties& properties,
                          const uint8_t* encoded_ref,
                          size_t ref_size) {
    if (encoded_ref == nullptr || ref_size == 0) {
        return "";
    }

    if (IsInlineData(encoded_ref)) {
        return DecodeInlineText(encoded_ref, ref_size);
    }

    if (ref_size != LOB_REFERENCE_SIZE) {
        throw SegcoreError(
            ErrorCode::UnexpectedError,
            fmt::format("Invalid LOB reference size: {}, expected: {}",
                        ref_size,
                        LOB_REFERENCE_SIZE));
    }

    auto reader = GetOrCreateReader(lob_base_path, fs, properties);

    auto result = reader->ReadText(encoded_ref, ref_size);
    if (!result.ok()) {
        throw SegcoreError(ErrorCode::UnexpectedError,
                           fmt::format("Failed to read text from {}: {}",
                                       lob_base_path,
                                       result.status().message()));
    }

    return std::move(result).ValueOrDie();
}

std::vector<std::string>
TextColumnCache::ReadBatch(const std::string& lob_base_path,
                           std::shared_ptr<arrow::fs::FileSystem> fs,
                           const milvus_storage::api::Properties& properties,
                           const std::vector<EncodedRef>& encoded_refs) {
    if (encoded_refs.empty()) {
        return {};
    }

    std::vector<std::string> results(encoded_refs.size());
    std::vector<size_t>
        pending_indices;  // Indices that need to be read from file
    std::vector<EncodedRef> pending_refs;

    for (size_t i = 0; i < encoded_refs.size(); i++) {
        const auto& ref = encoded_refs[i];

        if (ref.data == nullptr || ref.size == 0) {
            results[i] = "";
            continue;
        }

        if (IsInlineData(ref.data)) {
            results[i] = DecodeInlineText(ref.data, ref.size);
            continue;
        }

        pending_indices.push_back(i);
        pending_refs.push_back(ref);
    }

    if (!pending_refs.empty()) {
        auto reader = GetOrCreateReader(lob_base_path, fs, properties);

        auto batch_result = reader->ReadBatch(pending_refs);
        if (!batch_result.ok()) {
            throw SegcoreError(
                ErrorCode::UnexpectedError,
                fmt::format("Failed to read text batch from {}: {}",
                            lob_base_path,
                            batch_result.status().message()));
        }

        auto texts = std::move(batch_result).ValueOrDie();
        for (size_t i = 0; i < pending_indices.size() && i < texts.size();
             i++) {
            size_t original_idx = pending_indices[i];
            results[original_idx] = std::move(texts[i]);
        }
    }

    return results;
}

void
TextColumnCache::ReadBatchInto(
    const std::string& lob_base_path,
    std::shared_ptr<arrow::fs::FileSystem> fs,
    const milvus_storage::api::Properties& properties,
    const std::vector<EncodedRef>& encoded_refs,
    google::protobuf::RepeatedPtrField<std::string>* dst) {
    if (encoded_refs.empty()) {
        return;
    }

    std::vector<size_t> pending_indices;
    std::vector<EncodedRef> pending_refs;

    for (size_t i = 0; i < encoded_refs.size(); i++) {
        const auto& ref = encoded_refs[i];

        if (ref.data == nullptr || ref.size == 0) {
            dst->Mutable(i)->clear();
            continue;
        }

        if (IsInlineData(ref.data)) {
            *dst->Mutable(i) = DecodeInlineText(ref.data, ref.size);
            continue;
        }

        pending_indices.push_back(i);
        pending_refs.push_back(ref);
    }

    if (!pending_refs.empty()) {
        auto reader = GetOrCreateReader(lob_base_path, fs, properties);

        auto batch_result = reader->ReadBatch(pending_refs);
        if (!batch_result.ok()) {
            throw SegcoreError(
                ErrorCode::UnexpectedError,
                fmt::format("Failed to read text batch from {}: {}",
                            lob_base_path,
                            batch_result.status().message()));
        }

        auto texts = std::move(batch_result).ValueOrDie();
        for (size_t i = 0; i < pending_indices.size() && i < texts.size();
             i++) {
            *dst->Mutable(pending_indices[i]) = std::move(texts[i]);
        }
    }
}

void
TextColumnCache::Invalidate(const std::string& lob_base_path) {
    std::lock_guard<std::mutex> lock(reader_cache_mutex_);
    reader_cache_.remove(lob_base_path);
}

void
TextColumnCache::Clear() {
    std::lock_guard<std::mutex> lock(reader_cache_mutex_);
    reader_cache_.clean();
}

TextColumnCacheStats
TextColumnCache::GetStats() const {
    TextColumnCacheStats stats;
    stats.file_cache_hits = file_cache_hits_.load(std::memory_order_relaxed);
    stats.file_cache_misses =
        file_cache_misses_.load(std::memory_order_relaxed);

    {
        std::lock_guard<std::mutex> lock(reader_cache_mutex_);
        stats.current_file_cache_size = reader_cache_.size();
    }

    return stats;
}

static std::unique_ptr<TextColumnCache> g_text_column_cache;
static std::once_flag g_text_column_cache_init_flag;

TextColumnCache&
GetGlobalTextColumnCache() {
    std::call_once(g_text_column_cache_init_flag, []() {
        g_text_column_cache = std::make_unique<TextColumnCache>();
    });
    return *g_text_column_cache;
}

void
InitGlobalTextColumnCache(const TextColumnCacheConfig& config) {
    std::call_once(g_text_column_cache_init_flag, [&config]() {
        g_text_column_cache = std::make_unique<TextColumnCache>(config);
    });
}

}  // namespace milvus::segcore
