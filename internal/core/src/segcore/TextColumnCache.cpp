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

#include <exception>
#include <new>

#include "common/EasyAssert.h"
#include "cachinglayer/Utils.h"
#include "folly/OperationCancelled.h"
#include "folly/futures/Future.h"
#include "milvus-storage/common/extend_status.h"
#include "milvus-storage/lob_column/lob_reference.h"

namespace milvus::segcore {

namespace {

arrow::Result<std::unique_ptr<milvus_storage::lob_column::LobColumnReader>>
DefaultTextLobReaderFactory(
    std::shared_ptr<arrow::fs::FileSystem> fs,
    const milvus_storage::lob_column::LobColumnConfig& config) {
    return milvus_storage::lob_column::CreateLobColumnReader(std::move(fs),
                                                             config);
}

// Drive unbound continuations on the synchronous caller without imposing a
// background executor. The reader retains native scans until fan-in.
std::vector<std::string>
ReadLobBatch(const std::shared_ptr<CachedTextLobReader>& cached_reader,
             const std::vector<milvus_storage::lob_column::EncodedRef>& refs,
             const std::string& lob_base_path) {
    try {
        auto result = cachinglayer::SemiInlineGet(
            cached_reader->reader->ReadBatchAsync(refs));
        if (!result.ok()) {
            auto error = milvus_storage::ToSegcoreError(result.status());
            ThrowInfo(error.get_error_code(),
                      "Failed to read text batch from {}: {}",
                      lob_base_path,
                      error.what());
        }
        return std::move(result).ValueOrDie();
    } catch (const std::bad_alloc& error) {
        throw SegcoreError(ErrorCode::MemAllocateFailed, error.what());
    } catch (const folly::OperationCancelled& error) {
        throw SegcoreError(ErrorCode::FollyCancel, error.what());
    } catch (const folly::FutureCancellation& error) {
        throw SegcoreError(ErrorCode::FollyCancel, error.what());
    } catch (const folly::FutureException& error) {
        throw SegcoreError(ErrorCode::FollyOtherException, error.what());
    } catch (const std::exception&) {
        // Preserve typed errors and let the caller's boundary classify other
        // standard exceptions. Keep allocation/cancellation codes above.
        throw;
    } catch (...) {
        // Async callers do not all have a non-standard exception catch tail.
        throw SegcoreError(ErrorCode::UnexpectedError,
                           "Unknown async load exception");
    }
}

}  // namespace

TextColumnCache::TextColumnCache(const TextColumnCacheConfig& config)
    : TextColumnCache(config, DefaultTextLobReaderFactory) {
}

TextColumnCache::TextColumnCache(const TextColumnCacheConfig& config,
                                 TextLobReaderFactory reader_factory)
    : config_(config),
      reader_factory_(std::move(reader_factory)),
      reader_cache_(config.max_file_readers) {
}

TextColumnCache::~TextColumnCache() {
    Clear();
}

std::shared_ptr<CachedTextLobReader>
TextColumnCache::GetOrCreateReader(
    const std::string& lob_base_path,
    std::shared_ptr<arrow::fs::FileSystem> fs,
    const std::shared_ptr<const milvus_storage::api::Properties>& properties) {
    {
        std::lock_guard<std::mutex> lock(reader_cache_mutex_);
        auto cached = reader_cache_.get(lob_base_path);
        if (cached.has_value()) {
            file_cache_hits_.fetch_add(1, std::memory_order_relaxed);
            return cached.value();
        }
    }

    file_cache_misses_.fetch_add(1, std::memory_order_relaxed);

    milvus_storage::lob_column::LobColumnConfig config;
    config.lob_base_path = lob_base_path;
    config.properties = properties;

    auto reader_result = reader_factory_(std::move(fs), config);
    if (!reader_result.ok()) {
        auto error = milvus_storage::ToSegcoreError(reader_result.status());
        ThrowInfo(error.get_error_code(),
                  "Failed to create LobColumnReader for {}: {}",
                  lob_base_path,
                  error.what());
    }

    auto reader = std::shared_ptr<milvus_storage::lob_column::LobColumnReader>(
        std::move(reader_result).ValueOrDie().release());
    auto cached_reader =
        std::make_shared<CachedTextLobReader>(std::move(reader));

    {
        std::lock_guard<std::mutex> lock(reader_cache_mutex_);
        auto cached = reader_cache_.get(lob_base_path);
        if (cached.has_value()) {
            return cached.value();
        }
        reader_cache_.put(lob_base_path, cached_reader);
    }

    return cached_reader;
}

std::vector<std::string>
TextColumnCache::ReadBatch(
    const std::string& lob_base_path,
    std::shared_ptr<arrow::fs::FileSystem> fs,
    const std::shared_ptr<const milvus_storage::api::Properties>& properties,
    const std::vector<milvus_storage::lob_column::EncodedRef>& encoded_refs) {
    if (encoded_refs.empty()) {
        return {};
    }

    std::vector<std::string> results(encoded_refs.size());
    std::vector<size_t>
        pending_indices;  // Indices that need to be read from file
    std::vector<milvus_storage::lob_column::EncodedRef> pending_refs;

    for (size_t i = 0; i < encoded_refs.size(); i++) {
        const auto& ref = encoded_refs[i];

        if (ref.data == nullptr || ref.size == 0) {
            results[i] = "";
            continue;
        }

        if (milvus_storage::lob_column::IsInlineData(ref.data)) {
            results[i] = milvus_storage::lob_column::DecodeInlineText(ref.data,
                                                                      ref.size);
            continue;
        }

        pending_indices.push_back(i);
        pending_refs.push_back(ref);
    }

    if (!pending_refs.empty()) {
        auto cached_reader = GetOrCreateReader(lob_base_path, fs, properties);

        auto texts = ReadLobBatch(cached_reader, pending_refs, lob_base_path);

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
    const std::shared_ptr<const milvus_storage::api::Properties>& properties,
    const std::vector<milvus_storage::lob_column::EncodedRef>& encoded_refs,
    google::protobuf::RepeatedPtrField<std::string>* dst) {
    if (encoded_refs.empty()) {
        return;
    }

    std::vector<size_t> pending_indices;
    std::vector<milvus_storage::lob_column::EncodedRef> pending_refs;

    for (size_t i = 0; i < encoded_refs.size(); i++) {
        const auto& ref = encoded_refs[i];

        if (ref.data == nullptr || ref.size == 0) {
            dst->Mutable(i)->clear();
            continue;
        }

        if (milvus_storage::lob_column::IsInlineData(ref.data)) {
            *dst->Mutable(i) = milvus_storage::lob_column::DecodeInlineText(
                ref.data, ref.size);
            continue;
        }

        pending_indices.push_back(i);
        pending_refs.push_back(ref);
    }

    if (!pending_refs.empty()) {
        auto cached_reader = GetOrCreateReader(lob_base_path, fs, properties);

        auto texts = ReadLobBatch(cached_reader, pending_refs, lob_base_path);

        for (size_t i = 0; i < pending_indices.size() && i < texts.size();
             i++) {
            *dst->Mutable(pending_indices[i]) = std::move(texts[i]);
        }
    }
}

void
TextColumnCache::Clear() {
    std::lock_guard<std::mutex> lock(reader_cache_mutex_);
    reader_cache_.clean();
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

}  // namespace milvus::segcore
