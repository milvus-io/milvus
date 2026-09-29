// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file for additional information.
#pragma once

#include <algorithm>
#include <array>
#include <deque>
#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>

#include "mmap/ChunkedColumnInterface.h"

namespace milvus::exec {

// Worker-local raw-column reader, independent of expression operators. The
// column owns its metadata; each retained span owns its backing cache cell.
// Cache hits need no ownership copies or shared cache locks. The cache is
// associative: columns with more chunks do not alias a fixed number of slots.
class OffsetColumnReader {
 public:
    static constexpr size_t kMaxBytes = 32 * 1024 * 1024;
    static constexpr size_t kBatchRows = 64;

    OffsetColumnReader(const OffsetColumnReader&) = delete;
    OffsetColumnReader&
    operator=(const OffsetColumnReader&) = delete;

    explicit OffsetColumnReader(
        std::shared_ptr<const ChunkedColumnInterface> column)
        : column_(std::move(column)), row_count_(column_->NumRows()) {
        // Bound lookup metadata independently of retained cell bytes. Large
        // layouts use the column's own mapping and the associative cache.
        const auto chunks = column_->num_chunks();
        if (chunks > 0 && static_cast<size_t>(chunks) <= kMaxLookupChunks) {
            direct_.resize(chunks, nullptr);
            offsets_.reserve(chunks + 1);
            offsets_.push_back(0);
            for (int64_t i = 0; i < chunks; ++i) {
                const auto rows = column_->chunk_row_nums(i);
                AssertInfo(rows >= 0 && static_cast<size_t>(rows) <=
                                            row_count_ - offsets_.back(),
                           "invalid retained column chunk lengths");
                offsets_.push_back(offsets_.back() + rows);
            }
            AssertInfo(offsets_.back() == row_count_,
                       "retained column chunk lengths do not cover rows");
            // Uniform full chunks with a short final chunk are common, but
            // never assume that layout for irregular/empty chunks.
            uniform_rows_ = offsets_[1];
            for (size_t i = 1; i < static_cast<size_t>(chunks); ++i) {
                if (offsets_[i + 1] - offsets_[i] != uniform_rows_ &&
                    (i + 1 != static_cast<size_t>(chunks) ||
                     offsets_[i + 1] - offsets_[i] > uniform_rows_)) {
                    uniform_rows_ = 0;
                    break;
                }
            }
        }
    }

    SpanBase
    Read(milvus::OpContext* context, int64_t row, int64_t& offset) {
        auto resolved = Resolve(row);
        offset = resolved.second;
        ++reads_;
        const auto* entry = Find(resolved.first);
        return entry ? entry->pin.get() : Load(context, resolved.first);
    }

    void
    EndBatch() {
        transient_.reset();
    }

    // Resolve/prefetch independent addresses before consuming their values.
    // Flush pending loads BEFORE a cache miss can evict a backing owner.
    // Scratch is bounded, including for large iterative batches. Values and
    // validity retain caller order; predicate semantics stay entirely in Expr.
    // Optional chunk IDs let the caller apply its existing SkipIndex decisions
    // without resolving every offset a second time. The reader knows no ops.
    template <typename T>
    bool
    Gather(milvus::OpContext* context,
           const int32_t* rows,
           size_t count,
           T* values,
           bool* valid,
           size_t* chunk_ids = nullptr) {
        bool all_valid = true;
        std::array<const T*, kBatchRows> addresses;
        std::array<ValidityView, kBatchRows> validity;
        std::array<size_t, kBatchRows> local_offsets;
        for (size_t base = 0; base < count; base += kBatchRows) {
            const auto n = std::min(kBatchRows, count - base);
            size_t consumed = 0;
            auto consume = [&](size_t end) {
                for (; consumed < end; ++consumed) {
                    values[base + consumed] = *addresses[consumed];
                    const auto& mask = validity[consumed];
                    const bool is_valid =
                        !mask || mask[local_offsets[consumed]];
                    valid[base + consumed] = is_valid;
                    all_valid &= is_valid;
                }
            };
            for (size_t i = 0; i < n; ++i) {
                const auto [chunk_id, offset] = Resolve(rows[base + i]);
                if (chunk_ids != nullptr) {
                    chunk_ids[base + i] = chunk_id;
                }
                ++reads_;
                const auto* entry = Find(chunk_id);
                // No pin copies per lane: consume borrowed addresses before
                // Load can release any owner, including an oversized cell.
                if (!entry) {
                    consume(i);
                }
                const auto span =
                    entry ? entry->pin.get() : Load(context, chunk_id);
                AssertInfo(span.element_sizeof() == sizeof(T),
                           "offset reader scalar width mismatch");
                addresses[i] = static_cast<const T*>(span.data()) + offset;
                validity[i] = span.validity();
                local_offsets[i] = offset;
                __builtin_prefetch(addresses[i], 0, 1);
            }
            consume(n);
        }
        return all_valid;
    }

    size_t
    reads() const {
        return reads_;
    }
    size_t
    misses() const {
        return misses_;
    }
    size_t
    peak_bytes() const {
        return peak_bytes_;
    }
    size_t
    peak_pins() const {
        return peak_pins_;
    }

 private:
    struct Entry {
        size_t bytes;
        PinWrapper<SpanBase> pin;
    };
    // At most 1 MiB of dense pointer/offset metadata; fallback is sparse.
    static constexpr size_t kMaxLookupChunks =
        (1024 * 1024 - sizeof(size_t)) / (sizeof(Entry*) + sizeof(size_t));
    // Also bound metadata for zero/tiny cells, independently of cell bytes.
    static constexpr size_t kMaxEntries = kMaxBytes / (sizeof(Entry) + 64);

    std::pair<size_t, size_t>
    Resolve(int64_t row) const {
        AssertInfo(row >= 0 && static_cast<size_t>(row) < row_count_,
                   "offset reader row outside retained column");
        if (uniform_rows_ != 0) {
            return {row / uniform_rows_, row % uniform_rows_};
        }
        if (!offsets_.empty()) {
            auto upper =
                std::upper_bound(offsets_.begin(), offsets_.end(), row);
            const size_t chunk = upper - offsets_.begin() - 1;
            return {chunk, row - offsets_[chunk]};
        }
        return column_->GetChunkIDByOffset(row);
    }

    const Entry*
    Find(size_t chunk) const {
        if (chunk < direct_.size()) {
            return direct_[chunk];
        }
        auto it = entries_.find(chunk);
        return it == entries_.end() ? nullptr : &it->second;
    }

    void
    EvictFirst() {
        const auto chunk = fifo_.front();
        fifo_.pop_front();
        auto it = entries_.find(chunk);
        retained_bytes_ -= it->second.bytes;
        if (chunk < direct_.size()) {
            direct_[chunk] = nullptr;
        }
        entries_.erase(it);
    }

    SpanBase
    Load(milvus::OpContext* context, size_t chunk) {
        transient_.reset();
        auto [pin, bytes] = column_->PinOffsetSpan(context, chunk);
        ++misses_;
        if (bytes > kMaxBytes) {
            while (!fifo_.empty()) {
                EvictFirst();
            }
            // One required oversized cell is transient, never retained past
            // EndBatch. Prior borrowed addresses must already be consumed.
            transient_.emplace(std::move(pin));
            return transient_->get();
        }
        while (retained_bytes_ + bytes > kMaxBytes ||
               entries_.size() >= kMaxEntries) {
            EvictFirst();
        }
        // unordered_map references remain stable across rehash. FIFO eviction
        // only touches ownership on misses; a hit needs no LRU list updates.
        fifo_.push_back(chunk);
        try {
            auto [it, inserted] =
                entries_.emplace(chunk, Entry{bytes, std::move(pin)});
            if (chunk < direct_.size()) {
                direct_[chunk] = &it->second;
            }
            retained_bytes_ += bytes;
            peak_bytes_ = std::max(peak_bytes_, retained_bytes_);
            peak_pins_ = std::max(peak_pins_, entries_.size());
            return it->second.pin.get();
        } catch (...) {
            fifo_.pop_back();
            throw;
        }
    }

    std::shared_ptr<const ChunkedColumnInterface> column_;
    size_t row_count_;
    std::vector<Entry*> direct_;
    std::vector<size_t> offsets_;
    size_t uniform_rows_{0};
    std::unordered_map<size_t, Entry> entries_;
    std::deque<size_t> fifo_;
    std::optional<PinWrapper<SpanBase>> transient_;
    size_t retained_bytes_{0};
    size_t peak_bytes_{0}, peak_pins_{0}, reads_{0}, misses_{0};
};

}  // namespace milvus::exec
