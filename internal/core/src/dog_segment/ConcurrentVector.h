#pragma once
#include <tbb/concurrent_vector.h>

#include <atomic>
#include <cassert>
#include <deque>
#include <mutex>
#include <shared_mutex>
#include <vector>
#include "EasyAssert.h"
namespace milvus::dog_segment {

// we don't use std::array because capacity of concurrent_vector wastes too much memory
// template <typename Type>
// class FixedVector : public std::vector<Type> {
// public:
//    // This is a stupid workaround for tbb API to avoid memory copy
//    explicit FixedVector(int64_t size) : placeholder_size_(size) {
//    }
//    FixedVector(const FixedVector<Type>& placeholder_vec)
//        : std::vector<Type>(placeholder_vec.placeholder_size_), is_placeholder_(false) {
//        // Assert(placeholder_vec.is_placeholder_);
//    }
//    FixedVector(FixedVector<Type>&&) = delete;
//
//    FixedVector&
//    operator=(FixedVector<Type>&&) = delete;
//
//    FixedVector&
//    operator=(const FixedVector<Type>&) = delete;
//
//    bool is_placeholder() {
//        return is_placeholder_;
//    }
// private:
//    bool is_placeholder_ = true;
//    int placeholder_size_ = 0;
//};

template <typename Type>
using FixedVector = std::vector<Type>;
constexpr int64_t DefaultElementPerChunk = 32 * 1024;

template <typename Type>
class ThreadSafeVector {
 public:
    template <typename... Args>
    void
    emplace_to_at_least(int64_t size, Args... args) {
        if (size <= size_) {
            return;
        }
        // TODO: use multithread to speedup
        std::lock_guard lck(mutex_);
        while (vec_.size() < size) {
            vec_.emplace_back(std::forward<Args...>(args...));
            ++size_;
        }
    }
    const Type&
    operator[](int64_t index) const {
        Assert(index < size_);
        std::shared_lock lck(mutex_);
        return vec_[index];
    }

    Type&
    operator[](int64_t index) {
        Assert(index < size_);
        std::shared_lock lck(mutex_);
        return vec_[index];
    }

    int64_t
    size() const {
        return size_;
    }

 private:
    std::atomic<int64_t> size_ = 0;
    std::deque<Type> vec_;
    mutable std::shared_mutex mutex_;
};

class VectorBase {
 public:
    VectorBase() = default;
    virtual ~VectorBase() = default;

    virtual void
    grow_to_at_least(int64_t element_count) = 0;

    virtual void
    set_data_raw(ssize_t element_offset, void* source, ssize_t element_count) = 0;
};

template <typename Type, bool is_scalar = false, ssize_t ElementsPerChunk = DefaultElementPerChunk>
class ConcurrentVector : public VectorBase {
 public:
    // constants
    using Chunk = FixedVector<Type>;
    ConcurrentVector(ConcurrentVector&&) = delete;
    ConcurrentVector(const ConcurrentVector&) = delete;

    ConcurrentVector&
    operator=(ConcurrentVector&&) = delete;
    ConcurrentVector&
    operator=(const ConcurrentVector&) = delete;

 public:
    explicit ConcurrentVector(ssize_t dim = 1) : Dim(is_scalar ? 1 : dim), SizePerChunk(Dim * ElementsPerChunk) {
        Assert(is_scalar ? dim == 1 : dim != 1);
    }

    void
    grow_to_at_least(int64_t element_count) override {
        auto chunk_count = (element_count + ElementsPerChunk - 1) / ElementsPerChunk;
        chunks_.emplace_to_at_least(chunk_count, SizePerChunk);
    }

    void
    set_data_raw(ssize_t element_offset, void* source, ssize_t element_count) override {
        set_data(element_offset, static_cast<const Type*>(source), element_count);
    }

    void
    set_data(ssize_t element_offset, const Type* source, ssize_t element_count) {
        if (element_count == 0) {
            return;
        }
        this->grow_to_at_least(element_offset + element_count);
        auto chunk_id = element_offset / ElementsPerChunk;
        auto chunk_offset = element_offset % ElementsPerChunk;
        ssize_t source_offset = 0;
        // first partition:
        if (chunk_offset + element_count <= ElementsPerChunk) {
            // only first
            fill_chunk(chunk_id, chunk_offset, element_count, source, source_offset);
            return;
        }

        auto first_size = ElementsPerChunk - chunk_offset;
        fill_chunk(chunk_id, chunk_offset, first_size, source, source_offset);

        source_offset += ElementsPerChunk - chunk_offset;
        element_count -= first_size;
        ++chunk_id;

        // the middle
        while (element_count >= ElementsPerChunk) {
            fill_chunk(chunk_id, 0, ElementsPerChunk, source, source_offset);
            source_offset += ElementsPerChunk;
            element_count -= ElementsPerChunk;
            ++chunk_id;
        }

        // the final
        if (element_count > 0) {
            fill_chunk(chunk_id, 0, element_count, source, source_offset);
        }
    }

    const Chunk&
    get_chunk(ssize_t chunk_index) const {
        return chunks_[chunk_index];
    }

    // just for fun, don't use it directly
    const Type*
    get_element(ssize_t element_index) const {
        auto chunk_id = element_index / ElementsPerChunk;
        auto chunk_offset = element_index % ElementsPerChunk;
        return get_chunk(chunk_id).data() + chunk_offset * Dim;
    }

    const Type&
    operator[](ssize_t element_index) const {
        Assert(Dim == 1);
        auto chunk_id = element_index / ElementsPerChunk;
        auto chunk_offset = element_index % ElementsPerChunk;
        return get_chunk(chunk_id)[chunk_offset];
    }

    ssize_t
    chunk_size() const {
        return chunks_.size();
    }

 private:
    void
    fill_chunk(
        ssize_t chunk_id, ssize_t chunk_offset, ssize_t element_count, const Type* source, ssize_t source_offset) {
        if (element_count <= 0) {
            return;
        }
        auto chunk_max_size = chunks_.size();
        Assert(chunk_id < chunk_max_size);
        Chunk& chunk = chunks_[chunk_id];
        auto ptr = chunk.data();
        std::copy_n(source + source_offset * Dim, element_count * Dim, ptr + chunk_offset * Dim);
    }

    const ssize_t Dim;
    const ssize_t SizePerChunk;

 private:
    ThreadSafeVector<Chunk> chunks_;
};

}  // namespace milvus::dog_segment