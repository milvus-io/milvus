/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include <cstdint>
#include <cstdlib>
#include <cassert>
#include <cstring>

#include <algorithm>

#include <faiss/impl/platform_macros.h>

namespace faiss {

template<int A=32>
inline bool is_aligned_pointer(const void* x)
{
    size_t xi = (size_t)x;
    return xi % A == 0;
}

// class that manages suitably aligned arrays for SIMD
// T should be a POV type. The default alignment is 32 for AVX
template<class T, int A=32>
struct AlignedTableTightAlloc {
    T * ptr;
    size_t numel;

    AlignedTableTightAlloc(): ptr(nullptr), numel(0)
    { }

    explicit AlignedTableTightAlloc(size_t n): ptr(nullptr), numel(0)
    { resize(n); }

    size_t itemsize() const {return sizeof(T); }

    void resize(size_t n) {
        if (numel == n) {
            return;
        }
        T * new_ptr;
        if (n > 0) {
            int ret = posix_memalign((void**)&new_ptr, A, n * sizeof(T));
            if (ret != 0) {
                throw std::bad_alloc();
            }
            if (numel > 0) {
                memcpy(new_ptr, ptr, sizeof(T) * std::min(numel, n));
            }
        } else {
            new_ptr = nullptr;
        }
        numel = n;
        free(ptr);
        ptr = new_ptr;
    }

    void clear() {memset(ptr, 0, nbytes()); }
    size_t size() const {return numel; }
    size_t nbytes() const {return numel * sizeof(T); }

    T * get() {return ptr; }
    const T * get() const {return ptr; }
    T * data() {return ptr; }
    const T * data() const {return ptr; }
    T & operator [] (size_t i)  {return ptr[i]; }
    T operator [] (size_t i) const {return ptr[i]; }

    ~AlignedTableTightAlloc() {posix_memalign_free(ptr); }

    AlignedTableTightAlloc<T, A> & operator =
        (const AlignedTableTightAlloc<T, A> & other) {
        resize(other.numel);
        memcpy(ptr, other.ptr, sizeof(T) * numel);
        return *this;
    }

    AlignedTableTightAlloc(const AlignedTableTightAlloc<T, A> & other) {
        *this = other;
    }

};

// same as AlignedTableTightAlloc, but with geometric re-allocation
template<class T, int A=32>
struct AlignedTable {
    AlignedTableTightAlloc<T, A> tab;
    size_t numel = 0;

    static size_t round_capacity(size_t n) {
        if (n == 0) {
            return 0;
        }
        if (n < 8 * A) {
            return 8 * A;
        }
        size_t capacity = 8 * A;
        while (capacity < n) {
            capacity *= 2;
        }
        return capacity;
    }

    AlignedTable() {}

    explicit AlignedTable(size_t n):
        tab(round_capacity(n)),
        numel(n)
    { }

    size_t itemsize() const {return sizeof(T); }

    void resize(size_t n) {
        tab.resize(round_capacity(n));
        numel = n;
    }

    void clear() { tab.clear(); }
    size_t size() const {return numel; }
    size_t nbytes() const {return numel * sizeof(T); }

    T * get() {return tab.get(); }
    const T * get() const {return tab.get(); }
    T * data() {return tab.get(); }
    const T * data() const {return tab.get(); }
    T & operator [] (size_t i)  {return tab.ptr[i]; }
    T operator [] (size_t i) const {return tab.ptr[i]; }

    // assign and copy constructor should work as expected

};


} // namespace faiss
