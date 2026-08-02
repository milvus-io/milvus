// Licensed to the LF AI & Data foundation under Apache-2.0.
// Portions translated from Lance (lance_index::scalar::fmindex), Apache-2.0.
#pragma once
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <new>
#include <stdexcept>
#include <string>
#include <vector>

#if (defined(__unix__) || defined(__APPLE__)) && !defined(__SANITIZE_ADDRESS__)
#if defined(__has_feature)
#if !__has_feature(address_sanitizer)
#define FMIX_USE_MMAP_SCRATCH 1
#endif
#else
#define FMIX_USE_MMAP_SCRATCH 1
#endif
#endif

#ifdef FMIX_USE_MMAP_SCRATCH
#include <sys/mman.h>
#endif

#include "libsais.h"    // Apache-2.0, vendored under third_party/libsais
#include "libsais64.h"  // int64 SA path for corpora >= 2 GiB

namespace milvus::index::fmindex {

inline constexpr size_t kSuffixArrayExtraSpace = 6 * 1024;

// Large build-only arrays should disappear from RSS as soon as their phase is
// complete. Some allocators retain differently-sized freed blocks, so later
// wavelet allocations can otherwise stack on top of dead SA/text pages. Use a
// private mapping for large scratch buffers on POSIX and release it with munmap;
// small and sanitizer builds retain std::vector's checking/faster setup.
template <typename T>
class ScratchArray {
 public:
    explicit ScratchArray(size_t size) : size_(size) {
#ifdef FMIX_USE_MMAP_SCRATCH
        const size_t bytes = size_ * sizeof(T);
        if (bytes >= (size_t{1} << 20)) {
            void* mapping = mmap(nullptr,
                                 bytes,
                                 PROT_READ | PROT_WRITE,
                                 MAP_PRIVATE | MAP_ANON,
                                 -1,
                                 0);
            if (mapping != MAP_FAILED) {
                data_ = static_cast<T*>(mapping);
                mapped_bytes_ = bytes;
                return;
            }
        }
#endif
        owned_.resize(size_);
        data_ = owned_.data();
    }

    ScratchArray(const ScratchArray&) = delete;
    ScratchArray&
    operator=(const ScratchArray&) = delete;

    ~ScratchArray() {
        release();
    }

    T*
    data() {
        return data_;
    }

    const T*
    data() const {
        return data_;
    }

    T&
    operator[](size_t i) {
        return data_[i];
    }

    void
    release() {
#ifdef FMIX_USE_MMAP_SCRATCH
        if (mapped_bytes_ != 0) {
            munmap(data_, mapped_bytes_);
            mapped_bytes_ = 0;
        }
#endif
        std::vector<T>().swap(owned_);
        data_ = nullptr;
        size_ = 0;
    }

 private:
    size_t size_ = 0;
    T* data_ = nullptr;
    size_t mapped_bytes_ = 0;
    std::vector<T> owned_;
};

#ifdef FMIX_USE_MMAP_SCRATCH
#undef FMIX_USE_MMAP_SCRATCH
#endif

// libsais returns 0 on success, -1 on invalid arguments, and -2 on allocation
// failure (see libsais.h). Ignoring it would let a partially-built or
// uninitialized suffix array through, which silently produces a CORRUPT index
// that then serializes and uploads as if valid. Fail the build loudly instead —
// callers (FMIndex::Build) let this propagate so the index build task fails.
inline void
check_libsais_rc(int64_t rc) {
    if (rc == -2) {
        // libsais reserves -2 specifically for allocation failure. Preserve that
        // distinction so Milvus can surface MemAllocateFailed (retriable) instead
        // of collapsing an OOM into a permanent generic build error.
        throw std::bad_alloc();
    }
    if (rc != 0) {
        // -1 is an invalid-argument/internal-contract failure. It indicates a bug
        // in the build inputs assembled by this library, not resource pressure.
        throw std::runtime_error(
            "fmindex: suffix array construction failed (libsais rc=" +
            std::to_string(rc) + ")");
    }
}

// Suffix array via libsais (SA-IS, linear time). Input is a symbol vector
// (bytes remapped to 1..256 plus a sentinel 0, alphabet <= 257). Produces the
// standard suffix array (end-of-string smallest), matching the brute-force
// oracle. This convenience entry point is kept for the unit tests; FMIndex::
// Build writes directly into releasable scratch buffers below. For n < 2^31.
inline std::vector<uint32_t>
build_suffix_array(const std::vector<uint32_t>& s) {
    const size_t n = s.size();
    std::vector<uint32_t> sa(n);
    if (n == 0) {
        return sa;
    }
    if (n == 1) {
        sa[0] = 0;
        return sa;
    }
    // libsais_int wants a mutable int32 text and an int32 SA buffer.
    std::vector<int32_t> t(s.begin(), s.end());
    int32_t k = 0;
    for (uint32_t v : s) {
        k = std::max(k, static_cast<int32_t>(v));
    }
    ++k;                          // alphabet size = max symbol + 1
    const int32_t fs = 6 * 1024;  // recommended free space for performance
    std::vector<int32_t> saint(n + fs);
    int32_t rc =
        libsais_int(t.data(), saint.data(), static_cast<int32_t>(n), k, fs);
    check_libsais_rc(rc);
    for (size_t i = 0; i < n; ++i) {
        sa[i] = static_cast<uint32_t>(saint[i]);
    }
    return sa;
}

// Suffix array of a dense-symbol sequence that ALREADY carries its trailing
// sentinel (dense id 0, unique-smallest, appears once at t[m-1]). Content ids
// are in [2, sigma) and the document separator is id 1; because the remap is
// order-preserving, the id SA order equals the byte/token SA order. This is the
// v2 compact build path — the separator is a symbol OUTSIDE the byte alphabet,
// so any byte (including '\0') is storable and queryable. The input and output
// buffers are caller-owned scratch mappings so they can be unmapped immediately
// after the BWT/sampling phase. For m <= INT32_MAX.
inline void
build_suffix_array_symbols32(int32_t* t, size_t m, int32_t sigma, int32_t* sa) {
    if (m <= 1) {
        if (m == 1) {
            sa[0] = 0;
        }
        return;
    }
    constexpr int32_t fs = static_cast<int32_t>(kSuffixArrayExtraSpace);
#ifdef LIBSAIS_OPENMP
    int32_t rc = libsais_int_omp(t, sa, static_cast<int32_t>(m), sigma, fs, 0);
#else
    int32_t rc = libsais_int(t, sa, static_cast<int32_t>(m), sigma, fs);
#endif
    check_libsais_rc(rc);
}

// 64-bit counterpart (int64 positions), for m >= 2^31 and up to 2^63. Same
// semantics and identical result values as the 32-bit path; uses libsais64_long,
// the integer-alphabet SA over 64-bit indices. 8 bytes/position, so prefer the
// 32-bit path when m < 2^31.
inline std::vector<int64_t>
build_suffix_array_symbols64(int64_t* t, size_t m, int64_t sigma) {
    if (m <= 1) {
        return std::vector<int64_t>(m, 0);
    }
    const int64_t fs = 6 * 1024;
    std::vector<int64_t> sa(m + fs);
#ifdef LIBSAIS_OPENMP
    int64_t rc =
        libsais64_long_omp(t, sa.data(), static_cast<int64_t>(m), sigma, fs, 0);
#else
    int64_t rc =
        libsais64_long(t, sa.data(), static_cast<int64_t>(m), sigma, fs);
#endif
    check_libsais_rc(rc);
    sa.resize(m);
    return sa;
}

// Suffix array of the sentinel-terminated text, built directly over BYTES with
// the byte-oriented libsais — no int32 remap of the text is materialized, which
// is the bulk of the build-memory saving. libsais treats end-of-string as
// smaller than any byte, exactly our sentinel; and because the dense byte->id
// remap is order-preserving, the byte SA order equals the id SA order. The
// returned array has length n+1: element 0 is the sentinel suffix (position n,
// always smallest), elements 1..n are the byte SA. This is identical, position
// for position, to build_suffix_array(ids . [0]).
//
// `data` must already be in the order the index sorts by (i.e. case-folded by
// the caller when building a case-insensitive index).
//
// The SA stays int32 (values are non-negative positions <= n < 2^31), so no
// second uint32 buffer is allocated: libsais writes the n byte-SA entries, then
// we shift them up by one in place and drop the sentinel (position n) into
// slot 0. Peak SA memory is one int32 buffer, not two.
inline std::vector<int32_t>
build_suffix_array_bytes(const uint8_t* data, size_t n) {
    if (n == 0) {
        return std::vector<int32_t>{0};  // just the sentinel
    }
    const int32_t fs = 6 * 1024;  // recommended free space for performance
    std::vector<int32_t> sa(n + 1 + fs);
    // Write byte-SA into sa[0..n); sa[n..n+fs] is libsais scratch. With the
    // FMINDEX_OPENMP CMake switch (defines LIBSAIS_OPENMP), the parallel entry
    // point uses all OpenMP threads (bound by OMP_NUM_THREADS); otherwise it is
    // single-threaded.
#ifdef LIBSAIS_OPENMP
    int32_t rc = libsais_omp(
        data, sa.data(), static_cast<int32_t>(n), fs + 1, nullptr, 0);
#else
    int32_t rc =
        libsais(data, sa.data(), static_cast<int32_t>(n), fs + 1, nullptr);
#endif
    check_libsais_rc(rc);
    std::memmove(sa.data() + 1, sa.data(), n * sizeof(int32_t));
    sa[0] = static_cast<int32_t>(n);  // sentinel suffix sorts first
    sa.resize(n + 1);
    return sa;
}

// 64-bit counterpart of build_suffix_array_bytes: same semantics and identical
// result values, but the SA holds int64 positions so it supports corpora up to
// 2^63 bytes (the int32 version caps at 2 GiB). Uses 8 bytes/position, so the
// caller should prefer the 32-bit path when the corpus fits in < 2^31.
inline std::vector<int64_t>
build_suffix_array_bytes64(const uint8_t* data, size_t n) {
    if (n == 0) {
        return std::vector<int64_t>{0};  // just the sentinel
    }
    const int64_t fs = 6 * 1024;
    std::vector<int64_t> sa(n + 1 + fs);
#ifdef LIBSAIS_OPENMP
    int64_t rc = libsais64_omp(
        data, sa.data(), static_cast<int64_t>(n), fs + 1, nullptr, 0);
#else
    int64_t rc =
        libsais64(data, sa.data(), static_cast<int64_t>(n), fs + 1, nullptr);
#endif
    check_libsais_rc(rc);
    std::memmove(sa.data() + 1, sa.data(), n * sizeof(int64_t));
    sa[0] = static_cast<int64_t>(n);  // sentinel suffix sorts first
    sa.resize(n + 1);
    return sa;
}

}  // namespace milvus::index::fmindex
