// Licensed to the LF AI & Data foundation under Apache-2.0.
// 4-ary quad wavelet matrix (Ceregini-Kurpicz-Venturini, "Faster Wavelet Trees
// with Quad Vectors," DCC 2024). Two bits consumed per level => half the levels
// of a binary wavelet matrix => half the dependent cache misses on the
// backward-search hot path. Same interface as WaveletMatrix so FMIndex can swap.
#pragma once
#include <array>
#include <cstdint>
#include <utility>
#include <vector>
#include "index/fmindex/QuadVector.h"

namespace milvus::index::fmindex {

class WaveletMatrix4 {
 public:
    WaveletMatrix4() = default;

    // seq holds symbols in [0, 4^qlevels). uint16 fits the byte alphabet
    // (sigma <= 257, i.e. 256 bytes + separator/sentinel), which keeps the two
    // partition buffers (the build memory peak) half-size. Takes seq by value:
    // pass with std::move to build without copying it (the caller's buffer
    // becomes the working array). Memory during construction is two n-element
    // uint16 buffers (cur + next, ping-ponged across levels); digits are packed
    // directly into each QuadVector and recomputed for the stable scatter.
    WaveletMatrix4(std::vector<uint16_t> seq,
                   uint32_t qlevels,
                   uint32_t words_per_block = 1)
        : n_(seq.size()), qlevels_(qlevels) {
        qv_.reserve(qlevels_);
        start_.assign(qlevels_, {0, 0, 0, 0});
        std::vector<uint16_t> cur = std::move(seq);
        std::vector<uint16_t> next(n_);
        for (uint32_t l = 0; l < qlevels_; ++l) {
            uint32_t shift = 2 * (qlevels_ - 1 - l);
            std::array<size_t, 4> hist{0, 0, 0, 0};
            qv_.emplace_back(cur, shift, hist, words_per_block);
            start_[l][0] = 0;
            start_[l][1] = hist[0];
            start_[l][2] = hist[0] + hist[1];
            start_[l][3] = hist[0] + hist[1] + hist[2];
            // Stable counting-sort scatter cur -> next by digit (prefix-sum
            // cursors preserve within-group order), then ping-pong.
            std::array<size_t, 4> off{
                start_[l][0], start_[l][1], start_[l][2], start_[l][3]};
            for (size_t i = 0; i < n_; ++i) {
                const uint8_t digit = (cur[i] >> shift) & 3u;
                next[off[digit]++] = cur[i];
            }
            cur.swap(next);
        }
    }

    // count of symbol c in [0, i)
    size_t
    rank(uint32_t c, size_t i) const {
        size_t first = 0, last = i;
        for (uint32_t l = 0; l < qlevels_; ++l) {
            uint8_t d = (c >> (2 * (qlevels_ - 1 - l))) & 3u;
            first = start_[l][d] + qv_[l].rank(d, first);
            last = start_[l][d] + qv_[l].rank(d, last);
        }
        return last - first;
    }

    uint32_t
    access(size_t i) const {
        uint32_t sym = 0;
        size_t pos = i;
        for (uint32_t l = 0; l < qlevels_; ++l) {
            uint8_t d = qv_[l].at(pos);
            sym = (sym << 2) | d;
            pos = start_[l][d] + qv_[l].rank(d, pos);
        }
        return sym;
    }

    // descend lo and hi following c's digit path (no group-start baseline)
    std::pair<size_t, size_t>
    map2(uint32_t c, size_t lo, size_t hi) const {
        for (uint32_t l = 0; l < qlevels_; ++l) {
            uint8_t d = (c >> (2 * (qlevels_ - 1 - l))) & 3u;
            lo = start_[l][d] + qv_[l].rank(d, lo);
            hi = start_[l][d] + qv_[l].rank(d, hi);
        }
        return {lo, hi};
    }

    size_t
    map_zero(uint32_t c) const {
        size_t p = 0;
        for (uint32_t l = 0; l < qlevels_; ++l) {
            uint8_t d = (c >> (2 * (qlevels_ - 1 - l))) & 3u;
            p = start_[l][d] + qv_[l].rank(d, p);
        }
        return p;
    }

    // batched level-major descent with prefetch (memory-level parallelism)
    void
    map_batch(const uint32_t* c, size_t* lo, size_t* hi, size_t n) const {
        for (uint32_t l = 0; l < qlevels_; ++l) {
            const QuadVector& q = qv_[l];
            uint32_t shift = 2 * (qlevels_ - 1 - l);
            for (size_t k = 0; k < n; ++k) {
                q.prefetch(lo[k]);
                q.prefetch(hi[k]);
            }
            for (size_t k = 0; k < n; ++k) {
                uint8_t d = (c[k] >> shift) & 3u;
                lo[k] = start_[l][d] + q.rank(d, lo[k]);
                hi[k] = start_[l][d] + q.rank(d, hi[k]);
            }
        }
    }

    // Prefetch the level-0 quad word that access(i) / LF's first read touch.
    // Used to warm the LF-walk in batched locate (LocateDocsBatch).
    void
    prefetch_access(size_t i) const {
        if (qlevels_) {
            qv_[0].prefetch(i);
        }
    }

    size_t
    size() const {
        return n_;
    }
    uint32_t
    qlevels() const {
        return qlevels_;
    }

    // --- serialization ---
    const std::vector<QuadVector>&
    levels_qv() const {
        return qv_;
    }
    const std::vector<std::array<size_t, 4>>&
    starts() const {
        return start_;
    }
    static WaveletMatrix4
    from_parts(size_t n,
               uint32_t qlevels,
               std::vector<QuadVector> qv,
               std::vector<std::array<size_t, 4>> start) {
        WaveletMatrix4 wm;
        wm.n_ = n;
        wm.qlevels_ = qlevels;
        wm.qv_ = std::move(qv);
        wm.start_ = std::move(start);
        return wm;
    }

 private:
    size_t n_ = 0;
    uint32_t qlevels_ = 0;
    std::vector<QuadVector> qv_;
    std::vector<std::array<size_t, 4>> start_;
};

}  // namespace milvus::index::fmindex
