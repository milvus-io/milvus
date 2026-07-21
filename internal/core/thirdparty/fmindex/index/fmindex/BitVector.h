// Licensed to the LF AI & Data foundation under Apache-2.0.
// rank9 bit vector. Words may be OWNED (built in RAM) or VIEWED (a pointer into
// an mmap'd, 8-byte-aligned buffer, zero-copy). The rank directory is always
// owned and rebuilt on load, so it is never serialized.
#pragma once
#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

namespace milvus::index::fmindex {

class BitVector {
 public:
    BitVector() = default;

    // Owning builder: allocate zeroed words, fill via set(), then build_rank().
    explicit BitVector(size_t n) : n_(n), owned_words_((n + 63) / 64, 0ULL) {
        words_ = owned_words_.data();
        nwords_ = owned_words_.size();
    }

    BitVector(const BitVector&) = delete;
    BitVector&
    operator=(const BitVector&) = delete;
    BitVector(BitVector&& o) noexcept {
        *this = std::move(o);
    }
    BitVector&
    operator=(BitVector&& o) noexcept {
        n_ = o.n_;
        nwords_ = o.nwords_;
        ones_ = o.ones_;
        owned_words_ = std::move(o.owned_words_);
        dir_ = std::move(o.dir_);
        // re-point: owned buffer keeps its address after a vector move; a view
        // keeps the external pointer.
        words_ = owned_words_.empty() ? o.words_ : owned_words_.data();
        return *this;
    }

    void
    set(size_t i) {
        owned_words_[i >> 6] |= (1ULL << (i & 63));
    }

    bool
    get(size_t i) const {
        return (words_[i >> 6] >> (i & 63)) & 1ULL;
    }

    void
    build_rank() {
        const size_t nblocks = (nwords_ + 7) / 8;  // 8 words = 512 bits / block
        dir_.assign(2 * (nblocks + 1), 0);
        uint64_t total = 0;
        for (size_t b = 0; b < nblocks; ++b) {
            dir_[2 * b] = total;
            uint64_t l2 = 0;
            uint64_t rel = 0;
            for (size_t j = 0; j < 8; ++j) {
                if (j >= 1) {
                    l2 |= (rel & 0x1FFULL) << (9 * (j - 1));
                }
                size_t w = b * 8 + j;
                if (w < nwords_) {
                    uint64_t word = words_[w];
                    // Ignore padding bits past n_ in the last word — a VIEWED
                    // (mmap'd) buffer may have stray high bits set, which would
                    // otherwise inflate ones_ / count_ones().
                    if (w + 1 == nwords_ && (n_ & 63)) {
                        word &= (1ULL << (n_ & 63)) - 1;
                    }
                    rel += __builtin_popcountll(word);
                }
            }
            dir_[2 * b + 1] = l2;
            total += rel;
        }
        dir_[2 * nblocks] = total;
        ones_ = total;
    }

    size_t
    rank1(size_t i) const {
        size_t wi = i >> 6;
        size_t b = wi >> 3;
        size_t j = wi & 7;
        uint64_t r = dir_[2 * b];
        if (j) {
            r += (dir_[2 * b + 1] >> (9 * (j - 1))) & 0x1FFULL;
        }
        size_t off = i & 63;
        if (off) {
            r += __builtin_popcountll(words_[wi] & ((1ULL << off) - 1));
        }
        return r;
    }

    size_t
    rank0(size_t i) const {
        return i - rank1(i);
    }

    void
    prefetch(size_t i) const {
        size_t w = i >> 6;
        if (w >= nwords_) {
            w = nwords_ ? nwords_ - 1 : 0;
        }
        __builtin_prefetch(&dir_[2 * (w >> 3)], 0, 3);
        if (nwords_) {
            __builtin_prefetch(&words_[w], 0, 3);
        }
    }

    size_t
    size() const {
        return n_;
    }
    size_t
    count_ones() const {
        return ones_;
    }
    size_t
    word_count() const {
        return nwords_;
    }
    const uint64_t*
    words() const {
        return words_;
    }

    // Owning: copy/move words in, build rank.
    static BitVector
    from_words(size_t n, std::vector<uint64_t> words) {
        BitVector bv;
        bv.owned_words_ = std::move(words);
        bv.n_ = n;
        bv.words_ = bv.owned_words_.data();
        bv.nwords_ = bv.owned_words_.size();
        bv.build_rank();
        return bv;
    }

    // Zero-copy: view external 8-byte-aligned words; only the directory is built.
    static BitVector
    from_view(size_t n, const uint64_t* words, size_t nwords) {
        BitVector bv;
        bv.n_ = n;
        bv.words_ = words;
        bv.nwords_ = nwords;
        bv.build_rank();
        return bv;
    }

 private:
    size_t n_ = 0;
    const uint64_t* words_ = nullptr;
    size_t nwords_ = 0;
    uint64_t ones_ = 0;
    std::vector<uint64_t> owned_words_;  // non-empty iff owning
    std::vector<uint64_t> dir_;          // always owned (rebuilt on load)
};

}  // namespace milvus::index::fmindex
