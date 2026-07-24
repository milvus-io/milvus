// Licensed to the LF AI & Data foundation under Apache-2.0.
// 2-bit-packed vector with SWAR "count 2-bit lanes == v" rank + 2-level
// directory. Words may be OWNED (built in RAM) or VIEWED (a pointer into an
// mmap'd, 8-byte-aligned buffer, zero-copy). The directory is always owned and
// rebuilt on load. Building block of the 4-ary quad wavelet matrix.
//
// Block granularity: a rank block spans `words_per_block` 64-bit words (32
// symbols each). One `rel_` sample (8 B) is stored per block, so the directory
// is ~ 1/words_per_block of the packed words. Larger blocks shrink the
// directory (less resident RAM) at the cost of up to words_per_block-1 extra
// SWAR popcounts per rank. words_per_block=1 (the default) is the original
// 32-symbol block: directory ~= words, fastest rank.
#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

namespace milvus::index::fmindex {

class QuadVector {
 public:
    QuadVector() = default;

    explicit QuadVector(const std::vector<uint8_t>& syms,
                        uint32_t words_per_block = 1)
        : n_(syms.size()), owned_words_((syms.size() + 31) / 32, 0ULL) {
        set_words_per_block(words_per_block);
        for (size_t i = 0; i < n_; ++i) {
            owned_words_[i >> 5] |= (uint64_t(syms[i] & 3) << (2 * (i & 31)));
        }
        words_ = owned_words_.data();
        nwords_ = owned_words_.size();
        build_rank();
    }

    // Pack one 2-bit wavelet digit directly from the uint16 symbol stream while
    // collecting its histogram. Recomputing the digit during the subsequent
    // stable scatter avoids a separate one-byte-per-symbol digits array.
    QuadVector(const std::vector<uint16_t>& syms,
               uint32_t shift,
               std::array<size_t, 4>& hist,
               uint32_t words_per_block = 1)
        : n_(syms.size()), owned_words_((syms.size() + 31) / 32, 0ULL) {
        set_words_per_block(words_per_block);
        hist = {0, 0, 0, 0};
        for (size_t i = 0; i < n_; ++i) {
            const uint8_t digit = (syms[i] >> shift) & 3u;
            owned_words_[i >> 5] |=
                (uint64_t(digit) << (2 * (i & 31)));
            ++hist[digit];
        }
        words_ = owned_words_.data();
        nwords_ = owned_words_.size();
        build_rank();
    }

    QuadVector(const QuadVector&) = delete;
    QuadVector&
    operator=(const QuadVector&) = delete;
    QuadVector(QuadVector&& o) noexcept {
        *this = std::move(o);
    }
    QuadVector&
    operator=(QuadVector&& o) noexcept {
        n_ = o.n_;
        nwords_ = o.nwords_;
        wpb_ = o.wpb_;
        wpb_shift_ = o.wpb_shift_;
        owned_words_ = std::move(o.owned_words_);
        sb_ = std::move(o.sb_);
        rel_ = std::move(o.rel_);
        words_ = owned_words_.empty() ? o.words_ : owned_words_.data();
        return *this;
    }

    size_t
    rank(uint8_t v, size_t i) const {
        size_t w = i >> 5;            // word index (32 symbols/word)
        size_t b = w >> wpb_shift_;   // block index (words_per_block words/block)
        size_t sb = b >> kSbShift;
        size_t r = sb_[sb][v] + rel_[b][v];
        // Sum the full words from the block start up to word w, then the partial
        // word w. With words_per_block==1 the loop is empty (block == word).
        for (size_t ww = b << wpb_shift_; ww < w; ++ww) {
            r += swar_count(words_[ww], v, 32);
        }
        size_t off = i & 31;
        if (off) {
            r += swar_count(words_[w], v, off);
        }
        return r;
    }

    uint8_t
    at(size_t i) const {
        return (words_[i >> 5] >> (2 * (i & 31))) & 3u;
    }

    void
    prefetch(size_t i) const {
        size_t w = i >> 5;
        if (w >= nwords_) {
            w = nwords_ ? nwords_ - 1 : 0;
        }
        __builtin_prefetch(&rel_[(i >> 5) >> wpb_shift_], 0, 3);
        if (nwords_) {
            __builtin_prefetch(&words_[w], 0, 3);
        }
    }

    size_t
    size() const {
        return n_;
    }
    size_t
    word_count() const {
        return nwords_;
    }
    uint32_t
    words_per_block() const {
        return wpb_;
    }
    const uint64_t*
    words() const {
        return words_;
    }
    // Heap bytes held by the rebuilt rank directory (sb_ + rel_) — the part that
    // stays resident even when `words_` is an mmap view. Used by benchmarks to
    // report the space/latency tradeoff of words_per_block.
    size_t
    directory_bytes() const {
        return sb_.size() * sizeof(std::array<uint64_t, 4>) +
               rel_.size() * sizeof(std::array<uint16_t, 4>);
    }

    static QuadVector
    from_words(size_t n,
               std::vector<uint64_t> words,
               uint32_t words_per_block = 1) {
        QuadVector q;
        q.set_words_per_block(words_per_block);
        q.owned_words_ = std::move(words);
        q.n_ = n;
        q.words_ = q.owned_words_.data();
        q.nwords_ = q.owned_words_.size();
        q.build_rank();
        return q;
    }

    static QuadVector
    from_view(size_t n,
              const uint64_t* words,
              size_t nwords,
              uint32_t words_per_block = 1) {
        QuadVector q;
        q.set_words_per_block(words_per_block);
        q.n_ = n;
        q.words_ = words;
        q.nwords_ = nwords;
        q.build_rank();
        return q;
    }

 private:
    static constexpr size_t kBlocksPerSb = 64;  // 64 blocks per superblock
    static constexpr size_t kSbShift = 6;

    // Clamp/normalize words_per_block to a power of two in [1, 16]. The upper
    // bound keeps a superblock's symbol span (kBlocksPerSb * wpb * 32) within the
    // uint16 rel_ counter (64 * 16 * 32 = 32768 <= 65535). Callers (FMIndex)
    // validate up front; this is the last-line safety clamp.
    void
    set_words_per_block(uint32_t wpb) {
        if (wpb < 1) {
            wpb = 1;
        }
        if (wpb > 16) {
            wpb = 16;
        }
        uint32_t p = 1, sh = 0;
        while (p * 2 <= wpb) {
            p *= 2;
            ++sh;
        }
        wpb_ = p;
        wpb_shift_ = sh;
    }

    static size_t
    swar_count(uint64_t word, uint8_t v, size_t k) {
        constexpr uint64_t kLow = 0x5555555555555555ULL;
        uint64_t t = word ^ (uint64_t(v) * kLow);
        uint64_t eq = (~t) & (~(t >> 1)) & kLow;
        if (k < 32) {
            eq &= (1ULL << (2 * k)) - 1;
        }
        return __builtin_popcountll(eq);
    }

    void
    build_rank() {
        const size_t W = wpb_;
        const size_t nblocks = (nwords_ + W - 1) / W;  // ceil, one per rank block
        const size_t nsb = (nblocks + kBlocksPerSb - 1) / kBlocksPerSb;
        sb_.assign(nsb + 1, {0, 0, 0, 0});
        rel_.assign(nblocks + 1, {0, 0, 0, 0});
        std::array<uint64_t, 4> acc{0, 0, 0, 0};
        for (size_t b = 0; b <= nblocks; ++b) {
            if ((b & (kBlocksPerSb - 1)) == 0) {
                sb_[b >> kSbShift] = acc;
            }
            const auto& sbase = sb_[b >> kSbShift];
            for (uint8_t v = 0; v < 4; ++v) {
                rel_[b][v] = static_cast<uint16_t>(acc[v] - sbase[v]);
            }
            if (b < nblocks) {
                for (size_t k = 0; k < W; ++k) {
                    size_t ww = (b << wpb_shift_) + k;
                    if (ww >= nwords_) {
                        break;
                    }
                    size_t base = ww * 32;
                    size_t valid = n_ - base < 32 ? n_ - base : 32;
                    for (uint8_t v = 0; v < 4; ++v) {
                        acc[v] += swar_count(words_[ww], v, valid);
                    }
                }
            }
        }
    }

    size_t n_ = 0;
    const uint64_t* words_ = nullptr;
    size_t nwords_ = 0;
    uint32_t wpb_ = 1;        // words per rank block (power of two, <= 16)
    uint32_t wpb_shift_ = 0;  // log2(wpb_)
    std::vector<uint64_t> owned_words_;         // non-empty iff owning
    std::vector<std::array<uint64_t, 4>> sb_;   // owned (rebuilt)
    std::vector<std::array<uint16_t, 4>> rel_;  // owned (rebuilt)
};

}  // namespace milvus::index::fmindex
