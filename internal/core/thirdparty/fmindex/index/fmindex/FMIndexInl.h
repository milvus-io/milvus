// Licensed to the LF AI & Data foundation under Apache-2.0.
// Portions translated from Lance (lance_index::scalar::fmindex), Apache-2.0.
// Header-only implementation of FMIndex (included at the bottom of FMIndex.h).
#pragma once
#include "index/fmindex/FMIndex.h"
#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <functional>
#include <limits>
#include <map>
#include <new>
#include <set>
#include <stdexcept>
#include <tuple>
#include "index/fmindex/SuffixArray.h"

#ifdef FMIX_BUILD_MEM_PROFILE
#include <sys/resource.h>
#define FMIX_MEM(tag)                               \
    do {                                            \
        struct rusage ru;                           \
        getrusage(RUSAGE_SELF, &ru);                \
        std::fprintf(stderr,                        \
                     "  [mem] %-20s peak=%ld MB\n", \
                     tag,                           \
                     ru.ru_maxrss / (1024 * 1024)); \
    } while (0)
#else
#define FMIX_MEM(tag) ((void)0)
#endif

namespace milvus::index::fmindex {

inline void
FMIndex::Build(const std::vector<std::string_view>& docs,
               uint32_t sa_sample_rate,
               bool case_insensitive,
               bool force_wide,
               uint32_t block_bytes) {
    // Internal layout: each document's bytes are remapped to dense content ids,
    // and a separator symbol (dense id 1, OUTSIDE the byte alphabet) is injected
    // after each. That separator makes every query document-scoped: no query
    // byte maps to id 1, so no match can straddle a boundary — and this holds
    // for every byte value, so '\0' is ordinary queryable content. The internal
    // coordinate system (one symbol per content byte, one per separator) is
    // identical to the byte offsets, so doc_start_ / text_len_ are byte offsets.
    uint64_t internal_len = 0;
    doc_bounds_owned_.clear();
    doc_bounds_owned_.reserve(docs.size() + 1);
    for (const auto& d : docs) {
        doc_bounds_owned_.push_back(internal_len);
        if (internal_len == std::numeric_limits<uint64_t>::max() ||
            d.size() >
                std::numeric_limits<uint64_t>::max() - internal_len - 1) {
            throw std::length_error(
                "FMIndex: corpus length overflows uint64_t");
        }
        internal_len += d.size() + 1;  // content + one separator
    }
    doc_bounds_owned_.push_back(
        internal_len);  // end boundary (== internal_len)
    const size_t len = static_cast<size_t>(internal_len);

    // Suffix array is built with 32-bit indices under 2 GiB (compact) and 64-bit
    // indices at or above it. Only the astronomically large 2^63 ceiling is a
    // hard error.
    const bool use64 = force_wide || len >= (static_cast<size_t>(INT32_MAX));
    if (len >= (static_cast<size_t>(1) << 63)) {
        throw std::length_error("FMIndex: corpus too large (>= 2^63 bytes)");
    }
    sa_sample_rate_ = sa_sample_rate == 0 ? 1 : sa_sample_rate;
    // block_bytes -> words_per_block (8 B = one 64-bit word). Require a
    // power-of-two multiple of 8 in [8, 128]; QuadVector re-clamps defensively.
    {
        uint32_t bb = block_bytes == 0 ? 8 : block_bytes;
        uint32_t wpb = bb / 8;
        if (wpb < 1) {
            wpb = 1;
        }
        if (wpb > 16) {
            wpb = 16;
        }
        uint32_t p = 1;
        while (p * 2 <= wpb) {
            p *= 2;
        }
        words_per_block_ = p;
    }
    case_fold_ = case_insensitive;
    // Store positions 8 bytes wide once they can exceed uint32 (>= 4 GiB), or
    // when forced (lets tests exercise the wide path on small inputs).
    wide_storage_ = force_wide || len >= (static_cast<size_t>(1) << 32);
    text_len_ = len;

    // ASCII case fold: A-Z -> a-z when case_insensitive, identity otherwise.
    auto fold = [cf = case_fold_](uint8_t b) -> uint8_t {
        return (cf && b >= 'A' && b <= 'Z') ? static_cast<uint8_t>(b + 32) : b;
    };

    // 1. dense alphabet: id 0 = sentinel, id 1 = separator, ids 2..sigma-1 =
    //    distinct (folded) content bytes in ascending order (order-preserving so
    //    id order matches byte order). With case folding both cases of a letter
    //    alias to one id, so the query hot path stays a plain byte_to_id_ lookup.
    std::array<bool, 256> present{};
    for (const auto& d : docs) {
        for (unsigned char c : d) {
            present[fold(c)] = true;
        }
    }
    byte_to_id_.fill(-1);
    uint32_t id = 2;  // 0 = sentinel, 1 = separator (reserved, non-byte)
    for (int b = 0; b < 256; ++b) {
        if (present[b]) {
            byte_to_id_[b] = static_cast<int32_t>(id++);
        }
    }
    if (case_fold_) {
        for (int b = 'A'; b <= 'Z'; ++b) {
            byte_to_id_[b] =
                byte_to_id_[b + 32];  // uppercase aliases lowercase
        }
    }
    sigma_ = id;  // sentinel + separator + number of distinct content bytes
    uint32_t bits = 1;
    while ((1u << bits) < sigma_) {
        ++bits;
    }
    qlevels_ = (bits + 1) / 2;  // 2 bits per quad level

    const size_t m = len + 1;  // + trailing sentinel
    // 2. Materialize the dense-id symbol text t (content ids, separators id 1,
    //    trailing sentinel id 0) and build the SA. The compact path uses int32
    //    text + int32 SA in releasable scratch mappings. The SA is later
    //    overwritten with BWT symbols, so no separate BWT allocation overlaps
    //    the 4x text + 4x SA window.
    ScratchArray<int32_t> t32(use64 ? 0 : m);
    ScratchArray<int32_t> sa32(use64 ? 0 : m + kSuffixArrayExtraSpace);
    std::vector<int64_t> t64, sa64;
    std::vector<uint16_t> bwt;
    auto fill_symbols = [&](auto* t) {
        size_t pos = 0;
        for (const auto& d : docs) {
            for (unsigned char c : d) {
                t[pos++] = byte_to_id_[c];  // content id (>= 2), case-folded
            }
            t[pos++] = 1;  // separator
        }
        t[pos] = 0;  // trailing sentinel (pos == len == m-1)
    };
    if (use64) {
        t64.resize(m);
        fill_symbols(t64.data());
        sa64 = build_suffix_array_symbols64(
            t64.data(), m, static_cast<int64_t>(sigma_));
    } else {
        fill_symbols(t32.data());
        build_suffix_array_symbols32(
            t32.data(), m, static_cast<int32_t>(sigma_), sa32.data());
    }
    FMIX_MEM("after SA");
    auto sa_at = [&](size_t i) -> uint64_t {
        return use64 ? static_cast<uint64_t>(sa64[i])
                     : static_cast<uint64_t>(sa32[i]);
    };

    // 3. Sample the SA before its compact-path storage is repurposed. Build keeps
    //    sampled
    //    positions 4 bytes wide below 4 GiB, matching the serialized format,
    //    instead of temporarily widening every sample to uint64.
    BitVector samp(m);
    sample_vals_narrow_owned_.clear();
    sample_vals_wide_owned_.clear();
    const size_t expected_samples = (m - 1) / sa_sample_rate_ + 1;
    if (wide_storage_) {
        sample_vals_wide_owned_.reserve(expected_samples);
    } else {
        sample_vals_narrow_owned_.reserve(expected_samples);
    }
    for (size_t i = 0; i < m; ++i) {
        uint64_t v = sa_at(i);
        if (v % sa_sample_rate_ == 0) {
            samp.set(i);
            if (wide_storage_) {
                sample_vals_wide_owned_.push_back(v);
            } else {
                sample_vals_narrow_owned_.push_back(static_cast<uint32_t>(v));
            }
        }
    }
    samp.build_rank();
    sampled_bv_ = std::move(samp);
    FMIX_MEM("after sampling");

    // 4. Build the BWT. On the compact path, overwrite each consumed SA entry
    //    with its uint16 BWT symbol, release the text mapping, then narrow the SA
    //    scratch into the final BWT vector. The extra sequential narrowing pass
    //    is cheaper than a random in-place permutation and keeps peak RSS below
    //    the original text+SA+BWT window.
    if (use64) {
        bwt.resize(m);
        for (size_t i = 0; i < m; ++i) {
            const uint64_t v = static_cast<uint64_t>(sa64[i]);
            bwt[i] = v == 0 ? uint16_t{0} : static_cast<uint16_t>(t64[v - 1]);
        }
        std::vector<int64_t>().swap(t64);
        std::vector<int64_t>().swap(sa64);
    } else {
        for (size_t i = 0; i < m; ++i) {
            const int32_t v = sa32[i];
            sa32[i] = v == 0 ? 0 : t32[static_cast<size_t>(v - 1)];
        }
        t32.release();
        bwt.resize(m);
        for (size_t i = 0; i < m; ++i) {
            bwt[i] = static_cast<uint16_t>(sa32[i]);
        }
        sa32.release();
    }
    FMIX_MEM("after BWT");

    // 5. C-table over sigma_ (cumulative counts can reach m, so 64-bit).
    c_.assign(sigma_, 0);
    for (uint32_t sym : bwt) {
        c_[sym]++;
    }
    uint64_t acc = 0;
    for (uint32_t c = 0; c < sigma_; ++c) {
        uint64_t cnt = c_[c];
        c_[c] = acc;
        acc += cnt;
    }

    // 6. Quad wavelet matrix over the BWT. The moved BWT is one of the two
    //    ping-pong buffers; digits are packed directly without an n-byte side
    //    array.
    wm_ = WaveletMatrix4(std::move(bwt), qlevels_, words_per_block_);
    FMIX_MEM("after wavelet");
    first_.resize(sigma_);
    for (uint32_t c = 0; c < sigma_; ++c) {
        first_[c] = wm_.map_zero(c);
    }

    // Point the access views at the owned storage. Vector moves keep their heap
    // buffer address, so these stay valid when the whole FMIndex is moved.
    if (wide_storage_) {
        sv_wide_ = sample_vals_wide_owned_.data();
        sv_narrow_ = nullptr;
        n_samples_ = sample_vals_wide_owned_.size();
    } else {
        sv_narrow_ = sample_vals_narrow_owned_.data();
        sv_wide_ = nullptr;
        n_samples_ = sample_vals_narrow_owned_.size();
    }
    doc_start_ = doc_bounds_owned_.data();
    n_doc_bounds_ = doc_bounds_owned_.size();

    buildDerived();
}

inline void
FMIndex::buildDerived() {
    // id -> byte (inverse of byte_to_id_). For a case-folded index both 'A' and
    // 'a' map to one id; ascending iteration lets lowercase win, so Extract
    // returns the canonical lowercase byte.
    id_to_byte_.assign(sigma_, 0);
    for (int b = 0; b < 256; ++b) {
        if (byte_to_id_[b] > 0) {
            id_to_byte_[byte_to_id_[b]] = static_cast<uint8_t>(b);
        }
    }
    // The separator is a fixed synthetic symbol (dense id 1), not a byte. No
    // byte_to_id_ entry equals 1, so a query byte can never step onto it.
    sep_id_ = 1;
}

inline void
FMIndex::ensureIsaSample() const {
    // isa_sample_[k] = the BWT row whose suffix starts at text position k*rate.
    // Every multiple of rate in [0, text_len_] appears exactly once in the SA,
    // so this array is fully populated. Gives Extract an anchor within `rate`
    // steps of any position without needing select/psi. Built lazily on the
    // first Extract — the build walks ALL m rows and allocates m/rate x 8 B, a
    // cost Locate/Count workloads should never pay.
    std::call_once(*isa_once_, [this] {
        const size_t m = text_len_ + 1;
        isa_sample_.assign(text_len_ / sa_sample_rate_ + 1, 0);
        for (size_t r = 0; r < m; ++r) {
            if (sampled_bv_.get(r)) {
                uint64_t val = sample_val(sampled_bv_.rank1(r));
                isa_sample_[val / sa_sample_rate_] = static_cast<uint64_t>(r);
            }
        }
    });
}

inline std::string
FMIndex::Extract(uint64_t doc_id, uint64_t offset, size_t len) const {
    if (c_.empty() || doc_id + 1 >= n_doc_bounds_) {
        return {};  // empty index or doc_id out of range
    }
    ensureIsaSample();  // lazy: only Extract pays for the ISA anchor table
    // Translate (doc, offset) to internal coordinates and clamp to the document's
    // content end (the byte before its '\0'), so Extract never crosses into the
    // separator or the next document.
    uint64_t dstart = doc_start_[doc_id];
    uint64_t dlen = doc_start_[doc_id + 1] - 1 - dstart;  // content length
    if (offset >= dlen) {
        return {};
    }
    uint64_t pos = dstart + offset;
    uint64_t end = pos + std::min<uint64_t>(len, dlen - offset);
    // Anchor: a row whose suffix starts at a sampled position >= end, then walk
    // backward (LF) collecting BWT chars, which are the text bytes before each
    // row's suffix. Prefer the nearest sample above `end`; fall back to row 0
    // (the sentinel suffix, which starts at text_len_) when none exists.
    uint64_t k =
        (end + sa_sample_rate_ - 1) / sa_sample_rate_;  // ceil(end/rate)
    size_t row;
    uint64_t p;
    if (k < isa_sample_.size()) {
        row = isa_sample_[k];
        p = k * sa_sample_rate_;
    } else {
        row = 0;  // SA[0] is the sentinel-only suffix, starting at text_len_
        p = text_len_;
    }
    std::string out(static_cast<size_t>(end - pos), '\0');
    while (p > pos) {
        uint32_t sym = wm_.access(row);  // BWT[row] = T[p-1]
        if (p - 1 < end) {
            out[static_cast<size_t>(p - 1 - pos)] =
                static_cast<char>(id_to_byte_[sym]);
        }
        row = LF(row);
        --p;
    }
    return out;
}

inline FMIndex::LongestMatchResult
FMIndex::LongestMatch(const uint8_t* query, size_t qlen) const {
    LongestMatchResult best{0, 0, 0};
    if (c_.empty()) {
        return best;
    }
    const size_t m = text_len_ + 1;
    // For each end position e, extend the match backward (query[k..e]) as far as
    // the SA interval stays non-empty; track the global longest across all e.
    for (size_t e = 0; e < qlen; ++e) {
        size_t lo = 0, hi = m, len = 0;
        for (size_t k = e + 1; k-- > 0;) {
            int32_t id = byte_to_id_[query[k]];
            if (id < 0) {
                break;  // byte absent from the corpus — match cannot extend
            }
            auto pp = wm_.map2(static_cast<uint32_t>(id), lo, hi);
            size_t base = c_[id] - first_[id];
            size_t nlo = base + pp.first, nhi = base + pp.second;
            if (nlo >= nhi) {
                break;  // query[k..e] does not occur — stop extending
            }
            lo = nlo;
            hi = nhi;
            ++len;
            if (len > best.length) {
                best.length = len;
                best.query_pos = k;
                best.count = hi - lo;
            }
        }
    }
    return best;
}

inline std::vector<std::pair<uint8_t, size_t>>
FMIndex::NextTokenCounts(const uint8_t* pattern, size_t plen) const {
    std::vector<std::pair<uint8_t, size_t>> out;
    if (c_.empty()) {
        return out;
    }
    // No continuations if P itself does not occur.
    auto pr = BackwardSearch(pattern, plen);
    if (pr.first >= pr.second) {
        return out;
    }
    // count(P·b) = number of occurrences of the (plen+1)-length string P then b.
    std::vector<uint8_t> buf(pattern, pattern + plen);
    buf.push_back(0);
    for (int b = 0; b < 256; ++b) {  // every byte is real content in v2
        if (byte_to_id_[b] < 0) {
            continue;  // byte never appears in the corpus
        }
        buf[plen] = static_cast<uint8_t>(b);
        auto r = BackwardSearch(buf.data(), plen + 1);
        size_t cnt = r.second - r.first;
        if (cnt) {
            out.emplace_back(static_cast<uint8_t>(b), cnt);
        }
    }
    return out;
}

inline std::vector<uint64_t>
FMIndex::MatchingDocs(const uint8_t* pat, size_t plen) const {
    auto hits =
        LocateDocs(pat, plen);  // (doc, offset), cross-doc seam filtered
    std::vector<uint64_t> docs;
    docs.reserve(hits.size());
    for (auto& h : hits) {
        docs.push_back(h.first);
    }
    docs.erase(std::unique(docs.begin(), docs.end()), docs.end());  // sorted
    return docs;
}

inline std::vector<uint64_t>
FMIndex::FuzzyMatchingDocs(const uint8_t* pat, size_t plen, uint32_t k) const {
    if (c_.empty() || plen == 0) {
        return {};
    }
    const size_t m = text_len_ + 1;
    // Backtracking backward search: DFS over SA intervals, spending an error
    // budget on substitution / insertion (extra text char) / deletion (skipped
    // pattern char). The DFS never steps onto the separator id, so every matched
    // string T' lies inside one document — no length/boundary bookkeeping needed.
    // At i==0 record T''s SA interval; memoize (lo,hi,i)->min errors to prune.
    std::vector<std::pair<size_t, size_t>> hits;
    std::map<std::tuple<size_t, size_t, size_t>, uint32_t> visited;
    std::function<void(size_t, size_t, size_t, uint32_t)> dfs =
        [&](size_t lo, size_t hi, size_t i, uint32_t e) {
            if (i == 0) {
                hits.emplace_back(lo, hi);
                return;
            }
            auto key = std::make_tuple(lo, hi, i);
            auto v = visited.find(key);
            if (v != visited.end() && v->second <= e) {
                return;
            }
            visited[key] = e;
            // Every pattern byte maps to a content id (>= 2) or -1 if absent;
            // it can never be the separator (id 1), which no byte addresses.
            int32_t tid = byte_to_id_[pat[i - 1]];
            auto step = [&](uint32_t d, size_t& nlo, size_t& nhi) {
                auto pp = wm_.map2(d, lo, hi);
                size_t base = c_[d] - first_[d];
                nlo = base + pp.first;
                nhi = base + pp.second;
            };
            if (e == k) {  // budget spent: only exact matches from here on
                if (tid >= 0) {
                    size_t nlo, nhi;
                    step(static_cast<uint32_t>(tid), nlo, nhi);
                    if (nlo < nhi) {
                        dfs(nlo, nhi, i - 1, e);
                    }
                }
                return;
            }
            dfs(lo, hi, i - 1, e + 1);  // deletion: skip pat[i-1], no text char
            for (uint32_t d = 1; d < sigma_; ++d) {
                if (static_cast<int32_t>(d) == sep_id_) {
                    continue;  // never edit toward the separator (stays in-doc)
                }
                size_t nlo, nhi;
                step(d, nlo, nhi);
                if (nlo >= nhi) {
                    continue;
                }
                if (tid >= 0 && static_cast<uint32_t>(tid) == d) {
                    dfs(nlo, nhi, i - 1, e);  // match, no cost
                } else {
                    dfs(nlo, nhi, i - 1, e + 1);  // substitution
                }
                dfs(nlo, nhi, i, e + 1);  // insertion: extra text char
            }
        };
    dfs(0, m, plen, 0);

    // Union the documents of every matched interval (each match is in-document).
    std::set<uint64_t> docset;
    std::set<size_t> seen_rows;
    for (auto& h : hits) {
        for (size_t r = h.first; r < h.second; ++r) {
            if (!seen_rows.insert(r).second) {
                continue;  // this row already resolved by another interval
            }
            size_t row = r;
            uint64_t steps = 0;
            while (!sampled_bv_.get(row)) {
                row = LF(row);
                ++steps;
            }
            uint64_t pos = sample_val(sampled_bv_.rank1(row)) + steps;
            if (pos < text_len_) {
                docset.insert(docOf(pos));
            }
        }
    }
    return {docset.begin(), docset.end()};
}

inline size_t
FMIndex::LF(size_t i) const {
    uint32_t sym = wm_.access(i);
    return c_[sym] + wm_.rank(sym, i);
}

inline std::pair<size_t, size_t>
FMIndex::BackwardSearch(const uint8_t* pattern, size_t plen) const {
    if (c_.empty()) {
        return {0, 0};  // default-constructed / failed-Deserialize index
    }
    const size_t m = text_len_ + 1;
    if (plen == 0) {
        return {0, m};
    }
    size_t lo = 0, hi = m;
    for (size_t k = plen; k-- > 0;) {
        int32_t id = byte_to_id_[pattern[k]];
        if (id < 0) {
            return {0, 0};  // byte absent from the corpus: no match
        }
        auto pp = wm_.map2(static_cast<uint32_t>(id), lo, hi);
        size_t base = c_[id] - first_[id];
        lo = base + pp.first;
        hi = base + pp.second;
        if (lo >= hi) {
            return {0, 0};
        }
    }
    return {lo, hi};
}

inline std::vector<size_t>
FMIndex::CountBatch(
    const std::vector<std::pair<const uint8_t*, size_t>>& patterns) const {
    const size_t m = text_len_ + 1;
    const size_t B = patterns.size();
    std::vector<size_t> result(B, 0);
    if (c_.empty()) {
        return result;  // default-constructed / failed-Deserialize index
    }

    // Process in small tiles so the set of in-flight positions stays L1-resident
    // while still giving enough memory-level parallelism to overlap misses.
    constexpr size_t kTile = 32;

    std::vector<uint32_t> ids(kTile);
    std::vector<size_t> blo(kTile), bhi(kTile);
    std::vector<size_t> qslot(kTile);
    std::vector<size_t> lo(kTile), hi(kTile);
    std::vector<int64_t> posn(kTile);
    std::vector<uint8_t> active(kTile);

    for (size_t t0 = 0; t0 < B; t0 += kTile) {
        size_t t1 = std::min(t0 + kTile, B);
        size_t tn = t1 - t0;
        for (size_t s = 0; s < tn; ++s) {
            size_t len = patterns[t0 + s].second;
            if (len == 0) {
                result[t0 + s] = m;
                active[s] = 0;
                continue;
            }
            lo[s] = 0;
            hi[s] = m;
            posn[s] = static_cast<int64_t>(len) - 1;
            active[s] = 1;
        }
        for (;;) {
            size_t k = 0;  // active-in-tile count this round
            for (size_t s = 0; s < tn; ++s) {
                if (!active[s]) {
                    continue;
                }
                int32_t id = byte_to_id_[patterns[t0 + s].first[posn[s]]];
                if (id < 0) {
                    active[s] = 0;
                    continue;
                }
                ids[k] = static_cast<uint32_t>(id);
                blo[k] = lo[s];
                bhi[k] = hi[s];
                qslot[k] = s;
                ++k;
            }
            if (k == 0) {
                break;
            }
            wm_.map_batch(ids.data(), blo.data(), bhi.data(), k);
            for (size_t j = 0; j < k; ++j) {
                size_t s = qslot[j];
                uint32_t id = ids[j];
                size_t base = c_[id] - first_[id];
                size_t nlo = base + blo[j];
                size_t nhi = base + bhi[j];
                if (nlo >= nhi) {
                    active[s] = 0;
                    continue;
                }
                lo[s] = nlo;
                hi[s] = nhi;
                if (--posn[s] < 0) {
                    result[t0 + s] = nhi - nlo;
                    active[s] = 0;
                }
            }
        }
    }
    return result;
}

inline uint64_t
FMIndex::docOf(uint64_t internal_pos) const {
    const uint64_t* end = doc_start_ + n_doc_bounds_;
    auto it = std::upper_bound(doc_start_, end, internal_pos);
    return static_cast<uint64_t>(it - doc_start_) - 1;
}

inline uint64_t
FMIndex::locateRow(size_t row) const {
    uint64_t steps = 0;
    while (!sampled_bv_.get(row)) {
        row = LF(row);
        ++steps;
    }
    return sample_val(sampled_bv_.rank1(row)) + steps;
}

inline std::vector<uint64_t>
FMIndex::locateInternal(const uint8_t* pattern, size_t plen) const {
    if (plen == 0) {
        return {};  // empty pattern: no document-scoped hits (matches
            // FuzzyMatchingDocs); a raw Count still reports m positions.
    }
    auto r = BackwardSearch(pattern, plen);
    std::vector<uint64_t> out;
    for (size_t i = r.first; i < r.second; ++i) {
        uint64_t pos = locateRow(i);
        if (pos < text_len_) {
            out.push_back(pos);  // internal coordinate (separators included)
        }
    }
    std::sort(out.begin(), out.end());
    return out;
}

inline std::vector<std::pair<uint64_t, uint64_t>>
FMIndex::LocateDocs(const uint8_t* pattern, size_t plen) const {
    std::vector<uint64_t> positions = locateInternal(pattern, plen);
    std::vector<std::pair<uint64_t, uint64_t>> out;
    out.reserve(positions.size());
    for (uint64_t pos : positions) {
        // No occurrence can span a document (the pattern holds no '\0'), so every
        // hit is fully inside doc docOf(pos); the offset is measured from its
        // internal start.
        uint64_t doc = docOf(pos);
        out.emplace_back(doc, pos - doc_start_[doc]);
    }
    std::sort(out.begin(), out.end());
    return out;
}

inline std::vector<std::vector<std::pair<uint64_t, uint64_t>>>
FMIndex::LocateDocsBatch(
    const std::vector<std::pair<const uint8_t*, size_t>>& patterns) const {
    const size_t B = patterns.size();
    std::vector<std::vector<std::pair<uint64_t, uint64_t>>> out(B);
    if (c_.empty()) {
        return out;
    }

    // 1. Backward-search each pattern and enqueue one walk per occurrence row.
    //    (Backward search stays serial — the cost batched here is the LF-walk
    //    that Locate adds on top of Count. Batch the searches with CountBatch
    //    first if search itself dominates.)
    struct Walk {
        size_t row;
        uint64_t steps;
        uint32_t q;
    };
    std::vector<Walk> walks;
    for (size_t q = 0; q < B; ++q) {
        if (patterns[q].second == 0) {
            continue;
        }
        auto r = BackwardSearch(patterns[q].first, patterns[q].second);
        for (size_t i = r.first; i < r.second; ++i) {
            walks.push_back({i, 0, static_cast<uint32_t>(q)});
        }
    }

    // 2. Lock-step LF-walk the rows in tiles so independent walks' cache misses
    //    overlap. Same structure as CountBatch: a prefetch pass then a work pass,
    //    keeping the in-flight set small enough to stay L1-resident.
    constexpr size_t kTile = 32;
    std::vector<uint64_t> pos_flat(walks.size(), 0);
    std::vector<uint8_t> done(walks.size(), 0);
    std::vector<size_t> pend(kTile);
    for (size_t t0 = 0; t0 < walks.size(); t0 += kTile) {
        size_t tn = std::min(kTile, walks.size() - t0);
        for (;;) {
            // Collect still-walking rows; finalize any already on a sampled row.
            size_t np = 0;
            for (size_t s = 0; s < tn; ++s) {
                size_t gi = t0 + s;
                if (done[gi]) {
                    continue;
                }
                if (sampled_bv_.get(walks[gi].row)) {
                    pos_flat[gi] =
                        sample_val(sampled_bv_.rank1(walks[gi].row)) +
                        walks[gi].steps;
                    done[gi] = 1;
                    continue;
                }
                pend[np++] = gi;
            }
            if (np == 0) {
                break;
            }
            // Prefetch the two independent first-reads of each pending LF step.
            for (size_t j = 0; j < np; ++j) {
                wm_.prefetch_access(walks[pend[j]].row);
            }
            // Advance; prefetch next round's sampled-bit read as we go.
            for (size_t j = 0; j < np; ++j) {
                size_t gi = pend[j];
                walks[gi].row = LF(walks[gi].row);
                ++walks[gi].steps;
                sampled_bv_.prefetch(walks[gi].row);
            }
        }
    }

    // 3. Same (doc, offset) mapping + per-pattern sort as LocateDocs.
    std::vector<std::vector<uint64_t>> pos_per_q(B);
    for (size_t gi = 0; gi < walks.size(); ++gi) {
        if (done[gi] && pos_flat[gi] < text_len_) {
            pos_per_q[walks[gi].q].push_back(pos_flat[gi]);
        }
    }
    for (size_t q = 0; q < B; ++q) {
        auto& positions = pos_per_q[q];
        std::sort(positions.begin(), positions.end());
        out[q].reserve(positions.size());
        for (uint64_t pos : positions) {
            uint64_t doc = docOf(pos);
            out[q].emplace_back(doc, pos - doc_start_[doc]);
        }
        std::sort(out[q].begin(), out[q].end());
    }
    return out;
}

inline size_t
FMIndex::CountPrefixDocs(const uint8_t* pattern, size_t plen) const {
    if (plen == 0 || c_.empty()) {
        return 0;
    }
    auto r = BackwardSearch(pattern, plen);
    if (r.first >= r.second) {
        return 0;
    }
    // An occurrence sits on a document start iff the BWT symbol preceding it is
    // the sentinel (id 0, before doc 0) or a separator (id 1, before docs 1..N).
    // Each such row is a distinct document, so the count of {sentinel,separator}
    // rows in the pattern's SA interval [lo,hi) is the number of prefix docs.
    size_t at_hi = wm_.rank(0, r.second) + wm_.rank(1, r.second);
    size_t at_lo = wm_.rank(0, r.first) + wm_.rank(1, r.first);
    return at_hi - at_lo;
}

inline std::vector<uint64_t>
FMIndex::LocatePrefixDocs(const uint8_t* pattern, size_t plen) const {
    if (plen == 0 || c_.empty()) {
        return {};
    }
    auto r = BackwardSearch(pattern, plen);
    if (r.first >= r.second) {
        return {};
    }
    std::vector<uint64_t> docs;
    // The document-start occurrences of P are exactly the rows in P's SA interval
    // whose preceding BWT symbol is a separator (id 1, docs 1..N) or the sentinel
    // (id 0, doc 0). One more backward-search step by each of those two symbols
    // isolates precisely those rows — so we locate ONLY the answer set and never
    // touch the (possibly vast) interior occurrences of P.
    //
    // Separator-preceded rows: their suffix is "<sep>P...", so the located SA
    // position is the separator at doc_start_[d]-1; +1 lands on the document
    // start, whose docOf is the document that begins with P.
    auto sp = wm_.map2(static_cast<uint32_t>(sep_id_), r.first, r.second);
    size_t sbase = c_[sep_id_] - first_[sep_id_];
    for (size_t i = sbase + sp.first; i < sbase + sp.second; ++i) {
        docs.push_back(docOf(locateRow(i) + 1));
    }
    // Sentinel-preceded row (at most one): the sentinel is cyclically followed by
    // position 0, so a match means document 0 begins with P. No locate needed.
    auto tp = wm_.map2(0u, r.first, r.second);
    if (tp.second > tp.first) {
        docs.push_back(0);
    }
    std::sort(docs.begin(), docs.end());
    docs.erase(std::unique(docs.begin(), docs.end()), docs.end());
    return docs;
}

inline std::pair<size_t, size_t>
FMIndex::suffixDocInterval(const uint8_t* pattern, size_t plen) const {
    if (plen == 0 || c_.empty()) {
        return {0, 0};
    }
    // Seed on the separator symbol's SA interval: rows whose suffix starts with
    // the separator (id 1) are [c_[1], c_[2]) — one per document. c_[2] exists
    // whenever the corpus has any content byte; if not, no non-empty pattern can
    // match anyway (its bytes are absent below).
    size_t lo = c_[1];
    size_t hi = (c_.size() > 2) ? c_[2] : (text_len_ + 1);
    // Backward-extend by the pattern, right to left, to reach "pattern<sep>".
    for (size_t k = plen; k-- > 0;) {
        int32_t id = byte_to_id_[pattern[k]];
        if (id < 0) {
            return {0, 0};  // byte absent from the corpus
        }
        auto pp = wm_.map2(static_cast<uint32_t>(id), lo, hi);
        size_t base = c_[id] - first_[id];
        lo = base + pp.first;
        hi = base + pp.second;
        if (lo >= hi) {
            return {0, 0};
        }
    }
    return {lo, hi};
}

inline size_t
FMIndex::CountSuffixDocs(const uint8_t* pattern, size_t plen) const {
    auto r = suffixDocInterval(pattern, plen);
    return r.second - r.first;  // each row is a distinct document ending in P
}

inline std::vector<uint64_t>
FMIndex::LocateSuffixDocs(const uint8_t* pattern, size_t plen) const {
    auto r = suffixDocInterval(pattern, plen);
    std::vector<uint64_t> docs;
    docs.reserve(r.second - r.first);
    // Every row in the "pattern<sep>" interval is a genuine document-end hit, so
    // we locate exactly the answer set — no occurrence is located and discarded.
    // The located SA position is where the pattern begins; its document ends with
    // the pattern by construction.
    for (size_t i = r.first; i < r.second; ++i) {
        docs.push_back(docOf(locateRow(i)));
    }
    std::sort(docs.begin(), docs.end());
    docs.erase(std::unique(docs.begin(), docs.end()), docs.end());
    return docs;
}

// ------------------------- serialization -------------------------
// Format v3: a header of scalars/metadata/section-sizes, then the large payload
// arrays each padded to an 8-byte boundary so LoadView can point at them
// (zero-copy) from mmap'd memory. Only the wavelet/sampled word arrays are
// viewed, and so are the sampled-SA values and doc boundaries (via width-aware
// accessors) — a load copies nothing; only rank directories are rebuilt.
template <typename T>
void
put(std::string& s, const T& v) {
    s.append(reinterpret_cast<const char*>(&v), sizeof(T));
}
template <typename T>
T
get(const char*& p, const char* end) {
    if (p + sizeof(T) > end) {
        throw std::runtime_error("fmindex: truncated blob");
    }
    T v;
    std::memcpy(&v, p, sizeof(T));
    p += sizeof(T);
    return v;
}
constexpr uint32_t kMagic = 0x464D4958;  // "FMIX"
// v7: separator is an out-of-byte-alphabet symbol (dense id 1), so '\0' is
// ordinary content; content ids are 2..sigma. Not backward-compatible with v6.
// v8: the flags word carries words_per_block (rank-block granularity) in bits
// 8-15, so load rebuilds the directory at the same granularity Build used.
constexpr uint32_t kFormatVersion = 8;

inline void
FMIndex::writeHeader(std::string& s) const {
    put(s, kMagic);
    put(s, kFormatVersion);
    put(s, sa_sample_rate_);
    put(s, sigma_);
    put(s, qlevels_);
    // Flags live in the former 4-byte alignment pad (keeps text_len 8-aligned):
    // bit 0 = ASCII case-insensitive, bit 1 = 8-byte sampled-SA storage,
    // bits 8-15 = words_per_block (rank-block granularity).
    put(s,
        static_cast<uint32_t>((case_fold_ ? 1u : 0u) |
                              (wide_storage_ ? 2u : 0u) |
                              (words_per_block_ << 8)));
    put(s, text_len_);
    for (int32_t v : byte_to_id_) {
        put(s, v);
    }
    for (uint64_t c : c_) {
        put(s, c);
    }
    for (uint32_t l = 0; l < qlevels_; ++l) {
        for (int d = 0; d < 4; ++d) {
            put(s, static_cast<uint64_t>(wm_.starts()[l][d]));
        }
    }
    // section sizes (word counts / element counts)
    for (uint32_t l = 0; l < qlevels_; ++l) {
        put(s, static_cast<uint64_t>(wm_.levels_qv()[l].word_count()));
    }
    put(s, static_cast<uint64_t>(sampled_bv_.word_count()));
    put(s, static_cast<uint64_t>(n_samples_));
    put(s, static_cast<uint64_t>(n_doc_bounds_));
}

inline std::string
FMIndex::Serialize() const {
    std::string s;
    writeHeader(s);
    auto align8 = [&s] {
        while (s.size() & 7u) {
            s.push_back('\0');
        }
    };
    for (uint32_t l = 0; l < qlevels_; ++l) {
        align8();
        const QuadVector& q = wm_.levels_qv()[l];
        s.append(reinterpret_cast<const char*>(q.words()),
                 q.word_count() * sizeof(uint64_t));
    }
    align8();
    s.append(reinterpret_cast<const char*>(sampled_bv_.words()),
             sampled_bv_.word_count() * sizeof(uint64_t));
    align8();
    if (wide_storage_) {
        // Wide storage always has an 8-byte array behind sv_wide_ (owned after
        // Build, viewed after a wide load) — appendable as-is.
        s.append(reinterpret_cast<const char*>(sv_wide_),
                 n_samples_ * sizeof(uint64_t));
    } else {
        s.append(reinterpret_cast<const char*>(sv_narrow_),
                 n_samples_ * sizeof(uint32_t));
    }
    align8();
    s.append(reinterpret_cast<const char*>(doc_start_),
             n_doc_bounds_ * sizeof(uint64_t));
    return s;
}

inline FMIndex::SerializeFileStatus
FMIndex::SerializeToFile(const std::string& path) const {
    std::FILE* f = std::fopen(path.c_str(), "wb");
    if (!f) {
        return SerializeFileStatus::OpenFailed;
    }
    bool ok = true;
    int failure_errno = 0;
    try {
        // Only the small header is buffered; the large arrays stream straight to
        // the file (no intermediate full-index copy).
        std::string header;
        writeHeader(header);
        size_t off = 0;
        auto emit = [&](const void* p, size_t n) {
            if (n && std::fwrite(p, 1, n, f) != n) {
                ok = false;
                if (failure_errno == 0) {
                    failure_errno = errno;
                }
            }
            off += n;
        };
        static const char kZero[8] = {0};
        auto pad8 = [&] { emit(kZero, (8 - (off & 7u)) & 7u); };
        emit(header.data(), header.size());
        for (uint32_t l = 0; l < qlevels_; ++l) {
            pad8();
            const QuadVector& q = wm_.levels_qv()[l];
            emit(q.words(), q.word_count() * sizeof(uint64_t));
        }
        pad8();
        emit(sampled_bv_.words(), sampled_bv_.word_count() * sizeof(uint64_t));
        pad8();
        if (wide_storage_) {
            emit(sv_wide_, n_samples_ * sizeof(uint64_t));
        } else {
            emit(sv_narrow_, n_samples_ * sizeof(uint32_t));
        }
        pad8();
        emit(doc_start_, n_doc_bounds_ * sizeof(uint64_t));
    } catch (...) {
        std::fclose(f);
        std::remove(path.c_str());
        throw;
    }
    // fclose flushes the stdio buffer, so a write that only fails at flush time
    // (ENOSPC / EIO) surfaces HERE, not at fwrite. Treat a non-zero fclose as a
    // serialization failure: otherwise a silently-truncated file would return
    // success, get uploaded and CRC'd as valid, and only be caught when a
    // QueryNode fails to load it. On any failure, remove the partial file so the
    // caller never uploads it.
    if (std::fclose(f) != 0) {
        ok = false;
        if (failure_errno == 0) {
            failure_errno = errno;
        }
    }
    if (!ok) {
        std::remove(path.c_str());
        // stdio is expected to set errno on a short write / failed flush, but
        // the C contract does not require it. Never make the caller report
        // strerror(0) ("Success") for an operation that actually failed.
        errno = failure_errno == 0 ? EIO : failure_errno;
    }
    return ok ? SerializeFileStatus::Success : SerializeFileStatus::WriteFailed;
}

inline bool
FMIndex::parseView(const uint8_t* base, size_t size) {
    if (base == nullptr) {
        return false;
    }
    const char* p = reinterpret_cast<const char*>(base);
    const char* end = p + size;
    try {
        if (get<uint32_t>(p, end) != kMagic ||
            get<uint32_t>(p, end) != kFormatVersion) {
            return false;
        }
        sa_sample_rate_ = get<uint32_t>(p, end);
        if (sa_sample_rate_ == 0) {
            return false;
        }
        sigma_ = get<uint32_t>(p, end);
        qlevels_ = get<uint32_t>(p, end);
        uint32_t flags = get<uint32_t>(p, end);  // former pad, now flags
        case_fold_ = (flags & 1u) != 0;
        wide_storage_ = (flags & 2u) != 0;
        words_per_block_ = (flags >> 8) & 0xFFu;
        // Must be a power of two in [1, 16] (kBlocksPerSb * wpb * 32 <= 65535,
        // the uint16 rel_ counter). A mutated value would rebuild the directory
        // at the wrong granularity and read the rank samples out of step.
        if (words_per_block_ < 1 || words_per_block_ > 16 ||
            (words_per_block_ & (words_per_block_ - 1)) != 0) {
            return false;
        }
        if (sigma_ < 2 || sigma_ > 258 || qlevels_ > 5) {
            return false;  // 2 (sentinel+sep) .. 258 (+256 content bytes)
        }
        // qlevels_ must be exactly what Build derives from sigma_ (2 bits/level
        // over ceil(log2(sigma_)) bits). A mismatched value would consume the
        // wrong number of bits per symbol and silently alias distinct ids onto
        // one wavelet path — wrong (but memory-safe) query answers.
        {
            uint32_t bits = 1;
            while ((1u << bits) < sigma_) {
                ++bits;
            }
            if (qlevels_ != (bits + 1) / 2) {
                return false;
            }
        }
        text_len_ = get<uint64_t>(p, end);
        if (text_len_ >
                static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ||
            text_len_ >= std::numeric_limits<size_t>::max()) {
            return false;
        }
        for (int i = 0; i < 256; ++i) {
            byte_to_id_[i] = get<int32_t>(p, end);
            // A byte maps to -1 (absent) or a content id in [2, sigma_). Ids 0
            // (sentinel) and 1 (separator) are never byte ids; anything outside
            // this set would either index c_/first_ out of bounds or alias a
            // query byte onto the separator.
            if (byte_to_id_[i] != -1 &&
                (byte_to_id_[i] < 2 ||
                 byte_to_id_[i] >= static_cast<int32_t>(sigma_))) {
                return false;
            }
        }
        c_.resize(sigma_);
        for (uint32_t i = 0; i < sigma_; ++i) {
            c_[i] = get<uint64_t>(p, end);
        }
        std::vector<std::array<size_t, 4>> starts(qlevels_);
        for (uint32_t l = 0; l < qlevels_; ++l) {
            for (int d = 0; d < 4; ++d) {
                starts[l][d] = static_cast<size_t>(get<uint64_t>(p, end));
            }
        }
        std::vector<uint64_t> qnw(qlevels_);
        for (uint32_t l = 0; l < qlevels_; ++l) {
            qnw[l] = get<uint64_t>(p, end);
        }
        uint64_t sampled_nw = get<uint64_t>(p, end);
        uint64_t n_samples = get<uint64_t>(p, end);
        uint64_t n_docs = get<uint64_t>(p, end);

        const size_t m = static_cast<size_t>(text_len_) + 1;
        // The payload section sizes are fully determined by m; reject any blob
        // whose declared counts don't match (a mutated size would otherwise
        // drive align_view / from_view to read past the mapping).
        const uint64_t exp_qnw =
            m / 32 + (m % 32 != 0);  // 2-bit symbols, 32 per word
        const uint64_t exp_snw =
            m / 64 + (m % 64 != 0);  // 1-bit rows, 64 per word
        for (uint32_t l = 0; l < qlevels_; ++l) {
            if (qnw[l] != exp_qnw) {
                return false;
            }
        }
        const uint64_t expected_samples = text_len_ / sa_sample_rate_ + 1;
        if (sampled_nw != exp_snw || n_samples != expected_samples ||
            n_docs == 0 || n_docs > m) {
            return false;
        }
        auto checked_bytes = [](uint64_t count, size_t width) -> size_t {
            if (count > std::numeric_limits<size_t>::max() / width) {
                throw std::runtime_error("fmindex: payload size overflow");
            }
            return static_cast<size_t>(count) * width;
        };
        auto align_view = [&](size_t nbytes) -> const uint64_t* {
            size_t off =
                static_cast<size_t>(p - reinterpret_cast<const char*>(base));
            size_t pad = (8 - (off & 7u)) & 7u;
            size_t remaining = static_cast<size_t>(end - p);
            if (pad > remaining || nbytes > remaining - pad) {
                throw std::runtime_error("fmindex: truncated payload");
            }
            p += pad;
            const uint64_t* view = reinterpret_cast<const uint64_t*>(p);
            p += nbytes;
            return view;
        };

        std::vector<QuadVector> qvs;
        qvs.reserve(qlevels_);
        for (uint32_t l = 0; l < qlevels_; ++l) {
            const uint64_t* w =
                align_view(checked_bytes(qnw[l], sizeof(uint64_t)));
            qvs.push_back(
                QuadVector::from_view(m, w, qnw[l], words_per_block_));
        }
        // The group-start offsets are fully determined by the quad vectors: the
        // four per-level digit counts always sum to m, so the derived offsets are
        // in [0, m] and keep every map_zero/map2/rank descent in bounds. Reject a
        // blob whose stored `starts` disagree — otherwise a corrupt offset drives
        // the wavelet descent off the QuadVector directory (heap OOB) right here
        // in map_zero, before any query.
        for (uint32_t l = 0; l < qlevels_; ++l) {
            size_t c0 = qvs[l].rank(0, m), c1 = qvs[l].rank(1, m),
                   c2 = qvs[l].rank(2, m);
            std::array<size_t, 4> derived = {0, c0, c0 + c1, c0 + c1 + c2};
            if (starts[l] != derived) {
                return false;
            }
        }
        wm_ = WaveletMatrix4::from_parts(
            m, qlevels_, std::move(qvs), std::move(starts));
        first_.resize(sigma_);
        for (uint32_t c = 0; c < sigma_; ++c) {
            first_[c] = wm_.map_zero(c);
        }
        const uint64_t* sw =
            align_view(checked_bytes(sampled_nw, sizeof(uint64_t)));
        sampled_bv_ = BitVector::from_view(m, sw, sampled_nw);
        // The set-bit count must equal the number of sampled values; otherwise
        // sample_val(sampled_bv_.rank1(row)) could index past the array.
        if (sampled_bv_.count_ones() != n_samples) {
            return false;
        }
        // Sampled-SA values and doc boundaries are VIEWED in place, in whatever
        // width they were stored — no heap copy. At the default sample rate the
        // sample array alone is of the same order as the whole blob; copying it
        // (and rebuilding an equally-large ISA table) made the mmap load far
        // from zero-copy. Validation below reads the views once, allocates
        // nothing.
        const size_t pw = wide_storage_ ? 8 : 4;
        const uint64_t* svp = align_view(checked_bytes(n_samples, pw));
        if (wide_storage_) {
            sv_wide_ = svp;
            sv_narrow_ = nullptr;
        } else {
            sv_narrow_ = reinterpret_cast<const uint32_t*>(svp);
            sv_wide_ = nullptr;
        }
        n_samples_ = static_cast<size_t>(n_samples);
        // Sampled values are text positions in [0, text_len_]; a larger value
        // would drive ensureIsaSample's isa_sample_[val/rate] write out of
        // bounds on a later Extract.
        for (size_t i = 0; i < n_samples_; ++i) {
            if (sample_val(i) > text_len_ ||
                sample_val(i) % sa_sample_rate_ != 0) {
                return false;
            }
        }
        const uint64_t* dsp =
            align_view(checked_bytes(n_docs, sizeof(uint64_t)));
        doc_start_ = dsp;
        n_doc_bounds_ = static_cast<size_t>(n_docs);
        // doc_start_ must be a valid boundary list: start at 0, be strictly
        // increasing (each document adds at least its separator), and end at
        // text_len_. Otherwise docOf's upper_bound could underflow to a bogus
        // document id and read c_/doc_start_ out of bounds during a query.
        if (doc_start_[0] != 0 || doc_start_[n_doc_bounds_ - 1] != text_len_) {
            return false;
        }
        for (size_t i = 1; i < n_doc_bounds_; ++i) {
            if (doc_start_[i] <= doc_start_[i - 1]) {
                return false;
            }
        }
        buildDerived();  // id_to_byte_ only; isa_sample_ is lazy (Extract)
        return true;
    } catch (const std::bad_alloc&) {
        throw;
    } catch (const std::exception&) {
        return false;
    }
}

inline FMIndex
FMIndex::LoadView(const uint8_t* base, size_t size) {
    FMIndex fm;
    if (!fm.parseView(base, size)) {
        return {};
    }
    return fm;
}

inline FMIndex
FMIndex::Deserialize(const std::string& blob) {
    FMIndex fm;
    fm.owned_blob_.assign(blob.begin(), blob.end());
    if (!fm.parseView(fm.owned_blob_.data(), fm.owned_blob_.size())) {
        return {};
    }
    return fm;
}

inline FMIndex
FMIndex::Deserialize(std::vector<uint8_t>&& blob) {
    FMIndex fm;
    fm.owned_blob_ = std::move(blob);
    if (!fm.parseView(fm.owned_blob_.data(), fm.owned_blob_.size())) {
        return {};
    }
    return fm;
}

}  // namespace milvus::index::fmindex
