// Licensed to the LF AI & Data foundation under Apache-2.0.
// Portions translated from Lance (lance_index::scalar::fmindex), Apache-2.0.
#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include "index/fmindex/BitVector.h"
#include "index/fmindex/WaveletMatrix4.h"

namespace milvus::index::fmindex {

// FM-index over a set of documents. You feed the documents already split; the
// index concatenates them internally and injects a separator symbol after each
// one, so every query is inherently document-scoped (row semantics): no query
// can ever match across a document boundary, because any cross-document
// substring would have to straddle the separator. The separator is a symbol
// OUTSIDE the byte alphabet (dense id 1), NOT the byte '\0' — so every byte
// value 0..255, '\0' included, is ordinary content that is stored and queried
// byte-exactly. Dense alphabet: id 0 = sentinel, id 1 = separator, ids 2..sigma
// = distinct content bytes (ascending order-preserving), so the wavelet matrix
// uses only ceil(log2(sigma)) levels. Exact substring count / locate /
// (doc, offset) via BWT backward search; no false positives, no cross-document
// matches.
class FMIndex {
 public:
    enum class SerializeFileStatus {
        Success,
        OpenFailed,
        WriteFailed,
    };

    FMIndex() = default;

    // Build the index over a set of documents. Document i (0-based, in the order
    // given) is the unit every query result is attributed to and never crosses.
    // The documents are concatenated internally with a separator symbol after
    // each, so results are inherently per-document; you own nothing but the split.
    //
    //   docs            the documents, in order. Contents may be ANY bytes,
    //                   '\0' included — the separator is a symbol outside the
    //                   byte alphabet, so nothing is reserved. A UTF-8 CJK char
    //                   counts as its 3 bytes. The total internal size (sum of
    //                   doc sizes + one separator per doc) must be < 2^31 for the
    //                   compact path, < 2^63 always; for larger corpora, shard
    //                   and build one index per shard.
    //   sa_sample_rate  suffix-array sampling rate R (>= 1): store one SA value
    //                   every R text positions, recovering the rest via LF-
    //                   mapping. Pure space/locate trade-off, zero effect on
    //                   Count or correctness: larger R = smaller index but
    //                   slower Locate/LocateDocs (up to R-1 extra LF steps per
    //                   hit); smaller R = faster Locate, larger sample array.
    //                   Default 32 is balanced; use 4-8 if you Locate often,
    //                   64+ if you only ever Count.
    //   case_insensitive  when true, ASCII letters A-Z are folded to a-z at
    //                   build time (both cases share one symbol), so all queries
    //                   match case-insensitively with zero query-time cost. Only
    //                   ASCII case is folded; non-ASCII / UTF-8 bytes are left
    //                   exact. Extract still returns lowercase for folded letters.
    //   force_wide      normally the suffix array is built with 32-bit indices
    //                   under 2 GiB (less memory) and 64-bit indices above; set
    //                   true to force the 64-bit path regardless (to exercise the
    //                   wide path on small inputs in tests). The index is
    //                   identical either way.
    //   block_bytes     rank-directory block granularity, in bytes; a power-of-
    //                   two multiple of 8 in [8, 128]. One rel_ sample (8 B) is
    //                   kept per block, so the wavelet rank directory — the part
    //                   that stays resident even under an mmap view — is
    //                   ~ (8 / block_bytes) x the packed words. Default 64 (8
    //                   words/block): ~8x smaller directory than the finest
    //                   8-byte block at essentially no rank-throughput cost
    //                   across corpus sizes. Pure space/latency knob, zero
    //                   effect on correctness.
    void
    Build(const std::vector<std::string_view>& docs,
          uint32_t sa_sample_rate = 32,
          bool case_insensitive = false,
          bool force_wide = false,
          uint32_t block_bytes = 64);

    // Half-open SA interval [lo, hi) for pattern; lo==hi means no match.
    std::pair<size_t, size_t>
    BackwardSearch(const uint8_t* pattern, size_t plen) const;

    size_t
    Count(const uint8_t* pattern, size_t plen) const {
        auto r = BackwardSearch(pattern, plen);
        return r.second - r.first;
    }

    // Occurrence counts for many patterns at once. Runs all patterns' backward
    // searches in lock-step (one character each per round) so their rank cache
    // misses overlap (memory-level parallelism) — far higher throughput than
    // calling Count() in a loop. Result[i] corresponds to patterns[i].
    std::vector<size_t>
    CountBatch(
        const std::vector<std::pair<const uint8_t*, size_t>>& patterns) const;

    // Sorted (doc_id, offset_within_doc) of every occurrence. Because documents
    // are separator-delimited internally, no occurrence can span two documents, so
    // every hit is a genuine in-document match. An empty pattern returns no hits
    // (as do MatchingDocs / prefix / suffix / FuzzyMatchingDocs).
    std::vector<std::pair<uint64_t, uint64_t>>
    LocateDocs(const uint8_t* pattern, size_t plen) const;

    // Batched LocateDocs: result[i] is exactly LocateDocs(patterns[i]), but the
    // per-occurrence LF-walks of ALL patterns are run in lock-step tiles so their
    // walk cache-misses overlap (memory-level parallelism) — the locate analog of
    // CountBatch. Wins most when patterns have few hits each (the walk, not the
    // backward search, dominates), e.g. the document-scoped n-gram workload.
    std::vector<std::vector<std::pair<uint64_t, uint64_t>>>
    LocateDocsBatch(
        const std::vector<std::pair<const uint8_t*, size_t>>& patterns) const;

    // Documents that BEGIN with the pattern (anchored prefix match), as sorted,
    // unique document ids. A hit is an occurrence sitting exactly on a document's
    // start boundary.
    std::vector<uint64_t>
    LocatePrefixDocs(const uint8_t* pattern, size_t plen) const;
    // Number of documents that begin with the pattern. O(|pattern|): an
    // occurrence sits on a document start iff its preceding BWT symbol is the
    // sentinel or the separator, so the answer is a single rank difference over
    // the pattern's SA interval — no suffix-array locate at all.
    size_t
    CountPrefixDocs(const uint8_t* pattern, size_t plen) const;

    // Documents that END with the pattern (anchored suffix match), as sorted,
    // unique document ids. A hit is an occurrence whose end (pos + plen) lands
    // exactly on the document's end boundary.
    std::vector<uint64_t>
    LocateSuffixDocs(const uint8_t* pattern, size_t plen) const;
    // Number of documents that end with the pattern. O(|pattern|): the documents
    // ending with P are exactly the occurrences of "P<separator>", so a backward
    // search seeded on the separator interval yields the answer as the width of
    // the resulting SA interval — no suffix-array locate at all.
    size_t
    CountSuffixDocs(const uint8_t* pattern, size_t plen) const;

    // Recover the original bytes of document `doc_id`, T[offset, offset+len)
    // within that document — e.g. to show the context around a match from
    // LocateDocs. Never crosses into another document (clamps to the document's
    // end): returns fewer than len bytes if offset+len exceeds the document, and
    // empty if doc_id is out of range. O(len + sa_sample_rate) LF steps. On a
    // case_insensitive index, ASCII letters come back lowercased.
    std::string
    Extract(uint64_t doc_id, uint64_t offset, size_t len) const;

    // The longest substring of `query` that occurs in the corpus (fuzzy / partial
    // contamination: "how long a span of this benchmark item appears in training",
    // which catches paraphrased or truncated overlaps that an exact n-gram misses).
    // Because the corpus is separator-delimited, the reported span is always
    // contained in a single document — i.e. the longest match across all documents,
    // never one stitched across a document boundary. Returns the match length,
    // its start offset in `query`, and how many times it occurs in the corpus.
    // length == 0 means no single query byte occurs.
    // O(qlen * match_length) — intended for query-sized inputs.
    struct LongestMatchResult {
        size_t length;     // length of the longest matching substring
        size_t query_pos;  // its start offset within `query`
        size_t count;      // number of occurrences in the corpus
    };
    LongestMatchResult
    LongestMatch(const uint8_t* query, size_t qlen) const;

    // Distribution of the byte that FOLLOWS context P: (byte, count) for every
    // byte b such that P·b occurs, sorted by byte. This turns the corpus into an
    // n-gram model — P(next=b | P) = count_b / sum(counts). The separator symbol
    // is not a byte, so an occurrence of P at the end of its document contributes
    // no following byte; the sum can therefore be less than Count(P). O(sigma*plen).
    std::vector<std::pair<uint8_t, size_t>>
    NextTokenCounts(const uint8_t* pattern, size_t plen) const;

    // Sorted, unique document ids that contain `pat` (exact substring) — the
    // doc-granularity result a scalar filter needs (LIKE '%pat%'). Equivalent to
    // the distinct doc ids of LocateDocs.
    std::vector<uint64_t>
    MatchingDocs(const uint8_t* pat, size_t plen) const;

    // Streaming form of MatchingDocs: invoke `visit(doc_id)` for the document of
    // every occurrence of `pat`, with NO per-occurrence materialization — O(1)
    // extra memory instead of MatchingDocs' O(occurrences) temporaries (position
    // array + (doc, offset) array + doc array). A document containing the
    // pattern more than once is visited once per occurrence, in no particular
    // order — callers dedup for free by setting bits in a docs-sized bitmap.
    // This is what a scalar filter should use: a high-frequency pattern (e.g.
    // LIKE '%a%' over repetitive text) makes MatchingDocs allocate GBs while
    // this stays at the caller's single bitmap. An empty pattern visits nothing
    // (as MatchingDocs).
    template <typename Visitor>
    void
    VisitMatchingDocs(const uint8_t* pat, size_t plen, Visitor&& visit) const {
        if (c_.empty() || plen == 0) {
            return;
        }
        auto r = BackwardSearch(pat, plen);
        for (size_t i = r.first; i < r.second; ++i) {
            uint64_t pos = locateRow(i);
            if (pos < text_len_) {  // skip the sentinel suffix
                visit(docOf(pos));
            }
        }
    }

    // Sorted, unique document ids containing a substring within edit distance
    // <= k of `pat` (typo / variant tolerant: names, domains, codes). Also
    // doc-granularity. Implemented by backtracking backward search that never
    // steps through the separator symbol, so a matched substring always lies inside
    // one document. Cost grows fast with k and the alphabet — k is meant to be
    // small (1-2) over short patterns. k == 0 is exactly MatchingDocs.
    std::vector<uint64_t>
    FuzzyMatchingDocs(const uint8_t* pat, size_t plen, uint32_t k) const;

    // --- metadata accessors ---
    size_t
    bwt_size() const {
        return text_len_ + 1;
    }
    uint32_t
    alphabet() const {
        return sigma_;
    }
    uint32_t
    qlevels() const {
        return qlevels_;
    }
    uint64_t
    c_at(uint32_t c) const {
        return c_[c];
    }
    // True if sampled-SA positions are stored 8 bytes wide (corpus >= 4 GiB, or
    // force_wide); false for the compact 4-byte storage.
    bool
    wide() const {
        return wide_storage_;
    }
    bool
    case_insensitive() const {
        return case_fold_;
    }
    // Sampling rate persisted in the blob. Query-side cost decisions must use
    // this value rather than reconstructing it from external index metadata.
    uint32_t
    sa_sample_rate() const {
        return sa_sample_rate_;
    }
    // Number of indexed documents. The serialized boundary array contains one
    // extra terminal offset, so expose the logical count rather than its raw
    // element count. Milvus uses this to cross-check packed-file metadata before
    // allocating row-sized bitmaps.
    size_t
    document_count() const {
        return n_doc_bounds_ == 0 ? 0 : n_doc_bounds_ - 1;
    }
    // Rank-directory block size in bytes (words_per_block * 8). Larger = smaller
    // directory / less resident RAM, no throughput cost up to ~64. Default 64.
    uint32_t
    block_bytes() const {
        return words_per_block_ * 8;
    }
    // Total heap bytes of the wavelet rank directories — the part that stays
    // resident even when the packed words are an mmap view. Scales ~1/block_bytes.
    size_t
    rank_directory_bytes() const {
        size_t t = 0;
        for (const auto& q : wm_.levels_qv()) {
            t += q.directory_bytes();
        }
        return t;
    }
    // False for a default-constructed index or one whose Deserialize/LoadView
    // failed (a failed load yields an empty index that answers 0 to everything);
    // callers loading from disk should check this.
    bool
    valid() const {
        return !c_.empty();
    }

    // --- serialization ---
    // Serialize to a flat, 8-byte-aligned blob (payload arrays aligned so they
    // can be viewed zero-copy from an mmap).
    std::string
    Serialize() const;
    // Stream the serialized bytes straight to a file, without materializing a
    // full-index blob first (only a small header is buffered) — one fewer copy
    // than Serialize()+write on the save path.
    SerializeFileStatus
    SerializeToFile(const std::string& path) const;

    // Copy the blob and load (owns its bytes).
    //
    // Robustness contract (both Deserialize and LoadView): loading NEVER crashes
    // or reads out of bounds on any byte sequence, and a structurally-malformed
    // blob is rejected — the returned index has valid()==false (and answers 0 to
    // everything). This is verified by a byte-flipping fuzz test under ASan.
    // What loading does NOT do: detect corruption of the payload WORD arrays
    // (wavelet / sampled bitmap). A blob whose metadata is self-consistent but
    // whose payload bytes are corrupted can load as valid() and then return
    // wrong answers; guaranteeing byte integrity is the caller's job (e.g. a
    // storage-layer checksum). Queries assume a structurally-valid index.
    static FMIndex
    Deserialize(const std::string& blob);
    // Move overload: take ownership of `blob` as the backing store with NO
    // copy (owned_blob_ = std::move(blob)). Lets a caller that already holds
    // the serialized bytes in a vector<uint8_t> (e.g. a storage reader entry)
    // avoid a second full-index allocation. Same robustness contract.
    static FMIndex
    Deserialize(std::vector<uint8_t>&& blob);
    // Zero-copy load: view serialized bytes at [base, base+size) (must be
    // 8-byte aligned, e.g. mmap). The caller keeps that memory alive for the
    // index's lifetime; the large word arrays are not copied. Same robustness
    // contract as Deserialize.
    static FMIndex
    LoadView(const uint8_t* base, size_t size);

 private:
    size_t
    LF(size_t i) const;
    // Internal SA position of a single BWT row: walk LF until a sampled row, then
    // add the steps. The per-row core shared by locateInternal and the anchored
    // prefix/suffix locators (which locate only the boundary rows, not all
    // occurrences). Caller guarantees the row is a genuine content occurrence.
    uint64_t
    locateRow(size_t row) const;
    // SA positions (internal, separator-injected coordinates) of every
    // occurrence, sorted. Shared by Locate / LocateDocs / prefix / suffix.
    std::vector<uint64_t>
    locateInternal(const uint8_t* pattern, size_t plen) const;
    // Document id whose internal range contains internal position p.
    uint64_t
    docOf(uint64_t internal_pos) const;
    // Half-open SA interval of "pattern<separator>" — the occurrences of the
    // pattern immediately followed by a document separator, i.e. exactly the
    // document-END occurrences. Seeds a backward search on the separator's SA
    // interval and extends it by the pattern. Empty (lo==hi) means no document
    // ends with the pattern. Shared by CountSuffixDocs / LocateSuffixDocs.
    std::pair<size_t, size_t>
    suffixDocInterval(const uint8_t* pattern, size_t plen) const;
    // Rebuild the (small) in-RAM-only structures derived from the serialized
    // fields: id_to_byte_ (from byte_to_id_). Called after Build and after a
    // load. isa_sample_ is NOT built here — see ensureIsaSample().
    void
    buildDerived();
    // Build isa_sample_ on first use (Extract is its only consumer). Thread-safe
    // (std::call_once); costs one O(m) pass + m/rate x 8 B of heap, paid only by
    // workloads that actually Extract.
    void
    ensureIsaSample() const;
    // Fill *this by viewing serialized bytes at base: the wavelet/sampled word
    // arrays, the sampled-SA values AND the doc boundaries are all viewed in
    // place (validated read-only, never copied) — the only heap rebuilt on load
    // is the rank directories. Returns false on a truncated/corrupt/
    // incompatible blob.
    bool
    parseView(const uint8_t* base, size_t size);
    // Append the fixed header (everything before the aligned payload arrays).
    void
    writeHeader(std::string& s) const;

    uint32_t sa_sample_rate_ = 1;
    uint32_t words_per_block_ = 1;  // rank-block granularity (block_bytes / 8)
    bool case_fold_ = false;        // ASCII A-Z folded to a-z at build time
    bool wide_storage_ = false;     // sampled-SA positions stored 8B (>=4GiB)
    uint64_t text_len_ = 0;  // INTERNAL length incl. separators, no sentinel
    uint32_t sigma_ = 2;     // dense alphabet size (incl. sentinel + separator)
    uint32_t qlevels_ = 0;   // ceil(ceil(log2(sigma_)) / 2)
    // byte -> dense content id in [2, sigma), or -1 if the byte is absent from
    // the corpus. Ids 0 (sentinel) and 1 (separator) are never byte ids, so no
    // query byte can address the separator — that is what keeps '\0' queryable
    // as content while cross-document matches remain impossible.
    std::array<int32_t, 256> byte_to_id_{};
    WaveletMatrix4 wm_;        // 4-ary quad matrix over the BWT
    std::vector<uint64_t> c_;  // C-table, size sigma_ (counts up to len)
    std::vector<size_t>
        first_;             // per-symbol map_zero (derived, not serialized)
    BitVector sampled_bv_;  // row is SA-sampled? rank = sample index

    // SA values of sampled rows, in row order — serialized at 4 bytes when
    // len < 2^32 (narrow), else 8 (wide). Build keeps the same width in RAM; a
    // load VIEWS them straight from the (mmap'd or owned) blob with no heap copy
    // or widening pass.
    // Exactly one of sv_wide_ / sv_narrow_ is set on a valid index; all access
    // goes through sample_val(). At the default sample rate these arrays are of
    // the same order as the whole blob, so copying them on load would defeat
    // mmap's memory story.
    std::vector<uint32_t> sample_vals_narrow_owned_;  // Build-mode storage only
    std::vector<uint64_t> sample_vals_wide_owned_;    // Build-mode storage only
    const uint64_t* sv_wide_ = nullptr;
    const uint32_t* sv_narrow_ = nullptr;
    size_t n_samples_ = 0;

    uint64_t
    sample_val(size_t i) const {
        return sv_narrow_ != nullptr ? static_cast<uint64_t>(sv_narrow_[i])
                                     : sv_wide_[i];
    }

    // Internal document boundaries (offsets into the separator-injected buffer),
    // n_doc_bounds_ == n_docs+1 entries: doc_start_[d] = internal start of doc
    // d, doc_start_[n_docs] = text_len_. Doc d's content is [doc_start_[d],
    // doc_start_[d+1]-1) with the separator symbol at doc_start_[d+1]-1.
    // Same ownership scheme as the samples: Build owns doc_bounds_owned_, a
    // load views the 8-byte-aligned array in the blob directly.
    std::vector<uint64_t> doc_bounds_owned_;  // Build-mode storage only
    const uint64_t* doc_start_ = nullptr;
    size_t n_doc_bounds_ = 0;

    int32_t sep_id_ =
        1;  // dense id of the separator symbol (constant; not a byte)
    // Derived, in-RAM only (rebuilt on load, never serialized):
    std::vector<uint8_t>
        id_to_byte_;  // dense id -> byte (inverse of byte_to_id_)
    // isa_sample_[k] = row whose SA value = k*rate. Only Extract needs it, and
    // building it walks ALL m rows (an O(m) pass + m/rate x 8 B of heap) — so it
    // is built LAZILY on the first Extract call (thread-safe via isa_once_),
    // never at load time. Locate/Count/prefix/suffix never touch it.
    mutable std::vector<uint64_t> isa_sample_;
    mutable std::unique_ptr<std::once_flag> isa_once_ =
        std::make_unique<std::once_flag>();
    std::vector<uint8_t>
        owned_blob_;  // backs the views when Deserialized by copy
};

}  // namespace milvus::index::fmindex

// Header-only: the out-of-line member definitions live in FMIndexInl.h, included
// here so that including FMIndex.h alone provides the full implementation. The
// self-include of FMIndex.h inside FMIndexInl.h is a no-op via the include guard.
#include "index/fmindex/FMIndexInl.h"
