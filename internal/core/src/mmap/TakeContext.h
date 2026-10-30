// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. Licensed under the Apache License, Version 2.0.
#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include <folly/small_vector.h>

#include "common/EasyAssert.h"

namespace milvus {
class Chunk;
enum class TakePinPolicy;
namespace detail {
template <bool HasCandidateMask, TakePinPolicy PinPolicy>
class RawTakeResult;
}

// One cache-layer accessor may pin several physical cells together. Its
// complete cohort, not just the requested field, must be charged and evicted
// together. All pointers borrow lease; a caller retaining the cohort is safe
// even when the context evicts it from the retention cache.
struct TakeCellCohort {
    std::shared_ptr<void> lease;
    std::vector<std::pair<int64_t, void*>> cells;
    size_t bytes{0};
};

// Backend-private source captured from one immutable column generation.
// Fields in the same physical group use the same identity and cohort, while
// project resolves the field-specific Chunk without repinning it.
// pin, project and estimated_bytes borrow the generation held by identity.
// Invoke them only while this source (or another strong copy of identity)
// remains alive; copying a callback alone does not retain its generation.
// Copying/moving the complete source preserves that ownership contract.
struct TakeCellSource {
    std::shared_ptr<const void> identity;
    std::function<TakeCellCohort(const std::vector<int64_t>&)> pin;
    std::function<Chunk*(void*)> project;
    // A cold metadata hint used only to bound a pin cohort. Zero means
    // unknown: load one cell, then use the actual complete owner size.
    std::function<size_t(int64_t)> estimated_bytes;
};

struct TakeContextOptions {
    size_t retained_bytes{32UL << 20};
};

struct TakeContextStats {
    // Also the conservative same-worker reentrant pin/publication epoch. Keep
    // it enabled and monotonic across Clear(), not optional debug telemetry.
    uint64_t bulk_pins{0};
    uint64_t missing_cells{0};
    uint64_t cache_hits{0};
    uint64_t evicted_cohorts{0};
    size_t retained_bytes{0};
    size_t peak_retained_bytes{0};
};

// One active batch owns each physical cohort once, independently of cache
// retention. This is not predicate truth and has no input-domain limit.
class TakeBatchOwners final {
 public:
    TakeBatchOwners() = default;
    TakeBatchOwners(const TakeBatchOwners&) = delete;
    TakeBatchOwners&
    operator=(const TakeBatchOwners&) = delete;

    void
    Add(const std::shared_ptr<const TakeCellCohort>& cohort) {
        if (cohort == nullptr) {
            ReportNullOwner();
        }
        // Common bounded read prefixes use exact open-addressed membership,
        // rather than rescanning all owners for every row. The table contains
        // no strong references; owners_ remains the lifetime authority.
        // Larger owner sets retain the original unbounded, exact fallback.
        if (owners_.size() >= owner_slots_.size()) {
            if (std::find(owners_.begin(), owners_.end(), cohort) ==
                owners_.end()) {
                owners_.push_back(cohort);
            }
            return;
        }
        const auto hash =
            (static_cast<uint64_t>(reinterpret_cast<uintptr_t>(cohort.get())) >>
             4) *
            uint64_t{0x9e3779b97f4a7c15};
        auto slot = static_cast<size_t>(hash >> 58);
        while (owner_slots_[slot] != nullptr) {
            if (owner_slots_[slot] == cohort.get()) {
                return;
            }
            slot = (slot + 1) & (owner_slots_.size() - 1);
        }
        // Publish membership only after the owning insertion succeeds, so an
        // allocation failure cannot leave a false hit on a later retry.
        owners_.push_back(cohort);
        owner_slots_[slot] = cohort.get();
    }

    void
    Clear() {
        owners_.clear();
        owner_slots_.fill(nullptr);
    }

    size_t
    Size() const {
        return owners_.size();
    }

 private:
    static void
    ReportNullOwner();

    // Reuse the existing inline-storage container; larger batches spill safely.
    folly::small_vector<std::shared_ptr<const TakeCellCohort>, 4> owners_;
    std::array<const TakeCellCohort*, 64> owner_slots_{};
};

// Mutable, worker-private retention state shared by the Expr leaves in that
// worker. It stores source pins, never predicate truth or generated values.
// Its budget bounds retention between batches, not the temporary payload of
// an oversized in-flight batch. Unknown/oversized cohorts are never retained.
class TakeContext final {
 public:
    struct PinnedCell {
        std::shared_ptr<const TakeCellCohort> cohort;
        void* cell{nullptr};
    };

    explicit TakeContext(TakeContextOptions options = {});
    ~TakeContext();

    TakeContext(const TakeContext&) = delete;
    TakeContext&
    operator=(const TakeContext&) = delete;
    TakeContext(TakeContext&&) = delete;
    TakeContext&
    operator=(TakeContext&&) = delete;

    const TakeContextStats&
    Stats() const {
        return stats_;
    }

    size_t
    Budget() const {
        return options_.retained_bytes;
    }

    std::optional<PinnedCell>
    Find(const TakeCellSource& source, int64_t id);

    // The raw pointer borrows owners, which captures each distinct cohort
    // BEFORE any publication/eviction. It survives Clear and nested Takes
    // until the caller ends this batch with owners.Clear().
    std::optional<void*>
    FindBorrowed(const TakeCellSource& source,
                 int64_t id,
                 TakeBatchOwners& owners);

    // General ordered lookup. Duplicate IDs produce distinct output lanes
    // borrowing the same physical cell, and an empty request returns {}.
    std::vector<PinnedCell>
    Acquire(const TakeCellSource& source, const std::vector<int64_t>& ids);

    // A prepared caller has already retained its cache-hit leases and needs
    // only one owner for its unique missing IDs, in the caller's exact order.
    // This checked entry rejects negative/duplicate/already-retained IDs
    // BEFORE pinning or changing retention order/statistics. Empty returns
    // nullptr without work. <=64 validation IDs use stack scratch; larger
    // requests remain supported without an unbounded quadratic dedup scan.
    std::shared_ptr<const TakeCellCohort>
    AcquireUniqueMissing(const TakeCellSource& source,
                         const std::vector<int64_t>& ids);

    void
    Clear();

 private:
    // Raw preparation already proves exact unique misses. It can reuse the
    // same publication core when no metadata callback published meanwhile;
    // all other callers keep the checked public entry above.
    template <bool HasCandidateMask, TakePinPolicy PinPolicy>
    friend class detail::RawTakeResult;

    struct Entry;
    class Directory;

    // A binding-local lookup hint, never a payload owner. Entry addresses
    // survive deque insertion, but not eviction/Clear. Check context + epoch
    // BEFORE dereferencing entry, and capture the cohort for every batch.
    struct BorrowedCellCache {
        const TakeContext* context{nullptr};
        uint64_t epoch{0};
        int64_t id{-1};
        const Entry* entry{nullptr};
        void* cell{nullptr};
    };

    std::optional<void*>
    FindBorrowedCached(const TakeCellSource& source,
                       int64_t id,
                       TakeBatchOwners& owners,
                       BorrowedCellCache& cached);

    std::pair<const Entry*, void*>
    Lookup(const TakeCellSource& source, int64_t id) const;

    // The two public paths validate/plan their input before this shared
    // physical pin and full-owner publication core. IDs must be unique,
    // nonnegative and absent from the retained directory.
    std::shared_ptr<const TakeCellCohort>
    PinAndPublish(const TakeCellSource& source,
                  const std::vector<int64_t>& ids);

    void
    EvictBack();

    TakeContextOptions options_;
    TakeContextStats stats_;
    // Invalidate non-owning Entry hints BEFORE any retained owner is released.
    uint64_t borrow_epoch_{1};
    std::unique_ptr<Directory> directory_;
};
}  // namespace milvus
