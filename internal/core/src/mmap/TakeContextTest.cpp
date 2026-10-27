// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. Licensed under the Apache License, Version 2.0.
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <memory>
#include <stdexcept>
#include <type_traits>
#include <vector>

#include "mmap/TakeContext.h"
#include "mmap/ChunkedColumn.h"
#include "test_utils/OriginalCohereFixture.h"
#include "test_utils/cachinglayer_test_utils.h"

namespace milvus {
namespace {

// A transport-only pin source over unchanged original values. This tests
// retention accounting, not the real cache-layer cost or a nullable dataset.
struct Source {
    struct Lease {
        std::vector<int64_t> values;
    };
    std::shared_ptr<std::vector<int64_t>> original =
        std::make_shared<std::vector<int64_t>>(LoadOriginalCohereIds());
    std::shared_ptr<bool> fail = std::make_shared<bool>(false);

    TakeCellSource
    Get() const {
        return {
            original,
            [data = original, fault = fail](const std::vector<int64_t>& ids) {
                if (*fault) {
                    throw std::runtime_error("injected source read failure");
                }
                auto lease = std::make_shared<Lease>();
                lease->values.reserve(ids.size());
                for (auto id : ids) {
                    lease->values.push_back(data->at(id));
                }
                TakeCellCohort cohort;
                cohort.lease = lease;
                cohort.bytes = lease->values.size() * sizeof(int64_t);
                for (size_t i = 0; i < ids.size(); ++i) {
                    cohort.cells.emplace_back(ids[i], &lease->values[i]);
                }
                return cohort;
            },
            [](void*) -> Chunk* { return nullptr; },
            [](int64_t) { return sizeof(int64_t); }};
    }
};

TEST(TakeContextFoundation, WholeCohortDedupAndBoundedEviction) {
    static_assert(!std::is_copy_constructible_v<TakeContext>);
    static_assert(!std::is_move_constructible_v<TakeContext>);
    Source fixture;
    auto source = fixture.Get();
    TakeContext context({4 * sizeof(int64_t)});
    auto first = context.Acquire(source, {0, 1, 0});
    ASSERT_EQ(first.size(), 3);
    EXPECT_EQ(first[0].cell, first[2].cell);
    EXPECT_EQ(context.Stats().missing_cells, 2);
    EXPECT_EQ(context.Stats().retained_bytes, 2 * sizeof(int64_t));
    auto second = context.Acquire(source, {1, 2});
    EXPECT_EQ(second[0].cell, first[1].cell);
    EXPECT_EQ(context.Stats().bulk_pins, 2);
    auto third = context.Acquire(source, {3, 4});
    EXPECT_EQ(context.Stats().evicted_cohorts, 1);
    EXPECT_EQ(context.Stats().retained_bytes, 3 * sizeof(int64_t));
    EXPECT_FALSE(context.Find(source, 0).has_value());
    EXPECT_FALSE(context.Find(source, 1).has_value());
    // An active consumer, unlike cache retention, still owns an evicted pin.
    EXPECT_EQ(*static_cast<int64_t*>(first[0].cell), (*fixture.original)[0]);
    context.Clear();
    EXPECT_EQ(context.Stats().retained_bytes, 0);
}

TEST(TakeContextFoundation, SharedAndDifferentGenerationIdentity) {
    Source first;
    Source second;
    TakeContext context;
    auto shared_a = first.Get();
    auto shared_b = first.Get();
    auto separate = second.Get();
    auto a = context.Acquire(shared_a, {7, 3});
    auto b = context.Acquire(shared_b, {3, 7});
    EXPECT_EQ(a[0].cell, b[1].cell);
    EXPECT_EQ(context.Stats().bulk_pins, 1);
    auto other = context.Acquire(separate, {7});
    EXPECT_NE(a[0].cohort, other[0].cohort);
    EXPECT_EQ(context.Stats().bulk_pins, 2);
}

TEST(TakeContextFoundation, OversizedUnknownAndDisabledRetention) {
    Source fixture;
    auto source = fixture.Get();
    TakeContext small({sizeof(int64_t)});
    auto active = small.Acquire(source, {0, 1});
    ASSERT_EQ(active.size(), 2);
    EXPECT_EQ(small.Stats().retained_bytes, 0);
    EXPECT_EQ(*static_cast<int64_t*>(active[1].cell), (*fixture.original)[1]);
    auto unknown = source;
    unknown.pin = [pin = source.pin](const std::vector<int64_t>& ids) {
        auto result = pin(ids);
        result.bytes = 0;
        return result;
    };
    auto no_size = small.Acquire(unknown, {2});
    EXPECT_EQ(small.Stats().retained_bytes, 0);
    TakeContext disabled({0});
    auto uncached = disabled.Acquire(source, {3});
    EXPECT_EQ(disabled.Stats().retained_bytes, 0);
}

TEST(TakeContextFoundation, SourceFailureAndMalformedCohortAreNotCached) {
    Source fixture;
    auto source = fixture.Get();
    TakeContext context;
    *fixture.fail = true;
    EXPECT_THROW(context.Acquire(source, {0}), std::runtime_error);
    EXPECT_EQ(context.Stats().retained_bytes, 0);
    *fixture.fail = false;
    auto malformed = source;
    malformed.pin = [pin = source.pin](const std::vector<int64_t>& ids) {
        auto result = pin(ids);
        result.cells.clear();
        return result;
    };
    EXPECT_ANY_THROW(context.Acquire(malformed, {0}));
    EXPECT_EQ(context.Stats().retained_bytes, 0);
    auto good = context.Acquire(source, {0});
    ASSERT_EQ(good.size(), 1);
    EXPECT_EQ(context.Stats().retained_bytes, sizeof(int64_t));
    EXPECT_ANY_THROW(context.Acquire(source, {-1}));
}

TEST(TakeContextFoundation, GeneralAcquirePreservesEmptyOrderAndErrors) {
    Source fixture;
    auto source = fixture.Get();
    size_t pin_calls = 0;
    source.pin = [pin = source.pin, &pin_calls](const auto& ids) {
        ++pin_calls;
        return pin(ids);
    };
    TakeContext context({4 * sizeof(int64_t)});
    EXPECT_TRUE(context.Acquire(source, {}).empty());
    EXPECT_EQ(pin_calls, 0);
    EXPECT_EQ(context.Stats().bulk_pins, 0);
    EXPECT_EQ(context.Stats().retained_bytes, 0);

    const std::vector<int64_t> first_ids{11, 3, 11, 7, 3};
    auto first = context.Acquire(source, first_ids);
    ASSERT_EQ(first.size(), first_ids.size());
    ASSERT_EQ(first[0].cohort->cells.size(), 3);
    EXPECT_EQ(first[0].cohort->cells[0].first, 11);
    EXPECT_EQ(first[0].cohort->cells[1].first, 3);
    EXPECT_EQ(first[0].cohort->cells[2].first, 7);
    EXPECT_EQ(first[0].cell, first[2].cell);
    EXPECT_EQ(first[1].cell, first[4].cell);
    for (size_t i = 0; i < first_ids.size(); ++i) {
        EXPECT_EQ(*static_cast<int64_t*>(first[i].cell),
                  (*fixture.original)[first_ids[i]]);
    }

    const std::vector<int64_t> mixed_ids{3, 9, 11, 9};
    auto mixed = context.Acquire(source, mixed_ids);
    ASSERT_EQ(mixed.size(), mixed_ids.size());
    EXPECT_EQ(mixed[0].cell, first[1].cell);
    EXPECT_EQ(mixed[2].cell, first[0].cell);
    EXPECT_EQ(mixed[1].cell, mixed[3].cell);
    for (size_t i = 0; i < mixed_ids.size(); ++i) {
        EXPECT_EQ(*static_cast<int64_t*>(mixed[i].cell),
                  (*fixture.original)[mixed_ids[i]]);
    }
    EXPECT_EQ(pin_calls, 2);
    EXPECT_EQ(context.Stats().missing_cells, 4);
    EXPECT_ANY_THROW(context.Acquire(source, {-1}));
    EXPECT_EQ(pin_calls, 2);

    *fixture.fail = true;
    EXPECT_THROW(context.Acquire(source, {22, 22}), std::runtime_error);
    EXPECT_EQ(context.Stats().bulk_pins, 2);
    *fixture.fail = false;
    auto recovered = context.Acquire(source, {22, 22});
    ASSERT_EQ(recovered.size(), 2);
    EXPECT_EQ(recovered[0].cell, recovered[1].cell);
    EXPECT_EQ(*static_cast<int64_t*>(recovered[0].cell),
              (*fixture.original)[22]);
}

TEST(TakeContextFoundation, UniqueMissingChecksBeforePinAndRecovers) {
    Source fixture;
    auto source = fixture.Get();
    size_t pin_calls = 0;
    source.pin = [pin = source.pin, &pin_calls](const auto& ids) {
        ++pin_calls;
        return pin(ids);
    };
    TakeContext context({3 * sizeof(int64_t)});
    EXPECT_EQ(context.AcquireUniqueMissing(source, {}), nullptr);
    EXPECT_EQ(pin_calls, 0);
    EXPECT_EQ(context.Stats().bulk_pins, 0);
    auto first = context.AcquireUniqueMissing(source, {1});
    auto second = context.AcquireUniqueMissing(source, {2});
    const auto before = context.Stats();
    EXPECT_ANY_THROW(context.AcquireUniqueMissing(source, {-1}));
    EXPECT_ANY_THROW(context.AcquireUniqueMissing(source, {7, 7}));
    EXPECT_ANY_THROW(context.AcquireUniqueMissing(source, {9, 1}));
    EXPECT_ANY_THROW(context.AcquireUniqueMissing({}, {}));
    EXPECT_EQ(pin_calls, 2);
    EXPECT_EQ(context.Stats().bulk_pins, before.bulk_pins);
    EXPECT_EQ(context.Stats().missing_cells, before.missing_cells);
    EXPECT_EQ(context.Stats().cache_hits, before.cache_hits);
    EXPECT_EQ(context.Stats().evicted_cohorts, before.evicted_cohorts);
    EXPECT_EQ(context.Stats().retained_bytes, before.retained_bytes);
    EXPECT_EQ(context.Stats().peak_retained_bytes, before.peak_retained_bytes);

    auto next = context.AcquireUniqueMissing(source, {4, 3});
    ASSERT_EQ(next->cells.size(), 2);
    EXPECT_EQ(next->cells[0].first, 4);
    EXPECT_EQ(next->cells[1].first, 3);
    // Rejecting cached ID1 above must NOT promote it. This insertion evicts
    // the same oldest cohort as if the invalid requests had never happened.
    EXPECT_FALSE(context.Find(source, 1).has_value());
    EXPECT_TRUE(context.Find(source, 2).has_value());
    EXPECT_EQ(*static_cast<int64_t*>(first->cells[0].second),
              (*fixture.original)[1]);

    *fixture.fail = true;
    EXPECT_THROW(context.AcquireUniqueMissing(source, {5}), std::runtime_error);
    *fixture.fail = false;
    auto recovered = context.AcquireUniqueMissing(source, {5});
    ASSERT_EQ(recovered->cells.size(), 1);
    EXPECT_EQ(*static_cast<int64_t*>(recovered->cells[0].second),
              (*fixture.original)[5]);
    const auto retained = context.Stats().retained_bytes;
    auto malformed = source;
    malformed.pin = [pin = source.pin](const auto& ids) {
        auto result = pin(ids);
        result.cells.clear();
        return result;
    };
    EXPECT_ANY_THROW(context.AcquireUniqueMissing(malformed, {6, 7}));
    EXPECT_EQ(context.Stats().retained_bytes, retained);
    auto after_malformed = context.AcquireUniqueMissing(source, {6, 7});
    ASSERT_EQ(after_malformed->cells.size(), 2);

    // The validation stack threshold must not become a 64-ID domain cap.
    TakeContext large;
    std::vector<int64_t> large_ids;
    for (int64_t id = fixture.original->size(); id > 0; --id) {
        large_ids.push_back(id - 1);
    }
    auto large_cohort = large.AcquireUniqueMissing(source, large_ids);
    ASSERT_EQ(large_cohort->cells.size(), large_ids.size());
    for (size_t i = 0; i < large_ids.size(); ++i) {
        EXPECT_EQ(large_cohort->cells[i].first, large_ids[i]);
        EXPECT_EQ(*static_cast<int64_t*>(large_cohort->cells[i].second),
                  (*fixture.original)[large_ids[i]]);
    }
    TakeContext invalid_large;
    large_ids.back() = large_ids.front();
    const auto calls_before_invalid = pin_calls;
    EXPECT_ANY_THROW(invalid_large.AcquireUniqueMissing(source, large_ids));
    EXPECT_EQ(pin_calls, calls_before_invalid);
    EXPECT_EQ(invalid_large.Stats().retained_bytes, 0);
    large_ids.back() = -1;
    EXPECT_ANY_THROW(invalid_large.AcquireUniqueMissing(source, large_ids));
    EXPECT_EQ(pin_calls, calls_before_invalid);
}

TEST(TakeContextFoundation,
     EmptyDirectorySlotsRemainMissesAfterEvictionAndClear) {
    Source fixture;
    auto source = fixture.Get();
    size_t pin_calls = 0;
    source.pin = [pin = source.pin, &pin_calls](const auto& ids) {
        ++pin_calls;
        return pin(ids);
    };
    TakeContext context({2 * sizeof(int64_t)});
    auto first = context.AcquireUniqueMissing(source, {0, 1});
    auto second = context.AcquireUniqueMissing(source, {2, 3});
    const auto hits = context.Stats().cache_hits;
    EXPECT_FALSE(context.Find(source, 0).has_value());
    EXPECT_FALSE(context.Find(source, 1).has_value());
    EXPECT_EQ(context.Stats().cache_hits, hits);

    auto repinned = context.AcquireUniqueMissing(source, {0});
    EXPECT_NE(repinned.get(), first.get());
    auto mixed = context.Acquire(source, {1, 1, 0});
    ASSERT_EQ(mixed.size(), 3);
    EXPECT_EQ(mixed[0].cell, mixed[1].cell);
    EXPECT_NE(mixed[0].cohort.get(), first.get());
    EXPECT_EQ(mixed[2].cell, repinned->cells[0].second);
    EXPECT_EQ(pin_calls, 4);
    EXPECT_EQ(*static_cast<int64_t*>(mixed[0].cell), (*fixture.original)[1]);
    context.Clear();
    EXPECT_EQ(context.Stats().retained_bytes, 0);
    for (int64_t id = 0; id < 4; ++id) {
        EXPECT_FALSE(context.Find(source, id).has_value());
    }
    // A directory entry is not the lifetime of a consumer's borrowed owner.
    EXPECT_EQ(*static_cast<int64_t*>(first->cells[0].second),
              (*fixture.original)[0]);

    TakeContext sparse({sizeof(int64_t)});
    std::vector<int64_t> order;
    for (size_t i = 0; i < fixture.original->size(); ++i) {
        order.push_back((i * 73 + 11) % fixture.original->size());
    }
    std::shared_ptr<const TakeCellCohort> borrowed_first;
    const auto calls_before = pin_calls;
    for (size_t round = 0; round < 2; ++round) {
        for (size_t i = 0; i < order.size(); ++i) {
            auto cohort = sparse.AcquireUniqueMissing(source, {order[i]});
            if (round == 0 && i == 0) {
                borrowed_first = cohort;
            }
            EXPECT_EQ(*static_cast<int64_t*>(cohort->cells[0].second),
                      (*fixture.original)[order[i]]);
            EXPECT_EQ(sparse.Stats().retained_bytes, sizeof(int64_t));
            if (i > 0 || round > 0) {
                const auto previous =
                    order[(i + order.size() - 1) % order.size()];
                EXPECT_FALSE(sparse.Find(source, previous).has_value());
            }
            // Preserve the former C128 boundary requests as regressions.
            // D bounds directory allocation requests, not an ID/slot count;
            // these are miss/lifetime checks, not bucket-size introspection.
            if (i == 127 || i == 128 || i == 129 || i + 1 == order.size()) {
                EXPECT_EQ(sparse.Stats().missing_cells,
                          round * order.size() + i + 1);
                EXPECT_EQ(sparse.Stats().cache_hits, 0);
            }
        }
    }
    EXPECT_EQ(pin_calls - calls_before, 2 * order.size());
    EXPECT_EQ(*static_cast<int64_t*>(borrowed_first->cells[0].second),
              (*fixture.original)[order[0]]);

    *fixture.fail = true;
    EXPECT_THROW(sparse.AcquireUniqueMissing(source, {order[0]}),
                 std::runtime_error);
    *fixture.fail = false;
    auto recovered = sparse.AcquireUniqueMissing(source, {order[0]});
    EXPECT_EQ(*static_cast<int64_t*>(recovered->cells[0].second),
              (*fixture.original)[order[0]]);
    auto malformed = source;
    malformed.pin = [pin = source.pin](const auto& ids) {
        auto result = pin(ids);
        result.cells.clear();
        return result;
    };
    EXPECT_ANY_THROW(sparse.AcquireUniqueMissing(malformed, {order[1]}));
    auto after_error = sparse.AcquireUniqueMissing(source, {order[1]});
    EXPECT_EQ(*static_cast<int64_t*>(after_error->cells[0].second),
              (*fixture.original)[order[1]]);
    sparse.Clear();
    const auto hits_after_clear = sparse.Stats().cache_hits;
    for (auto id : order) {
        EXPECT_FALSE(sparse.Find(source, id).has_value());
    }
    EXPECT_EQ(sparse.Stats().cache_hits, hits_after_clear);
    EXPECT_EQ(sparse.Stats().retained_bytes, 0);
}

TEST(TakeContextFoundation, EmptyDirectoryDoesNotOwnOrAliasGeneration) {
    // Transport-only lifetime protocol: this fixture's cohort independently
    // owns original values. A real CacheAccessor can itself retain its slot;
    // this does not claim that all real generations may die before their pins.
    alignas(Source) std::array<std::byte, sizeof(Source)> storage;
    auto make_generation = [&] {
        auto* object =
            std::construct_at(reinterpret_cast<Source*>(storage.data()));
        return std::shared_ptr<Source>(object, [](Source* source) {
            std::destroy_at(source);  // Stack storage must never be deleted.
        });
    };
    TakeContext context({sizeof(int64_t)});
    Source other;
    auto first_generation = make_generation();
    auto first_source = first_generation->Get();
    // Get normally identifies its original vector. For this protocol test,
    // make the ACTUAL cache identity be the controlled generation address.
    first_source.identity = first_generation;
    const auto* original_identity = first_source.identity.get();
    const auto original_value = (*first_generation->original)[7];
    std::weak_ptr<Source> first_weak = first_generation;
    auto old_cohort = context.AcquireUniqueMissing(first_source, {7});
    context.AcquireUniqueMissing(other.Get(), {3});
    EXPECT_FALSE(context.Find(first_source, 7).has_value());

    auto complete_source_copy = first_source;
    first_source = {};
    first_generation.reset();
    EXPECT_FALSE(first_weak.expired());
    complete_source_copy = {};
    ASSERT_TRUE(first_weak.expired());
    EXPECT_EQ(*static_cast<int64_t*>(old_cohort->cells[0].second),
              original_value);

    auto next_generation = make_generation();
    auto next_source = next_generation->Get();
    next_source.identity = next_generation;
    ASSERT_EQ(next_source.identity.get(), original_identity);
    const auto next_value = (*next_generation->original)[7];
    size_t new_pin_calls = 0;
    next_source.pin = [pin = next_source.pin, &new_pin_calls](const auto& ids) {
        ++new_pin_calls;
        return pin(ids);
    };
    auto next_cohort = context.AcquireUniqueMissing(next_source, {7});
    ASSERT_EQ(new_pin_calls, 1);
    EXPECT_NE(next_cohort.get(), old_cohort.get());
    EXPECT_NE(next_cohort->cells[0].second, old_cohort->cells[0].second);
    EXPECT_EQ(*static_cast<int64_t*>(next_cohort->cells[0].second), next_value);
    EXPECT_TRUE(context.Find(next_source, 7).has_value());

    std::weak_ptr<Source> next_weak = next_generation;
    next_source = {};
    next_generation.reset();
    EXPECT_FALSE(next_weak.expired());  // The active entry still owns it.
    context.Clear();
    EXPECT_TRUE(next_weak.expired());  // Empty keys must own nothing.
    EXPECT_EQ(*static_cast<int64_t*>(next_cohort->cells[0].second), next_value);
}

TEST(TakeContextFoundation, ReadsPreserveOriginalFifoEvictionOrder) {
    Source fixture;
    auto source = fixture.Get();
    TakeContext context({3 * sizeof(int64_t)});
    for (size_t mode = 0; mode < 3; ++mode) {
        context.Clear();
        context.AcquireUniqueMissing(source, {0});
        context.AcquireUniqueMissing(source, {1});
        TakeBatchOwners owners;
        if (mode == 0) {
            EXPECT_TRUE(context.Find(source, 0).has_value());
        } else if (mode == 1) {
            EXPECT_EQ(context.Acquire(source, {0, 0}).size(), 2);
        } else {
            EXPECT_TRUE(context.FindBorrowed(source, 0, owners).has_value());
        }
        context.AcquireUniqueMissing(source, {2, 3});
        // A read of0 must not promote its cohort ahead of the younger1.
        EXPECT_FALSE(context.Find(source, 0).has_value());
        EXPECT_TRUE(context.Find(source, 1).has_value());
    }
}

TEST(TakeContextFoundation,
     BatchOwnersDeduplicateAcrossNestedPublicationAndClear) {
    Source fixture;
    auto source = fixture.Get();
    TakeContext context({4 * sizeof(int64_t)});
    auto first = context.AcquireUniqueMissing(source, {0, 1, 2, 3});
    const auto before = first.use_count();
    TakeBatchOwners owners;
    auto a = context.FindBorrowed(source, 0, owners);
    auto b = context.FindBorrowed(source, 3, owners);
    ASSERT_TRUE(a.has_value());
    ASSERT_TRUE(b.has_value());
    EXPECT_EQ(owners.Size(), 1);
    EXPECT_EQ(first.use_count(), before + 1);
    std::weak_ptr<const TakeCellCohort> old_weak = first;
    first.reset();
    auto nested = context.AcquireUniqueMissing(source, {4, 5, 6, 7});
    TakeBatchOwners nested_owners;
    auto nested_value = context.FindBorrowed(source, 6, nested_owners);
    ASSERT_TRUE(nested_value.has_value());
    context.Clear();
    EXPECT_FALSE(old_weak.expired());
    EXPECT_EQ(*static_cast<int64_t*>(*a), (*fixture.original)[0]);
    EXPECT_EQ(*static_cast<int64_t*>(*b), (*fixture.original)[3]);
    EXPECT_EQ(*static_cast<int64_t*>(*nested_value), (*fixture.original)[6]);
    owners.Clear();
    EXPECT_TRUE(old_weak.expired());

    // More than four distinct owners must use overflow, never alias or cap.
    TakeContext large({8 * sizeof(int64_t)});
    TakeBatchOwners many;
    std::array<void*, 7> cells;
    std::vector<std::weak_ptr<const TakeCellCohort>> weak;
    for (size_t i = 0; i < cells.size(); ++i) {
        auto cohort = large.AcquireUniqueMissing(source, {int64_t(10 + i)});
        weak.push_back(cohort);
        auto cell = large.FindBorrowed(source, 10 + i, many);
        ASSERT_TRUE(cell.has_value());
        cells[i] = *cell;
        EXPECT_EQ(many.Size(), i + 1);
    }
    large.Clear();
    for (size_t i = 0; i < cells.size(); ++i) {
        EXPECT_FALSE(weak[i].expired());
        EXPECT_EQ(*static_cast<int64_t*>(cells[i]),
                  (*fixture.original)[10 + i]);
    }
    many.Clear();
    for (const auto& cohort : weak) {
        EXPECT_TRUE(cohort.expired());
    }
}

TEST(TakeContextFoundation,
     DirectoryBudgetFallsBackWithoutRepinningSparseCells) {
    Source fixture;
    const auto data = fixture.original;
    constexpr int64_t sparse_base = int64_t{1} << 40;
    std::vector<int64_t> ids;
    for (size_t i = 0; i < data->size(); ++i) {
        ids.push_back(i < 3 ? int64_t(i) : sparse_base + i);
    }
    size_t pin_calls = 0;
    TakeCellSource base;
    base.pin = [data, &pin_calls](const auto& requested) {
        ++pin_calls;
        TakeCellCohort cohort;
        cohort.lease = data;
        cohort.bytes = data->size() * sizeof(int64_t);
        for (auto id : requested) {
            const auto offset = id < 3 ? id : id - sparse_base;
            AssertInfo(offset >= 0 && size_t(offset) < data->size(),
                       "invalid test physical ID");
            cohort.cells.emplace_back(id, &data->at(offset));
        }
        return cohort;
    };
    base.project = [](void*) -> Chunk* { return nullptr; };
    // Identity tags and sparse physical IDs are CONTROL metadata. All32
    // sources access the SAME original1024 values, never inflated rows.
    std::vector<TakeCellSource> sources;
    std::vector<std::shared_ptr<const TakeCellCohort>> cohorts;
    TakeContext context;
    for (size_t i = 0; i < 32; ++i) {
        auto source = base;
        source.identity = std::make_shared<char>();
        sources.push_back(std::move(source));
        cohorts.push_back(context.AcquireUniqueMissing(sources.back(), ids));
    }
    const auto directory = context.DirectoryStats();
    EXPECT_EQ(directory.budget_bytes, 1UL << 20);
    EXPECT_LE(directory.requested_bytes, directory.budget_bytes);
    EXPECT_LE(directory.peak_requested_bytes, directory.budget_bytes);
    ASSERT_FALSE(directory.complete);  // Exercise partial/absent indexing.
    ASSERT_EQ(pin_calls, sources.size());
    for (size_t field = 0; field < sources.size(); ++field) {
        auto result = context.Acquire(sources[field], ids);
        ASSERT_EQ(result.size(), ids.size());
        for (size_t row = 0; row < ids.size(); ++row) {
            EXPECT_EQ(result[row].cohort.get(), cohorts[field].get());
            EXPECT_EQ(*static_cast<int64_t*>(result[row].cell), data->at(row));
        }
    }
    EXPECT_EQ(pin_calls, sources.size());  // Fallback is not a physical miss.
    context.Clear();
    EXPECT_EQ(context.Stats().retained_bytes, 0);
    EXPECT_TRUE(context.DirectoryStats().complete);
    EXPECT_LE(context.DirectoryStats().requested_bytes, 1UL << 20);
    EXPECT_FALSE(context.Find(sources.back(), ids.back()).has_value());
}

TEST(TakeContextFoundation,
     EqualIdentityAddressesWithDifferentOwnersRemainExact) {
    Source fixture;
    auto a = fixture.Get();
    auto b = fixture.Get();
    auto control_a = std::make_shared<char>();
    auto control_b = std::make_shared<char>();
    // Only protocol ownership differs. Values remain original, unchanged.
    a.identity = std::shared_ptr<const void>(control_a, fixture.original.get());
    b.identity = std::shared_ptr<const void>(control_b, fixture.original.get());
    ASSERT_EQ(a.identity.get(), b.identity.get());
    size_t pins = 0;
    a.pin = [pin = a.pin, &pins](const auto& ids) {
        ++pins;
        return pin(ids);
    };
    b.pin = [pin = b.pin, &pins](const auto& ids) {
        ++pins;
        return pin(ids);
    };
    TakeContext context;
    auto a0 = context.AcquireUniqueMissing(a, {0});
    auto b1 = context.AcquireUniqueMissing(b, {1});
    auto a2 = context.AcquireUniqueMissing(a, {2});
    auto b3 = context.AcquireUniqueMissing(b, {3});
    auto find_a0 = context.Find(a, 0);
    auto find_b1 = context.Find(b, 1);
    ASSERT_TRUE(find_a0.has_value());
    ASSERT_TRUE(find_b1.has_value());
    EXPECT_EQ(find_a0->cohort.get(), a0.get());
    EXPECT_EQ(find_b1->cohort.get(), b1.get());
    EXPECT_FALSE(context.Find(a, 1).has_value());
    EXPECT_FALSE(context.Find(b, 0).has_value());
    auto repeated = context.Acquire(a, {0, 2, 0});
    EXPECT_EQ(repeated[0].cohort.get(), a0.get());
    EXPECT_EQ(repeated[1].cohort.get(), a2.get());
    EXPECT_EQ(repeated[2].cell, repeated[0].cell);
    EXPECT_ANY_THROW(context.AcquireUniqueMissing(a, {0}));
    EXPECT_EQ(pins, 4);
}

TEST(TakeContextFoundation, LargeUnorderedDomainDoesNotAliasSlots) {
    Source fixture;
    auto source = fixture.Get();
    TakeContext context;
    std::vector<int64_t> requests;
    for (int64_t i = 1023; i >= 0; --i) {
        requests.push_back(i);
    }
    requests.push_back(11);
    auto values = context.Acquire(source, requests);
    ASSERT_EQ(values.size(), requests.size());
    EXPECT_EQ(context.Stats().missing_cells, 1024);
    for (size_t i = 0; i < requests.size(); ++i) {
        EXPECT_EQ(*static_cast<int64_t*>(values[i].cell),
                  (*fixture.original)[requests[i]]);
    }
    auto repeat = context.Acquire(source, requests);
    EXPECT_EQ(context.Stats().bulk_pins, 1);
    EXPECT_LE(context.Stats().retained_bytes, 32UL << 20);
}

TEST(TakeContextFoundation, ActualRawColumnCrossBatchReuseAndLargeDomain) {
    const auto original = LoadOriginalCohereIds();
    std::vector<std::vector<int64_t>> buffers;
    std::vector<int64_t> rows;
    std::vector<std::unique_ptr<Chunk>> chunks;
    // Split ORIGINAL rows; do not manufacture values or increase source size.
    // More than 32 chunks specifically guards against fixed-slot aliasing.
    for (size_t first = 0; first < original.size(); first += 17) {
        const auto count = std::min<size_t>(17, original.size() - first);
        buffers.emplace_back(original.begin() + first,
                             original.begin() + first + count);
        auto& values = buffers.back();
        rows.push_back(count);
        chunks.push_back(std::make_unique<FixedWidthChunk>(
            count,
            1,
            reinterpret_cast<char*>(values.data()),
            count * sizeof(int64_t),
            sizeof(int64_t),
            false,
            nullptr));
    }
    auto translator = std::make_unique<TestChunkTranslator>(
        rows, "take_context_original_cohere", std::move(chunks));
    auto slot = cachinglayer::Manager::GetInstance().CreateCacheSlot<Chunk>(
        std::move(translator), nullptr);
    FieldMeta field(FieldName("original"),
                    FieldId(100),
                    DataType::INT64,
                    false,
                    std::nullopt);
    auto column = std::make_shared<ChunkedColumn>(std::move(slot), field);
    auto context = std::make_shared<TakeContext>();
    std::vector<int64_t> requests;
    for (size_t i = 0; i < original.size(); ++i) {
        requests.push_back((i * 73 + 11) % original.size());
    }
    requests.push_back(11);
    const auto options = ChunkedColumnInterface::TakeOptions{
        OffsetView::From(requests.data(), requests.size()),
        TargetType::Int64,
        nullptr,
        context};
    for (size_t iteration = 0; iteration < 2; ++iteration) {
        auto take = column->Take(nullptr, options);
        ASSERT_NE(take, nullptr);
        int64_t visited = 0;
        take->VisitViews<int64_t>(
            32,
            [&](const int64_t* values,
                ValidityView validity,
                const bool* skipped,
                int64_t first,
                int64_t count) {
                for (int64_t i = 0; i < count; ++i) {
                    EXPECT_TRUE(validity[i]);
                    EXPECT_FALSE(skipped[i]);
                    EXPECT_EQ(values[i], original[requests[first + i]]);
                }
                visited += count;
            });
        EXPECT_EQ(visited, requests.size());
        EXPECT_EQ(context->Stats().missing_cells, rows.size());
    }
    EXPECT_EQ(context->Stats().retained_bytes,
              original.size() * sizeof(int64_t));
}
}  // namespace
}  // namespace milvus
