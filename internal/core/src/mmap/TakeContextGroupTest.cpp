// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. Licensed under the Apache License, Version 2.0.
#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>
#include <deque>
#include <functional>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "mmap/ChunkedColumnGroup.h"
#include "mmap/ChunkedColumnFilter.h"
#include "mmap/detail/PreparedCellSlots.h"
#include "test_utils/OriginalNqTextFixture.h"
#include "test_utils/cachinglayer_test_utils.h"

namespace milvus {
namespace {

static_assert(!std::is_copy_constructible_v<detail::PreparedCellSlots>);
static_assert(!std::is_copy_assignable_v<detail::PreparedCellSlots>);
static_assert(!std::is_move_constructible_v<detail::PreparedCellSlots>);
static_assert(!std::is_move_assignable_v<detail::PreparedCellSlots>);

// These buffers serialize unchanged original title/text fields into the real
// StringChunk representation. No rows or scalar values are generated. Owning
// the bytes in each Chunk, rather than in the fixture, makes stale borrowed
// views observable when its last cache-layer pin is released and evicted.
class OriginalStringChunk final : public StringChunk {
 public:
    OriginalStringChunk(int32_t count, std::shared_ptr<std::vector<char>> bytes)
        : StringChunk(count,
                      bytes->data(),
                      bytes->size(),
                      false,
                      std::make_shared<ChunkMmapGuard>(nullptr, 0, "")),
          bytes_(std::move(bytes)) {
    }

 private:
    std::shared_ptr<std::vector<char>> bytes_;
};

std::shared_ptr<std::vector<char>>
OriginalStringBytes(const std::vector<OriginalNqTextRow>& rows,
                    size_t first,
                    size_t count,
                    bool title) {
    const auto header = (count + 1) * sizeof(uint32_t);
    size_t size = header;
    for (size_t i = first; i < first + count; ++i) {
        size += (title ? rows[i].title : rows[i].text).size();
    }
    auto bytes = std::make_shared<std::vector<char>>(size);
    uint32_t position = header;
    std::memcpy(bytes->data(), &position, sizeof(position));
    for (size_t lane = 0; lane < count; ++lane) {
        const auto& value =
            title ? rows[first + lane].title : rows[first + lane].text;
        std::memcpy(bytes->data() + position, value.data(), value.size());
        position += value.size();
        std::memcpy(bytes->data() + (lane + 1) * sizeof(position),
                    &position,
                    sizeof(position));
    }
    return bytes;
}

struct OriginalGroupFixture {
    static constexpr int64_t kRowsPerGroup = 16;

    explicit OriginalGroupFixture(const std::string& key,
                                  size_t rows_per_group = kRowsPerGroup)
        : original(LoadOriginalNqTextRows()) {
        if (rows_per_group == 0) {
            throw std::invalid_argument("original group row count is zero");
        }
        std::vector<int64_t> row_counts;
        std::vector<std::unique_ptr<GroupChunk>> chunks;
        for (size_t first = 0; first < original.size();
             first += rows_per_group) {
            const auto count =
                std::min(rows_per_group, original.size() - first);
            auto title = OriginalStringBytes(original, first, count, true);
            auto text = OriginalStringBytes(original, first, count, false);
            title_bytes.emplace_back(title);
            text_bytes.emplace_back(text);
            std::unordered_map<FieldId, std::shared_ptr<Chunk>> fields;
            fields.emplace(FieldId(101),
                           std::make_shared<OriginalStringChunk>(count, title));
            fields.emplace(FieldId(102),
                           std::make_shared<OriginalStringChunk>(count, text));
            auto chunk = std::make_unique<GroupChunk>(std::move(fields));
            group_bytes.push_back(chunk->Size());
            row_counts.push_back(count);
            chunks.push_back(std::move(chunk));
        }
        auto translator = std::make_unique<TestGroupChunkTranslator>(
            2, std::move(row_counts), key, std::move(chunks));
        // Match production's immutable physical-size metadata. Batch
        // preparation must budget complete title+text owners, not one field.
        auto* metadata =
            static_cast<segcore::storagev2translator::GroupCTMeta*>(
                translator->meta());
        metadata->chunk_memory_size_.assign(group_bytes.begin(),
                                            group_bytes.end());
        group = std::make_shared<ChunkedColumnGroup>(std::move(translator));
        title = std::make_shared<ProxyChunkColumn>(
            group,
            FieldId(101),
            FieldMeta(FieldName("original_title"),
                      FieldId(101),
                      DataType::VARCHAR,
                      false,
                      std::nullopt));
        text = std::make_shared<ProxyChunkColumn>(
            group,
            FieldId(102),
            FieldMeta(FieldName("original_text"),
                      FieldId(102),
                      DataType::VARCHAR,
                      false,
                      std::nullopt));
    }

    size_t
    OneGroupBudget() const {
        return *std::max_element(group_bytes.begin(), group_bytes.end());
    }

    std::vector<OriginalNqTextRow> original;
    std::vector<std::weak_ptr<std::vector<char>>> title_bytes;
    std::vector<std::weak_ptr<std::vector<char>>> text_bytes;
    std::vector<size_t> group_bytes;
    std::shared_ptr<ChunkedColumnGroup> group;
    std::shared_ptr<ProxyChunkColumn> title;
    std::shared_ptr<ProxyChunkColumn> text;
};

struct ProjectionControl {
    size_t calls{0};
    bool fail_once{false};
    void* last_backing{nullptr};
    std::function<void()> action;
};

class ObservedProjectionOriginalProxy final : public ProxyChunkColumn {
 public:
    ObservedProjectionOriginalProxy(std::shared_ptr<ChunkedColumnGroup> group,
                                    std::shared_ptr<ProjectionControl> control)
        : ProxyChunkColumn(std::move(group),
                           FieldId(101),
                           FieldMeta(FieldName("original_title"),
                                     FieldId(101),
                                     DataType::VARCHAR,
                                     false,
                                     std::nullopt)),
          control_(std::move(control)) {
    }

    TakeCellSource
    MakeTakeCellSource(milvus::OpContext* context) const override {
        auto source = ProxyChunkColumn::MakeTakeCellSource(context);
        source.project = [project = std::move(source.project),
                          control = control_](void* backing) {
            ++control->calls;
            control->last_backing = backing;
            if (std::exchange(control->fail_once, false)) {
                throw std::runtime_error(
                    "controlled original projection failure");
            }
            if (control->action) {
                auto action = std::move(control->action);
                action();
            }
            return project(backing);
        };
        return source;
    }

 private:
    std::shared_ptr<ProjectionControl> control_;
};

// Fault controls change only transport behavior, never original source data.
class FaultingOriginalProxy final : public ProxyChunkColumn {
 public:
    FaultingOriginalProxy(std::shared_ptr<ChunkedColumnGroup> group,
                          std::shared_ptr<bool> fail)
        : ProxyChunkColumn(std::move(group),
                           FieldId(101),
                           FieldMeta(FieldName("original_title"),
                                     FieldId(101),
                                     DataType::VARCHAR,
                                     false,
                                     std::nullopt)),
          fail_(std::move(fail)) {
    }

    TakeCellSource
    MakeTakeCellSource(milvus::OpContext* context) const override {
        auto source = ProxyChunkColumn::MakeTakeCellSource(context);
        source.pin = [pin = std::move(source.pin),
                      fail = fail_](const std::vector<int64_t>& ids) {
            if (*fail) {
                throw std::runtime_error("controlled take preparation failure");
            }
            return pin(ids);
        };
        return source;
    }

 private:
    std::shared_ptr<bool> fail_;
};

struct MetadataReentryControl {
    enum class Action { AcquireRequestedCell, ClearOnly };
    std::shared_ptr<TakeContext> context;
    int64_t trigger_cell{1};
    Action action{Action::AcquireRequestedCell};
    bool armed{true};
    size_t calls{0};
};

// Reenter only at the backend's cold physical-size metadata callback. The
// original source, data, complete group ownership and source identity remain
// unchanged; this models a legal callback invalidating an earlier miss proof.
class MetadataReentrantOriginalProxy final : public ProxyChunkColumn {
 public:
    MetadataReentrantOriginalProxy(
        std::shared_ptr<ChunkedColumnGroup> group,
        std::shared_ptr<MetadataReentryControl> control)
        : ProxyChunkColumn(std::move(group),
                           FieldId(101),
                           FieldMeta(FieldName("original_title"),
                                     FieldId(101),
                                     DataType::VARCHAR,
                                     false,
                                     std::nullopt)),
          control_(std::move(control)) {
    }

    TakeCellSource
    MakeTakeCellSource(milvus::OpContext* context) const override {
        auto source = ProxyChunkColumn::MakeTakeCellSource(context);
        // Capture the COMPLETE unwrapped source, not a bare callback: it owns
        // the immutable group required by its raw-capture pin/size functions.
        source.estimated_bytes = [original = source,
                                  control = control_](int64_t id) {
            if (control->armed && id == control->trigger_cell) {
                control->armed = false;
                ++control->calls;
                if (control->action ==
                    MetadataReentryControl::Action::AcquireRequestedCell) {
                    auto held = control->context->Acquire(original, {id});
                    if (held.size() != 1) {
                        throw std::runtime_error(
                            "metadata reentry lost its cell");
                    }
                } else {
                    control->context->Clear();
                }
            }
            return original.estimated_bytes ? original.estimated_bytes(id)
                                            : size_t{0};
        };
        return source;
    }

 private:
    std::shared_ptr<MetadataReentryControl> control_;
};

struct PinReentryControl {
    std::shared_ptr<TakeContext> context;
    int64_t inner_id{0};
    bool different_source_epoch{false};
    bool armed{true};
    size_t calls{0};
    std::weak_ptr<void> outer_lease;
    TakeCellSource inner_source;
};

class PinReentrantOriginalProxy final : public ProxyChunkColumn {
 public:
    PinReentrantOriginalProxy(std::shared_ptr<ChunkedColumnGroup> group,
                              std::shared_ptr<PinReentryControl> control)
        : ProxyChunkColumn(std::move(group),
                           FieldId(101),
                           FieldMeta(FieldName("original_title"),
                                     FieldId(101),
                                     DataType::VARCHAR,
                                     false,
                                     std::nullopt)),
          control_(std::move(control)) {
    }

    TakeCellSource
    MakeTakeCellSource(milvus::OpContext* context) const override {
        auto source = ProxyChunkColumn::MakeTakeCellSource(context);
        source.pin = [original = source,
                      control = control_](const std::vector<int64_t>& ids) {
            auto outer = original.pin(ids);
            if (control->armed) {
                control->armed = false;
                ++control->calls;
                control->outer_lease = outer.lease;
                control->inner_source = original;
                if (control->different_source_epoch) {
                    struct EpochOwner {
                        std::shared_ptr<const void> original_generation;
                    };
                    // Distinct control epoch, identical get()/physical bytes.
                    // The new owner still strongly retains the real immutable
                    // group required by the original raw-capture callbacks.
                    auto epoch = std::make_shared<EpochOwner>(
                        EpochOwner{original.identity});
                    control->inner_source.identity =
                        std::shared_ptr<const void>(std::move(epoch),
                                                    original.identity.get());
                }
                auto inner = control->context->Acquire(control->inner_source,
                                                       {control->inner_id});
                if (inner.size() != 1) {
                    throw std::runtime_error("pin reentry lost its inner cell");
                }
            }
            return outer;
        };
        return source;
    }

 private:
    std::shared_ptr<PinReentryControl> control_;
};

// A backend transport control: ordinary Take remains fully supported over
// unchanged original data, but the OPTIONAL reusable capability is absent.
class NoReusableOriginalProxy final : public ProxyChunkColumn {
 public:
    explicit NoReusableOriginalProxy(std::shared_ptr<ChunkedColumnGroup> group)
        : ProxyChunkColumn(std::move(group),
                           FieldId(101),
                           FieldMeta(FieldName("original_title"),
                                     FieldId(101),
                                     DataType::VARCHAR,
                                     false,
                                     std::nullopt)) {
    }

    std::unique_ptr<TakeBinding>
    CreateTakeBinding(milvus::OpContext*, TakeBindingOptions) const override {
        return nullptr;
    }
};

template <typename Offsets>
void
ExpectOriginalTitleViews(const TakeResult& result,
                         const Offsets& offsets,
                         const std::vector<OriginalNqTextRow>& original,
                         int64_t capacity = 65,
                         bool verify_item_access = true) {
    ASSERT_EQ(result.Size(), offsets.size());
    int64_t visited = 0;
    result.VisitViews<std::string_view>(
        capacity,
        [&](const std::string_view* values,
            ValidityView valid,
            const bool* skipped,
            int64_t first,
            int64_t count) {
            EXPECT_EQ(first, visited);
            EXPECT_GT(count, 0);
            EXPECT_LE(first + count, offsets.size());
            for (int64_t lane = 0; lane < count; ++lane) {
                EXPECT_TRUE(valid[lane]);
                EXPECT_FALSE(skipped[lane]);
                EXPECT_EQ(values[lane], original[offsets[first + lane]].title);
            }
            visited += count;
        });
    EXPECT_EQ(visited, offsets.size());
    // Projection-count tests measure only the borrowed batch API. Item access
    // has its own pin/projection path; all existing callers still verify both.
    if (!verify_item_access) {
        return;
    }
    const auto items = result.Access<std::string_view>();
    EXPECT_EQ(items.Size(), offsets.size());
    for (size_t lane = 0; lane < offsets.size(); ++lane) {
        auto item = items[lane];
        EXPECT_TRUE(item.is_valid);
        EXPECT_FALSE(item.data_skipped);
        ASSERT_TRUE(item.value.has_value());
        EXPECT_EQ(*item.value, original[offsets[lane]].title);
    }
}

TEST(TakeContextGroupFoundation, FieldsShareOwnerButGenerationsDoNotAlias) {
    OriginalGroupFixture fixture("take-group-original-nq-shared");
    auto title_source = fixture.title->MakeTakeCellSource(nullptr);
    auto text_source = fixture.text->MakeTakeCellSource(nullptr);
    EXPECT_EQ(title_source.identity, text_source.identity);
    TakeContext context({fixture.OneGroupBudget()});
    auto first = context.Acquire(title_source, {0});
    auto same = context.Acquire(text_source, {0});
    ASSERT_EQ(first.size(), 1);
    ASSERT_EQ(same.size(), 1);
    EXPECT_EQ(first[0].cohort, same[0].cohort);
    EXPECT_EQ(first[0].cell, same[0].cell);
    EXPECT_EQ(context.Stats().bulk_pins, 1);
    EXPECT_EQ(context.Stats().retained_bytes, fixture.group_bytes[0]);
    auto* title =
        static_cast<StringChunk*>(title_source.project(first[0].cell));
    auto* text = static_cast<StringChunk*>(text_source.project(same[0].cell));
    EXPECT_NE(title, text);
    EXPECT_EQ((*title)[0], fixture.original[0].title);
    EXPECT_EQ((*text)[0], fixture.original[0].text);
    EXPECT_EQ(first[0].cohort->bytes, title->Size() + text->Size());

    // Hold an old physical lease while moving the result and walking enough
    // distinct real groups to evict it from the one-group retention budget.
    auto held = std::move(first);
    same.clear();
    for (size_t id = 1; id < fixture.group_bytes.size(); ++id) {
        auto next = context.Acquire(title_source, {static_cast<int64_t>(id)});
        EXPECT_LE(context.Stats().retained_bytes, fixture.OneGroupBudget());
    }
    EXPECT_GT(context.Stats().evicted_cohorts, 0);
    EXPECT_FALSE(context.Find(title_source, 0).has_value());
    fixture.group->ManualEvictCache();
    ASSERT_FALSE(fixture.title_bytes[0].expired());
    ASSERT_FALSE(fixture.text_bytes[0].expired());
    EXPECT_EQ((*title)[0], fixture.original[0].title);
    EXPECT_EQ((*text)[0], fixture.original[0].text);

    OriginalGroupFixture next_generation("take-group-original-nq-next");
    auto other_source = next_generation.title->MakeTakeCellSource(nullptr);
    EXPECT_NE(title_source.identity, other_source.identity);
    const auto before = context.Stats().bulk_pins;
    auto other = context.Acquire(other_source, {0});
    EXPECT_EQ(context.Stats().bulk_pins, before + 1);
    EXPECT_NE(held[0].cohort, other[0].cohort);
    EXPECT_NE(held[0].cell, other[0].cell);
    EXPECT_EQ(
        (*static_cast<StringChunk*>(other_source.project(other[0].cell)))[0],
        fixture.original[0].title);
}

TEST(TakeContextGroupFoundation,
     BorrowedViewsSurviveEvictionUntilConsumerEnds) {
    OriginalGroupFixture fixture("take-group-original-nq-borrowed", 1);
    ASSERT_GT(fixture.group_bytes.size(), 32);
    auto context = std::make_shared<TakeContext>(
        TakeContextOptions{fixture.OneGroupBudget()});
    std::vector<int64_t> offsets;
    for (size_t lane = 0; lane < fixture.original.size(); ++lane) {
        offsets.push_back((lane * 73 + 11) % fixture.original.size());
    }
    offsets.insert(offsets.end(), {0, 73, 128});
    auto take = fixture.title->Take(
        nullptr,
        ChunkedColumnInterface::TakeOptions{
            OffsetView::From(offsets.data(), offsets.size()),
            TargetType::StringView,
            nullptr,
            context});
    ASSERT_NE(take, nullptr);
    auto moved = std::move(take);
    std::vector<std::string> copies;
    copies.reserve(offsets.size());
    moved->VisitViews<std::string_view>(
        offsets.size(),
        [&](const std::string_view* values,
            ValidityView validity,
            const bool* skipped,
            int64_t first,
            int64_t count) {
            ASSERT_EQ(first, copies.size());
            ASSERT_GT(count, 0);
            ASSERT_LE(first + count, offsets.size());
            EXPECT_LE(context->Stats().retained_bytes,
                      fixture.OneGroupBudget());
            if (first + count == offsets.size()) {
                EXPECT_GT(context->Stats().evicted_cohorts, 0);
                context->Clear();
                // The callback may be a short contiguous prefix. Destroy
                // unused cache cells only on the LAST prefix: this fixture's
                // move-only translator cannot reload already-destroyed data.
                // Every CURRENT borrowed title/text owner must remain alive.
                fixture.group->ManualEvictCache();
                for (int64_t lane = 0; lane < count; ++lane) {
                    const auto id = offsets[first + lane];  // One row per cell.
                    ASSERT_FALSE(fixture.title_bytes[id].expired());
                    ASSERT_FALSE(fixture.text_bytes[id].expired());
                }
            }
            for (int64_t lane = 0; lane < count; ++lane) {
                EXPECT_TRUE(validity[lane]);
                EXPECT_FALSE(skipped[lane]);
                EXPECT_EQ(values[lane],
                          fixture.original[offsets[first + lane]].title);
                copies.emplace_back(values[lane]);
            }
        });
    EXPECT_EQ(context->Stats().retained_bytes, 0);
    moved.reset();
    fixture.group->ManualEvictCache();
    for (size_t id = 0; id < fixture.group_bytes.size(); ++id) {
        EXPECT_TRUE(fixture.title_bytes[id].expired());
        EXPECT_TRUE(fixture.text_bytes[id].expired());
    }
    ASSERT_EQ(copies.size(), offsets.size());
    for (size_t lane = 0; lane < offsets.size(); ++lane) {
        EXPECT_EQ(copies[lane], fixture.original[offsets[lane]].title);
    }
}

TEST(TakeContextGroupFoundation,
     PreparedPrefixProtectsHitsAndBudgetsOnlyMissing) {
    OriginalGroupFixture fixture("take-group-original-nq-prepared-abcd");
    ASSERT_GE(fixture.group_bytes.size(), 4);
    const auto first_pair = fixture.group_bytes[0] + fixture.group_bytes[1];
    const auto missing_pair = fixture.group_bytes[2] + fixture.group_bytes[3];
    const auto budget = std::max(first_pair, missing_pair);
    ASSERT_LT(budget, first_pair + missing_pair);
    auto context = std::make_shared<TakeContext>(TakeContextOptions{budget});
    auto source = fixture.title->MakeTakeCellSource(nullptr);
    {
        auto seeded = context->Acquire(source, {0, 1});
        ASSERT_EQ(seeded.size(), 2);
    }
    const auto before = context->Stats();
    const auto rows = OriginalGroupFixture::kRowsPerGroup;
    // A/B are cached; C/D are missing. Values remain the ORIGINAL source
    // fields. Repeated offsets here are request lanes, not duplicated rows.
    const std::vector<int64_t> offsets{
        2 * rows, 0, 3 * rows, rows, 2 * rows, 0};
    auto take = fixture.title->Take(
        nullptr,
        ChunkedColumnInterface::TakeOptions{
            OffsetView::From(offsets.data(), offsets.size()),
            TargetType::StringView,
            nullptr,
            context});
    ASSERT_NE(take, nullptr);
    int64_t visited = 0;
    take->VisitViews<std::string_view>(
        64,
        [&](const std::string_view* values,
            ValidityView valid,
            const bool* skipped,
            int64_t first,
            int64_t count) {
            ASSERT_EQ(first, 0);
            ASSERT_EQ(count, offsets.size());
            // Including A/B in the MISS budget or repinning after publication
            // would split this prefix or add pins. Only C/D may be new misses.
            EXPECT_EQ(context->Stats().missing_cells - before.missing_cells, 2);
            EXPECT_EQ(context->Stats().bulk_pins - before.bulk_pins, 1);
            EXPECT_LE(context->Stats().retained_bytes, budget);
            EXPECT_GT(context->Stats().evicted_cohorts, before.evicted_cohorts);
            context->Clear();
            fixture.group->ManualEvictCache();
            for (size_t id = 0; id < 4; ++id) {
                ASSERT_FALSE(fixture.title_bytes[id].expired());
                ASSERT_FALSE(fixture.text_bytes[id].expired());
            }
            for (int64_t lane = 0; lane < count; ++lane) {
                EXPECT_TRUE(valid[lane]);
                EXPECT_FALSE(skipped[lane]);
                EXPECT_EQ(values[lane],
                          fixture.original[offsets[first + lane]].title);
            }
            visited += count;
        });
    EXPECT_EQ(visited, offsets.size());
    EXPECT_EQ(context->Stats().missing_cells - before.missing_cells, 2);
    EXPECT_EQ(context->Stats().bulk_pins - before.bulk_pins, 1);
    EXPECT_EQ(context->Stats().retained_bytes, 0);
    take.reset();
    fixture.group->ManualEvictCache();
    for (size_t id = 0; id < 4; ++id) {
        EXPECT_TRUE(fixture.title_bytes[id].expired());
        EXPECT_TRUE(fixture.text_bytes[id].expired());
    }
}

TEST(TakeContextGroupFoundation,
     PreparedPrefixRecoversFromPrepareAndConsumerErrors) {
    OriginalGroupFixture fixture("take-group-original-nq-errors");
    auto fail = std::make_shared<bool>(true);
    auto column = std::make_shared<FaultingOriginalProxy>(fixture.group, fail);
    auto context = std::make_shared<TakeContext>(
        TakeContextOptions{fixture.OneGroupBudget()});
    const auto rows = OriginalGroupFixture::kRowsPerGroup;
    const std::vector<int64_t> offsets{0, rows, 2 * rows, 3 * rows, 0};
    auto take =
        column->Take(nullptr,
                     ChunkedColumnInterface::TakeOptions{
                         OffsetView::From(offsets.data(), offsets.size()),
                         TargetType::StringView,
                         nullptr,
                         context});
    ASSERT_NE(take, nullptr);
    int64_t visited = 0;
    auto verify = [&](const std::string_view* values,
                      ValidityView valid,
                      const bool* skipped,
                      int64_t first,
                      int64_t count) {
        EXPECT_EQ(first, visited);
        for (int64_t lane = 0; lane < count; ++lane) {
            EXPECT_TRUE(valid[lane]);
            EXPECT_FALSE(skipped[lane]);
            EXPECT_EQ(values[lane],
                      fixture.original[offsets[first + lane]].title);
        }
        visited += count;
    };
    EXPECT_THROW(take->VisitViews<std::string_view>(64, verify),
                 std::runtime_error);
    EXPECT_EQ(visited, 0);
    EXPECT_EQ(context->Stats().bulk_pins, 0);
    EXPECT_EQ(context->Stats().retained_bytes, 0);
    *fail = false;
    EXPECT_NO_THROW(take->VisitViews<std::string_view>(64, verify));
    EXPECT_EQ(visited, offsets.size());
    EXPECT_THROW(
        take->VisitViews<std::string_view>(
            64,
            [&](const std::string_view* values,
                ValidityView,
                const bool*,
                int64_t first,
                int64_t) {
                EXPECT_EQ(values[0], fixture.original[offsets[first]].title);
                throw std::runtime_error("controlled view consumer failure");
            }),
        std::runtime_error);
    visited = 0;
    EXPECT_NO_THROW(take->VisitViews<std::string_view>(64, verify));
    EXPECT_EQ(visited, offsets.size());
    EXPECT_LE(context->Stats().retained_bytes, fixture.OneGroupBudget());
}

TEST(TakeContextGroupFoundation, ReentrantErrorDoesNotReleaseOuterBorrow) {
    OriginalGroupFixture fixture("take-group-original-nq-reentrant");
    auto context = std::make_shared<TakeContext>(
        TakeContextOptions{fixture.OneGroupBudget()});
    // One original physical group, so no fixture reload is required after
    // evicting the cache while the outer consumer still owns its views.
    const std::vector<int64_t> offsets{0, 1, 2, 3};
    auto take = fixture.title->Take(
        nullptr,
        ChunkedColumnInterface::TakeOptions{
            OffsetView::From(offsets.data(), offsets.size()),
            TargetType::StringView,
            nullptr,
            context});
    ASSERT_NE(take, nullptr);
    bool consumed = false;
    take->VisitViews<std::string_view>(
        64,
        [&](const std::string_view* values,
            ValidityView valid,
            const bool* skipped,
            int64_t first,
            int64_t count) {
            ASSERT_EQ(first, 0);
            ASSERT_EQ(count, offsets.size());
            EXPECT_ANY_THROW(
                take->VisitViews<std::string_view>(64,
                                                   [](const std::string_view*,
                                                      ValidityView,
                                                      const bool*,
                                                      int64_t,
                                                      int64_t) {}));
            context->Clear();
            fixture.group->ManualEvictCache();
            ASSERT_FALSE(fixture.title_bytes[0].expired());
            ASSERT_FALSE(fixture.text_bytes[0].expired());
            for (int64_t lane = 0; lane < count; ++lane) {
                EXPECT_TRUE(valid[lane]);
                EXPECT_FALSE(skipped[lane]);
                EXPECT_EQ(values[lane], fixture.original[offsets[lane]].title);
            }
            consumed = true;
        });
    EXPECT_TRUE(consumed);
    take.reset();
    fixture.group->ManualEvictCache();
    EXPECT_TRUE(fixture.title_bytes[0].expired());
    EXPECT_TRUE(fixture.text_bytes[0].expired());
}

TEST(TakeContextGroupFoundation,
     OriginalViewsKeepLegacyFallbackAndLargeBuffers) {
    OriginalGroupFixture fixture("take-group-original-nq-legacy-bulk");
    std::vector<int64_t> offsets;
    for (size_t lane = 0; lane < fixture.original.size() + 3; ++lane) {
        offsets.push_back((lane * 73 + 11) % fixture.original.size());
    }
    for (const int64_t capacity : {32, 65}) {
        // No TakeContext: the optional bulk hook must keep the original item
        // path. 65 also exercises the existing heap scratch, not a64-ID API.
        auto take = fixture.title->Take(
            nullptr,
            ChunkedColumnInterface::TakeOptions{
                OffsetView::From(offsets.data(), offsets.size()),
                TargetType::StringView,
                nullptr});
        ASSERT_NE(take, nullptr);
        int64_t visited = 0;
        take->VisitViews<std::string_view>(
            capacity,
            [&](const std::string_view* values,
                ValidityView valid,
                const bool* skipped,
                int64_t first,
                int64_t count) {
                EXPECT_EQ(first, visited);
                EXPECT_GT(count, 0);
                EXPECT_LE(count, capacity);
                for (int64_t lane = 0; lane < count; ++lane) {
                    EXPECT_TRUE(valid[lane]);
                    EXPECT_FALSE(skipped[lane]);
                    EXPECT_EQ(values[lane],
                              fixture.original[offsets[first + lane]].title);
                }
                visited += count;
            });
        EXPECT_EQ(visited, offsets.size());
    }
}

TEST(TakeContextGroupFoundation,
     OriginalSkipModesRemainCompatibleWithBulkFill) {
    OriginalGroupFixture fixture("take-group-original-nq-skip-modes");
    const auto largest = std::max_element(fixture.original.begin(),
                                          fixture.original.end(),
                                          [](const auto& a, const auto& b) {
                                              return a.title < b.title;
                                          })
                             ->title;
    std::vector<int64_t> offsets;
    for (size_t lane = 0; lane < 65; ++lane) {
        offsets.push_back((lane * 73 + 11) % fixture.original.size());
    }
    using Filter = detail::ColumnFilter;
    for (const auto mode : {Filter::MetricsSource::PreloadedStatistics,
                            Filter::MetricsSource::LoadedPayload}) {
        auto context = std::make_shared<TakeContext>();
        // An independent original-field bound proves title>max(title) cannot
        // match any real row in a cell. No source values/nulls are fabricated.
        auto filter = std::make_shared<Filter>(mode, [&](int64_t id) {
            const auto first = id * OriginalGroupFixture::kRowsPerGroup;
            const auto last =
                std::min<int64_t>(first + OriginalGroupFixture::kRowsPerGroup,
                                  fixture.original.size());
            return std::all_of(
                fixture.original.begin() + first,
                fixture.original.begin() + last,
                [&](const auto& row) { return row.title <= largest; });
        });
        auto take = fixture.title->Take(
            nullptr,
            ChunkedColumnInterface::TakeOptions{
                OffsetView::From(offsets.data(), offsets.size()),
                TargetType::StringView,
                filter,
                context});
        ASSERT_NE(take, nullptr);
        int64_t visited = 0;
        take->VisitViews<std::string_view>(
            33,
            [&](const std::string_view* values,
                ValidityView valid,
                const bool* skipped,
                int64_t first,
                int64_t count) {
                EXPECT_EQ(first, visited);
                for (int64_t lane = 0; lane < count; ++lane) {
                    EXPECT_TRUE(valid[lane]);
                    EXPECT_TRUE(skipped[lane]);
                    EXPECT_TRUE(values[lane].empty());
                }
                visited += count;
            });
        EXPECT_EQ(visited, offsets.size());
        if (mode == Filter::MetricsSource::PreloadedStatistics) {
            EXPECT_EQ(context->Stats().bulk_pins, 0);
            EXPECT_EQ(context->Stats().missing_cells, 0);
        } else {
            EXPECT_GT(context->Stats().bulk_pins, 0);
            EXPECT_GT(context->Stats().missing_cells, 0);
        }
    }
}

TEST(TakeContextGroupFoundation, NestedDifferentTakeKeepsOuterBorrowedViews) {
    OriginalGroupFixture fixture("take-group-original-nq-nested-owners");
    const auto outer_group = std::max_element(fixture.group_bytes.begin(),
                                              fixture.group_bytes.end()) -
                             fixture.group_bytes.begin();
    const auto inner_group = (outer_group + 1) % fixture.group_bytes.size();
    auto context = std::make_shared<TakeContext>(
        TakeContextOptions{fixture.group_bytes[outer_group]});
    std::vector<int64_t> outer_offsets;
    for (auto lane : {0, 7, 2, 7, 15}) {
        outer_offsets.push_back(
            outer_group * OriginalGroupFixture::kRowsPerGroup + lane);
    }
    auto outer = fixture.title->Take(
        nullptr,
        ChunkedColumnInterface::TakeOptions{
            OffsetView::From(outer_offsets.data(), outer_offsets.size()),
            TargetType::StringView,
            nullptr,
            context});
    ASSERT_NE(outer, nullptr);
    size_t outer_calls = 0;
    outer->VisitViews<std::string_view>(
        32,
        [&](const std::string_view* values,
            ValidityView valid,
            const bool* skipped,
            int64_t first,
            int64_t count) {
            ++outer_calls;
            ASSERT_EQ(first, 0);
            ASSERT_EQ(count, outer_offsets.size());
            std::vector<int64_t> inner_offsets{
                int64_t(inner_group * OriginalGroupFixture::kRowsPerGroup),
                int64_t(inner_group * OriginalGroupFixture::kRowsPerGroup + 3)};
            auto inner =
                fixture.text->Take(nullptr,
                                   ChunkedColumnInterface::TakeOptions{
                                       OffsetView::From(inner_offsets.data(),
                                                        inner_offsets.size()),
                                       TargetType::StringView,
                                       nullptr,
                                       context});
            ASSERT_NE(inner, nullptr);
            inner->VisitViews<std::string_view>(
                32,
                [&](const std::string_view* nested_values,
                    ValidityView nested_valid,
                    const bool* nested_skipped,
                    int64_t nested_first,
                    int64_t nested_count) {
                    EXPECT_EQ(nested_first, 0);
                    EXPECT_EQ(nested_count, inner_offsets.size());
                    EXPECT_GT(context->Stats().evicted_cohorts, 0);
                    context->Clear();
                    fixture.group->ManualEvictCache();
                    ASSERT_FALSE(fixture.title_bytes[outer_group].expired());
                    ASSERT_FALSE(fixture.text_bytes[inner_group].expired());
                    for (int64_t i = 0; i < nested_count; ++i) {
                        EXPECT_TRUE(nested_valid[i]);
                        EXPECT_FALSE(nested_skipped[i]);
                        EXPECT_EQ(nested_values[i],
                                  fixture.original[inner_offsets[i]].text);
                    }
                });
            for (int64_t i = 0; i < count; ++i) {
                EXPECT_TRUE(valid[i]);
                EXPECT_FALSE(skipped[i]);
                EXPECT_EQ(values[i], fixture.original[outer_offsets[i]].title);
            }
        });
    EXPECT_EQ(outer_calls, 1);
    outer.reset();
    fixture.group->ManualEvictCache();
    EXPECT_TRUE(fixture.title_bytes[outer_group].expired());
    EXPECT_TRUE(fixture.text_bytes[inner_group].expired());
    EXPECT_EQ(context->Stats().retained_bytes, 0);
}

TEST(TakeContextGroupFoundation, SourceIdentityOwnsRawCallbackGeneration) {
    OriginalGroupFixture fixture("take-group-original-nq-source-owner");
    auto source = fixture.title->MakeTakeCellSource(nullptr);
    auto copied_source = source;
    auto moved_source = std::move(source);
    source = {};
    std::weak_ptr<ChunkedColumnGroup> weak = fixture.group;
    fixture.title.reset();
    fixture.text.reset();
    fixture.group.reset();
    ASSERT_FALSE(weak.expired());
    TakeContext context;
    auto pinned = context.Acquire(moved_source, {0});
    ASSERT_EQ(pinned.size(), 1);
    auto* chunk =
        static_cast<StringChunk*>(copied_source.project(pinned[0].cell));
    EXPECT_EQ((*chunk)[0], fixture.original[0].title);
    pinned.clear();
    context.Clear();
    moved_source = {};
    ASSERT_FALSE(weak.expired());
    copied_source = {};
    EXPECT_TRUE(weak.expired());
}

TEST(TakeContextGroupFoundation,
     MetadataAcquireReentryRejectsStaleMissProofAndRecovers) {
    OriginalGroupFixture fixture("take-group-original-nq-metadata-acquire");
    auto context = std::make_shared<TakeContext>();
    auto control = std::make_shared<MetadataReentryControl>();
    control->context = context;
    auto column = std::make_shared<MetadataReentrantOriginalProxy>(
        fixture.group, control);
    const auto rows = OriginalGroupFixture::kRowsPerGroup;
    const std::vector<int64_t> offsets{0, rows, 2 * rows, 0, rows};
    auto take =
        column->Take(nullptr,
                     ChunkedColumnInterface::TakeOptions{
                         OffsetView::From(offsets.data(), offsets.size()),
                         TargetType::StringView,
                         nullptr,
                         context});
    ASSERT_NE(take, nullptr);
    int64_t visited = 0;
    auto verify = [&](const std::string_view* values,
                      ValidityView valid,
                      const bool* skipped,
                      int64_t first,
                      int64_t count) {
        EXPECT_EQ(first, visited);
        for (int64_t lane = 0; lane < count; ++lane) {
            EXPECT_TRUE(valid[lane]);
            EXPECT_FALSE(skipped[lane]);
            EXPECT_EQ(values[lane],
                      fixture.original[offsets[first + lane]].title);
        }
        visited += count;
    };
    // The metadata callback pins cell1 AFTER the prefix classified it missing.
    // This must use the public checked path, which rejects now-retained IDs,
    // not publish a duplicate physical pin through the private shortcut.
    EXPECT_ANY_THROW(take->VisitViews<std::string_view>(64, verify));
    EXPECT_EQ(control->calls, 1);
    EXPECT_EQ(visited, 0);
    EXPECT_EQ(context->Stats().bulk_pins, 1);
    EXPECT_EQ(context->Stats().missing_cells, 1);
    auto source = fixture.title->MakeTakeCellSource(nullptr);
    EXPECT_TRUE(context->Find(source, 1).has_value());
    // The same result must reset its failed preparation and remain reusable.
    EXPECT_NO_THROW(take->VisitViews<std::string_view>(64, verify));
    EXPECT_EQ(visited, offsets.size());
    EXPECT_EQ(control->calls, 1);
    EXPECT_EQ(context->Stats().bulk_pins, 2);
    EXPECT_EQ(context->Stats().missing_cells, 3);
}

TEST(TakeContextGroupFoundation,
     MetadataClearReentryKeepsCapturedHitOwnersAndDoesNotRepin) {
    OriginalGroupFixture fixture("take-group-original-nq-metadata-clear");
    auto context = std::make_shared<TakeContext>();
    auto source = fixture.title->MakeTakeCellSource(nullptr);
    {
        auto seeded = context->Acquire(source, {0});
        ASSERT_EQ(seeded.size(), 1);
    }
    const auto before = context->Stats();
    auto control = std::make_shared<MetadataReentryControl>();
    control->context = context;
    control->action = MetadataReentryControl::Action::ClearOnly;
    auto column = std::make_shared<MetadataReentrantOriginalProxy>(
        fixture.group, control);
    const auto rows = OriginalGroupFixture::kRowsPerGroup;
    const std::vector<int64_t> offsets{0, rows, 0, 2 * rows, rows};
    auto take =
        column->Take(nullptr,
                     ChunkedColumnInterface::TakeOptions{
                         OffsetView::From(offsets.data(), offsets.size()),
                         TargetType::StringView,
                         nullptr,
                         context});
    ASSERT_NE(take, nullptr);
    int64_t visited = 0;
    take->VisitViews<std::string_view>(
        64,
        [&](const std::string_view* values,
            ValidityView valid,
            const bool* skipped,
            int64_t first,
            int64_t count) {
            EXPECT_EQ(first, 0);
            ASSERT_EQ(count, offsets.size());
            EXPECT_EQ(control->calls, 1);
            EXPECT_EQ(context->Stats().bulk_pins - before.bulk_pins, 1);
            EXPECT_EQ(context->Stats().missing_cells - before.missing_cells, 2);
            // Metadata Clear removed cached cell0, but its captured batch
            // owner must protect it together with newly pinned cells1/2.
            context->Clear();
            fixture.group->ManualEvictCache();
            for (size_t id = 0; id < 3; ++id) {
                ASSERT_FALSE(fixture.title_bytes[id].expired());
                ASSERT_FALSE(fixture.text_bytes[id].expired());
            }
            for (int64_t lane = 0; lane < count; ++lane) {
                EXPECT_TRUE(valid[lane]);
                EXPECT_FALSE(skipped[lane]);
                EXPECT_EQ(values[lane],
                          fixture.original[offsets[first + lane]].title);
            }
            visited += count;
        });
    EXPECT_EQ(visited, offsets.size());
    EXPECT_EQ(control->calls, 1);
    EXPECT_EQ(context->Stats().bulk_pins - before.bulk_pins, 1);
    EXPECT_EQ(context->Stats().retained_bytes, 0);
    take.reset();
    fixture.group->ManualEvictCache();
    for (size_t id = 0; id < 3; ++id) {
        EXPECT_TRUE(fixture.title_bytes[id].expired());
        EXPECT_TRUE(fixture.text_bytes[id].expired());
    }
}

TEST(TakeContextGroupFoundation,
     PreparedExactKeysPreserveCollidingPhysicalIdsAndDuplicateLanes) {
    // One row per cell: 0 and128 collide in the prepared low-seven-bit table.
    // Fill its entire64-cell prefix without a large corpus or repeated rows.
    OriginalGroupFixture fixture("take-group-original-nq-colliding-ids", 1);
    ASSERT_EQ(fixture.original.size(), 129);
    ASSERT_EQ(fixture.group_bytes.size(), fixture.original.size());
    auto context = std::make_shared<TakeContext>();
    std::vector<int64_t> offsets;
    for (int64_t row = 0; row < 61; ++row) {
        offsets.push_back(row);
    }
    offsets.insert(offsets.end(), {64, 127, 128});
    offsets.insert(offsets.end(), {0, 128, 64, 127, 128, 0, 127});
    auto take = fixture.title->Take(
        nullptr,
        ChunkedColumnInterface::TakeOptions{
            OffsetView::From(offsets.data(), offsets.size()),
            TargetType::StringView,
            nullptr,
            context});
    ASSERT_NE(take, nullptr);
    int64_t visited = 0;
    size_t callbacks = 0;
    take->VisitViews<std::string_view>(
        257,
        [&](const std::string_view* values,
            ValidityView valid,
            const bool* skipped,
            int64_t first,
            int64_t count) {
            EXPECT_EQ(first, visited);
            EXPECT_EQ(count, callbacks == 0 ? 64 : 7);
            for (int64_t lane = 0; lane < count; ++lane) {
                EXPECT_TRUE(valid[lane]);
                EXPECT_FALSE(skipped[lane]);
                EXPECT_EQ(values[lane],
                          fixture.original[offsets[first + lane]].title);
            }
            visited += count;
            ++callbacks;
        });
    EXPECT_EQ(visited, offsets.size());
    EXPECT_EQ(callbacks, 2);
    EXPECT_EQ(context->Stats().bulk_pins, 1);
    EXPECT_EQ(context->Stats().missing_cells, 64);
    EXPECT_LE(context->Stats().retained_bytes, context->Budget());
}

TEST(TakeContextGroupFoundation,
     SourcePinReentryFailsClosedAndPreservesInnerPublication) {
    // Same cell, different cell, and distinct source-control epoch respectively.
    // These are transport controls over the SAME original data, not new rows.
    for (int mode = 0; mode < 3; ++mode) {
        OriginalGroupFixture fixture("take-group-original-nq-pin-reentry-" +
                                     std::to_string(mode));
        auto context = std::make_shared<TakeContext>();
        auto control = std::make_shared<PinReentryControl>();
        control->context = context;
        control->inner_id = mode == 1 ? 1 : 0;
        control->different_source_epoch = mode == 2;
        auto column =
            std::make_shared<PinReentrantOriginalProxy>(fixture.group, control);
        const std::vector<int64_t> offsets{0, 3, 1, 3};
        auto take =
            column->Take(nullptr,
                         ChunkedColumnInterface::TakeOptions{
                             OffsetView::From(offsets.data(), offsets.size()),
                             TargetType::StringView,
                             nullptr,
                             context});
        ASSERT_NE(take, nullptr);
        int64_t visited = 0;
        auto verify = [&](const std::string_view* values,
                          ValidityView valid,
                          const bool* skipped,
                          int64_t first,
                          int64_t count) {
            EXPECT_EQ(first, visited);
            for (int64_t lane = 0; lane < count; ++lane) {
                EXPECT_TRUE(valid[lane]);
                EXPECT_FALSE(skipped[lane]);
                EXPECT_EQ(values[lane],
                          fixture.original[offsets[first + lane]].title);
            }
            visited += count;
        };
        EXPECT_ANY_THROW(take->VisitViews<std::string_view>(64, verify));
        EXPECT_EQ(visited, 0);
        EXPECT_EQ(control->calls, 1);
        // Only the inner publication affects context statistics/retention.
        // The failed outer accessor must not remain hidden in any owner list.
        EXPECT_TRUE(control->outer_lease.expired());
        EXPECT_EQ(context->Stats().bulk_pins, 1);
        EXPECT_EQ(context->Stats().missing_cells, 1);
        EXPECT_EQ(context->Stats().retained_bytes,
                  fixture.group_bytes[control->inner_id]);
        auto inner = context->Find(control->inner_source, control->inner_id);
        ASSERT_TRUE(inner.has_value());
        auto* chunk = static_cast<StringChunk*>(
            control->inner_source.project(inner->cell));
        EXPECT_EQ((*chunk)[0],
                  fixture
                      .original[control->inner_id *
                                OriginalGroupFixture::kRowsPerGroup]
                      .title);
        // The same failed Take is reusable. A same-source/same-id inner pin
        // is a hit; other cases add exactly one normal outer publication.
        EXPECT_NO_THROW(take->VisitViews<std::string_view>(64, verify));
        EXPECT_EQ(visited, offsets.size());
        EXPECT_EQ(control->calls, 1);
        EXPECT_EQ(context->Stats().bulk_pins, mode == 0 ? 1 : 2);
        EXPECT_EQ(context->Stats().missing_cells, mode == 0 ? 1 : 2);
        EXPECT_TRUE(control->outer_lease.expired());
        EXPECT_TRUE(context->Find(control->inner_source, control->inner_id)
                        .has_value());
    }
}

TEST(TakeContextGroupFoundation,
     PreparedCellSlotsPreserveFullInt64KeysAndRejectSixtyFifth) {
    const auto original = LoadOriginalNqTextRows();
    std::vector<size_t> distinct_rows;
    for (size_t row = 0; row < original.size() && distinct_rows.size() < 64;
         ++row) {
        const bool new_value = std::none_of(
            distinct_rows.begin(), distinct_rows.end(), [&](size_t previous) {
                return original[previous].text == original[row].text;
            });
        if (new_value) {
            distinct_rows.push_back(row);
        }
    }
    ASSERT_EQ(distinct_rows.size(), 64);
    std::vector<int64_t> keys;
    for (int64_t index = 0; index < 60; ++index) {
        keys.push_back((int64_t{1} << 40) + index * 128);
    }
    keys.insert(keys.end(),
                {0,
                 std::numeric_limits<int32_t>::max(),
                 int64_t{std::numeric_limits<int32_t>::max()} + 1,
                 std::numeric_limits<int64_t>::max()});
    ASSERT_EQ(keys.size(), detail::PreparedCellSlots::kMaximumCells);
    // Keys are cache-control metadata, NOT scalar IDs or manufactured values.
    // Associate each with one unchanged, naturally distinct original NQ text.
    std::vector<std::string_view> values;
    for (auto row : distinct_rows) {
        values.emplace_back(original[row].text);
    }
    detail::PreparedCellSlots slots;
    const auto initial = slots.Find(keys.front());
    ASSERT_EQ(initial.cell_index, detail::PreparedCellSlots::kEmpty);
    EXPECT_ANY_THROW(slots.Insert(initial, keys.front(), 1));
    EXPECT_EQ(slots.Find(keys.front()).cell_index,
              detail::PreparedCellSlots::kEmpty);
    for (size_t index = 0; index < keys.size(); ++index) {
        const auto match = slots.Find(keys[index]);
        ASSERT_EQ(match.cell_index, detail::PreparedCellSlots::kEmpty);
        slots.Insert(match, keys[index], index);
    }
    auto verify = [&] {
        // Reverse and repeated requests change lookup order only. Every full
        // key must still resolve its own original value after collisions.
        for (size_t lane = 0; lane < 2 * keys.size() + 3; ++lane) {
            const auto expected = keys.size() - 1 - lane % keys.size();
            const auto match = slots.Find(keys[expected]);
            ASSERT_EQ(match.cell_index, expected);
            EXPECT_EQ(values[match.cell_index],
                      original[distinct_rows[expected]].text);
        }
    };
    verify();
    const int64_t sixty_fifth = int64_t{1} << 50;
    const auto missing = slots.Find(sixty_fifth);
    ASSERT_EQ(missing.cell_index, detail::PreparedCellSlots::kEmpty);
    EXPECT_ANY_THROW(slots.Insert(missing, sixty_fifth, 64));
    EXPECT_EQ(slots.Find(sixty_fifth).cell_index,
              detail::PreparedCellSlots::kEmpty);
    EXPECT_ANY_THROW(slots.Insert(slots.Find(keys.front()), keys.front(), 64));
    // Failed insertion must not alter any previously published key/value.
    verify();
}

TEST(TakeContextGroupFoundation,
     RetainedDequeGrowthKeepsDirectoryPointersAndLiveBorrowOwners) {
    constexpr int64_t rows_per_group = 1;
    OriginalGroupFixture fixture("take-group-original-nq-deque-growth",
                                 rows_per_group);
    ASSERT_EQ(fixture.original.size(), 129);
    ASSERT_EQ(fixture.group_bytes.size(), 129);
    size_t budget = 0;
    for (size_t id = 0; id < 64; ++id) {
        budget += fixture.group_bytes[id];
    }
    auto context = std::make_shared<TakeContext>(TakeContextOptions{budget});
    auto source = fixture.title->MakeTakeCellSource(nullptr);
    // Exercise empty Clear before the first retainable publication without
    // adding a debug API for the private lazy owner queue.
    context->Clear();
    EXPECT_EQ(context->Stats().bulk_pins, 0);
    EXPECT_EQ(context->Stats().retained_bytes, 0);

    // Independent chronological FIFO model: production pushes/pops the
    // opposite ends. Only IDs and COMPLETE physical byte costs are modeled;
    // this model never retains any payload/cohort ownership.
    std::deque<std::pair<size_t, size_t>> model;
    std::vector<bool> present(fixture.group_bytes.size(), false);
    size_t model_bytes = 0;
    size_t model_evictions = 0;
    auto model_pin = [&](size_t id) {
        const auto bytes = fixture.group_bytes[id];
        ASSERT_LE(bytes, budget);
        while (model_bytes > budget - bytes) {
            ASSERT_FALSE(model.empty());
            present[model.front().first] = false;
            model_bytes -= model.front().second;
            model.pop_front();
            ++model_evictions;
        }
        model.emplace_back(id, bytes);
        present[id] = true;
        model_bytes += bytes;
    };
    auto model_clear = [&] {
        model.clear();
        std::fill(present.begin(), present.end(), false);
        model_bytes = 0;
    };
    auto check_model = [&] {
        const auto pins_before_find = context->Stats().bulk_pins;
        EXPECT_EQ(context->Stats().retained_bytes, model_bytes);
        EXPECT_EQ(context->Stats().evicted_cohorts, model_evictions);
        for (size_t id = 0; id < present.size(); ++id) {
            auto hit = context->Find(source, id);
            EXPECT_EQ(hit.has_value(), present[id]) << "cell " << id;
            if (hit.has_value()) {
                auto* chunk =
                    static_cast<StringChunk*>(source.project(hit->cell));
                ASSERT_NE(chunk, nullptr);
                EXPECT_EQ((*chunk)[0],
                          fixture.original[id * rows_per_group].title);
            }
        }
        // Directory hits and misses must not become backend repins.
        EXPECT_EQ(context->Stats().bulk_pins, pins_before_find);
    };

    // Sixty-four independent owners cross deque storage-block/map growth.
    // Directory Entry pointers must remain valid across every endpoint push.
    for (size_t id = 0; id < 64; ++id) {
        {
            auto pinned = context->Acquire(source, {static_cast<int64_t>(id)});
            ASSERT_EQ(pinned.size(), 1);
        }
        model_pin(id);
        check_model();
    }
    EXPECT_EQ(context->Stats().bulk_pins, 64);
    EXPECT_EQ(context->Stats().missing_cells, 64);
    EXPECT_EQ(context->Stats().retained_bytes, budget);
    EXPECT_EQ(model.size(), 64);

    std::vector<int64_t> offsets;
    for (int64_t id = 0; id < 64; ++id) {
        offsets.push_back(id * rows_per_group);
    }
    auto take = fixture.title->Take(
        nullptr,
        ChunkedColumnInterface::TakeOptions{
            OffsetView::From(offsets.data(), offsets.size()),
            TargetType::StringView,
            nullptr,
            context});
    ASSERT_NE(take, nullptr);
    size_t callbacks = 0;
    take->VisitViews<std::string_view>(
        64,
        [&](const std::string_view* values,
            ValidityView valid,
            const bool* skipped,
            int64_t first,
            int64_t count) {
            ++callbacks;
            ASSERT_EQ(first, 0);
            ASSERT_EQ(count, 64);
            EXPECT_EQ(context->Stats().bulk_pins, 64);
            for (size_t id = 64; id < 128; ++id) {
                {
                    auto pinned =
                        context->Acquire(source, {static_cast<int64_t>(id)});
                    ASSERT_EQ(pinned.size(), 1);
                }
                model_pin(id);
                check_model();
                EXPECT_EQ(context->Stats().bulk_pins, id + 1);
                for (int64_t lane = 0; lane < count; ++lane) {
                    EXPECT_TRUE(valid[lane]);
                    EXPECT_FALSE(skipped[lane]);
                    EXPECT_EQ(values[lane],
                              fixture.original[offsets[lane]].title);
                }
            }
            EXPECT_GT(model_evictions, 0);
            EXPECT_FALSE(present[0]);

            // Clear invalidates all directory Entry pointers before dropping
            // the queue. The outer views still borrow their captured owners.
            context->Clear();
            model_clear();
            check_model();
            // Reuse the same cleared context while original physical cells
            // are still live. Do not ask the move-only fixture translator to
            // reconstruct data after ManualEvict destroys an unused cell.
            const std::vector<int64_t> fresh_offsets{0, 0, 0};
            {
                auto fresh = fixture.text->Take(
                    nullptr,
                    ChunkedColumnInterface::TakeOptions{
                        OffsetView::From(fresh_offsets.data(),
                                         fresh_offsets.size()),
                        TargetType::StringView,
                        nullptr,
                        context});
                ASSERT_NE(fresh, nullptr);
                size_t fresh_visited = 0;
                fresh->VisitViews<std::string_view>(
                    64,
                    [&](const std::string_view* fresh_values,
                        ValidityView fresh_valid,
                        const bool* fresh_skipped,
                        int64_t fresh_first,
                        int64_t fresh_count) {
                        EXPECT_EQ(fresh_first, 0);
                        ASSERT_EQ(fresh_count, fresh_offsets.size());
                        for (int64_t lane = 0; lane < fresh_count; ++lane) {
                            EXPECT_TRUE(fresh_valid[lane]);
                            EXPECT_FALSE(fresh_skipped[lane]);
                            EXPECT_EQ(
                                fresh_values[lane],
                                fixture.original[fresh_offsets[lane]].text);
                        }
                        fresh_visited += fresh_count;
                    });
                EXPECT_EQ(fresh_visited, fresh_offsets.size());
            }
            model_pin(0);
            check_model();
            EXPECT_EQ(context->Stats().bulk_pins, 129);
            EXPECT_EQ(context->Stats().missing_cells, 129);
            context->Clear();
            model_clear();
            check_model();
            fixture.group->ManualEvictCache();
            for (int64_t lane = 0; lane < count; ++lane) {
                ASSERT_FALSE(fixture.title_bytes[lane].expired());
                ASSERT_FALSE(fixture.text_bytes[lane].expired());
                EXPECT_EQ(values[lane], fixture.original[offsets[lane]].title);
            }
        });
    EXPECT_EQ(callbacks, 1);
    EXPECT_EQ(context->Stats().retained_bytes, 0);
    take.reset();
    fixture.group->ManualEvictCache();
    for (size_t id = 0; id < 128; ++id) {
        EXPECT_TRUE(fixture.title_bytes[id].expired());
        EXPECT_TRUE(fixture.text_bytes[id].expired());
    }
}

TEST(TakeContextGroupFoundation,
     LocationScratchSurvivesShortInputsPartialLocateErrorsAndClear) {
    OriginalGroupFixture fixture("take-group-original-nq-location-scratch");
    auto context = std::make_shared<TakeContext>();
    auto binding = fixture.title->CreateTakeBinding(
        nullptr, TakeBindingOptions{TargetType::StringView, {}, context});
    ASSERT_NE(binding, nullptr);
    for (const size_t count : {64, 1, 33, 2, 257, 32, 65}) {
        for (const auto invalid :
             {-1L, static_cast<int64_t>(fixture.original.size())}) {
            // Only invalid REQUEST metadata changes. No source row/value is
            // replaced. Fail within the first prefix, after some locations
            // have already been written into the persistent scratch.
            std::vector<int64_t> offsets{0, 17, 3, invalid, 99};
            auto lease =
                binding->Bind(OffsetView::From(offsets.data(), offsets.size()));
            size_t consumed = 0;
            EXPECT_ANY_THROW(lease.Result().VisitViews<std::string_view>(
                32,
                [&](const std::string_view*,
                    ValidityView,
                    const bool*,
                    int64_t,
                    int64_t) { ++consumed; }));
            EXPECT_EQ(consumed, 0);
        }
        context->Clear();
        std::vector<int64_t> offsets;
        for (size_t lane = 0; lane < count; ++lane) {
            offsets.push_back((lane * 73 + 11) % fixture.original.size());
        }
        auto lease =
            binding->Bind(OffsetView::From(offsets.data(), offsets.size()));
        ExpectOriginalTitleViews(lease.Result(), offsets, fixture.original, 32);
    }
}

TEST(TakeContextGroupFoundation,
     ReusableBindingDoesNotRebindOrdinaryTakeOrItsAccessor) {
    OriginalGroupFixture fixture("take-group-original-nq-binding-ordinary");
    const std::vector<int64_t> ordinary_offsets{0, 17, 3, 128, 17};
    auto ordinary = fixture.title->Take(
        nullptr,
        ChunkedColumnInterface::TakeOptions{
            OffsetView::From(ordinary_offsets.data(), ordinary_offsets.size()),
            TargetType::StringView,
            nullptr});
    ASSERT_NE(ordinary, nullptr);
    const auto old_accessor = ordinary->Access<std::string_view>();
    auto verify_old = [&] {
        EXPECT_EQ(ordinary->Size(), ordinary_offsets.size());
        EXPECT_EQ(old_accessor.Size(), ordinary_offsets.size());
        for (size_t lane = 0; lane < ordinary_offsets.size(); ++lane) {
            const auto item = old_accessor[lane];
            ASSERT_TRUE(item.value.has_value());
            EXPECT_EQ(*item.value,
                      fixture.original[ordinary_offsets[lane]].title);
        }
    };
    auto context = std::make_shared<TakeContext>();
    auto binding = fixture.title->CreateTakeBinding(
        nullptr, TakeBindingOptions{TargetType::StringView, {}, context});
    ASSERT_NE(binding, nullptr);
    const std::vector<std::vector<int64_t>> inputs{
        {128, 3, 0, 128, 17}, {}, {33}, {65, 1, 65, 0, 64, 17, 99}};
    for (const auto& offsets : inputs) {
        {
            auto lease =
                binding->Bind(OffsetView::From(offsets.data(), offsets.size()));
            ExpectOriginalTitleViews(lease.Result(), offsets, fixture.original);
            verify_old();
        }
        verify_old();
    }
    binding.reset();
    context->Clear();
    verify_old();
}

TEST(TakeContextGroupFoundation,
     ReusableBindingPreservesInputTypesOrderLargeTailsAndLeaseMoves) {
    OriginalGroupFixture fixture("take-group-original-nq-binding-shapes");
    auto context = std::make_shared<TakeContext>();
    auto binding = fixture.title->CreateTakeBinding(
        nullptr, TakeBindingOptions{TargetType::StringView, {}, context});
    ASSERT_NE(binding, nullptr);
    for (bool wide : {false, true}) {
        for (size_t count : {size_t{0},
                             size_t{1},
                             size_t{31},
                             size_t{32},
                             size_t{33},
                             size_t{64},
                             size_t{65},
                             size_t{257},
                             fixture.original.size(),
                             size_t{2}}) {
            std::vector<int32_t> narrow;
            std::vector<int64_t> offsets;
            for (size_t lane = 0; lane < count; ++lane) {
                // Permute REQUESTS only; never duplicate source rows/data.
                offsets.push_back((lane * 73 + 11) % fixture.original.size());
            }
            if (count > 1) {
                offsets.back() = offsets.front();
            }
            narrow.assign(offsets.begin(), offsets.end());
            const auto input = wide ? OffsetView::From(offsets.data(), count)
                                    : OffsetView::From(narrow.data(), count);
            EXPECT_FALSE(binding->HasActiveInput());
            {
                auto lease = binding->Bind(input);
                EXPECT_TRUE(binding->HasActiveInput());
                static_assert(!std::is_copy_constructible_v<decltype(lease)>);
                static_assert(!std::is_copy_assignable_v<decltype(lease)>);
                static_assert(
                    std::is_nothrow_move_constructible_v<decltype(lease)>);
                static_assert(
                    std::is_nothrow_move_assignable_v<decltype(lease)>);
                auto moved = std::move(lease);
                EXPECT_ANY_THROW(lease.Result());
                ExpectOriginalTitleViews(
                    moved.Result(), offsets, fixture.original);
            }
            EXPECT_FALSE(binding->HasActiveInput());
            // Both source buffers may disappear immediately after the lease;
            // the next Bind must not read an old offset span or cached size.
        }
    }
    auto sibling = fixture.title->CreateTakeBinding(
        nullptr, TakeBindingOptions{TargetType::StringView, {}, context});
    ASSERT_NE(sibling, nullptr);
    const std::vector<int64_t> first_offsets{0, 3, 0};
    const std::vector<int64_t> second_offsets{16, 19};
    {
        auto first = binding->Bind(
            OffsetView::From(first_offsets.data(), first_offsets.size()));
        auto second = sibling->Bind(
            OffsetView::From(second_offsets.data(), second_offsets.size()));
        second =
            std::move(first);  // Releases sibling input, takes first lease.
        EXPECT_TRUE(binding->HasActiveInput());
        EXPECT_FALSE(sibling->HasActiveInput());
        EXPECT_ANY_THROW(first.Result());
        ExpectOriginalTitleViews(
            second.Result(), first_offsets, fixture.original);
        auto replacement = sibling->Bind(
            OffsetView::From(second_offsets.data(), second_offsets.size()));
        ExpectOriginalTitleViews(
            replacement.Result(), second_offsets, fixture.original);
        EXPECT_ANY_THROW(binding->Bind(
            OffsetView::From(second_offsets.data(), second_offsets.size())));
    }
    EXPECT_FALSE(binding->HasActiveInput());
    EXPECT_FALSE(sibling->HasActiveInput());
}

TEST(TakeContextGroupFoundation,
     ReusableBindingOwnedDataOutlivesLeaseLaterInputsAndBinding) {
    OriginalGroupFixture fixture("take-group-original-nq-binding-owned");
    const auto small = std::find_if(
        fixture.original.begin(), fixture.original.end(), [](const auto& row) {
            return !row.title.empty() && row.title.size() <= 15;
        });
    const auto large = std::find_if(
        fixture.original.begin(), fixture.original.end(), [](const auto& row) {
            return row.title.size() > 15;
        });
    ASSERT_NE(small, fixture.original.end());
    ASSERT_NE(large, fixture.original.end());
    const auto short_row = small - fixture.original.begin();
    const auto long_row = large - fixture.original.begin();
    const std::vector<int64_t> first_offsets{short_row, long_row, short_row};
    const std::vector<int64_t> second_offsets{
        long_row, short_row, long_row, short_row};
    auto context = std::make_shared<TakeContext>();
    auto binding = fixture.title->CreateTakeBinding(
        nullptr, TakeBindingOptions{TargetType::StringView, {}, context});
    ASSERT_NE(binding, nullptr);
    ChunkedColumnInterface::OwnedTakeData first_owned, second_owned;
    auto verify_owned = [&](const auto& owned, const auto& offsets) {
        ASSERT_NE(owned.owner, nullptr);
        ASSERT_EQ(owned.size, offsets.size());
        const auto* values = owned.values.template data_as<std::string_view>();
        for (size_t lane = 0; lane < offsets.size(); ++lane) {
            EXPECT_EQ(values[lane], fixture.original[offsets[lane]].title);
        }
    };
    {
        auto lease = binding->Bind(
            OffsetView::From(first_offsets.data(), first_offsets.size()));
        first_owned = lease.Result().GetOwn();
        verify_owned(first_owned, first_offsets);
    }
    {
        auto lease = binding->Bind(
            OffsetView::From(second_offsets.data(), second_offsets.size()));
        EXPECT_FALSE(lease.Result().IsOwned());
        second_owned = lease.Result().GetOwn();
        verify_owned(first_owned, first_offsets);
        verify_owned(second_owned, second_offsets);
    }
    binding.reset();
    context->Clear();
    fixture.group->ManualEvictCache();
    // Copies include naturally short/long ORIGINAL strings. Escaping owned
    // values must not depend on any physical pin or the reusable object.
    verify_owned(first_owned, first_offsets);
    verify_owned(second_owned, second_offsets);
    auto moved = std::move(first_owned);
    verify_owned(moved, first_offsets);
    for (auto row : first_offsets) {
        const auto id = row / OriginalGroupFixture::kRowsPerGroup;
        EXPECT_TRUE(fixture.title_bytes[id].expired());
        EXPECT_TRUE(fixture.text_bytes[id].expired());
    }
}

TEST(TakeContextGroupFoundation,
     ReusableBindingBusyAndConsumerErrorsPreserveOuterInput) {
    OriginalGroupFixture fixture("take-group-original-nq-binding-busy");
    auto context = std::make_shared<TakeContext>();
    auto binding = fixture.title->CreateTakeBinding(
        nullptr, TakeBindingOptions{TargetType::StringView, {}, context});
    ASSERT_NE(binding, nullptr);
    const std::vector<int64_t> outer_offsets{0, 3, 1, 3};
    const std::vector<int64_t> inner_offsets{16, 18, 16};
    const auto inner_input =
        OffsetView::From(inner_offsets.data(), inner_offsets.size());
    EXPECT_FALSE(binding->HasActiveInput());
    {
        auto lease = binding->Bind(
            OffsetView::From(outer_offsets.data(), outer_offsets.size()));
        EXPECT_TRUE(binding->HasActiveInput());
        const auto old_accessor = lease.Result().Access<std::string_view>();
        EXPECT_ANY_THROW(binding->Bind(inner_input));
        bool consumed = false;
        EXPECT_THROW(
            lease.Result().VisitViews<std::string_view>(
                64,
                [&](const std::string_view* values,
                    ValidityView,
                    const bool*,
                    int64_t first,
                    int64_t count) {
                    EXPECT_EQ(first, 0);
                    EXPECT_EQ(count, outer_offsets.size());
                    EXPECT_ANY_THROW(binding->Bind(inner_input));
                    context->Clear();
                    for (int64_t lane = 0; lane < count; ++lane) {
                        EXPECT_EQ(values[lane],
                                  fixture.original[outer_offsets[lane]].title);
                    }
                    consumed = true;
                    throw std::runtime_error(
                        "controlled reusable consumer error");
                }),
            std::runtime_error);
        EXPECT_TRUE(consumed);
        // Prefix cleanup after the thrown consumer is not EndInput. The
        // still-live lease keeps its original input and remains busy.
        EXPECT_ANY_THROW(binding->Bind(inner_input));
        EXPECT_TRUE(binding->HasActiveInput());
        EXPECT_EQ(old_accessor.Size(), outer_offsets.size());
        const auto old = old_accessor[0];
        ASSERT_TRUE(old.value.has_value());
        EXPECT_EQ(*old.value, fixture.original[outer_offsets[0]].title);
        ExpectOriginalTitleViews(
            lease.Result(), outer_offsets, fixture.original);
    }
    EXPECT_FALSE(binding->HasActiveInput());
    EXPECT_ANY_THROW(
        binding->Bind(OffsetView::From(outer_offsets.data(), int64_t{-1})));
    EXPECT_ANY_THROW(binding->Bind(
        OffsetView::From(static_cast<const int64_t*>(nullptr), int64_t{1})));
    EXPECT_FALSE(binding->HasActiveInput());
    {
        auto lease = binding->Bind(inner_input);
        ExpectOriginalTitleViews(
            lease.Result(), inner_offsets, fixture.original);
    }
}

TEST(TakeContextGroupFoundation,
     ReusableBindingSourceErrorsFailClosedAndRecoverOnNextLease) {
    // Controlled transport failures cover pin throwing, metadata publishing,
    // then pin publishing same ID/different ID/different source epoch.
    for (int mode = 0; mode < 5; ++mode) {
        OriginalGroupFixture fixture("take-group-original-nq-binding-source-" +
                                     std::to_string(mode));
        auto context = std::make_shared<TakeContext>();
        auto fail = std::make_shared<bool>(true);
        std::shared_ptr<ProxyChunkColumn> column;
        std::shared_ptr<MetadataReentryControl> metadata;
        std::shared_ptr<PinReentryControl> pin;
        if (mode == 0) {
            column =
                std::make_shared<FaultingOriginalProxy>(fixture.group, fail);
        } else if (mode == 1) {
            metadata = std::make_shared<MetadataReentryControl>();
            metadata->context = context;
            column = std::make_shared<MetadataReentrantOriginalProxy>(
                fixture.group, metadata);
        } else {
            pin = std::make_shared<PinReentryControl>();
            pin->context = context;
            pin->inner_id = mode == 3 ? 1 : 0;
            pin->different_source_epoch = mode == 4;
            column =
                std::make_shared<PinReentrantOriginalProxy>(fixture.group, pin);
        }
        auto binding = column->CreateTakeBinding(
            nullptr, TakeBindingOptions{TargetType::StringView, {}, context});
        ASSERT_NE(binding, nullptr);
        const auto rows = OriginalGroupFixture::kRowsPerGroup;
        const std::vector<int64_t> offsets{0, rows, 2 * rows, 0, rows};
        size_t consumed = 0;
        {
            auto lease =
                binding->Bind(OffsetView::From(offsets.data(), offsets.size()));
            EXPECT_ANY_THROW(lease.Result().VisitViews<std::string_view>(
                64,
                [&](const std::string_view*,
                    ValidityView,
                    const bool*,
                    int64_t,
                    int64_t count) { consumed += count; }));
            EXPECT_EQ(consumed, 0);
        }
        EXPECT_EQ(context->Stats().bulk_pins, mode == 0 ? 0 : 1);
        EXPECT_EQ(context->Stats().missing_cells, mode == 0 ? 0 : 1);
        if (metadata) {
            EXPECT_EQ(metadata->calls, 1);
        }
        if (pin) {
            EXPECT_EQ(pin->calls, 1);
            EXPECT_TRUE(pin->outer_lease.expired());
            EXPECT_TRUE(
                context->Find(pin->inner_source, pin->inner_id).has_value());
        }
        *fail = false;
        {
            auto lease =
                binding->Bind(OffsetView::From(offsets.data(), offsets.size()));
            ExpectOriginalTitleViews(lease.Result(), offsets, fixture.original);
        }
        EXPECT_EQ(context->Stats().bulk_pins, mode == 0 ? 1 : 2);
        EXPECT_LE(context->Stats().retained_bytes, context->Budget());
    }
}

TEST(TakeContextGroupFoundation,
     UnsupportedReusableCapabilityKeepsFreshTakeFallback) {
    OriginalGroupFixture fixture("take-group-original-nq-binding-unsupported");
    auto column = std::make_shared<NoReusableOriginalProxy>(fixture.group);
    auto context = std::make_shared<TakeContext>();
    auto binding = column->CreateTakeBinding(
        nullptr, TakeBindingOptions{TargetType::StringView, {}, context});
    EXPECT_EQ(binding, nullptr);
    const std::vector<std::vector<int64_t>> inputs{
        {0, 17, 3, 128, 17}, {65}, {128, 0, 64, 128}};
    for (const auto& offsets : inputs) {
        auto fresh =
            column->Take(nullptr,
                         ChunkedColumnInterface::TakeOptions{
                             OffsetView::From(offsets.data(), offsets.size()),
                             TargetType::StringView,
                             nullptr,
                             context});
        ASSERT_NE(fresh, nullptr);
        ExpectOriginalTitleViews(*fresh, offsets, fixture.original);
    }
    EXPECT_GT(context->Stats().bulk_pins, 0);
    // This is the generic unsupported-capability contract, not a claim of
    // physical Vortex coverage or a new backend implementation.
}

TEST(TakeContextGroupFoundation,
     ReusableProjectionCachesMetadataWithoutRetainingPayload) {
    OriginalGroupFixture fixture("take-group-original-nq-projection-reuse");
    auto context = std::make_shared<TakeContext>();
    auto control = std::make_shared<ProjectionControl>();
    auto column = std::make_shared<ObservedProjectionOriginalProxy>(
        fixture.group, control);
    auto source = column->MakeTakeCellSource(nullptr);
    std::vector<int64_t> cells, offsets;
    for (size_t id = 0; id < fixture.group_bytes.size(); ++id) {
        cells.push_back(id);
        offsets.push_back(id * OriginalGroupFixture::kRowsPerGroup);
        offsets.push_back(
            std::min<int64_t>(offsets.back() + 1, fixture.original.size() - 1));
    }
    {
        auto warm = context->Acquire(source, cells);
        ASSERT_EQ(warm.size(), cells.size());
    }
    auto binding = column->CreateTakeBinding(
        nullptr, TakeBindingOptions{TargetType::StringView, {}, context});
    ASSERT_NE(binding, nullptr);
    for (size_t pass = 0; pass < 3; ++pass) {
        {
            auto lease =
                binding->Bind(OffsetView::From(offsets.data(), offsets.size()));
            ExpectOriginalTitleViews(
                lease.Result(), offsets, fixture.original, 65, false);
        }
        EXPECT_EQ(control->calls, cells.size());
        std::reverse(offsets.begin(), offsets.end());
    }
    EXPECT_EQ(context->Stats().bulk_pins, 1);
    binding.reset();
    context->Clear();
    fixture.group->ManualEvictCache();
    for (size_t id = 0; id < cells.size(); ++id) {
        EXPECT_TRUE(fixture.title_bytes[id].expired());
        EXPECT_TRUE(fixture.text_bytes[id].expired());
    }
    TakeBatchOwners owners;
    EXPECT_ANY_THROW(owners.Add(nullptr));
    EXPECT_EQ(owners.Size(), 0);
}

TEST(TakeContextGroupFoundation,
     ReusableProjectionChecksCompleteIdsAcrossSlotCollisions) {
    constexpr int64_t rows_per_group = 1;
    OriginalGroupFixture fixture("take-group-original-nq-projection-collision",
                                 rows_per_group);
    auto context = std::make_shared<TakeContext>();
    auto control = std::make_shared<ProjectionControl>();
    auto column = std::make_shared<ObservedProjectionOriginalProxy>(
        fixture.group, control);
    std::vector<int64_t> cells;
    for (size_t id = 0; id < fixture.group_bytes.size(); ++id) {
        cells.push_back(id);
    }
    {
        auto warm =
            context->Acquire(column->MakeTakeCellSource(nullptr), cells);
        ASSERT_EQ(warm.size(), 129);
    }
    auto binding = column->CreateTakeBinding(
        nullptr, TakeBindingOptions{TargetType::StringView, {}, context});
    ASSERT_NE(binding, nullptr);
    const std::vector<int64_t> ids{0, 64, 0, 64, 127, 63, 127, 63};
    for (auto id : ids) {
        const std::vector<int64_t> offsets{id * rows_per_group};
        auto lease =
            binding->Bind(OffsetView::From(offsets.data(), offsets.size()));
        ExpectOriginalTitleViews(
            lease.Result(), offsets, fixture.original, 65, false);
    }
    EXPECT_EQ(control->calls, ids.size());
    EXPECT_EQ(context->Stats().bulk_pins, 1);
}

TEST(TakeContextGroupFoundation,
     ReusableProjectionInvalidatesAfterClearAndReentrantPublication) {
    OriginalGroupFixture fixture("take-group-original-nq-projection-epoch");
    auto context = std::make_shared<TakeContext>();
    auto control = std::make_shared<ProjectionControl>();
    auto column = std::make_shared<ObservedProjectionOriginalProxy>(
        fixture.group, control);
    auto binding = column->CreateTakeBinding(
        nullptr, TakeBindingOptions{TargetType::StringView, {}, context});
    ASSERT_NE(binding, nullptr);
    const std::vector<int64_t> offsets{0, 3, 0};
    auto eval = [&] {
        auto lease =
            binding->Bind(OffsetView::From(offsets.data(), offsets.size()));
        ExpectOriginalTitleViews(
            lease.Result(), offsets, fixture.original, 65, false);
    };
    eval();
    EXPECT_EQ(control->calls, 1);
    auto* same_backing = control->last_backing;
    context->Clear();
    eval();
    EXPECT_EQ(control->calls, 2);
    // The cache-layer GroupChunk can retain its exact physical address across
    // Clear and another pin. Pointer equality alone cannot prove an epoch.
    EXPECT_EQ(control->last_backing, same_backing);
    context->Clear();
    auto original_source = fixture.title->MakeTakeCellSource(nullptr);
    control->action = [context, original_source] {
        context->Clear();
        auto nested = context->Acquire(original_source, {0});
        EXPECT_EQ(nested.size(), 1);
    };
    eval();
    EXPECT_EQ(control->calls, 3);
    eval();
    EXPECT_EQ(control->calls, 4);
    eval();
    EXPECT_EQ(control->calls, 4);
}

TEST(TakeContextGroupFoundation,
     ReusableProjectionDoesNotCacheThrownCallbackResults) {
    OriginalGroupFixture fixture("take-group-original-nq-projection-throw");
    auto context = std::make_shared<TakeContext>();
    auto control = std::make_shared<ProjectionControl>();
    control->fail_once = true;
    auto column = std::make_shared<ObservedProjectionOriginalProxy>(
        fixture.group, control);
    auto binding = column->CreateTakeBinding(
        nullptr, TakeBindingOptions{TargetType::StringView, {}, context});
    ASSERT_NE(binding, nullptr);
    const std::vector<int64_t> offsets{0, 1, 0};
    auto eval = [&] {
        auto lease =
            binding->Bind(OffsetView::From(offsets.data(), offsets.size()));
        ExpectOriginalTitleViews(
            lease.Result(), offsets, fixture.original, 65, false);
    };
    EXPECT_THROW(eval(), std::runtime_error);
    EXPECT_EQ(control->calls, 1);
    eval();
    EXPECT_EQ(control->calls, 2);
    eval();
    EXPECT_EQ(control->calls, 2);
    EXPECT_EQ(context->Stats().bulk_pins, 1);
}

TEST(TakeContextGroupFoundation,
     NonNullableReadsAndValidityOnlyStillRejectInvalidOriginalOffsets) {
    OriginalGroupFixture fixture("take-group-original-nq-offset-validation");
    for (bool contextual : {false, true}) {
        auto context = contextual ? std::make_shared<TakeContext>() : nullptr;
        for (int64_t bad : {int64_t{-1},
                            static_cast<int64_t>(fixture.original.size()),
                            std::numeric_limits<int64_t>::max()}) {
            const std::vector<int64_t> offsets{bad};
            auto result = fixture.title->Take(
                nullptr,
                ChunkedColumnInterface::TakeOptions{
                    OffsetView::From(offsets.data(), offsets.size()),
                    TargetType::StringView,
                    nullptr,
                    context});
            ASSERT_NE(result, nullptr);
            EXPECT_ANY_THROW(result->Get<std::string_view>(0));
            EXPECT_ANY_THROW(result->IsValid(0));
            int64_t consumed = 0;
            EXPECT_ANY_THROW(result->VisitViews<std::string_view>(
                32,
                [&](const std::string_view*,
                    ValidityView,
                    const bool*,
                    int64_t,
                    int64_t count) { consumed += count; }));
            EXPECT_EQ(consumed, 0);
            EXPECT_ANY_THROW(result->GetOwn());
        }
        const std::vector<int64_t> offsets{0, 3, 128, 0};
        auto result = fixture.title->Take(
            nullptr,
            ChunkedColumnInterface::TakeOptions{
                OffsetView::From(offsets.data(), offsets.size()),
                TargetType::StringView,
                nullptr,
                context});
        ASSERT_NE(result, nullptr);
        for (size_t lane = 0; lane < offsets.size(); ++lane) {
            EXPECT_TRUE(result->IsValid(lane));
        }
        ExpectOriginalTitleViews(*result, offsets, fixture.original);
    }
}
}  // namespace
}  // namespace milvus
