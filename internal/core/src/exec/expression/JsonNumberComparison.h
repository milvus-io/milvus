// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include <simdjson.h>

#include "ankerl/unordered_dense.h"
#include "common/bson_view.h"
#include "pb/plan.pb.h"

namespace milvus::exec {

inline int
CompareInt64ToDouble(int64_t lhs, double rhs) {
    constexpr double kInt64Lower = -0x1p63;
    constexpr double kInt64Upper = 0x1p63;
    if (rhs < kInt64Lower) {
        return 1;
    }
    if (rhs >= kInt64Upper) {
        return -1;
    }

    const auto rhs_integer = static_cast<int64_t>(rhs);
    if (lhs < rhs_integer) {
        return -1;
    }
    if (lhs > rhs_integer) {
        return 1;
    }

    const auto rhs_integer_as_double = static_cast<double>(rhs_integer);
    if (rhs > rhs_integer_as_double) {
        return -1;
    }
    if (rhs < rhs_integer_as_double) {
        return 1;
    }
    return 0;
}

inline int
CompareUint64ToDouble(uint64_t lhs, double rhs) {
    constexpr double kUint64Upper = 0x1p64;
    if (rhs < 0) {
        return 1;
    }
    if (rhs >= kUint64Upper) {
        return -1;
    }

    const auto rhs_integer = static_cast<uint64_t>(rhs);
    if (lhs < rhs_integer) {
        return -1;
    }
    if (lhs > rhs_integer) {
        return 1;
    }

    const auto rhs_integer_as_double = static_cast<double>(rhs_integer);
    if (rhs > rhs_integer_as_double) {
        return -1;
    }
    if (rhs < rhs_integer_as_double) {
        return 1;
    }
    return 0;
}

inline bool
Int64RoundTripsThroughDouble(int64_t value) {
    return CompareInt64ToDouble(value, static_cast<double>(value)) == 0;
}

inline std::optional<int64_t>
DoubleToInt64Exact(double value) {
    constexpr double kInt64Lower = -0x1p63;
    constexpr double kInt64Upper = 0x1p63;
    if (!std::isfinite(value) || value < kInt64Lower || value >= kInt64Upper) {
        return std::nullopt;
    }
    const auto integer = static_cast<int64_t>(value);
    return CompareInt64ToDouble(integer, value) == 0
               ? std::optional<int64_t>(integer)
               : std::nullopt;
}

enum class JsonNumberLookupStrategy {
    // Production callers use AUTO. Explicit strategies exist so tests can
    // verify semantic parity and benchmarks can measure the crossover.
    AUTO,
    LINEAR_SCAN,
    HASH_LOOKUP,
};

// Small candidate lists avoid hash-table construction. Keep this threshold
// benchmarked by JsonNumberCandidateMatcherBenchmark.cpp for both membership
// and ContainsAll position lookup.
inline constexpr size_t kMaxLinearScanJsonNumberCandidates = 4;

namespace detail {

using JsonNumberCandidateValue = std::variant<int64_t, double>;

inline double
CanonicalJsonDouble(double value) {
    // Equal keys must hash identically; normalize negative zero.
    return value == 0.0 ? 0.0 : value;
}

inline size_t
CountMatchableJsonNumberCandidates(
    const std::vector<proto::plan::GenericValue>& candidates,
    bool* has_numeric_candidates = nullptr) {
    size_t count = 0;
    bool has_numeric = false;
    for (const auto& candidate : candidates) {
        if (candidate.has_int64_val()) {
            has_numeric = true;
            ++count;
        } else if (candidate.has_float_val()) {
            has_numeric = true;
            if (!std::isnan(candidate.float_val())) {
                ++count;
            }
        }
    }
    if (has_numeric_candidates != nullptr) {
        *has_numeric_candidates = has_numeric;
    }
    return count;
}

inline JsonNumberLookupStrategy
ResolveJsonNumberLookupStrategy(size_t matchable_candidate_count,
                                JsonNumberLookupStrategy requested_strategy) {
    if (requested_strategy != JsonNumberLookupStrategy::AUTO) {
        return requested_strategy;
    }
    return matchable_candidate_count <= kMaxLinearScanJsonNumberCandidates
               ? JsonNumberLookupStrategy::LINEAR_SCAN
               : JsonNumberLookupStrategy::HASH_LOOKUP;
}

inline bool
JsonNumberCandidateMatches(const JsonNumberCandidateValue& candidate,
                           int64_t probe) {
    if (const auto* integer = std::get_if<int64_t>(&candidate)) {
        return *integer == probe;
    }
    return CompareInt64ToDouble(probe, std::get<double>(candidate)) == 0;
}

inline bool
JsonNumberCandidateMatches(const JsonNumberCandidateValue& candidate,
                           double probe) {
    if (std::isnan(probe)) {
        return false;
    }
    if (const auto* integer = std::get_if<int64_t>(&candidate)) {
        return CompareInt64ToDouble(*integer, probe) == 0;
    }
    return std::get<double>(candidate) == probe;
}

}  // namespace detail

// Precompiled membership-only matcher for numeric IN, Contains, and
// ContainsAny. Large candidate lists use two typed sets. Exact cross-type
// aliases make 2 and 2.0 equal without normalizing precise int64 values to
// double. No candidate positions are retained.
class JsonNumberMembershipMatcher {
 public:
    using LookupStrategy = JsonNumberLookupStrategy;
    static constexpr size_t kMaxLinearScanNumericCandidates =
        kMaxLinearScanJsonNumberCandidates;

    explicit JsonNumberMembershipMatcher(
        const std::vector<proto::plan::GenericValue>& candidates,
        LookupStrategy requested_strategy = LookupStrategy::AUTO) {
        const auto matchable_candidate_count =
            detail::CountMatchableJsonNumberCandidates(
                candidates, &has_numeric_candidates_);
        lookup_strategy_ = detail::ResolveJsonNumberLookupStrategy(
            matchable_candidate_count, requested_strategy);

        if (lookup_strategy_ == LookupStrategy::LINEAR_SCAN) {
            linear_candidates_.reserve(matchable_candidate_count);
        } else {
            int64_values_.max_load_factor(0.5f);
            double_values_.max_load_factor(0.5f);
            int64_values_.reserve(matchable_candidate_count);
            double_values_.reserve(matchable_candidate_count);
        }

        for (const auto& candidate : candidates) {
            if (candidate.has_int64_val()) {
                AddInt64(candidate.int64_val());
            } else if (candidate.has_float_val() &&
                       !std::isnan(candidate.float_val())) {
                AddDouble(candidate.float_val());
            }
        }
    }

    bool
    HasNumericCandidates() const {
        // NaN is a numeric query literal even though it cannot match. Keep it
        // distinct from a candidate list containing no numeric literals so
        // predicate validity remains unchanged across lookup strategies.
        return has_numeric_candidates_;
    }

    LookupStrategy
    lookup_strategy() const {
        return lookup_strategy_;
    }

    bool
    MatchesAny(int64_t probe) const {
        if (lookup_strategy_ == LookupStrategy::LINEAR_SCAN) {
            return MatchesAnyByLinearScan(probe);
        }
        return int64_values_.contains(probe);
    }

    bool
    MatchesAny(double probe) const {
        if (std::isnan(probe)) {
            return false;
        }
        if (lookup_strategy_ == LookupStrategy::LINEAR_SCAN) {
            return MatchesAnyByLinearScan(probe);
        }
        return double_values_.contains(detail::CanonicalJsonDouble(probe));
    }

    bool
    MatchesAnyWithUint64DoubleFallback(
        const simdjson::ondemand::number& number) const {
        if (number.is_int64()) {
            return MatchesAny(number.get_int64());
        }
        if (number.is_uint64()) {
            // JSON stats and typed JSON indexes persist uint64 as double.
            return MatchesAny(static_cast<double>(number.get_uint64()));
        }
        return MatchesAny(number.get_double());
    }

    bool
    MatchesAnyBsonNumber(const milvus::bson::value_view& value) const {
        if (auto integer32 =
                milvus::BsonView::GetValueFromBsonView<int32_t>(value)) {
            return MatchesAny(static_cast<int64_t>(*integer32));
        }
        if (auto integer =
                milvus::BsonView::GetValueFromBsonView<int64_t>(value)) {
            return MatchesAny(*integer);
        }
        if (auto floating =
                milvus::BsonView::GetValueFromBsonView<double>(value)) {
            return MatchesAny(*floating);
        }
        return false;
    }

    bool
    MatchesAnyBsonNumberAtOffset(milvus::BsonView& bson,
                                 size_t offset,
                                 bool& is_number) const {
        if (auto integer = bson.ParseAsValueAtOffset<int64_t>(offset)) {
            is_number = true;
            return MatchesAny(*integer);
        }
        if (auto floating = bson.ParseAsValueAtOffset<double>(offset)) {
            is_number = true;
            return MatchesAny(*floating);
        }
        is_number = false;
        return false;
    }

 private:
    void
    AddInt64(int64_t value) {
        if (lookup_strategy_ == LookupStrategy::LINEAR_SCAN) {
            linear_candidates_.emplace_back(value);
            return;
        }
        int64_values_.insert(value);
        if (Int64RoundTripsThroughDouble(value)) {
            double_values_.insert(
                detail::CanonicalJsonDouble(static_cast<double>(value)));
        }
    }

    void
    AddDouble(double value) {
        value = detail::CanonicalJsonDouble(value);
        if (lookup_strategy_ == LookupStrategy::LINEAR_SCAN) {
            linear_candidates_.emplace_back(value);
            return;
        }
        double_values_.insert(value);
        if (auto integer = DoubleToInt64Exact(value)) {
            int64_values_.insert(*integer);
        }
    }

    template <typename T>
    bool
    MatchesAnyByLinearScan(T probe) const {
        for (const auto& candidate : linear_candidates_) {
            if (detail::JsonNumberCandidateMatches(candidate, probe)) {
                return true;
            }
        }
        return false;
    }

 private:
    std::vector<detail::JsonNumberCandidateValue> linear_candidates_;
    ankerl::unordered_dense::set<int64_t> int64_values_;
    ankerl::unordered_dense::set<double> double_values_;
    LookupStrategy lookup_strategy_{LookupStrategy::LINEAR_SCAN};
    bool has_numeric_candidates_{false};
};

// ContainsAll needs to know every original query-literal position that a JSON
// number satisfies. For example, one JSON value 2 satisfies both positions in
// [2, 2.0]. This index deliberately owns the extra value-to-position vectors;
// membership-only predicates use JsonNumberMembershipMatcher instead.
class JsonNumberCandidatePositionIndex {
 public:
    using CandidatePositions = std::vector<size_t>;
    using LookupStrategy = JsonNumberLookupStrategy;
    static constexpr size_t kMaxLinearScanNumericCandidates =
        kMaxLinearScanJsonNumberCandidates;

    explicit JsonNumberCandidatePositionIndex(
        const std::vector<proto::plan::GenericValue>& candidates,
        LookupStrategy requested_strategy = LookupStrategy::AUTO) {
        const auto matchable_candidate_count =
            detail::CountMatchableJsonNumberCandidates(candidates);
        lookup_strategy_ = detail::ResolveJsonNumberLookupStrategy(
            matchable_candidate_count, requested_strategy);

        if (lookup_strategy_ == LookupStrategy::LINEAR_SCAN) {
            linear_candidates_.reserve(matchable_candidate_count);
        } else {
            int64_positions_by_value_.max_load_factor(0.5f);
            double_positions_by_value_.max_load_factor(0.5f);
            int64_positions_by_value_.reserve(matchable_candidate_count);
            double_positions_by_value_.reserve(matchable_candidate_count);
        }

        for (size_t position = 0; position < candidates.size(); ++position) {
            const auto& candidate = candidates[position];
            if (candidate.has_int64_val()) {
                AddInt64(candidate.int64_val(), position);
            } else if (candidate.has_float_val() &&
                       !std::isnan(candidate.float_val())) {
                AddDouble(candidate.float_val(), position);
            }
        }
    }

    LookupStrategy
    lookup_strategy() const {
        return lookup_strategy_;
    }

    template <typename OnMatch>
    void
    VisitMatchingPositions(int64_t probe, OnMatch&& on_match) const {
        if (lookup_strategy_ == LookupStrategy::LINEAR_SCAN) {
            VisitLinearMatchingPositions(probe, on_match);
            return;
        }
        VisitHashMatchingPositions(int64_positions_by_value_, probe, on_match);
    }

    template <typename OnMatch>
    void
    VisitMatchingPositions(double probe, OnMatch&& on_match) const {
        if (std::isnan(probe)) {
            return;
        }
        if (lookup_strategy_ == LookupStrategy::LINEAR_SCAN) {
            VisitLinearMatchingPositions(probe, on_match);
            return;
        }
        VisitHashMatchingPositions(double_positions_by_value_,
                                   detail::CanonicalJsonDouble(probe),
                                   on_match);
    }

    template <typename OnMatch>
    void
    VisitMatchingPositionsWithUint64DoubleFallback(
        const simdjson::ondemand::number& number, OnMatch&& on_match) const {
        if (number.is_int64()) {
            VisitMatchingPositions(number.get_int64(),
                                   std::forward<OnMatch>(on_match));
        } else if (number.is_uint64()) {
            VisitMatchingPositions(static_cast<double>(number.get_uint64()),
                                   std::forward<OnMatch>(on_match));
        } else {
            VisitMatchingPositions(number.get_double(),
                                   std::forward<OnMatch>(on_match));
        }
    }

    template <typename OnMatch>
    void
    VisitMatchingPositionsForBsonNumber(const milvus::bson::value_view& value,
                                        OnMatch&& on_match) const {
        if (auto integer32 =
                milvus::BsonView::GetValueFromBsonView<int32_t>(value)) {
            VisitMatchingPositions(static_cast<int64_t>(*integer32),
                                   std::forward<OnMatch>(on_match));
        } else if (auto integer =
                       milvus::BsonView::GetValueFromBsonView<int64_t>(value)) {
            VisitMatchingPositions(*integer, std::forward<OnMatch>(on_match));
        } else if (auto floating =
                       milvus::BsonView::GetValueFromBsonView<double>(value)) {
            VisitMatchingPositions(*floating, std::forward<OnMatch>(on_match));
        }
    }

 private:
    struct PositionedNumberCandidate {
        detail::JsonNumberCandidateValue value;
        size_t position;
    };

    void
    AddInt64(int64_t value, size_t position) {
        if (lookup_strategy_ == LookupStrategy::LINEAR_SCAN) {
            linear_candidates_.push_back({value, position});
            return;
        }
        int64_positions_by_value_[value].push_back(position);
        if (Int64RoundTripsThroughDouble(value)) {
            double_positions_by_value_[detail::CanonicalJsonDouble(
                                           static_cast<double>(value))]
                .push_back(position);
        }
    }

    void
    AddDouble(double value, size_t position) {
        value = detail::CanonicalJsonDouble(value);
        if (lookup_strategy_ == LookupStrategy::LINEAR_SCAN) {
            linear_candidates_.push_back({value, position});
            return;
        }
        double_positions_by_value_[value].push_back(position);
        if (auto integer = DoubleToInt64Exact(value)) {
            int64_positions_by_value_[*integer].push_back(position);
        }
    }

    template <typename T, typename OnMatch>
    void
    VisitLinearMatchingPositions(T probe, OnMatch& on_match) const {
        for (const auto& candidate : linear_candidates_) {
            if (detail::JsonNumberCandidateMatches(candidate.value, probe) &&
                on_match(candidate.position)) {
                return;
            }
        }
    }

    template <typename Map, typename Key, typename OnMatch>
    static void
    VisitHashMatchingPositions(const Map& positions_by_value,
                               const Key& probe,
                               OnMatch& on_match) {
        auto it = positions_by_value.find(probe);
        if (it == positions_by_value.end()) {
            return;
        }
        for (auto candidate_position : it->second) {
            if (on_match(candidate_position)) {
                return;
            }
        }
    }

 private:
    std::vector<PositionedNumberCandidate> linear_candidates_;
    ankerl::unordered_dense::map<int64_t, CandidatePositions>
        int64_positions_by_value_;
    ankerl::unordered_dense::map<double, CandidatePositions>
        double_positions_by_value_;
    LookupStrategy lookup_strategy_{LookupStrategy::LINEAR_SCAN};
};

template <typename Target, typename Source>
std::optional<Target>
ConvertJsonNumberExact(Source value) {
    static_assert(
        (std::is_same_v<Target, int64_t> || std::is_same_v<Target, double>)&&(
            std::is_same_v<Source, int64_t> || std::is_same_v<Source, double>));
    if constexpr (std::is_same_v<Target, Source>) {
        return value;
    } else if constexpr (std::is_same_v<Target, int64_t>) {
        return DoubleToInt64Exact(value);
    } else {
        return Int64RoundTripsThroughDouble(value)
                   ? std::optional<double>(static_cast<double>(value))
                   : std::nullopt;
    }
}

template <typename Target>
std::optional<Target>
GetBsonNumberExact(const milvus::bson::value_view& value) {
    static_assert(std::is_same_v<Target, int64_t> ||
                  std::is_same_v<Target, double>);
    if (auto integer32 =
            milvus::BsonView::GetValueFromBsonView<int32_t>(value)) {
        return ConvertJsonNumberExact<Target>(static_cast<int64_t>(*integer32));
    }
    if (auto integer = milvus::BsonView::GetValueFromBsonView<int64_t>(value)) {
        return ConvertJsonNumberExact<Target>(*integer);
    }
    if (auto floating = milvus::BsonView::GetValueFromBsonView<double>(value)) {
        return ConvertJsonNumberExact<Target>(*floating);
    }
    return std::nullopt;
}

template <typename Target>
std::optional<Target>
ParseBsonNumberExact(milvus::BsonView& bson, size_t offset, bool& is_number) {
    static_assert(std::is_same_v<Target, int64_t> ||
                  std::is_same_v<Target, double>);
    if (auto integer = bson.ParseAsValueAtOffset<int64_t>(offset)) {
        is_number = true;
        return ConvertJsonNumberExact<Target>(*integer);
    }
    if (auto floating = bson.ParseAsValueAtOffset<double>(offset)) {
        is_number = true;
        return ConvertJsonNumberExact<Target>(*floating);
    }
    is_number = false;
    return std::nullopt;
}

inline std::optional<int>
CompareJsonNumberToBound(int64_t number,
                         const proto::plan::GenericValue& bound) {
    if (bound.has_int64_val()) {
        const auto rhs = bound.int64_val();
        return number < rhs ? -1 : number > rhs ? 1 : 0;
    }
    if (bound.has_float_val()) {
        const auto rhs = bound.float_val();
        return std::isnan(rhs)
                   ? std::nullopt
                   : std::optional<int>(CompareInt64ToDouble(number, rhs));
    }
    return std::nullopt;
}

inline std::optional<int>
CompareJsonNumberToBound(double number,
                         const proto::plan::GenericValue& bound) {
    if (std::isnan(number)) {
        return std::nullopt;
    }
    if (bound.has_int64_val()) {
        return -CompareInt64ToDouble(bound.int64_val(), number);
    }
    if (bound.has_float_val()) {
        const auto rhs = bound.float_val();
        if (std::isnan(rhs)) {
            return std::nullopt;
        }
        return number < rhs ? -1 : number > rhs ? 1 : 0;
    }
    return std::nullopt;
}

inline std::optional<int>
CompareBsonNumberToBound(const milvus::bson::value_view& number,
                         const proto::plan::GenericValue& bound) {
    if (auto integer32 =
            milvus::BsonView::GetValueFromBsonView<int32_t>(number)) {
        return CompareJsonNumberToBound(static_cast<int64_t>(*integer32),
                                        bound);
    }
    if (auto integer =
            milvus::BsonView::GetValueFromBsonView<int64_t>(number)) {
        return CompareJsonNumberToBound(*integer, bound);
    }
    if (auto floating =
            milvus::BsonView::GetValueFromBsonView<double>(number)) {
        return CompareJsonNumberToBound(*floating, bound);
    }
    return std::nullopt;
}

inline std::optional<int>
CompareBsonNumberToBound(milvus::BsonView& bson,
                         size_t offset,
                         const proto::plan::GenericValue& bound) {
    if (auto integer = bson.ParseAsValueAtOffset<int64_t>(offset)) {
        return CompareJsonNumberToBound(*integer, bound);
    }
    if (auto floating = bson.ParseAsValueAtOffset<double>(offset)) {
        return CompareJsonNumberToBound(*floating, bound);
    }
    return std::nullopt;
}

inline std::optional<int>
CompareBsonArrayNumberToBound(const milvus::bson::array_view& array,
                              size_t index,
                              const proto::plan::GenericValue& bound) {
    if (auto integer32 = milvus::BsonView::GetNthElementInArray<int32_t>(
            array.data(), index)) {
        return CompareJsonNumberToBound(static_cast<int64_t>(*integer32),
                                        bound);
    }
    if (auto integer = milvus::BsonView::GetNthElementInArray<int64_t>(
            array.data(), index)) {
        return CompareJsonNumberToBound(*integer, bound);
    }
    if (auto floating = milvus::BsonView::GetNthElementInArray<double>(
            array.data(), index)) {
        return CompareJsonNumberToBound(*floating, bound);
    }
    return std::nullopt;
}

inline std::optional<int>
CompareJsonNumberToBound(const simdjson::ondemand::number& number,
                         const proto::plan::GenericValue& bound) {
    if (number.is_int64()) {
        return CompareJsonNumberToBound(number.get_int64(), bound);
    }
    if (number.is_uint64()) {
        const auto lhs = number.get_uint64();
        if (bound.has_int64_val()) {
            const auto rhs = bound.int64_val();
            if (rhs < 0) {
                return 1;
            }
            const auto unsigned_rhs = static_cast<uint64_t>(rhs);
            return lhs < unsigned_rhs ? -1 : lhs > unsigned_rhs ? 1 : 0;
        }
        if (bound.has_float_val()) {
            const auto rhs = bound.float_val();
            if (std::isnan(rhs)) {
                return std::nullopt;
            }
            return CompareUint64ToDouble(number.get_uint64(), rhs);
        }
        return std::nullopt;
    }
    return CompareJsonNumberToBound(number.get_double(), bound);
}

// JSON stats and typed JSON indexes store uint64 values as double. Preserve
// that established behavior while comparing int64 JSON values exactly.
inline std::optional<int>
CompareJsonNumberToBoundWithUint64DoubleFallback(
    const simdjson::ondemand::number& number,
    const proto::plan::GenericValue& bound) {
    if (number.is_uint64()) {
        return CompareJsonNumberToBound(
            static_cast<double>(number.get_uint64()), bound);
    }
    return CompareJsonNumberToBound(number, bound);
}

inline bool
JsonNumberMatchesOp(int comparison, proto::plan::OpType op) {
    switch (op) {
        case proto::plan::OpType::Equal:
            return comparison == 0;
        case proto::plan::OpType::NotEqual:
            return comparison != 0;
        case proto::plan::OpType::GreaterThan:
            return comparison > 0;
        case proto::plan::OpType::GreaterEqual:
            return comparison >= 0;
        case proto::plan::OpType::LessThan:
            return comparison < 0;
        case proto::plan::OpType::LessEqual:
            return comparison <= 0;
        default:
            return false;
    }
}

}  // namespace milvus::exec
