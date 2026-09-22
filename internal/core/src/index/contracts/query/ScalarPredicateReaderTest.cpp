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

#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "index/contracts/query/IScalarPredicateReader.h"
#include "index/test_utils/CaseTestDriver.h"

namespace milvus::index::test {
namespace {

template <typename T>
using PredicateTestValue =
    std::conditional_t<std::is_same_v<T, bool>, uint8_t, ScalarTestValue<T>>;

template <typename T>
T
QueryReaderValue(const PredicateTestValue<T>& value) {
    if constexpr (std::is_same_v<T, std::string_view>) {
        return std::string_view(value);
    } else if constexpr (std::is_same_v<T, bool>) {
        return value != 0;
    } else {
        return value;
    }
}

template <typename T>
T
DataReaderValue(const ScalarTestValue<T>& value) {
    if constexpr (std::is_same_v<T, std::string_view>) {
        return std::string_view(value);
    } else {
        return value;
    }
}

template <typename T, typename Query>
TargetBitmap
RunWithQueryValues(const std::vector<PredicateTestValue<T>>& values,
                   Query&& query) {
    if (values.empty()) {
        return std::forward<Query>(query)(0, nullptr);
    }
    if constexpr (std::is_same_v<T, std::string_view>) {
        const auto views = MakeStringViews(values);
        return std::forward<Query>(query)(views.size(), views.data());
    } else if constexpr (std::is_same_v<T, bool>) {
        auto bools = std::make_unique<bool[]>(values.size());
        for (size_t i = 0; i < values.size(); ++i) {
            bools[i] = values[i] != 0;
        }
        return std::forward<Query>(query)(values.size(), bools.get());
    } else {
        return std::forward<Query>(query)(values.size(), values.data());
    }
}

template <typename T>
bool
MatchesUnary(const T& candidate, const T& value, CompareOp op) {
    switch (op) {
        case CompareOp::Equal:
            return candidate == value;
        case CompareOp::NotEqual:
            return candidate != value;
        case CompareOp::GreaterThan:
            return candidate > value;
        case CompareOp::GreaterEqual:
            return candidate >= value;
        case CompareOp::LessThan:
            return candidate < value;
        case CompareOp::LessEqual:
            return candidate <= value;
    }
    throw std::logic_error("unknown scalar comparison operator");
}

template <typename T>
bool
MatchesInterval(
    const T& candidate, const T& lo, bool lo_inc, const T& hi, bool hi_inc) {
    const bool above_lo = lo_inc ? candidate >= lo : candidate > lo;
    const bool below_hi = hi_inc ? candidate <= hi : candidate < hi;
    return above_lo && below_hi;
}

template <typename T>
struct In {
    using ValueType = T;
    using Reader = IScalarPredicateReader<T>;
    static constexpr auto kCapability = &ReaderCaps::predicate;

    struct Args {
        std::vector<PredicateTestValue<T>> keys;
    };

    static TargetBitmap
    Run(const Reader& reader, const Args& args) {
        return RunWithQueryValues<T>(args.keys,
                                     [&reader](size_t size, const T* values) {
                                         return reader.In(size, values);
                                     });
    }

    static TargetBitmap
    Oracle(const ScalarTestData<T>& data, const Args& args) {
        TargetBitmap expected(data.values.size(), false);
        for (size_t i = 0; i < data.values.size(); ++i) {
            const auto value = DataReaderValue<T>(data.values[i]);
            const auto matches = std::any_of(
                args.keys.begin(), args.keys.end(), [&](const auto& key) {
                    return value == QueryReaderValue<T>(key);
                });
            if (data.validity[i] && matches) {
                expected.set(i);
            }
        }
        return expected;
    }
};

template <typename T>
struct NotIn {
    using ValueType = T;
    using Reader = IScalarPredicateReader<T>;
    static constexpr auto kCapability = &ReaderCaps::predicate;

    struct Args {
        std::vector<PredicateTestValue<T>> keys;
    };

    static TargetBitmap
    Run(const Reader& reader, const Args& args) {
        return RunWithQueryValues<T>(args.keys,
                                     [&reader](size_t size, const T* values) {
                                         return reader.NotIn(size, values);
                                     });
    }

    static TargetBitmap
    Oracle(const ScalarTestData<T>& data, const Args& args) {
        auto expected = In<T>::Oracle(data, {.keys = args.keys});
        expected.flip();
        expected &= data.validity;
        return expected;
    }
};

template <typename T>
struct UnaryRange {
    using ValueType = T;
    using Reader = IScalarPredicateReader<T>;
    static constexpr auto kCapability = &ReaderCaps::predicate;

    struct Args {
        PredicateTestValue<T> value;
        CompareOp op;
    };

    static TargetBitmap
    Run(const Reader& reader, const Args& args) {
        const auto value = QueryReaderValue<T>(args.value);
        return reader.Range(value, args.op);
    }

    static TargetBitmap
    Oracle(const ScalarTestData<T>& data, const Args& args) {
        TargetBitmap expected(data.values.size(), false);
        const auto value = QueryReaderValue<T>(args.value);
        for (size_t i = 0; i < data.values.size(); ++i) {
            if (data.validity[i] &&
                MatchesUnary(
                    DataReaderValue<T>(data.values[i]), value, args.op)) {
                expected.set(i);
            }
        }
        return expected;
    }
};

template <typename T>
struct IntervalRange {
    using ValueType = T;
    using Reader = IScalarPredicateReader<T>;
    static constexpr auto kCapability = &ReaderCaps::predicate;

    struct Args {
        PredicateTestValue<T> lo;
        bool lo_inc;
        PredicateTestValue<T> hi;
        bool hi_inc;
    };

    static TargetBitmap
    Run(const Reader& reader, const Args& args) {
        const auto lo = QueryReaderValue<T>(args.lo);
        const auto hi = QueryReaderValue<T>(args.hi);
        return reader.Range(lo, args.lo_inc, hi, args.hi_inc);
    }

    static TargetBitmap
    Oracle(const ScalarTestData<T>& data, const Args& args) {
        TargetBitmap expected(data.values.size(), false);
        const auto lo = QueryReaderValue<T>(args.lo);
        const auto hi = QueryReaderValue<T>(args.hi);
        for (size_t i = 0; i < data.values.size(); ++i) {
            if (data.validity[i] &&
                MatchesInterval(DataReaderValue<T>(data.values[i]),
                                lo,
                                args.lo_inc,
                                hi,
                                args.hi_inc)) {
                expected.set(i);
            }
        }
        return expected;
    }
};

template <typename T>
struct PredicateCaseValues {
    PredicateTestValue<T> minimum;
    PredicateTestValue<T> low;
    PredicateTestValue<T> middle;
    PredicateTestValue<T> high;
    PredicateTestValue<T> maximum;
    PredicateTestValue<T> nullable_duplicate;
    PredicateTestValue<T> missing;
    PredicateTestValue<T> representative;
    PredicateTestValue<T> representative_missing;
    std::vector<PredicateTestValue<T>> all_values;
};

template <typename T>
PredicateCaseValues<T>
CaseValues() {
    if constexpr (std::is_same_v<T, bool>) {
        return {
            .minimum = false,
            .low = false,
            .middle = false,
            .high = true,
            .maximum = true,
            .nullable_duplicate = true,
            .missing = true,
            .representative = true,
            .representative_missing = false,
            .all_values = {false, true},
        };
    } else if constexpr (std::is_floating_point_v<T>) {
        return {
            .minimum = std::numeric_limits<T>::lowest(),
            .low = static_cast<T>(-1.5),
            .middle = static_cast<T>(0.0),
            .high = static_cast<T>(1.5),
            .maximum = std::numeric_limits<T>::max(),
            .nullable_duplicate = std::numeric_limits<T>::max(),
            .missing = static_cast<T>(0.5),
            .representative = static_cast<T>(1.5),
            .representative_missing = static_cast<T>(0.5),
            .all_values = {std::numeric_limits<T>::lowest(),
                           static_cast<T>(-1.5),
                           static_cast<T>(0.0),
                           static_cast<T>(1.5),
                           std::numeric_limits<T>::max()},
        };
    } else if constexpr (std::is_same_v<T, std::string_view>) {
        const std::string embedded_nul("a\0b", 3);
        const std::string utf8_cat("\xE7\x8C\xAB", 3);
        const std::string long_value(80, 'x');
        return {
            .minimum = "",
            .low = "a",
            .middle = "ab",
            .high = long_value,
            .maximum = utf8_cat,
            .nullable_duplicate = long_value,
            .missing = "missing",
            .representative = "single",
            .representative_missing = "missing",
            .all_values = {"", "a", "ab", embedded_nul, utf8_cat, long_value},
        };
    } else {
        static_assert(std::is_integral_v<T> && std::is_signed_v<T>);
        return {
            .minimum = std::numeric_limits<T>::lowest(),
            .low = static_cast<T>(-1),
            .middle = static_cast<T>(0),
            .high = static_cast<T>(1),
            .maximum = std::numeric_limits<T>::max(),
            .nullable_duplicate = std::numeric_limits<T>::max(),
            .missing = static_cast<T>(2),
            .representative = static_cast<T>(1),
            .representative_missing = static_cast<T>(2),
            .all_values = {std::numeric_limits<T>::lowest(),
                           static_cast<T>(-1),
                           static_cast<T>(0),
                           static_cast<T>(1),
                           std::numeric_limits<T>::max()},
        };
    }
}

template <typename T>
void
AddEdgeMembershipCases(IndexTestCases& cases) {
    const auto values = CaseValues<T>();

    cases.Add(IndexTestCase<T>{
        .name = "InEmptyKeys",
        .dataset = "PredicateEdges",
        .body =
            Query<In<T>>{
                .args = {.keys = {}},
            },
    });
    if constexpr (!std::is_same_v<T, bool>) {
        cases.Add(IndexTestCase<T>{
            .name = "InMissingOnly",
            .dataset = "PredicateEdges",
            .body =
                Query<In<T>>{
                    .args = {.keys = {values.missing}},
                },
        });
    }
    cases.Add(IndexTestCase<T>{
        .name = "InAllValues",
        .dataset = "PredicateEdges",
        .body =
            Query<In<T>>{
                .args = {.keys = values.all_values},
            },
    });
    cases.Add(IndexTestCase<T>{
        .name = "InRepeatedKeys",
        .dataset = "PredicateEdges",
        .body =
            Query<In<T>>{
                .args = {.keys = {values.middle, values.middle, values.middle}},
            },
    });
    cases.Add(IndexTestCase<T>{
        .name = "InNullableStoredValue",
        .dataset = "PredicateEdges",
        .body =
            Query<In<T>>{
                .args = {.keys = {values.nullable_duplicate}},
            },
    });
    if constexpr (!std::is_same_v<T, bool>) {
        cases.Add(IndexTestCase<T>{
            .name = "InMixedHitAndMiss",
            .dataset = "PredicateEdges",
            .body =
                Query<In<T>>{
                    .args = {.keys = {values.low, values.missing}},
                },
        });
    }

    cases.Add(IndexTestCase<T>{
        .name = "NotInEmptyKeys",
        .dataset = "PredicateEdges",
        .body =
            Query<NotIn<T>>{
                .args = {.keys = {}},
            },
    });
    if constexpr (!std::is_same_v<T, bool>) {
        cases.Add(IndexTestCase<T>{
            .name = "NotInMissingOnly",
            .dataset = "PredicateEdges",
            .body =
                Query<NotIn<T>>{
                    .args = {.keys = {values.missing}},
                },
        });
    }
    cases.Add(IndexTestCase<T>{
        .name = "NotInAllValues",
        .dataset = "PredicateEdges",
        .body =
            Query<NotIn<T>>{
                .args = {.keys = values.all_values},
            },
    });
    cases.Add(IndexTestCase<T>{
        .name = "NotInRepeatedKeys",
        .dataset = "PredicateEdges",
        .body =
            Query<NotIn<T>>{
                .args = {.keys = {values.middle, values.middle, values.middle}},
            },
    });
    cases.Add(IndexTestCase<T>{
        .name = "NotInNullableStoredValue",
        .dataset = "PredicateEdges",
        .body =
            Query<NotIn<T>>{
                .args = {.keys = {values.nullable_duplicate}},
            },
    });
    if constexpr (!std::is_same_v<T, bool>) {
        cases.Add(IndexTestCase<T>{
            .name = "NotInMixedHitAndMiss",
            .dataset = "PredicateEdges",
            .body =
                Query<NotIn<T>>{
                    .args = {.keys = {values.low, values.missing}},
                },
        });
    }
}

template <typename T>
void
AddUnaryRangeCases(IndexTestCases& cases) {
    const auto values = CaseValues<T>();
    const std::vector<std::pair<std::string, CompareOp>> operations = {
        {"Equal", CompareOp::Equal},
        {"NotEqual", CompareOp::NotEqual},
        {"GreaterThan", CompareOp::GreaterThan},
        {"GreaterEqual", CompareOp::GreaterEqual},
        {"LessThan", CompareOp::LessThan},
        {"LessEqual", CompareOp::LessEqual},
    };
    for (const auto& [name, op] : operations) {
        cases.Add(IndexTestCase<T>{
            .name = "Unary" + name,
            .dataset = "PredicateEdges",
            .body =
                Query<UnaryRange<T>>{
                    .args = {.value = values.middle, .op = op},
                },
        });
    }

    cases.Add(IndexTestCase<T>{
        .name = "UnaryLessThanMinimum",
        .dataset = "PredicateEdges",
        .body =
            Query<UnaryRange<T>>{
                .args = {.value = values.minimum, .op = CompareOp::LessThan},
            },
    });
    cases.Add(IndexTestCase<T>{
        .name = "UnaryLessEqualMinimum",
        .dataset = "PredicateEdges",
        .body =
            Query<UnaryRange<T>>{
                .args = {.value = values.minimum, .op = CompareOp::LessEqual},
            },
    });
    cases.Add(IndexTestCase<T>{
        .name = "UnaryGreaterThanMaximum",
        .dataset = "PredicateEdges",
        .body =
            Query<UnaryRange<T>>{
                .args = {.value = values.maximum, .op = CompareOp::GreaterThan},
            },
    });
    cases.Add(IndexTestCase<T>{
        .name = "UnaryGreaterEqualMaximum",
        .dataset = "PredicateEdges",
        .body =
            Query<UnaryRange<T>>{
                .args = {.value = values.maximum,
                         .op = CompareOp::GreaterEqual},
            },
    });
    if constexpr (std::is_floating_point_v<T>) {
        cases.Add(IndexTestCase<T>{
            .name = "UnaryEqualNegativeZero",
            .dataset = "PredicateEdges",
            .body =
                Query<UnaryRange<T>>{
                    .args = {.value = static_cast<T>(-0.0),
                             .op = CompareOp::Equal},
                },
        });
        cases.Add(IndexTestCase<T>{
            .name = "UnaryEqualPositiveZero",
            .dataset = "PredicateEdges",
            .body =
                Query<UnaryRange<T>>{
                    .args = {.value = static_cast<T>(0.0),
                             .op = CompareOp::Equal},
                },
        });
    }
}

template <typename T>
void
AddIntervalRangeCases(IndexTestCases& cases) {
    const auto values = CaseValues<T>();
    for (const auto [lo_inc, hi_inc, suffix] :
         {std::tuple{false, false, "OpenOpen"},
          std::tuple{false, true, "OpenClosed"},
          std::tuple{true, false, "ClosedOpen"},
          std::tuple{true, true, "ClosedClosed"}}) {
        cases.Add(IndexTestCase<T>{
            .name = std::string("Interval") + suffix,
            .dataset = "PredicateEdges",
            .body =
                Query<IntervalRange<T>>{
                    .args = {.lo = values.low,
                             .lo_inc = lo_inc,
                             .hi = values.high,
                             .hi_inc = hi_inc},
                },
        });
        cases.Add(IndexTestCase<T>{
            .name = std::string("IntervalEqualBounds") + suffix,
            .dataset = "PredicateEdges",
            .body =
                Query<IntervalRange<T>>{
                    .args = {.lo = values.middle,
                             .lo_inc = lo_inc,
                             .hi = values.middle,
                             .hi_inc = hi_inc},
                },
        });
        cases.Add(IndexTestCase<T>{
            .name = std::string("IntervalReversedBounds") + suffix,
            .dataset = "PredicateEdges",
            .body =
                Query<IntervalRange<T>>{
                    .args = {.lo = values.high,
                             .lo_inc = lo_inc,
                             .hi = values.low,
                             .hi_inc = hi_inc},
                },
        });
    }
    cases.Add(IndexTestCase<T>{
        .name = "IntervalClosedEndpoints",
        .dataset = "PredicateEdges",
        .body =
            Query<IntervalRange<T>>{
                .args = {.lo = values.minimum,
                         .lo_inc = true,
                         .hi = values.maximum,
                         .hi_inc = true},
            },
    });
    cases.Add(IndexTestCase<T>{
        .name = "IntervalOpenEndpoints",
        .dataset = "PredicateEdges",
        .body =
            Query<IntervalRange<T>>{
                .args = {.lo = values.minimum,
                         .lo_inc = false,
                         .hi = values.maximum,
                         .hi_inc = false},
            },
    });
}

template <typename T>
void
AddInputShapeCases(IndexTestCases& cases) {
    const auto values = CaseValues<T>();

    cases.Add(IndexTestCase<T>{
        .name = "InAllValuesWithoutValidity",
        .dataset = "PredicateAllValid",
        .body =
            Query<In<T>>{
                .args = {.keys = values.all_values},
            },
    });
    cases.Add(IndexTestCase<T>{
        .name = "NotInEmptyWithoutValidity",
        .dataset = "PredicateAllValid",
        .body =
            Query<NotIn<T>>{
                .args = {.keys = {}},
            },
    });
    cases.Add(IndexTestCase<T>{
        .name = "UnaryNotEqualWithoutValidity",
        .dataset = "PredicateAllValid",
        .body =
            Query<UnaryRange<T>>{
                .args = {.value = values.missing, .op = CompareOp::NotEqual},
            },
    });
    cases.Add(IndexTestCase<T>{
        .name = "IntervalEndpointsWithoutValidity",
        .dataset = "PredicateAllValid",
        .body =
            Query<IntervalRange<T>>{
                .args = {.lo = values.minimum,
                         .lo_inc = true,
                         .hi = values.maximum,
                         .hi_inc = true},
            },
    });

    cases.Add(IndexTestCase<T>{
        .name = "InAllValuesAllNull",
        .dataset = "PredicateAllNull",
        .body =
            Query<In<T>>{
                .args = {.keys = values.all_values},
            },
    });
    cases.Add(IndexTestCase<T>{
        .name = "NotInEmptyAllNull",
        .dataset = "PredicateAllNull",
        .body =
            Query<NotIn<T>>{
                .args = {.keys = {}},
            },
    });
    cases.Add(IndexTestCase<T>{
        .name = "UnaryNotEqualAllNull",
        .dataset = "PredicateAllNull",
        .body =
            Query<UnaryRange<T>>{
                .args = {.value = values.missing, .op = CompareOp::NotEqual},
            },
    });
    cases.Add(IndexTestCase<T>{
        .name = "IntervalEndpointsAllNull",
        .dataset = "PredicateAllNull",
        .body =
            Query<IntervalRange<T>>{
                .args = {.lo = values.minimum,
                         .lo_inc = true,
                         .hi = values.maximum,
                         .hi_inc = true},
            },
    });

    cases.Add(IndexTestCase<T>{
        .name = "InSingleRow",
        .dataset = "PredicateSingleRow",
        .body =
            Query<In<T>>{
                .args = {.keys = {values.representative}},
            },
    });
    cases.Add(IndexTestCase<T>{
        .name = "NotInSingleRow",
        .dataset = "PredicateSingleRow",
        .body =
            Query<NotIn<T>>{
                .args = {.keys = {values.representative}},
            },
    });
    cases.Add(IndexTestCase<T>{
        .name = "UnaryEqualSingleRow",
        .dataset = "PredicateSingleRow",
        .body =
            Query<UnaryRange<T>>{
                .args = {.value = values.representative,
                         .op = CompareOp::Equal},
            },
    });
    cases.Add(IndexTestCase<T>{
        .name = "IntervalEqualSingleRow",
        .dataset = "PredicateSingleRow",
        .body =
            Query<IntervalRange<T>>{
                .args = {.lo = values.representative,
                         .lo_inc = true,
                         .hi = values.representative,
                         .hi_inc = true},
            },
    });

    cases.Add(IndexTestCase<T>{
        .name = "InRepeatedKeysAllEqual",
        .dataset = "PredicateAllEqual",
        .body =
            Query<In<T>>{
                .args = {.keys = {values.representative,
                                  values.representative}},
            },
    });
    cases.Add(IndexTestCase<T>{
        .name = "InMissingAllEqual",
        .dataset = "PredicateAllEqual",
        .body =
            Query<In<T>>{
                .args = {.keys = {values.representative_missing}},
            },
    });
    cases.Add(IndexTestCase<T>{
        .name = "NotInMissingAllEqual",
        .dataset = "PredicateAllEqual",
        .body =
            Query<NotIn<T>>{
                .args = {.keys = {values.representative_missing}},
            },
    });
    cases.Add(IndexTestCase<T>{
        .name = "UnaryEqualAllEqual",
        .dataset = "PredicateAllEqual",
        .body =
            Query<UnaryRange<T>>{
                .args = {.value = values.representative,
                         .op = CompareOp::Equal},
            },
    });
    cases.Add(IndexTestCase<T>{
        .name = "UnaryNotEqualAllEqual",
        .dataset = "PredicateAllEqual",
        .body =
            Query<UnaryRange<T>>{
                .args = {.value = values.representative,
                         .op = CompareOp::NotEqual},
            },
    });
    cases.Add(IndexTestCase<T>{
        .name = "IntervalClosedAllEqual",
        .dataset = "PredicateAllEqual",
        .body =
            Query<IntervalRange<T>>{
                .args = {.lo = values.representative,
                         .lo_inc = true,
                         .hi = values.representative,
                         .hi_inc = true},
            },
    });
    cases.Add(IndexTestCase<T>{
        .name = "IntervalOpenAllEqual",
        .dataset = "PredicateAllEqual",
        .body =
            Query<IntervalRange<T>>{
                .args = {.lo = values.representative,
                         .lo_inc = false,
                         .hi = values.representative,
                         .hi_inc = false},
            },
    });
}

template <typename T>
void
AddTypeCases(IndexTestCases& cases) {
    AddEdgeMembershipCases<T>(cases);
    AddUnaryRangeCases<T>(cases);
    AddIntervalRangeCases<T>(cases);
    AddInputShapeCases<T>(cases);
}

template <typename T>
struct HighCardinalityCaseValues {
    std::vector<PredicateTestValue<T>> keys;
    PredicateTestValue<T> threshold;
    PredicateTestValue<T> lo;
    PredicateTestValue<T> hi;
};

template <typename T>
HighCardinalityCaseValues<T>
HighCardinalityValues() {
    if constexpr (std::is_same_v<T, int8_t>) {
        return {
            .keys = {static_cast<int8_t>(-100),
                     static_cast<int8_t>(7),
                     static_cast<int8_t>(99),
                     static_cast<int8_t>(127)},
            .threshold = static_cast<int8_t>(0),
            .lo = static_cast<int8_t>(-10),
            .hi = static_cast<int8_t>(10),
        };
    } else if constexpr (std::is_same_v<T, std::string_view>) {
        return {
            .keys = {"value_0", "value_999", "missing"},
            .threshold = "value_1000",
            .lo = "value_10",
            .hi = "value_100",
        };
    } else {
        return {
            .keys = {static_cast<T>(-1000),
                     static_cast<T>(7),
                     static_cast<T>(999),
                     static_cast<T>(2000)},
            .threshold = static_cast<T>(0),
            .lo = static_cast<T>(-10),
            .hi = static_cast<T>(10),
        };
    }
}

template <typename T>
void
AddHighCardinalityCases(IndexTestCases& cases) {
    const auto values = HighCardinalityValues<T>();
    cases.Add(IndexTestCase<T>{
        .name = "InHighCardinality",
        .dataset = "TenThousandHighCardinality",
        .body =
            Query<In<T>>{
                .args = {.keys = values.keys},
            },
    });
    cases.Add(IndexTestCase<T>{
        .name = "NotInHighCardinality",
        .dataset = "TenThousandHighCardinality",
        .body =
            Query<NotIn<T>>{
                .args = {.keys = values.keys},
            },
    });
    cases.Add(IndexTestCase<T>{
        .name = "UnaryHighCardinality",
        .dataset = "TenThousandHighCardinality",
        .body =
            Query<UnaryRange<T>>{
                .args = {.value = values.threshold,
                         .op = CompareOp::GreaterEqual},
            },
    });
    cases.Add(IndexTestCase<T>{
        .name = "IntervalHighCardinality",
        .dataset = "TenThousandHighCardinality",
        .body =
            Query<IntervalRange<T>>{
                .args = {.lo = values.lo,
                         .lo_inc = true,
                         .hi = values.hi,
                         .hi_inc = false},
            },
    });
}

template <typename T>
void
AddInfinityCases(IndexTestCases& cases) {
    static_assert(std::is_floating_point_v<T>);
    const auto negative = -std::numeric_limits<T>::infinity();
    const auto positive = std::numeric_limits<T>::infinity();

    cases.Add(IndexTestCase<T>{
        .name = "InBothInfinities",
        .dataset = "PredicateFloatInfinities",
        .body =
            Query<In<T>>{
                .args = {.keys = {negative, positive}},
            },
    });
    cases.Add(IndexTestCase<T>{
        .name = "NotInBothInfinities",
        .dataset = "PredicateFloatInfinities",
        .body =
            Query<NotIn<T>>{
                .args = {.keys = {negative, positive}},
            },
    });
    cases.Add(IndexTestCase<T>{
        .name = "UnaryGreaterThanNegativeInfinity",
        .dataset = "PredicateFloatInfinities",
        .body =
            Query<UnaryRange<T>>{
                .args = {.value = negative, .op = CompareOp::GreaterThan},
            },
    });
    cases.Add(IndexTestCase<T>{
        .name = "UnaryLessThanPositiveInfinity",
        .dataset = "PredicateFloatInfinities",
        .body =
            Query<UnaryRange<T>>{
                .args = {.value = positive, .op = CompareOp::LessThan},
            },
    });
    cases.Add(IndexTestCase<T>{
        .name = "IntervalClosedInfinities",
        .dataset = "PredicateFloatInfinities",
        .body =
            Query<IntervalRange<T>>{
                .args = {.lo = negative,
                         .lo_inc = true,
                         .hi = positive,
                         .hi_inc = true},
            },
    });
    cases.Add(IndexTestCase<T>{
        .name = "IntervalOpenInfinities",
        .dataset = "PredicateFloatInfinities",
        .body =
            Query<IntervalRange<T>>{
                .args = {.lo = negative,
                         .lo_inc = false,
                         .hi = positive,
                         .hi_inc = false},
            },
    });
}

const IndexTestCases&
PredicateCases() {
    static const auto cases = [] {
        IndexTestCases cases;

        AddTypeCases<bool>(cases);
        AddTypeCases<int8_t>(cases);
        AddTypeCases<int16_t>(cases);
        AddTypeCases<int32_t>(cases);
        AddTypeCases<int64_t>(cases);
        AddTypeCases<float>(cases);
        AddTypeCases<double>(cases);
        AddTypeCases<std::string_view>(cases);

        cases.Add(IndexTestCase<bool>{
            .name = "InFalseAllFalse",
            .dataset = "PredicateAllFalse",
            .body =
                Query<In<bool>>{
                    .args = {.keys = {false}},
                },
        });
        cases.Add(IndexTestCase<bool>{
            .name = "InTrueAllFalse",
            .dataset = "PredicateAllFalse",
            .body =
                Query<In<bool>>{
                    .args = {.keys = {true}},
                },
        });
        cases.Add(IndexTestCase<bool>{
            .name = "NotInFalseAllFalse",
            .dataset = "PredicateAllFalse",
            .body =
                Query<NotIn<bool>>{
                    .args = {.keys = {false}},
                },
        });
        cases.Add(IndexTestCase<bool>{
            .name = "NotInTrueAllFalse",
            .dataset = "PredicateAllFalse",
            .body =
                Query<NotIn<bool>>{
                    .args = {.keys = {true}},
                },
        });
        cases.Add(IndexTestCase<bool>{
            .name = "NotInTrueAllTrue",
            .dataset = "PredicateAllEqual",
            .body =
                Query<NotIn<bool>>{
                    .args = {.keys = {true}},
                },
        });

        AddHighCardinalityCases<int8_t>(cases);
        AddHighCardinalityCases<int16_t>(cases);
        AddHighCardinalityCases<int32_t>(cases);
        AddHighCardinalityCases<int64_t>(cases);
        AddHighCardinalityCases<float>(cases);
        AddHighCardinalityCases<double>(cases);
        AddHighCardinalityCases<std::string_view>(cases);

        AddInfinityCases<float>(cases);
        AddInfinityCases<double>(cases);

        cases.Add(IndexTestCase<int64_t>{
            .name = "InLargeRowCount",
            .dataset = "HundredThousandRows",
            .body =
                Query<In<int64_t>>{
                    .args = {.keys = {7, 31, 7}},
                },
        });
        cases.Add(IndexTestCase<int64_t>{
            .name = "NotInLargeRowCount",
            .dataset = "HundredThousandRows",
            .body =
                Query<NotIn<int64_t>>{
                    .args = {.keys = {7, 31, 7}},
                },
        });
        cases.Add(IndexTestCase<int64_t>{
            .name = "IntervalLargeRowCount",
            .dataset = "HundredThousandRows",
            .body =
                Query<IntervalRange<int64_t>>{
                    .args =
                        {.lo = 7, .lo_inc = true, .hi = 31, .hi_inc = false},
                },
        });
        cases.Add(IndexTestCase<int64_t>{
            .name = "Membership",
            .dataset = "HundredThousandRows",
            .body =
                QueryBatch<int64_t>{
                    {"In",
                     Query<In<int64_t>>{
                         .args = {.keys = {7, 31}},
                     }},
                    {"NotIn",
                     Query<NotIn<int64_t>>{
                         .args = {.keys = {7, 31}},
                     }},
                },
        });

        const auto ints = CaseValues<int64_t>();
        cases.Add(IndexTestCase<int64_t>{
            .name = "InAcrossBatches",
            .dataset = "PredicateEdgesMultiBatch",
            .body =
                Query<In<int64_t>>{
                    .args = {.keys = {ints.low, ints.nullable_duplicate}},
                },
        });
        cases.Add(IndexTestCase<int64_t>{
            .name = "NotInAcrossBatches",
            .dataset = "PredicateEdgesMultiBatch",
            .body =
                Query<NotIn<int64_t>>{
                    .args = {.keys = {ints.low, ints.nullable_duplicate}},
                },
        });
        cases.Add(IndexTestCase<int64_t>{
            .name = "UnaryAcrossBatches",
            .dataset = "PredicateEdgesMultiBatch",
            .body =
                Query<UnaryRange<int64_t>>{
                    .args = {.value = ints.middle,
                             .op = CompareOp::GreaterEqual},
                },
        });
        cases.Add(IndexTestCase<int64_t>{
            .name = "IntervalAcrossBatches",
            .dataset = "PredicateEdgesMultiBatch",
            .body =
                Query<IntervalRange<int64_t>>{
                    .args = {.lo = ints.low,
                             .lo_inc = true,
                             .hi = ints.high,
                             .hi_inc = true},
                },
        });
        cases.Add(IndexTestCase<int64_t>{
            .name = "InAcrossEmptyBatches",
            .dataset = "PredicateEdgesWithEmptyBatches",
            .body =
                Query<In<int64_t>>{
                    .args = {.keys = {ints.low, ints.nullable_duplicate}},
                },
        });

        const auto strings = CaseValues<std::string_view>();
        const std::string embedded_nul("a\0b", 3);
        cases.Add(IndexTestCase<std::string_view>{
            .name = "InEmptyString",
            .dataset = "PredicateEdges",
            .body =
                Query<In<std::string_view>>{
                    .args = {.keys = {""}},
                },
        });
        cases.Add(IndexTestCase<std::string_view>{
            .name = "InPrefixValueIsExact",
            .dataset = "PredicateEdges",
            .body =
                Query<In<std::string_view>>{
                    .args = {.keys = {"a"}},
                },
        });
        cases.Add(IndexTestCase<std::string_view>{
            .name = "InEmbeddedNulIsLengthAware",
            .dataset = "PredicateEdges",
            .body =
                Query<In<std::string_view>>{
                    .args = {.keys = {embedded_nul}},
                },
        });
        cases.Add(IndexTestCase<std::string_view>{
            .name = "NotInEmbeddedNulIsLengthAware",
            .dataset = "PredicateEdges",
            .body =
                Query<NotIn<std::string_view>>{
                    .args = {.keys = {embedded_nul}},
                },
        });
        cases.Add(IndexTestCase<std::string_view>{
            .name = "UnaryEqualEmbeddedNulIsLengthAware",
            .dataset = "PredicateEdges",
            .body =
                Query<UnaryRange<std::string_view>>{
                    .args = {.value = embedded_nul, .op = CompareOp::Equal},
                },
        });
        cases.Add(IndexTestCase<std::string_view>{
            .name = "UnaryNotEqualEmbeddedNulIsLengthAware",
            .dataset = "PredicateEdges",
            .body =
                Query<UnaryRange<std::string_view>>{
                    .args = {.value = embedded_nul, .op = CompareOp::NotEqual},
                },
        });
        cases.Add(IndexTestCase<std::string_view>{
            .name = "IntervalEmbeddedNulIsLengthAware",
            .dataset = "PredicateEdges",
            .body =
                Query<IntervalRange<std::string_view>>{
                    .args = {.lo = embedded_nul,
                             .lo_inc = true,
                             .hi = embedded_nul,
                             .hi_inc = true},
                },
        });
        cases.Add(IndexTestCase<int64_t>{
            .name = "InManualOffsets",
            .dataset = "RepeatedNullable",
            .body =
                Query<In<int64_t>>{
                    .args = {.keys = {10, 99}},
                    .expected = ManualHits({0, 3}),
                },
        });
        cases.Add(IndexTestCase<int64_t>{
            .name = "InManualNoHits",
            .dataset = "RepeatedNullable",
            .body =
                Query<In<int64_t>>{
                    .args = {.keys = {99}},
                    .expected = ManualHits({}),
                },
        });
        cases.Add(IndexTestCase<std::string_view>{
            .name = "InAcrossBatches",
            .dataset = "PredicateEdgesMultiBatch",
            .body =
                Query<In<std::string_view>>{
                    .args = {.keys = {strings.low, strings.nullable_duplicate}},
                },
        });
        cases.Add(IndexTestCase<std::string_view>{
            .name = "NotInAcrossBatches",
            .dataset = "PredicateEdgesMultiBatch",
            .body =
                Query<NotIn<std::string_view>>{
                    .args = {.keys = {strings.low, strings.nullable_duplicate}},
                },
        });
        cases.Add(IndexTestCase<std::string_view>{
            .name = "UnaryAcrossBatches",
            .dataset = "PredicateEdgesMultiBatch",
            .body =
                Query<UnaryRange<std::string_view>>{
                    .args = {.value = strings.middle,
                             .op = CompareOp::GreaterEqual},
                },
        });
        cases.Add(IndexTestCase<std::string_view>{
            .name = "IntervalAcrossBatches",
            .dataset = "PredicateEdgesMultiBatch",
            .body =
                Query<IntervalRange<std::string_view>>{
                    .args = {.lo = strings.low,
                             .lo_inc = true,
                             .hi = strings.high,
                             .hi_inc = true},
                },
        });

        return cases;
    }();
    return cases;
}

class ScalarPredicateReaderTest : public ::testing::TestWithParam<FilterParam> {
};

TEST_P(ScalarPredicateReaderTest, MatchesExpectedOffsets) {
    GetParam().run();
}

INSTANTIATE_TEST_SUITE_P(ScalarReaders,
                         ScalarPredicateReaderTest,
                         ::testing::ValuesIn(PredicateCases().All()),
                         FilterParamName);

}  // namespace
}  // namespace milvus::index::test
