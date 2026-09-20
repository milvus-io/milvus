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

#include "index/test_utils/ScalarTestData.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <random>
#include <span>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace milvus::index::test {
namespace {

template <typename T>
ScalarTestData<T>
MakePredicateEdges() {
    static_assert(std::is_integral_v<T> && !std::is_same_v<T, bool>);
    ScalarTestData<T> data({std::numeric_limits<T>::lowest(),
                            static_cast<T>(-1),
                            static_cast<T>(0),
                            static_cast<T>(1),
                            std::numeric_limits<T>::max(),
                            std::numeric_limits<T>::max()});
    data.validity.reset(4);
    return data;
}

template <>
ScalarTestData<bool>
MakePredicateEdges<bool>() {
    ScalarTestData<bool> data({false, true, false, true, true, false});
    data.validity.reset(4);
    return data;
}

template <>
ScalarTestData<float>
MakePredicateEdges<float>() {
    ScalarTestData<float> data({std::numeric_limits<float>::lowest(),
                                -1.5F,
                                -0.0F,
                                0.0F,
                                1.5F,
                                std::numeric_limits<float>::max(),
                                std::numeric_limits<float>::max()});
    data.validity.reset(5);
    return data;
}

template <>
ScalarTestData<double>
MakePredicateEdges<double>() {
    ScalarTestData<double> data({std::numeric_limits<double>::lowest(),
                                 -1.5,
                                 -0.0,
                                 0.0,
                                 1.5,
                                 std::numeric_limits<double>::max(),
                                 std::numeric_limits<double>::max()});
    data.validity.reset(5);
    return data;
}

template <>
ScalarTestData<std::string_view>
MakePredicateEdges<std::string_view>() {
    const std::string long_value(80, 'x');
    ScalarTestData<std::string_view> data({"",
                                           "a",
                                           "ab",
                                           std::string("a\0b", 3),
                                           "\xE7\x8C\xAB",
                                           long_value,
                                           long_value});
    data.validity.reset(5);
    return data;
}

template <typename T>
ScalarTestValue<T>
RepresentativeValue() {
    if constexpr (std::is_same_v<T, bool>) {
        return true;
    } else if constexpr (std::is_same_v<T, std::string_view>) {
        return "single";
    } else if constexpr (std::is_floating_point_v<T>) {
        return static_cast<T>(1.5);
    } else {
        return static_cast<T>(1);
    }
}

template <typename T>
void
AddPredicateDataSets(DataCatalog& catalog) {
    catalog.Add<T>({
        .name = "PredicateEdges",
        .requires_nullable = true,
        .make_data = [] { return MakePredicateEdges<T>(); },
    });
    catalog.Add<T>({
        .name = "PredicateAllValid",
        .make_data =
            [] {
                auto edges = MakePredicateEdges<T>();
                ScalarTestData<T> data(std::move(edges.values));
                data.validity_present = false;
                return data;
            },
    });
    catalog.Add<T>({
        .name = "PredicateAllNull",
        .requires_nullable = true,
        .make_data =
            [] {
                auto data = MakePredicateEdges<T>();
                data.validity.reset();
                return data;
            },
    });
    catalog.Add<T>({
        .name = "PredicateSingleRow",
        .make_data =
            [] { return ScalarTestData<T>({RepresentativeValue<T>()}); },
    });
    catalog.Add<T>({
        .name = "PredicateAllEqual",
        .make_data =
            [] {
                const auto value = RepresentativeValue<T>();
                return ScalarTestData<T>({value, value, value, value});
            },
    });
    catalog.Add<T>({
        .name = "PredicateEdgesMultiBatch",
        .requires_nullable = true,
        .make_data =
            [] {
                auto data = MakePredicateEdges<T>();
                data.batch_sizes = {2, 1, data.values.size() - 3};
                return data;
            },
    });
}

template <typename T>
ScalarTestData<T>
MakeTenThousandHighCardinality() {
    ScalarTestData<T> data(std::vector<T>(10'000));
    for (size_t i = 0; i < data.values.size(); ++i) {
        const auto value = static_cast<int64_t>(i % 2000) - 1000;
        data.values[i] = static_cast<T>(value);
    }
    data.validity_present = false;
    return data;
}

template <>
ScalarTestData<int8_t>
MakeTenThousandHighCardinality<int8_t>() {
    ScalarTestData<int8_t> data(std::vector<int8_t>(10'000));
    for (size_t i = 0; i < data.values.size(); ++i) {
        data.values[i] =
            static_cast<int8_t>(static_cast<int64_t>(i % 200) - 100);
    }
    data.validity_present = false;
    return data;
}

template <>
ScalarTestData<std::string_view>
MakeTenThousandHighCardinality<std::string_view>() {
    std::vector<std::string> values;
    values.reserve(10'000);
    for (size_t i = 0; i < 10'000; ++i) {
        values.push_back("value_" + std::to_string(i % 2000));
    }
    ScalarTestData<std::string_view> data(std::move(values));
    data.validity_present = false;
    return data;
}

template <typename T>
void
AddHighCardinalityDataSet(DataCatalog& catalog) {
    catalog.Add<T>({
        .name = "TenThousandHighCardinality",
        .make_data = [] { return MakeTenThousandHighCardinality<T>(); },
    });
}

ScalarTestData<std::string_view>
MakePatternStringsNullable() {
    ScalarTestData<std::string_view> data({
        "apple",
        "application",
        "apply",
        "banana",
        "band",
        "cat",
        "category",
        "dog",
        "application",
        "",
        "apple",
        "hello_world",
        "new_world",
        "world_peace",
        "hello",
        "world",
        "say hello",
        "a_b",
        "axb",
        "a%b",
        "ab",
        "abc",
        "aXc",
        "a1c",
        "abcd",
        "ac",
        "abbc",
        "100%",
        "100percent",
        "50%off",
        "10%_off",
        "10%aoff",
        "10%boff",
        "10a_off",
        "path\\to\\file",
        "caf\xC3\xA9",
        "\xE4\xBD\xA0\xE5\xA5\xBD\xE4\xB8\x96\xE7\x95\x8C",
        "hello\xE4\xBD\xA0\xE5\xA5\xBDworld",
        "emoji\xF0\x9F\x98\x80test",
        "a\xC3\xA9"
        "b",
        "a\tb",
        "a\nb",
        "a\r\nb",
        "   ",
        "!@#$",
        "aaaaa",
        "ababab",
        std::string(200, 'a'),
        std::string(300, 'a'),
        std::string(199, 'a'),
        "say Hello",
        "say HELLO",
        "user_123@gmail.com",
        "file.txt",
        "fileTtxt",
        "()[]{}stuff",
        "an error occurred",
        "noerrorhere",
        "xyzabcdef",
        "123abc456",
        "ABc",
        "tested",
        "testing",
        "tester",
        "pretesting",
        "hello world",
        "world hello",
        "worldwide",
        "revolution",
        "new peace",
        "%a",
        "hello%a",
        "helloa",
        "_",
        "\xE7\x9C\x9F\xE5\xA5\xBD",
        "file_name.txt",
        "%percent%",
        "_underscore_",
        "exact",
        "not exact",
        "fgk",
        "abcdefg",
        "abcfg",
        "abceg",
        "ababc",
        "abababc",
        "\xC2\xA5"
        "100",
        "$100",
        "hello\xF0\x9F\x98\x80",
        "smtp://mail.com",
        "https://example.com",
        "ftp://files.com",
        "\xE4\xB8\xAD\xE6\x96\x87"
        "123",
        "a\rb",
        "xabcy",
        "xacy",
        "error: connection timeout",
        "timeout then error",
        "a\\b",
        "a/b",
        "a.b.c",
        "aXbXc",
        "abc\ndef",
        "abcXdef",
        "a\n\n\nz",
    });
    data.validity.reset(10);
    return data;
}

ScalarTestData<std::string_view>
MakePatternBinaryNullable() {
    ScalarTestData<std::string_view> data({
        "ab",
        "hello",
        "",
        std::string("a\0b", 3),
        std::string("\0hello", 6),
        std::string("hello\0", 6),
        std::string("a\0\0b", 4),
        std::string("\0", 1),
        std::string("\0\0", 2),
        std::string("a\0b\0c", 5),
        std::string("he\0llo", 6),
        std::string("a\0b", 3),
    });
    data.validity.reset(11);
    return data;
}

ScalarTestData<std::string_view>
MakePatternAllNull() {
    ScalarTestData<std::string_view> data(
        {"apple", "", "\xE4\xBD\xA0\xE5\xA5\xBD", std::string("a\0b", 3)});
    data.validity.reset();
    return data;
}

ScalarTestData<std::string_view>
MakePatternSelective() {
    std::vector<std::string> values;
    values.reserve(1000);
    for (size_t i = 0; i < 1000; ++i) {
        std::string value(500, 'x');
        if (i % 2 == 0) {
            value += "COMMON";
        }
        if (i % 250 == 0) {
            value += "ZEBRA";
        }
        if (i >= 100 && (i - 100) % 250 == 0) {
            value = "QOP" + value;
        }
        values.push_back(std::move(value));
    }
    return ScalarTestData<std::string_view>(std::move(values));
}

ScalarTestData<std::string_view>
MakePatternRandomBytes() {
    std::mt19937 rng(0xF3A1u);
    const auto random_string = [&rng](size_t max_length, uint32_t alphabet) {
        const auto length = static_cast<size_t>(rng()) % (max_length + 1);
        std::string value;
        value.reserve(length);
        for (size_t i = 0; i < length; ++i) {
            value.push_back(static_cast<char>(rng() % alphabet));
        }
        return value;
    };

    std::vector<std::string> values;
    values.reserve(300);
    for (size_t i = 0; i < 280; ++i) {
        values.push_back(random_string(12, 4));
    }
    for (size_t i = 0; i < 20; ++i) {
        values.push_back(random_string(20, 256));
    }
    return ScalarTestData<std::string_view>(std::move(values));
}

ScalarTestData<std::string_view>
MakePatternHighCardinality() {
    std::vector<std::string> values;
    values.reserve(1000);
    for (size_t i = 0; i < 1000; ++i) {
        auto digits = std::to_string(i);
        std::string value = "key_";
        value.append(4 - digits.size(), '0');
        value += digits;
        values.push_back(std::move(value));
    }
    return ScalarTestData<std::string_view>(std::move(values));
}

template <typename T>
void
AddNestedElementsDataSet(DataCatalog& catalog) {
    catalog.Add<T>({
        .name = "NestedElements",
        .input_shape = BackendInputShape::NestedElements,
        .domain = Domain::Element,
        .make_data =
            [] {
                auto edges = MakePredicateEdges<T>();
                ScalarTestData<T> data(std::move(edges.values));
                data.validity_present = false;
                data.domain = Domain::Element;
                return data;
            },
    });
}

template <typename T>
ScalarTestData<ArrayView>
MakeArrayRows(DataType element_type, bool nullable) {
    using Storage = std::conditional_t<std::is_same_v<T, int8_t> ||
                                           std::is_same_v<T, int16_t>,
                                       int32_t,
                                       T>;
    const std::array<Storage, 3> first = {
        static_cast<Storage>(1),
        static_cast<Storage>(2),
        static_cast<Storage>(2),
    };
    const std::array<Storage, 1> invalid = {static_cast<Storage>(9)};
    const std::array<Storage, 2> last = {
        static_cast<Storage>(2),
        static_cast<Storage>(3),
    };
    ScalarTestData<ArrayView> data({
        OwnedArrayValue::FromFixed<Storage>(element_type, first),
        OwnedArrayValue::FromFixed<Storage>(element_type,
                                            std::span<const Storage>{}),
        OwnedArrayValue::FromFixed<Storage>(element_type, invalid),
        OwnedArrayValue::FromFixed<Storage>(element_type, last),
    });
    data.batch_sizes = {0, 2, 0, 2, 0};
    if (nullable) {
        data.validity.reset(2);
    } else {
        data.validity_present = false;
    }
    return data;
}

template <>
ScalarTestData<ArrayView>
MakeArrayRows<std::string_view>(DataType element_type, bool nullable) {
    ScalarTestData<ArrayView> data({
        OwnedArrayValue::FromStrings(element_type, {"a", "b", "b"}),
        OwnedArrayValue::FromStrings(element_type, {}),
        OwnedArrayValue::FromStrings(element_type, {"ignored"}),
        OwnedArrayValue::FromStrings(element_type, {"b", "c"}),
    });
    data.batch_sizes = {0, 2, 0, 2, 0};
    if (nullable) {
        data.validity.reset(2);
    } else {
        data.validity_present = false;
    }
    return data;
}

template <typename T>
void
AddArrayRowsDataSets(DataCatalog& catalog,
                     std::string_view type_name,
                     DataType element_type) {
    const auto prefix = "ArrayRows" + std::string(type_name);
    catalog.Add<ArrayView>({
        .name = prefix + "Nullable",
        .requires_nullable = true,
        .input_shape = BackendInputShape::ArrayRows,
        .logical_value_type = element_type,
        .make_data =
            [element_type] { return MakeArrayRows<T>(element_type, true); },
    });
    catalog.Add<ArrayView>({
        .name = prefix + "AllValid",
        .input_shape = BackendInputShape::ArrayRows,
        .logical_value_type = element_type,
        .make_data =
            [element_type] { return MakeArrayRows<T>(element_type, false); },
    });
}

}  // namespace

const DataCatalog&
ScalarDataSets() {
    static const auto datasets = [] {
        DataCatalog catalog;

        AddPredicateDataSets<bool>(catalog);
        AddPredicateDataSets<int8_t>(catalog);
        AddPredicateDataSets<int16_t>(catalog);
        AddPredicateDataSets<int32_t>(catalog);
        AddPredicateDataSets<int64_t>(catalog);
        AddPredicateDataSets<float>(catalog);
        AddPredicateDataSets<double>(catalog);
        AddPredicateDataSets<std::string_view>(catalog);

        AddNestedElementsDataSet<bool>(catalog);
        AddNestedElementsDataSet<int8_t>(catalog);
        AddNestedElementsDataSet<int16_t>(catalog);
        AddNestedElementsDataSet<int32_t>(catalog);
        AddNestedElementsDataSet<int64_t>(catalog);
        AddNestedElementsDataSet<float>(catalog);
        AddNestedElementsDataSet<double>(catalog);
        AddNestedElementsDataSet<std::string_view>(catalog);

        AddArrayRowsDataSets<bool>(catalog, "Bool", DataType::BOOL);
        AddArrayRowsDataSets<int8_t>(catalog, "Int8", DataType::INT8);
        AddArrayRowsDataSets<int16_t>(catalog, "Int16", DataType::INT16);
        AddArrayRowsDataSets<int32_t>(catalog, "Int32", DataType::INT32);
        AddArrayRowsDataSets<int64_t>(catalog, "Int64", DataType::INT64);
        AddArrayRowsDataSets<float>(catalog, "Float", DataType::FLOAT);
        AddArrayRowsDataSets<double>(catalog, "Double", DataType::DOUBLE);
        AddArrayRowsDataSets<std::string_view>(
            catalog, "Varchar", DataType::VARCHAR);

        AddHighCardinalityDataSet<int8_t>(catalog);
        AddHighCardinalityDataSet<int16_t>(catalog);
        AddHighCardinalityDataSet<int32_t>(catalog);
        AddHighCardinalityDataSet<int64_t>(catalog);
        AddHighCardinalityDataSet<float>(catalog);
        AddHighCardinalityDataSet<double>(catalog);
        AddHighCardinalityDataSet<std::string_view>(catalog);
        catalog.Add<bool>({
            .name = "PredicateAllFalse",
            .make_data =
                [] {
                    ScalarTestData<bool> data({false, false, false, false});
                    data.validity_present = false;
                    return data;
                },
        });
        catalog.Add<float>({
            .name = "PredicateFloatInfinities",
            .make_data =
                [] {
                    ScalarTestData<float> data({
                        -std::numeric_limits<float>::infinity(),
                        -1.5F,
                        0.0F,
                        1.5F,
                        std::numeric_limits<float>::infinity(),
                    });
                    data.validity_present = false;
                    return data;
                },
        });
        catalog.Add<double>({
            .name = "PredicateFloatInfinities",
            .make_data =
                [] {
                    ScalarTestData<double> data({
                        -std::numeric_limits<double>::infinity(),
                        -1.5,
                        0.0,
                        1.5,
                        std::numeric_limits<double>::infinity(),
                    });
                    data.validity_present = false;
                    return data;
                },
        });

        catalog.Add<int64_t>({
            .name = "RepeatedNullable",
            .requires_nullable = true,
            .make_data =
                [] {
                    ScalarTestData<int64_t> data({10, 10, 30, 10});
                    data.validity.reset(1);
                    return data;
                },
        });
        catalog.Add<int64_t>({
            .name = "HundredThousandRows",
            .make_data =
                [] {
                    ScalarTestData<int64_t> data(std::vector<int64_t>(100'000));
                    for (size_t i = 0; i < data.values.size(); ++i) {
                        data.values[i] = static_cast<int64_t>(i % 1000);
                    }
                    data.validity_present = false;
                    return data;
                },
        });
        catalog.Add<int64_t>({
            .name = "PredicateEdgesWithEmptyBatches",
            .requires_nullable = true,
            .make_data =
                [] {
                    auto data = MakePredicateEdges<int64_t>();
                    data.batch_sizes = {0, 2, 0, 1, 3, 0};
                    return data;
                },
        });
        catalog.Add<int64_t>({
            .name = "BitBoundaryNullable",
            .requires_nullable = true,
            .make_data =
                [] {
                    ScalarTestData<int64_t> data(std::vector<int64_t>(70));
                    for (size_t i = 0; i < data.values.size(); ++i) {
                        data.values[i] = static_cast<int64_t>(i) - 35;
                    }
                    for (const auto offset : {62U, 63U, 64U, 65U}) {
                        data.validity.reset(offset);
                    }
                    data.batch_sizes = {3, 60, 1, 1, 5};
                    return data;
                },
        });
        catalog.Add<std::string_view>({
            .name = "RepeatedNullable",
            .requires_nullable = true,
            .make_data =
                [] {
                    const std::string prefix(64, 'x');
                    ScalarTestData<std::string_view> data({
                        prefix + std::to_string(10),
                        prefix + std::to_string(10),
                        prefix + std::to_string(30),
                        prefix + std::to_string(10),
                    });
                    data.validity.reset(1);
                    return data;
                },
        });
        catalog.Add<std::string_view>({
            .name = "NullVsEmptyString",
            .requires_nullable = true,
            .make_data =
                [] {
                    ScalarTestData<std::string_view> data({"", "", "x"});
                    data.validity.reset(1);
                    return data;
                },
        });
        catalog.Add<std::string_view>({
            .name = "PatternStringsNullable",
            .requires_nullable = true,
            .make_data = MakePatternStringsNullable,
        });
        catalog.Add<std::string_view>({
            .name = "PatternBinaryNullable",
            .requires_nullable = true,
            .make_data = MakePatternBinaryNullable,
        });
        catalog.Add<std::string_view>({
            .name = "PatternAllNull",
            .requires_nullable = true,
            .make_data = MakePatternAllNull,
        });
        catalog.Add<std::string_view>({
            .name = "PatternSelective",
            .make_data = MakePatternSelective,
        });
        catalog.Add<std::string_view>({
            .name = "PatternRandomBytes",
            .make_data = MakePatternRandomBytes,
        });
        catalog.Add<std::string_view>({
            .name = "PatternHighCardinality",
            .make_data = MakePatternHighCardinality,
        });

        RegisterTextAndCandidateDataSets(catalog);
        RegisterJsonDataSets(catalog);

        return catalog;
    }();
    return datasets;
}

}  // namespace milvus::index::test
