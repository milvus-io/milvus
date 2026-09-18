// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the
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

#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace milvus::index::test {
namespace {

void
SetJsonDocumentMetadata(ScalarTestData<std::string_view>& data,
                        std::string_view root = {}) {
    data.metadata["json_path"] = root;
}

template <typename T>
void
SetProjectedMetadata(ScalarTestData<T>& data,
                     std::vector<size_t> non_exist_offsets,
                     std::string_view path = "/a") {
    data.metadata["json_path"] = path;
    data.metadata["non_exist_offsets"] = std::move(non_exist_offsets);
}

template <typename T>
OwnedArrayValue
FixedArray(DataType element_type, std::initializer_list<T> values) {
    return OwnedArrayValue::FromFixed<T>(
        element_type, std::span<const T>(values.begin(), values.size()));
}

OwnedArrayValue
StringArray(std::initializer_list<std::string_view> values) {
    std::vector<std::string> owned;
    owned.reserve(values.size());
    for (const auto value : values) {
        owned.emplace_back(value);
    }
    return OwnedArrayValue::FromStrings(DataType::VARCHAR, owned);
}

ScalarTestData<std::string_view>
MakeJsonEmployees() {
    ScalarTestData<std::string_view> data({
        R"({"profile":{"name":{"first":"Alice","last":"Smith","preferred_name":"Al"},"team":{"name":"Engineering","supervisor":{"name":"Bob"}},"is_active":true,"employee_id":1001,"skills":["cpp","rust","python"],"scores":[95,88,92]}})",
        R"({"profile":{"name":{"first":"Bob","last":"Johnson","preferred_name":null},"team":{"name":"Product","supervisor":{"name":"Charlie"}},"is_active":false,"employee_id":1002,"skills":["java","python"],"scores":[85,90]}})",
        R"({"profile":{"name":{"first":"Charlie","last":"Williams"},"team":{"name":"Design","supervisor":{"name":"Alice"}},"is_active":true,"employee_id":1003,"skills":["python","javascript"],"scores":[87,91,89]}})",
    });
    data.validity_present = false;
    SetJsonDocumentMetadata(data);
    return data;
}

ScalarTestData<std::string_view>
MakeJsonTypeFamilies() {
    ScalarTestData<std::string_view> data({
        R"({"a":1})",
        R"({"a":1.5})",
        R"({"a":"one"})",
        R"({"a":true})",
        R"({"a":[2,3]})",
        R"({"a":["two"]})",
        R"({"a":[false]})",
        R"({"a":{"b":1}})",
        R"({"a":[]})",
        R"({"a":null})",
        R"({})",
        R"({"a":"a\u0000b"})",
        R"({"a":"猫"})",
        R"({"a":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"})",
    });
    data.validity_present = false;
    SetJsonDocumentMetadata(data);
    return data;
}

ScalarTestData<std::string_view>
MakeJsonPrecision() {
    ScalarTestData<std::string_view> data({
        R"({"a":-10})",
        R"({"a":1})",
        R"({"a":10})",
        R"({"a":10.5})",
        R"({"a":9223372036854775808})",
        R"({"a":18446744073709551615})",
        R"({"a":"1"})",
        R"({})",
    });
    data.validity_present = false;
    SetJsonDocumentMetadata(data);
    return data;
}

ScalarTestData<std::string_view>
MakeJsonFieldNullable() {
    ScalarTestData<std::string_view> data({
        R"({"a":"alpha"})",
        R"({"a":"ignored"})",
        R"({"a":"beta"})",
        R"({"a":null})",
        R"({})",
    });
    data.validity.reset(1);
    data.batch_sizes = {0, 2, 0, 3, 0};
    SetJsonDocumentMetadata(data);
    return data;
}

ScalarTestData<std::string_view>
MakeJsonRootPrefix() {
    ScalarTestData<std::string_view> data({
        R"({"profile":{"name":"Alice","active":true},"profiled":{"name":"wrong"}})",
        R"({"profile":{"name":"Bob","active":false}})",
        R"({"profile":{"name":"猫","active":true}})",
    });
    data.validity_present = false;
    SetJsonDocumentMetadata(data, "/profile");
    return data;
}

ScalarTestData<std::string_view>
MakeJsonEscapedPaths() {
    ScalarTestData<std::string_view> data({
        R"({"a/b":"slash","m~n":"tilde","arr":["zero","one"]})",
        R"({"a/b":"other","m~n":1,"arr":[]})",
        R"({})",
    });
    data.validity_present = false;
    SetJsonDocumentMetadata(data);
    return data;
}

ScalarTestData<std::string_view>
MakeJsonAllMissing() {
    ScalarTestData<std::string_view> data({R"({"b":1})", R"({})", R"(42)"});
    data.validity_present = false;
    SetJsonDocumentMetadata(data);
    return data;
}

ScalarTestData<double>
MakeProjectedDoubleTriState(bool multi_batch) {
    ScalarTestData<double> data({1.0, 2.0, 0.0, 0.0, 0.0, 3.0});
    for (const auto offset : {1U, 2U, 3U, 4U}) {
        data.validity.reset(offset);
    }
    if (multi_batch) {
        data.batch_sizes = {0, 2, 0, 1, 3, 0};
    }
    SetProjectedMetadata(data, {1, 2, 4});
    return data;
}

ScalarTestData<double>
MakeProjectedDoubleAllValid() {
    ScalarTestData<double> data({-10.0, 1.0, 10.0, 10.5, 42.0});
    data.validity_present = false;
    SetProjectedMetadata(data, {});
    return data;
}

ScalarTestData<bool>
MakeProjectedBoolTriState() {
    ScalarTestData<bool> data({true, false, false, false, false, true});
    for (const auto offset : {1U, 2U, 3U, 4U}) {
        data.validity.reset(offset);
    }
    SetProjectedMetadata(data, {1, 2, 4});
    return data;
}

ScalarTestData<bool>
MakeProjectedBoolAllValid() {
    ScalarTestData<bool> data({true, false, true, false});
    data.validity_present = false;
    SetProjectedMetadata(data, {});
    return data;
}

ScalarTestData<std::string_view>
MakeProjectedVarcharTriState() {
    ScalarTestData<std::string_view> data(
        {"alpha", "missing", "null", "bad", "field", "beta"});
    for (const auto offset : {1U, 2U, 3U, 4U}) {
        data.validity.reset(offset);
    }
    SetProjectedMetadata(data, {1, 2, 4});
    return data;
}

ScalarTestData<std::string_view>
MakeProjectedVarcharAllValid() {
    ScalarTestData<std::string_view> data(
        {"", "alpha", "alphabet", std::string("a\0b", 3), "猫", "beta"});
    data.validity_present = false;
    SetProjectedMetadata(data, {});
    return data;
}

ScalarTestData<JsonProjectedString>
MakeProjectedNgramTriState() {
    using State = JsonProjectedStringState;
    ScalarTestData<JsonProjectedString> data({
        {"alpha", State::Value},
        {"", State::NoValue},
        {"", State::NoValue},
        {"", State::NoValue},
        {"", State::FieldNull},
        {"alphabet", State::Value},
    });
    data.validity_present = false;
    SetProjectedMetadata(data, {1, 2, 4});
    return data;
}

ScalarTestData<JsonProjectedString>
MakeProjectedNgramAllValid() {
    using State = JsonProjectedStringState;
    ScalarTestData<JsonProjectedString> data({
        {"alpha", State::Value},
        {"alphabet", State::Value},
        {"beta", State::Value},
        {"猫咪", State::Value},
    });
    data.validity_present = false;
    SetProjectedMetadata(data, {});
    return data;
}

ScalarTestData<ArrayView>
MakeProjectedArrayBool() {
    ScalarTestData<ArrayView> data({
        FixedArray<bool>(DataType::BOOL, {true, false}),
        FixedArray<bool>(DataType::BOOL, {}),
        FixedArray<bool>(DataType::BOOL, {false}),
        FixedArray<bool>(DataType::BOOL, {true, true}),
        FixedArray<bool>(DataType::BOOL, {}),
        FixedArray<bool>(DataType::BOOL, {}),
        FixedArray<bool>(DataType::BOOL, {}),
    });
    for (const auto offset : {4U, 5U, 6U}) {
        data.validity.reset(offset);
    }
    data.batch_sizes = {2, 0, 2, 3};
    SetProjectedMetadata(data, {4, 5, 6});
    return data;
}

ScalarTestData<ArrayView>
MakeProjectedArrayDouble() {
    ScalarTestData<ArrayView> data({
        FixedArray<double>(DataType::DOUBLE, {1.0, 2.0}),
        FixedArray<double>(DataType::DOUBLE, {}),
        FixedArray<double>(DataType::DOUBLE, {3.5, 4.0}),
        FixedArray<double>(DataType::DOUBLE, {2.0, 2.0}),
        FixedArray<double>(DataType::DOUBLE, {}),
        FixedArray<double>(DataType::DOUBLE, {}),
        FixedArray<double>(DataType::DOUBLE, {}),
    });
    for (const auto offset : {4U, 5U, 6U}) {
        data.validity.reset(offset);
    }
    data.batch_sizes = {2, 0, 2, 3};
    SetProjectedMetadata(data, {4, 5, 6});
    return data;
}

ScalarTestData<ArrayView>
MakeProjectedArrayVarchar() {
    ScalarTestData<ArrayView> data({
        StringArray({"alpha", "beta"}),
        StringArray({}),
        StringArray({"beta", "gamma"}),
        StringArray({"alpha", "alpha"}),
        StringArray({}),
        StringArray({}),
        StringArray({}),
    });
    for (const auto offset : {4U, 5U, 6U}) {
        data.validity.reset(offset);
    }
    data.batch_sizes = {2, 0, 2, 3};
    SetProjectedMetadata(data, {4, 5, 6});
    return data;
}

}  // namespace

void
RegisterJsonDataSets(DataCatalog& catalog) {
    catalog.Add<std::string_view>({
        .name = "JsonEmployees",
        .input_shape = BackendInputShape::JsonDocument,
        .logical_value_type = DataType::JSON,
        .make_data = MakeJsonEmployees,
    });
    catalog.Add<std::string_view>({
        .name = "JsonTypeFamilies",
        .input_shape = BackendInputShape::JsonDocument,
        .logical_value_type = DataType::JSON,
        .make_data = MakeJsonTypeFamilies,
    });
    catalog.Add<std::string_view>({
        .name = "JsonPrecision",
        .input_shape = BackendInputShape::JsonDocument,
        .logical_value_type = DataType::JSON,
        .make_data = MakeJsonPrecision,
    });
    catalog.Add<std::string_view>({
        .name = "JsonFieldNullable",
        .requires_nullable = true,
        .input_shape = BackendInputShape::JsonDocument,
        .logical_value_type = DataType::JSON,
        .make_data = MakeJsonFieldNullable,
    });
    catalog.Add<std::string_view>({
        .name = "JsonRootPrefix",
        .input_shape = BackendInputShape::JsonDocument,
        .logical_value_type = DataType::JSON,
        .make_data = MakeJsonRootPrefix,
    });
    catalog.Add<std::string_view>({
        .name = "JsonEscapedPaths",
        .input_shape = BackendInputShape::JsonDocument,
        .logical_value_type = DataType::JSON,
        .make_data = MakeJsonEscapedPaths,
    });
    catalog.Add<std::string_view>({
        .name = "JsonAllMissing",
        .input_shape = BackendInputShape::JsonDocument,
        .logical_value_type = DataType::JSON,
        .make_data = MakeJsonAllMissing,
    });

    catalog.Add<double>({
        .name = "JsonProjectedDoubleTriState",
        .requires_nullable = true,
        .input_shape = BackendInputShape::JsonProjected,
        .logical_value_type = DataType::DOUBLE,
        .make_data = [] { return MakeProjectedDoubleTriState(false); },
    });
    catalog.Add<double>({
        .name = "JsonProjectedDoubleMultiBatch",
        .requires_nullable = true,
        .input_shape = BackendInputShape::JsonProjected,
        .logical_value_type = DataType::DOUBLE,
        .make_data = [] { return MakeProjectedDoubleTriState(true); },
    });
    catalog.Add<double>({
        .name = "JsonProjectedDoubleAllValid",
        .input_shape = BackendInputShape::JsonProjected,
        .logical_value_type = DataType::DOUBLE,
        .make_data = MakeProjectedDoubleAllValid,
    });
    catalog.Add<bool>({
        .name = "JsonProjectedBoolTriState",
        .requires_nullable = true,
        .input_shape = BackendInputShape::JsonProjected,
        .logical_value_type = DataType::BOOL,
        .make_data = MakeProjectedBoolTriState,
    });
    catalog.Add<bool>({
        .name = "JsonProjectedBoolAllValid",
        .input_shape = BackendInputShape::JsonProjected,
        .logical_value_type = DataType::BOOL,
        .make_data = MakeProjectedBoolAllValid,
    });
    catalog.Add<std::string_view>({
        .name = "JsonProjectedVarcharTriState",
        .requires_nullable = true,
        .input_shape = BackendInputShape::JsonProjected,
        .logical_value_type = DataType::VARCHAR,
        .make_data = MakeProjectedVarcharTriState,
    });
    catalog.Add<std::string_view>({
        .name = "JsonProjectedVarcharAllValid",
        .input_shape = BackendInputShape::JsonProjected,
        .logical_value_type = DataType::VARCHAR,
        .make_data = MakeProjectedVarcharAllValid,
    });
    catalog.Add<JsonProjectedString>({
        .name = "JsonProjectedNgramTriState",
        .requires_nullable = true,
        .input_shape = BackendInputShape::JsonProjected,
        .logical_value_type = DataType::VARCHAR,
        .make_data = MakeProjectedNgramTriState,
    });
    catalog.Add<JsonProjectedString>({
        .name = "JsonProjectedNgramAllValid",
        .input_shape = BackendInputShape::JsonProjected,
        .logical_value_type = DataType::VARCHAR,
        .make_data = MakeProjectedNgramAllValid,
    });
    catalog.Add<ArrayView>({
        .name = "JsonProjectedArrayBool",
        .requires_nullable = true,
        .input_shape = BackendInputShape::JsonProjected,
        .logical_value_type = DataType::BOOL,
        .make_data = MakeProjectedArrayBool,
    });
    catalog.Add<ArrayView>({
        .name = "JsonProjectedArrayDouble",
        .requires_nullable = true,
        .input_shape = BackendInputShape::JsonProjected,
        .logical_value_type = DataType::DOUBLE,
        .make_data = MakeProjectedArrayDouble,
    });
    catalog.Add<ArrayView>({
        .name = "JsonProjectedArrayVarchar",
        .requires_nullable = true,
        .input_shape = BackendInputShape::JsonProjected,
        .logical_value_type = DataType::VARCHAR,
        .make_data = MakeProjectedArrayVarchar,
    });
}

}  // namespace milvus::index::test
