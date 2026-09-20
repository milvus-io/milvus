// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License
// at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "index/test_utils/ScalarTestData.h"

#include <string>
#include <utility>
#include <vector>

#include "common/Geometry.h"
#include "index/scalar/ngram/JsonProjectedString.h"

namespace milvus::index::test {
namespace {

std::string
Wkb(std::string_view wkt) {
    return Geometry(GetThreadLocalGEOSContext(), std::string(wkt).c_str())
        .to_wkb_string();
}

ScalarTestData<std::string_view>
MakeTextEnglishNullable() {
    ScalarTestData<std::string_view> data({
        "football, basketball, pingpang",
        "",
        "swimming, football",
        "",
        "foo",
        "",
        "bar",
        "",
        "basketball swimming",
        "football pingpang",
        "",
        "football swimming",
    });
    for (const auto offset : {1, 3, 5, 7}) {
        data.validity.reset(offset);
    }
    data.batch_sizes = {3, 3, 2, 4};
    return data;
}

ScalarTestData<std::string_view>
MakeTextEnglishAllValid() {
    ScalarTestData<std::string_view> data({
        "football, basketball, pingpang",
        "swimming, football",
        "basketball swimming",
        "football pingpang",
        "",
        "football swimming",
    });
    data.validity_present = false;
    data.batch_sizes = {2, 1, 3};
    return data;
}

ScalarTestData<std::string_view>
MakeTextAllNull() {
    ScalarTestData<std::string_view> data(
        {"football", "basketball", "青铜时代"});
    data.validity.reset();
    data.batch_sizes = {1, 2};
    return data;
}

ScalarTestData<std::string_view>
MakeTextSingleBatchNullable() {
    ScalarTestData<std::string_view> data({"alpha", "", "beta", ""});
    data.validity.reset(1);
    data.validity.reset(3);
    return data;
}

ScalarTestData<std::string_view>
MakeTextJiebaNullable() {
    ScalarTestData<std::string_view> data({
        "青铜时代",
        "黄金时代",
        "白银时代",
        "黄金",
        "",
    });
    data.validity.reset(4);
    data.batch_sizes = {2, 1, 2};
    return data;
}

ScalarTestData<std::string_view>
MakeTextUnicodeNullable() {
    ScalarTestData<std::string_view> data({
        "café déjà vu",
        "naïve café",
        "你好 世界",
        "你好世界",
        "emoji 😀 test",
        "😀 emoji",
        "punctuation: football!",
        std::string(512, 'x'),
        "",
        "football",
    });
    data.validity.reset(9);
    data.batch_sizes = {4, 3, 3};
    return data;
}

ScalarTestData<std::string_view>
MakeNgramWiki() {
    ScalarTestData<std::string_view> data({
        "'Indira Davelba Murillo Alvarado (Tegucigalpa, the youngest of "
        "eight siblings. She attended primary school at the Escuela 14 de "
        "Julio, and her secondary studies at the Instituto school called "
        "\"Indi del Bosque\", where she taught the children of Honduran "
        "women'",
        "Richmond Green Secondary School is a public secondary school in "
        "Richmond Hill, Ontario, Canada.",
        "The Gymnasium in 2002 Gymnasium Philippinum or Philippinum High "
        "School is an almost 500-year-old secondary school in Marburg, "
        "Hesse, Germany.",
        "Sir Winston Churchill Secondary School is a Canadian secondary "
        "school located in St. Catharines, Ontario.",
        "Sir Winston Churchill Secondary School",
    });
    data.validity_present = false;
    return data;
}

ScalarTestData<std::string_view>
MakeNgramCoreNullable() {
    ScalarTestData<std::string_view> data({
        "hello",
        "hello world",
        "world hello",
        "say hello there",
        "helloworld",
        "worldhello",
        "testing",
        "tested",
        "tester",
        "test case",
        "application",
        "apple pie",
        "pineapple",
        "banana split",
        "aaaa",
        "aaa",
        "aaab",
        "abaa",
        "abab",
        "ababab",
        "xaax",
        "xaaax",
        "elementary school secondary",
        "abc",
        "xabc",
        "abcx",
        "xabcx",
        "",
        "irrelevant",
        "hello world",
        "secondary school",
    });
    data.validity.reset(29);
    data.batch_sizes = {7, 8, 6, 10};
    return data;
}

ScalarTestData<std::string_view>
MakeNgramOverlap() {
    ScalarTestData<std::string_view> data({
        "aa",
        "aaa",
        "aaaa",
        "aaaaa",
        "aaaaaa",
        "ab",
        "aba",
        "abab",
        "ababab",
        "abba",
        "aab",
        "baa",
        "xaax",
        "xaaax",
        "xaaaax",
        "abcabc",
        "abcabcabc",
    });
    data.validity_present = false;
    return data;
}

ScalarTestData<std::string_view>
MakeNgramUtf8() {
    ScalarTestData<std::string_view> data({
        "café latte",
        "hello café",
        "你好世界",
        "test你好test",
        "emoji😀test",
        "normal text",
        "café café",
        "你好你好",
    });
    data.validity_present = false;
    return data;
}

ScalarTestData<std::string_view>
MakeNgramEscapes() {
    ScalarTestData<std::string_view> data({
        "100% complete",
        "50% off sale",
        "file_name.txt",
        "path\\to\\file",
        "normal text",
        "%percent%",
        "_underscore_",
        "test\\escape",
    });
    data.validity_present = false;
    return data;
}

ScalarTestData<JsonProjectedString>
MakeNgramJsonProjected() {
    using State = JsonProjectedStringState;
    ScalarTestData<JsonProjectedString> data({
        {.value = "", .state = State::FieldNull},
        {.value = "", .state = State::NoValue},
        {.value = "Milvus project", .state = State::Value},
        {.value = "Zilliz cloud", .state = State::Value},
        {.value = "Query Node", .state = State::Value},
        {.value = "Data Node", .state = State::Value},
        {.value = "", .state = State::Value},
        {.value = "Milvus", .state = State::Value},
        {.value = "", .state = State::NoValue},
        {.value = "Zilliz cloud", .state = State::Value},
    });
    data.validity_present = false;
    data.batch_sizes = {3, 2, 5};
    data.metadata["non_exist_offsets"] = std::vector<size_t>{0, 1, 8};
    return data;
}

ScalarTestData<std::string_view>
MakeSpatialWkbNullable() {
    ScalarTestData<std::string_view> data({
        Wkb("POINT(0 0)"),
        Wkb("POINT(2 2)"),
        Wkb("POLYGON((0 0,0 3,3 3,3 0,0 0))"),
        Wkb("POLYGON((0 0,0 2,2 2,2 0,0 0))"),
        Wkb("POLYGON((1 1,1 3,3 3,3 1,1 1))"),
        Wkb("POLYGON((4 4,4 5,5 5,5 4,4 4))"),
        Wkb("LINESTRING(-1 0,4 0)"),
        Wkb("POINT(5 5)"),
        Wkb("GEOMETRYCOLLECTION EMPTY"),
        std::string("\x01\x02\x03\x04", 4),
        Wkb("POINT(1 1)"),
        Wkb("POINT(1 1)"),
    });
    data.validity.reset(10);
    data.batch_sizes = {4, 3, 5};
    return data;
}

ScalarTestData<std::string_view>
MakeSpatialWkbAllValid() {
    ScalarTestData<std::string_view> data({
        Wkb("POINT(1 1)"),
        Wkb("POINT(2 2)"),
        Wkb("POINT(3 3)"),
        Wkb("GEOMETRYCOLLECTION EMPTY"),
        std::string("\x01\x02\x03\x04", 4),
    });
    data.validity_present = false;
    data.batch_sizes = {2, 3};
    return data;
}

ScalarTestData<std::string_view>
MakeSpatialAllNull() {
    ScalarTestData<std::string_view> data(
        {Wkb("POINT(0 0)"), Wkb("POINT(1 1)"), Wkb("POINT(2 2)")});
    data.validity.reset();
    data.batch_sizes = {1, 2};
    return data;
}

template <typename T>
ScalarTestData<T>
MakeEmpty() {
    ScalarTestData<T> data({});
    data.validity_present = false;
    return data;
}

}  // namespace

void
RegisterTextAndCandidateDataSets(DataCatalog& catalog) {
    catalog.Add<std::string_view>({
        .name = "TextEnglishNullable",
        .requires_nullable = true,
        .input_shape = BackendInputShape::Scalar,
        .make_data = MakeTextEnglishNullable,
    });
    catalog.Add<std::string_view>({
        .name = "TextEnglishAllValid",
        .input_shape = BackendInputShape::Scalar,
        .make_data = MakeTextEnglishAllValid,
    });
    catalog.Add<std::string_view>({
        .name = "TextAllNull",
        .requires_nullable = true,
        .input_shape = BackendInputShape::Scalar,
        .make_data = MakeTextAllNull,
    });
    catalog.Add<std::string_view>({
        .name = "TextSingleBatchNullable",
        .requires_nullable = true,
        .input_shape = BackendInputShape::Scalar,
        .make_data = MakeTextSingleBatchNullable,
    });
    catalog.Add<std::string_view>({
        .name = "TextEmpty",
        .input_shape = BackendInputShape::Scalar,
        .make_data = MakeEmpty<std::string_view>,
    });
    catalog.Add<std::string_view>({
        .name = "TextJiebaNullable",
        .requires_nullable = true,
        .input_shape = BackendInputShape::Scalar,
        .make_data = MakeTextJiebaNullable,
    });
    catalog.Add<std::string_view>({
        .name = "TextUnicodeNullable",
        .requires_nullable = true,
        .input_shape = BackendInputShape::Scalar,
        .make_data = MakeTextUnicodeNullable,
    });

    catalog.Add<std::string_view>({
        .name = "NgramWiki",
        .input_shape = BackendInputShape::Scalar,
        .make_data = MakeNgramWiki,
    });
    catalog.Add<std::string_view>({
        .name = "NgramCoreNullable",
        .requires_nullable = true,
        .input_shape = BackendInputShape::Scalar,
        .make_data = MakeNgramCoreNullable,
    });
    catalog.Add<std::string_view>({
        .name = "NgramOverlap",
        .input_shape = BackendInputShape::Scalar,
        .make_data = MakeNgramOverlap,
    });
    catalog.Add<std::string_view>({
        .name = "NgramUtf8",
        .input_shape = BackendInputShape::Scalar,
        .make_data = MakeNgramUtf8,
    });
    catalog.Add<std::string_view>({
        .name = "NgramEscapes",
        .input_shape = BackendInputShape::Scalar,
        .make_data = MakeNgramEscapes,
    });
    catalog.Add<std::string_view>({
        .name = "NgramAllNull",
        .requires_nullable = true,
        .input_shape = BackendInputShape::Scalar,
        .make_data =
            [] {
                ScalarTestData<std::string_view> data(
                    {"hello", "testing", "application"});
                data.validity.reset();
                return data;
            },
    });
    catalog.Add<std::string_view>({
        .name = "NgramEmpty",
        .input_shape = BackendInputShape::Scalar,
        .make_data = MakeEmpty<std::string_view>,
    });
    catalog.Add<JsonProjectedString>({
        .name = "NgramJsonProjected",
        .requires_nullable = true,
        .input_shape = BackendInputShape::JsonProjected,
        .logical_value_type = DataType::VARCHAR,
        .make_data = MakeNgramJsonProjected,
    });

    catalog.Add<std::string_view>({
        .name = "SpatialWkbNullable",
        .requires_nullable = true,
        .input_shape = BackendInputShape::SpatialWkb,
        .logical_value_type = DataType::GEOMETRY,
        .make_data = MakeSpatialWkbNullable,
    });
    catalog.Add<std::string_view>({
        .name = "SpatialWkbAllValid",
        .input_shape = BackendInputShape::SpatialWkb,
        .logical_value_type = DataType::GEOMETRY,
        .make_data = MakeSpatialWkbAllValid,
    });
    catalog.Add<std::string_view>({
        .name = "SpatialAllNull",
        .requires_nullable = true,
        .input_shape = BackendInputShape::SpatialWkb,
        .logical_value_type = DataType::GEOMETRY,
        .make_data = MakeSpatialAllNull,
    });
    catalog.Add<std::string_view>({
        .name = "SpatialEmpty",
        .input_shape = BackendInputShape::SpatialWkb,
        .logical_value_type = DataType::GEOMETRY,
        .make_data = MakeEmpty<std::string_view>,
    });
}

}  // namespace milvus::index::test
