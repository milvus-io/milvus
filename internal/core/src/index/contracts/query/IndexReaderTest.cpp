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

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

#include "index/contracts/query/IJsonIndexReader.h"
#include "index/contracts/query/INgramReader.h"
#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/IPatternMatchReader.h"
#include "index/contracts/query/IScalarPredicateReader.h"
#include "index/contracts/query/IScalarValueReader.h"
#include "index/contracts/query/ISpatialReader.h"
#include "index/contracts/query/ITextMatchReader.h"
#include "index/test_utils/CaseTestDriver.h"

namespace milvus::index::test {
namespace {

template <typename Interface>
bool
Implements(const IIndexReaderBase& reader) {
    return dynamic_cast<const Interface*>(&reader) != nullptr;
}

template <typename T>
void
ExpectInterfacesMatchCaps(const IIndexReaderBase& reader) {
    const auto caps = reader.Caps();
    EXPECT_EQ(Implements<IScalarPredicateReader<T>>(reader), caps.predicate);
    EXPECT_EQ(Implements<IPatternMatchReader>(reader), caps.pattern_match);
    EXPECT_EQ(Implements<ITextMatchReader>(reader), caps.text_match);
    EXPECT_EQ(Implements<INgramReader>(reader), caps.ngram_candidates);
    EXPECT_EQ(Implements<ISpatialReader>(reader), caps.spatial);
    EXPECT_EQ(Implements<IScalarValueReader<T>>(reader), caps.value_lookup);
    EXPECT_EQ(Implements<IJsonIndexReader>(reader), caps.json_paths);

    // INullReader is independent of ReaderCaps. Every centrally registered
    // scalar, candidate, and concrete text profile in this matrix provides it.
    EXPECT_TRUE(Implements<INullReader>(reader));
}

template <typename T>
void
ObserveReaderMetadata(const ReaderBackend& backend,
                      const ScalarTestData<T>& data,
                      IIndexReaderBasePtr& reader) {
    ASSERT_NE(reader, nullptr);
    EXPECT_EQ(reader->Count(), data.values.size());
    EXPECT_EQ(reader->CoordDomain(), data.domain);
    EXPECT_EQ(reader->ValueType(), backend.ExpectedValueType());

    const auto caps = reader->Caps();
    EXPECT_EQ(caps.nested, data.domain == Domain::Element);
    EXPECT_FALSE(caps.cheap_value_lookup && !caps.value_lookup);
    if (caps.nested || caps.ngram_candidates || caps.spatial) {
        EXPECT_FALSE(caps.exact);
    }

    ExpectInterfacesMatchCaps<T>(*reader);

    const auto first_memory = reader->MemoryUsage();
    const auto first_usage = reader->CellByteSize();
    EXPECT_GE(first_memory, 0);
    EXPECT_GE(first_usage.memory_bytes, 0);
    EXPECT_GE(first_usage.file_bytes, 0);
    EXPECT_EQ(reader->MemoryUsage(), first_memory);
    const auto second_usage = reader->CellByteSize();
    EXPECT_EQ(second_usage.memory_bytes, first_usage.memory_bytes);
    EXPECT_EQ(second_usage.file_bytes, first_usage.file_bytes);
}

template <typename T>
void
AddReaderMetadataCases(IndexTestCases& cases) {
    cases.Add(IndexTestCase<T>{
        .name = "RowMetadataAndInterfaces",
        .dataset = "PredicateSingleRow",
        .input_lifetime = InputLifetime::ReleaseBeforeBody,
        .body = Observe<T>{.run = ObserveReaderMetadata<T>},
    });
    cases.Add(IndexTestCase<T>{
        .name = "ElementMetadataAndInterfaces",
        .dataset = "NestedElements",
        .input_shape = BackendInputShape::NestedElements,
        .domain = Domain::Element,
        .input_lifetime = InputLifetime::ReleaseBeforeBody,
        .body = Observe<T>{.run = ObserveReaderMetadata<T>},
    });
}

const IndexTestCases&
ReaderCases() {
    static const auto cases = [] {
        IndexTestCases result;
        AddReaderMetadataCases<bool>(result);
        AddReaderMetadataCases<int8_t>(result);
        AddReaderMetadataCases<int16_t>(result);
        AddReaderMetadataCases<int32_t>(result);
        AddReaderMetadataCases<int64_t>(result);
        AddReaderMetadataCases<float>(result);
        AddReaderMetadataCases<double>(result);
        AddReaderMetadataCases<std::string_view>(result);
        return result;
    }();
    return cases;
}

class IndexReaderTest : public ::testing::TestWithParam<FilterParam> {};

TEST_P(IndexReaderTest, ReportsConsistentMetadataAndInterfaces) {
    GetParam().run();
}

INSTANTIATE_TEST_SUITE_P(ScalarReaders,
                         IndexReaderTest,
                         ::testing::ValuesIn(ReaderCases().All()),
                         FilterParamName);

}  // namespace
}  // namespace milvus::index::test
