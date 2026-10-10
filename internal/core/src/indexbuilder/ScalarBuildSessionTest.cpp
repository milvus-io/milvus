// Copyright(C) 2019 - 2020 Zilliz.All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <gtest/gtest.h>

#include <array>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "common/Array.h"
#include "index/Families.h"
#include "index/IndexTypeAdapter.h"
#include "index/Meta.h"
#include "index/PackedIndexLoad.h"
#include "index/contracts/Registry.h"
#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/IScalarPredicateReader.h"
#include "indexbuilder/BuildSession.h"
#include "indexbuilder/IndexBuildCapiAdapter.h"
#include "storage/InsertData.h"
#include "storage/PayloadReader.h"
#include "storage/Util.h"
#include "storage/artifact/LocalDirectory.h"

namespace milvus::indexbuilder {
namespace {

class ScalarBuildSessionTest : public ::testing::Test {
 protected:
    void SetUp() override {
        root_ = storage::LocalDirectory::CreateOwned(
            std::filesystem::temp_directory_path().string(),
            "scalar-build-session-XXXXXX", "scalar build session test");
    }

    proto::indexcgo::BuildIndexInfo
    Info(DataType field_type, const char* index_type) const {
        proto::indexcgo::BuildIndexInfo info;
        info.set_collectionid(1);
        info.set_partitionid(2);
        info.set_segmentid(3);
        info.set_buildid(1000);
        info.set_index_version(1);
        info.set_current_scalar_index_version(3);
        auto* field = info.mutable_field_schema();
        field->set_fieldid(101);
        field->set_name("values");
        field->set_data_type(static_cast<proto::schema::DataType>(field_type));
        auto* param = info.add_index_params();
        param->set_key(index::INDEX_TYPE);
        param->set_value(index_type);
        if (std::string_view(index_type) == index::HYBRID_INDEX_TYPE) {
            // These fixtures have three distinct valid values; retain bitmap
            // routing while exercising the complete build-session input.
            param = info.add_index_params();
            param->set_key(index::BITMAP_INDEX_CARDINALITY_LIMIT);
            param->set_value("4");
        }
        info.mutable_storage_config()->set_storage_type("local");
        info.mutable_storage_config()->set_root_path(root_->Path());
        return info;
    }

    std::string
    WriteBinlog(const storage::FileManagerContext& context,
                const FieldDataPtr& field) const {
        const auto path = root_->Path() + "/insert/1";
        auto payload = std::make_shared<storage::PayloadReader>(field);
        storage::InsertData insert(payload);
        insert.SetFieldDataMeta(context.fieldDataMeta);
        insert.SetTimestamps(0, 100);
        auto bytes = insert.Serialize(storage::Remote);
        context.chunkManagerPtr->Write(path, bytes.data(), bytes.size());
        return path;
    }

    index::IIndexReaderBasePtr
    BuildAndLoad(PreparedBuild prepared) const {
        const auto family = prepared.request.family;
        const auto params = prepared.request.params;
        const auto context = prepared.file_manager_context;
        prepared.request.staging_parent = root_->Path() + "/build";
        BuildSession session(std::move(prepared.request), context);
        session.BuildFromSource();
        const auto stats = session.Publish();
        EXPECT_GT(stats.MemSize(), 0);
        EXPECT_EQ(stats.Files().size(), 1);
        if (stats.Files().size() != 1) {
            return nullptr;
        }
        storage::LoadOptions options;
        options.params = params;
        auto source = index::InspectPackedIndexFile(
            {stats.Files()[0].file_name}, context);
        const auto resolved_family =
            index::ResolvePackedLoadFamily(family, source->IndexMeta(), params);
        return index::LoaderRegistry::Instance().Lookup(resolved_family).Load(
            {index::OpenedIndexSource{index::PackedIndexSource{
                 std::shared_ptr<storage::AsyncIndexEntryReader>(
                     std::move(source))}},
             std::move(options)});
    }

    std::shared_ptr<storage::LocalDirectory> root_;
};

template <typename T>
class ScalarMissingBinlogPrefixTest : public ScalarBuildSessionTest {};
using PrefixTypes = ::testing::Types<int8_t, int16_t, int32_t, int64_t, std::string>;
TYPED_TEST_SUITE(ScalarMissingBinlogPrefixTest, PrefixTypes);

template <typename T>
constexpr DataType PrefixType() {
    if constexpr (std::is_same_v<T, int8_t>) return DataType::INT8;
    if constexpr (std::is_same_v<T, int16_t>) return DataType::INT16;
    if constexpr (std::is_same_v<T, int32_t>) return DataType::INT32;
    if constexpr (std::is_same_v<T, int64_t>) return DataType::INT64;
    return DataType::VARCHAR;
}

template <typename T>
T PrefixValue(int value) {
    if constexpr (std::is_same_v<T, std::string>) {
        return std::to_string(value);
    } else {
        return static_cast<T>(value);
    }
}

TYPED_TEST(ScalarMissingBinlogPrefixTest, HybridFillsNullOrDefaultBeforeSourceRows) {
    using T = TypeParam;
    using QueryT = std::conditional_t<std::is_same_v<T, std::string>, std::string_view, T>;
    constexpr int64_t missing = 100;
    const std::vector<T> values{PrefixValue<T>(5), PrefixValue<T>(30),
                                PrefixValue<T>(10), PrefixValue<T>(5),
                                PrefixValue<T>(30), PrefixValue<T>(10)};
    const std::array<uint8_t, 1> validity{0b00010101};
    for (const bool with_default : {false, true}) {
        SCOPED_TRACE(with_default);
        auto info = this->Info(PrefixType<T>(), index::HYBRID_INDEX_TYPE);
        info.set_num_rows(missing + values.size());
        info.set_lack_binlog_rows(missing);
        auto* field = info.mutable_field_schema();
        field->set_nullable(true);
        if (with_default) {
            auto* value = field->mutable_default_value();
            if constexpr (std::is_same_v<T, std::string>) value->set_string_data("10");
            else if constexpr (std::is_same_v<T, int64_t>) value->set_long_data(10);
            else value->set_int_data(10);
        }
        // Serialize a real binlog, then adapt the complete wire request.
        auto prepared = AdaptBuildIndexInfo(info, BuildPurpose::ScalarIndex);
        // The session derives the missing prefix from the decoded source rows.
        EXPECT_EQ(prepared.request.expected_rows -
                      static_cast<int64_t>(values.size()),
                  missing);
        auto data = storage::CreateFieldData(PrefixType<T>(), DataType::NONE, true, 1, values.size());
        data->FillFieldData(values.data(), validity.data(), values.size(), 0);
        const auto path = this->WriteBinlog(prepared.file_manager_context, data);
        info.add_insert_files(path);
        prepared = AdaptBuildIndexInfo(info, BuildPurpose::ScalarIndex);
        auto reader = this->BuildAndLoad(std::move(prepared));
        ASSERT_NE(reader, nullptr);
        ASSERT_EQ(reader->Count(), missing + values.size());
        EXPECT_EQ(reader->CoordDomain(), index::Domain::Row);
        const auto* predicate = dynamic_cast<const index::IScalarPredicateReader<QueryT>*>(reader.get());
        const auto* nulls = dynamic_cast<const index::INullReader*>(reader.get());
        ASSERT_NE(predicate, nullptr);
        ASSERT_NE(nulls, nullptr);
        const T default_value = PrefixValue<T>(10);
        const QueryT key = default_value;
        const auto in = predicate->In(1, &key);
        const auto not_in = predicate->NotIn(1, &key);
        const auto is_null = nulls->IsNull();
        const auto is_not_null = nulls->IsNotNull();
        for (int64_t row = 0; row < reader->Count(); ++row) {
            const bool valid = row < missing ? with_default : ((row - missing) % 2 == 0);
            const T& value = row < missing ? default_value : values[row - missing];
            EXPECT_EQ(in[row], valid && value == default_value) << row;
            EXPECT_EQ(not_in[row], valid && value != default_value) << row;
            EXPECT_EQ(is_null[row], !valid) << row;
            EXPECT_EQ(is_not_null[row], valid) << row;
        }
        for (const auto op : {index::CompareOp::LessThan, index::CompareOp::LessEqual,
                              index::CompareOp::GreaterThan, index::CompareOp::GreaterEqual}) {
            const auto hits = predicate->Range(key, op);
            ASSERT_EQ(hits.size(), reader->Count());
            for (int64_t row = 0; row < reader->Count(); ++row) {
                const bool valid = row < missing ? with_default : ((row - missing) % 2 == 0);
                const T& value = row < missing ? default_value : values[row - missing];
                const bool hit = op == index::CompareOp::LessThan ? value < default_value :
                                 op == index::CompareOp::LessEqual ? value <= default_value :
                                 op == index::CompareOp::GreaterThan ? value > default_value : value >= default_value;
                EXPECT_EQ(hits[row], valid && hit) << row;
            }
        }
        for (const bool lower_inclusive : {false, true}) {
            for (const bool upper_inclusive : {false, true}) {
                const T lower = PrefixValue<T>(10);
                const T upper = PrefixValue<T>(30);
                const auto hits = predicate->Range(QueryT(lower), lower_inclusive, QueryT(upper), upper_inclusive);
                ASSERT_EQ(hits.size(), reader->Count());
                for (int64_t row = 0; row < reader->Count(); ++row) {
                    const bool valid = row < missing ? with_default : ((row - missing) % 2 == 0);
                    const T& value = row < missing ? default_value : values[row - missing];
                    const bool hit = (lower_inclusive ? value >= lower : value > lower) &&
                                     (upper_inclusive ? value <= upper : value < upper);
                    EXPECT_EQ(hits[row], valid && hit) << row;
                }
            }
        }
    }
}

TEST_F(ScalarBuildSessionTest, StructArraySubFieldKeepsNestedBitmapAndHybridRouting) {
    for (const auto value_type : {DataType::INT32, DataType::VARCHAR}) {
      for (const auto* index_type : {index::BITMAP_INDEX_TYPE, index::HYBRID_INDEX_TYPE}) {
        SCOPED_TRACE(::testing::Message() << index_type << "/" << static_cast<int>(value_type));
        auto info = Info(DataType::ARRAY, index_type);
        info.set_num_rows(4);
        auto* field = info.mutable_field_schema();
        field->set_name("profile[values]");
        field->set_element_type(static_cast<proto::schema::DataType>(value_type));
        auto prepared = AdaptBuildIndexInfo(info, BuildPurpose::ScalarIndex);
        EXPECT_EQ(prepared.request.family, std::string(index_type) == index::BITMAP_INDEX_TYPE ? index::families::kBitmap : index::families::kHybrid);
        EXPECT_EQ(prepared.request.value_type, value_type);
        EXPECT_EQ(prepared.request.params.at("nested"), true);
        std::vector<Array> arrays;
        arrays.reserve(4);
        for (const auto& values : std::vector<std::vector<int32_t>>{{10, 20}, {}, {}, {20, 30}}) {
            proto::schema::ScalarField scalar;
            if (value_type == DataType::VARCHAR) {
                for (const auto value : values) scalar.mutable_string_data()->add_data(std::to_string(value));
                if (values.empty()) scalar.mutable_string_data();
            } else {
                for (const auto value : values) scalar.mutable_int_data()->add_data(value);
                if (values.empty()) scalar.mutable_int_data();
            }
            arrays.emplace_back(scalar);
        }
        auto data = storage::CreateFieldData(DataType::ARRAY, value_type, false);
        data->FillFieldData(arrays.data(), arrays.size());
        info.add_insert_files(WriteBinlog(prepared.file_manager_context, data));
        prepared = AdaptBuildIndexInfo(info, BuildPurpose::ScalarIndex);
        auto reader = BuildAndLoad(std::move(prepared));
        ASSERT_NE(reader, nullptr);
        EXPECT_EQ(reader->CoordDomain(), index::Domain::Element);
        EXPECT_EQ(reader->Count(), 4);
        auto check = [&](const auto& key) {
            using T = std::decay_t<decltype(key)>;
            const auto* predicate = dynamic_cast<const index::IScalarPredicateReader<T>*>(reader.get());
            ASSERT_NE(predicate, nullptr);
            const auto hits = predicate->In(1, &key);
            ASSERT_EQ(hits.size(), 4);
            EXPECT_FALSE(hits[0]);
            EXPECT_TRUE(hits[1]);
            EXPECT_TRUE(hits[2]);
            EXPECT_FALSE(hits[3]);
        };
        if (value_type == DataType::VARCHAR) check(std::string_view("20"));
        else check(int32_t{20});
      }
    }
}

}  // namespace
}  // namespace milvus::indexbuilder
