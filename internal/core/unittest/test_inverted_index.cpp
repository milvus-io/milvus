// Copyright (C) 2019-2020 Zilliz. All rights reserved.
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
#include <functional>
#include <boost/filesystem.hpp>
#include <unordered_set>

#include "common/Tracer.h"
#include "index/InvertedIndexTantivy.h"
#include "storage/Util.h"
#include "storage/InsertData.h"
#include "indexbuilder/IndexFactory.h"
#include "index/IndexFactory.h"
#include "test_utils/indexbuilder_test_utils.h"
#include "index/Meta.h"

using namespace milvus;

namespace milvus::test {
auto
gen_field_meta(int64_t collection_id = 1,
               int64_t partition_id = 2,
               int64_t segment_id = 3,
               int64_t field_id = 101,
               DataType data_type = DataType::NONE,
               DataType element_type = DataType::NONE,
               bool nullable = false) -> storage::FieldDataMeta {
    auto meta = storage::FieldDataMeta{
        .collection_id = collection_id,
        .partition_id = partition_id,
        .segment_id = segment_id,
        .field_id = field_id,
    };
    meta.field_schema.set_data_type(
        static_cast<proto::schema::DataType>(data_type));
    meta.field_schema.set_element_type(
        static_cast<proto::schema::DataType>(element_type));
    meta.field_schema.set_nullable(nullable);
    return meta;
}

auto
gen_index_meta(int64_t segment_id = 3,
               int64_t field_id = 101,
               int64_t index_build_id = 1000,
               int64_t index_version = 10000) -> storage::IndexMeta {
    return storage::IndexMeta{
        .segment_id = segment_id,
        .field_id = field_id,
        .build_id = index_build_id,
        .index_version = index_version,
    };
}

auto
gen_local_storage_config(const std::string& root_path)
    -> storage::StorageConfig {
    auto ret = storage::StorageConfig{};
    ret.storage_type = "local";
    ret.root_path = root_path;
    return ret;
}

struct ChunkManagerWrapper {
    ChunkManagerWrapper(storage::ChunkManagerPtr cm) : cm_(cm) {
    }

    ~ChunkManagerWrapper() {
        for (const auto& file : written_) {
            cm_->Remove(file);
        }

        boost::filesystem::remove_all(cm_->GetRootPath());
    }

    void
    Write(const std::string& filepath, void* buf, uint64_t len) {
        written_.insert(filepath);
        cm_->Write(filepath, buf, len);
    }

    const storage::ChunkManagerPtr cm_;
    std::unordered_set<std::string> written_;
};
}  // namespace milvus::test

template <typename T,
          DataType dtype,
          DataType element_type = DataType::NONE,
          bool nullable = false>
void
test_run() {
    int64_t collection_id = 1;
    int64_t partition_id = 2;
    int64_t segment_id = 3;
    int64_t field_id = 101;
    int64_t index_build_id = 4000;
    int64_t index_version = 4000;

    auto field_meta = test::gen_field_meta(collection_id,
                                           partition_id,
                                           segment_id,
                                           field_id,
                                           dtype,
                                           element_type,
                                           nullable);
    auto index_meta = test::gen_index_meta(
        segment_id, field_id, index_build_id, index_version);

    std::string root_path = "/tmp/test-inverted-index/";
    auto storage_config = test::gen_local_storage_config(root_path);
    auto cm = storage::CreateChunkManager(storage_config);

    size_t nb = 10000;
    std::vector<T> data_gen;
    boost::container::vector<T> data;
    FixedVector<bool> valid_data;
    if constexpr (!std::is_same_v<T, bool>) {
        data_gen = GenSortedArr<T>(nb);
    } else {
        for (size_t i = 0; i < nb; i++) {
            data_gen.push_back(rand() % 2 == 0);
        }
    }
    if (nullable) {
        valid_data.reserve(nb);
        for (size_t i = 0; i < nb; i++) {
            valid_data.push_back(rand() % 2 == 0);
        }
    }
    for (auto x : data_gen) {
        data.push_back(x);
    }

    auto field_data = storage::CreateFieldData(dtype, nullable);
    if (nullable) {
        int byteSize = (nb + 7) / 8;
        uint8_t* valid_data_ = new uint8_t[byteSize];
        for (int i = 0; i < nb; i++) {
            bool value = valid_data[i];
            int byteIndex = i / 8;
            int bitIndex = i % 8;
            if (value) {
                valid_data_[byteIndex] |= (1 << bitIndex);
            } else {
                valid_data_[byteIndex] &= ~(1 << bitIndex);
            }
        }
        field_data->FillFieldData(data.data(), valid_data_, data.size());
        delete[] valid_data_;
    } else {
        field_data->FillFieldData(data.data(), data.size());
    }
    // std::cout << "length:" << field_data->get_num_rows() << std::endl;
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    insert_data.SetFieldDataMeta(field_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::Remote);

    auto get_binlog_path = [=](int64_t log_id) {
        return fmt::format("{}/{}/{}/{}/{}",
                           collection_id,
                           partition_id,
                           segment_id,
                           field_id,
                           log_id);
    };

    auto log_path = get_binlog_path(0);

    auto cm_w = test::ChunkManagerWrapper(cm);
    cm_w.Write(log_path, serialized_bytes.data(), serialized_bytes.size());

    storage::FileManagerContext ctx(field_meta, index_meta, cm);
    std::vector<std::string> index_files;

    {
        Config config;
        config["index_type"] = milvus::index::INVERTED_INDEX_TYPE;
        config["insert_files"] = std::vector<std::string>{log_path};

        auto index = indexbuilder::IndexFactory::GetInstance().CreateIndex(
            dtype, config, ctx);
        index->Build();

        auto create_index_result = index->Upload();
        auto memSize = create_index_result->GetMemSize();
        auto serializedSize = create_index_result->GetSerializedSize();
        ASSERT_GT(memSize, 0);
        ASSERT_GT(serializedSize, 0);
        index_files = create_index_result->GetIndexFiles();
    }

    {
        index::CreateIndexInfo index_info{};
        index_info.index_type = milvus::index::INVERTED_INDEX_TYPE;
        index_info.field_type = dtype;

        Config config;
        config["index_files"] = index_files;

        ctx.set_for_loading_index(true);
        auto index =
            index::IndexFactory::GetInstance().CreateIndex(index_info, ctx);
        index->Load(milvus::tracer::TraceContext{}, config);

        auto cnt = index->Count();
        ASSERT_EQ(cnt, nb);

        using IndexType = index::ScalarIndex<T>;
        auto real_index = dynamic_cast<IndexType*>(index.get());

        if constexpr (!std::is_floating_point_v<T>) {
            // hard to compare floating-point value.
            {
                boost::container::vector<T> test_data;
                std::unordered_set<T> s;
                size_t nq = 10;
                for (size_t i = 0; i < nq && i < nb; i++) {
                    test_data.push_back(data[i]);
                    s.insert(data[i]);
                }
                auto bitset =
                    real_index->In(test_data.size(), test_data.data());
                ASSERT_EQ(cnt, bitset.size());
                for (size_t i = 0; i < bitset.size(); i++) {
                    if (nullable && !valid_data[i]) {
                        ASSERT_EQ(bitset[i], false);
                    } else {
                        ASSERT_EQ(bitset[i], s.find(data[i]) != s.end());
                    }
                }
            }

            {
                boost::container::vector<T> test_data;
                std::unordered_set<T> s;
                size_t nq = 10;
                for (size_t i = 0; i < nq && i < nb; i++) {
                    test_data.push_back(data[i]);
                    s.insert(data[i]);
                }
                auto bitset =
                    real_index->NotIn(test_data.size(), test_data.data());
                ASSERT_EQ(cnt, bitset.size());
                for (size_t i = 0; i < bitset.size(); i++) {
                    if (nullable && !valid_data[i]) {
                        ASSERT_EQ(bitset[i], false);
                    } else {
                        ASSERT_NE(bitset[i], s.find(data[i]) != s.end());
                    }
                }
            }

            {
                auto bitset = real_index->IsNull();
                ASSERT_EQ(cnt, bitset.size());
                for (size_t i = 0; i < bitset.size(); i++) {
                    if (nullable && !valid_data[i]) {
                        ASSERT_EQ(bitset[i], true);
                    } else {
                        ASSERT_EQ(bitset[i], false);
                    }
                }
            }

            {
                auto bitset = real_index->IsNotNull();
                ASSERT_EQ(cnt, bitset.size());
                for (size_t i = 0; i < bitset.size(); i++) {
                    if (nullable && !valid_data[i]) {
                        ASSERT_EQ(bitset[i], false);
                    } else {
                        ASSERT_EQ(bitset[i], true);
                    }
                }
            }
        }

        using RefFunc = std::function<bool(int64_t)>;

        if constexpr (!std::is_same_v<T, bool>) {
            // range query on boolean is not reasonable.

            {
                std::vector<std::tuple<T, OpType, RefFunc>> test_cases{
                    {20,
                     OpType::GreaterThan,
                     [&](int64_t i) -> bool { return data[i] > 20; }},
                    {20,
                     OpType::GreaterEqual,
                     [&](int64_t i) -> bool { return data[i] >= 20; }},
                    {20,
                     OpType::LessThan,
                     [&](int64_t i) -> bool { return data[i] < 20; }},
                    {20,
                     OpType::LessEqual,
                     [&](int64_t i) -> bool { return data[i] <= 20; }},
                };
                for (const auto& [test_value, op, ref] : test_cases) {
                    auto bitset = real_index->Range(test_value, op);
                    ASSERT_EQ(cnt, bitset.size());
                    for (size_t i = 0; i < nb; i++) {
                        auto ans = bitset[i];
                        auto should = ref(i);
                        if (nullable && !valid_data[i]) {
                            ASSERT_EQ(ans, false);
                        } else {
                            ASSERT_EQ(ans, should)
                                << "op: " << op << ", @" << i
                                << ", ans: " << ans << ", ref: " << should;
                        }
                    }
                }
            }

            {
                std::vector<std::tuple<T, bool, T, bool, RefFunc>> test_cases{
                    {1,
                     false,
                     20,
                     false,
                     [&](int64_t i) -> bool {
                         return 1 < data[i] && data[i] < 20;
                     }},
                    {1,
                     false,
                     20,
                     true,
                     [&](int64_t i) -> bool {
                         return 1 < data[i] && data[i] <= 20;
                     }},
                    {1,
                     true,
                     20,
                     false,
                     [&](int64_t i) -> bool {
                         return 1 <= data[i] && data[i] < 20;
                     }},
                    {1,
                     true,
                     20,
                     true,
                     [&](int64_t i) -> bool {
                         return 1 <= data[i] && data[i] <= 20;
                     }},
                };
                for (const auto& [lb, lb_inclusive, ub, ub_inclusive, ref] :
                     test_cases) {
                    auto bitset =
                        real_index->Range(lb, lb_inclusive, ub, ub_inclusive);
                    ASSERT_EQ(cnt, bitset.size());
                    for (size_t i = 0; i < nb; i++) {
                        auto ans = bitset[i];
                        auto should = ref(i);
                        if (nullable && !valid_data[i]) {
                            ASSERT_EQ(ans, false);
                        } else {
                            ASSERT_EQ(ans, should)
                                << "@" << i << ", ans: " << ans
                                << ", ref: " << should;
                        }
                    }
                }
            }
        }
    }
}

template <bool nullable = false>
void
test_string() {
    using T = std::string;
    DataType dtype = DataType::VARCHAR;

    int64_t collection_id = 1;
    int64_t partition_id = 2;
    int64_t segment_id = 3;
    int64_t field_id = 101;
    int64_t index_build_id = 4001;
    int64_t index_version = 4001;

    auto field_meta = test::gen_field_meta(collection_id,
                                           partition_id,
                                           segment_id,
                                           field_id,
                                           dtype,
                                           DataType::NONE,
                                           nullable);
    auto index_meta = test::gen_index_meta(
        segment_id, field_id, index_build_id, index_version);

    std::string root_path = "/tmp/test-inverted-index/";
    auto storage_config = test::gen_local_storage_config(root_path);
    auto cm = storage::CreateChunkManager(storage_config);

    size_t nb = 10000;
    boost::container::vector<T> data;
    FixedVector<bool> valid_data;
    for (size_t i = 0; i < nb; i++) {
        data.push_back(std::to_string(rand()));
    }
    if (nullable) {
        valid_data.reserve(nb);
        for (size_t i = 0; i < nb; i++) {
            valid_data.push_back(rand() % 2 == 0);
        }
    }

    auto field_data = storage::CreateFieldData(dtype, nullable);
    if (nullable) {
        int byteSize = (nb + 7) / 8;
        uint8_t* valid_data_ = new uint8_t[byteSize];
        for (int i = 0; i < nb; i++) {
            bool value = valid_data[i];
            int byteIndex = i / 8;
            int bitIndex = i % 8;
            if (value) {
                valid_data_[byteIndex] |= (1 << bitIndex);
            } else {
                valid_data_[byteIndex] &= ~(1 << bitIndex);
            }
        }
        field_data->FillFieldData(data.data(), valid_data_, data.size());
        delete[] valid_data_;
    } else {
        field_data->FillFieldData(data.data(), data.size());
    }
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    insert_data.SetFieldDataMeta(field_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::Remote);

    auto get_binlog_path = [=](int64_t log_id) {
        return fmt::format("{}/{}/{}/{}/{}",
                           collection_id,
                           partition_id,
                           segment_id,
                           field_id,
                           log_id);
    };

    auto log_path = get_binlog_path(0);

    auto cm_w = test::ChunkManagerWrapper(cm);
    cm_w.Write(log_path, serialized_bytes.data(), serialized_bytes.size());

    storage::FileManagerContext ctx(field_meta, index_meta, cm);
    std::vector<std::string> index_files;

    {
        Config config;
        config["index_type"] = milvus::index::INVERTED_INDEX_TYPE;
        config["insert_files"] = std::vector<std::string>{log_path};

        auto index = indexbuilder::IndexFactory::GetInstance().CreateIndex(
            dtype, config, ctx);
        index->Build();

        auto create_index_result = index->Upload();
        auto memSize = create_index_result->GetMemSize();
        auto serializedSize = create_index_result->GetSerializedSize();
        ASSERT_GT(memSize, 0);
        ASSERT_GT(serializedSize, 0);
        index_files = create_index_result->GetIndexFiles();
    }

    {
        index::CreateIndexInfo index_info{};
        index_info.index_type = milvus::index::INVERTED_INDEX_TYPE;
        index_info.field_type = dtype;

        Config config;
        config["index_files"] = index_files;

        ctx.set_for_loading_index(true);
        auto index =
            index::IndexFactory::GetInstance().CreateIndex(index_info, ctx);
        index->Load(milvus::tracer::TraceContext{}, config);

        auto cnt = index->Count();
        ASSERT_EQ(cnt, nb);

        using IndexType = index::ScalarIndex<T>;
        auto real_index = dynamic_cast<IndexType*>(index.get());

        {
            boost::container::vector<T> test_data;
            std::unordered_set<T> s;
            size_t nq = 10;
            for (size_t i = 0; i < nq && i < nb; i++) {
                test_data.push_back(data[i]);
                s.insert(data[i]);
            }
            auto bitset = real_index->In(test_data.size(), test_data.data());
            ASSERT_EQ(cnt, bitset.size());
            for (size_t i = 0; i < bitset.size(); i++) {
                if (nullable && !valid_data[i]) {
                    ASSERT_EQ(bitset[i], false);
                } else {
                    ASSERT_EQ(bitset[i], s.find(data[i]) != s.end());
                }
            }
        }

        {
            boost::container::vector<T> test_data;
            std::unordered_set<T> s;
            size_t nq = 10;
            for (size_t i = 0; i < nq && i < nb; i++) {
                test_data.push_back(data[i]);
                s.insert(data[i]);
            }
            auto bitset = real_index->NotIn(test_data.size(), test_data.data());
            ASSERT_EQ(cnt, bitset.size());
            for (size_t i = 0; i < bitset.size(); i++) {
                if (nullable && !valid_data[i]) {
                    ASSERT_EQ(bitset[i], false);
                } else {
                    ASSERT_NE(bitset[i], s.find(data[i]) != s.end());
                }
            }
        }

        using RefFunc = std::function<bool(int64_t)>;

        {
            std::vector<std::tuple<T, OpType, RefFunc>> test_cases{
                {"20",
                 OpType::GreaterThan,
                 [&](int64_t i) -> bool { return data[i] > "20"; }},
                {"20",
                 OpType::GreaterEqual,
                 [&](int64_t i) -> bool { return data[i] >= "20"; }},
                {"20",
                 OpType::LessThan,
                 [&](int64_t i) -> bool { return data[i] < "20"; }},
                {"20",
                 OpType::LessEqual,
                 [&](int64_t i) -> bool { return data[i] <= "20"; }},
            };
            for (const auto& [test_value, op, ref] : test_cases) {
                auto bitset = real_index->Range(test_value, op);
                ASSERT_EQ(cnt, bitset.size());
                for (size_t i = 0; i < bitset.size(); i++) {
                    auto ans = bitset[i];
                    auto should = ref(i);
                    if (nullable && !valid_data[i]) {
                        ASSERT_EQ(ans, false);
                    } else {
                        ASSERT_EQ(ans, should)
                            << "op: " << op << ", @" << i << ", ans: " << ans
                            << ", ref: " << should;
                    }
                }
            }
        }

        {
            std::vector<std::tuple<T, bool, T, bool, RefFunc>> test_cases{
                {"1",
                 false,
                 "20",
                 false,
                 [&](int64_t i) -> bool {
                     return "1" < data[i] && data[i] < "20";
                 }},
                {"1",
                 false,
                 "20",
                 true,
                 [&](int64_t i) -> bool {
                     return "1" < data[i] && data[i] <= "20";
                 }},
                {"1",
                 true,
                 "20",
                 false,
                 [&](int64_t i) -> bool {
                     return "1" <= data[i] && data[i] < "20";
                 }},
                {"1",
                 true,
                 "20",
                 true,
                 [&](int64_t i) -> bool {
                     return "1" <= data[i] && data[i] <= "20";
                 }},
            };
            for (const auto& [lb, lb_inclusive, ub, ub_inclusive, ref] :
                 test_cases) {
                auto bitset =
                    real_index->Range(lb, lb_inclusive, ub, ub_inclusive);
                ASSERT_EQ(cnt, bitset.size());
                for (size_t i = 0; i < nb; i++) {
                    auto ans = bitset[i];
                    auto should = ref(i);
                    if (nullable && !valid_data[i]) {
                        ASSERT_EQ(ans, false);
                    } else {
                        ASSERT_EQ(ans, should) << "@" << i << ", ans: " << ans
                                               << ", ref: " << should;
                    }
                }
            }
        }

        {
            auto dataset = std::make_shared<Dataset>();
            auto prefix = data[0];
            dataset->Set(index::OPERATOR_TYPE, OpType::PrefixMatch);
            dataset->Set(index::MATCH_VALUE, prefix);
            auto bitset = real_index->Query(dataset);
            ASSERT_EQ(cnt, bitset.size());
            for (size_t i = 0; i < bitset.size(); i++) {
                auto should = boost::starts_with(data[i], prefix);
                if (nullable && !valid_data[i]) {
                    should = false;
                }
                ASSERT_EQ(bitset[i], should);
            }
        }

        {
            ASSERT_TRUE(real_index->SupportRegexQuery());
            auto prefix = data[0];
            auto bitset = real_index->RegexQuery(prefix + "(.|\n)*");
            ASSERT_EQ(cnt, bitset.size());
            for (size_t i = 0; i < bitset.size(); i++) {
                auto should = boost::starts_with(data[i], prefix);
                if (nullable && !valid_data[i]) {
                    should = false;
                }
                ASSERT_EQ(bitset[i], should);
            }
        }
    }
}

TEST(InvertedIndex, Naive) {
    test_run<int8_t, DataType::INT8>();
    test_run<int16_t, DataType::INT16>();
    test_run<int32_t, DataType::INT32>();
    test_run<int64_t, DataType::INT64>();

    test_run<bool, DataType::BOOL>();

    test_run<float, DataType::FLOAT>();
    test_run<double, DataType::DOUBLE>();

    test_string();
    test_run<int8_t, DataType::INT8, DataType::NONE, true>();
    test_run<int16_t, DataType::INT16, DataType::NONE, true>();
    test_run<int32_t, DataType::INT32, DataType::NONE, true>();
    test_run<int64_t, DataType::INT64, DataType::NONE, true>();

    test_run<bool, DataType::BOOL, DataType::NONE, true>();

    test_run<float, DataType::FLOAT, DataType::NONE, true>();
    test_run<double, DataType::DOUBLE, DataType::NONE, true>();

    test_string<true>();
}
