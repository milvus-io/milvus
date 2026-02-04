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

#include <boost/algorithm/string/predicate.hpp>
#include <boost/container/vector.hpp>
#include <boost/cstdint.hpp>
#include <boost/filesystem/operations.hpp>
#include <fmt/core.h>
#include <folly/FBVector.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <stdlib.h>
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include "bitset/bitset.h"
#include "bitset/detail/proxy.h"
#include "common/Consts.h"
#include "common/FieldDataInterface.h"
#include "common/Tracer.h"
#include "common/RegexQuery.h"
#include "common/TracerBase.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "gtest/gtest.h"
#include "index/Index.h"
#include "index/InvertedIndexTantivy.h"
#include "index/IndexFactory.h"
#include "index/IndexInfo.h"
#include "index/IndexStats.h"
#include "index/Meta.h"
#include "index/ScalarIndex.h"
#include "indexbuilder/IndexCreatorBase.h"
#include "indexbuilder/IndexFactory.h"
#include "knowhere/dataset.h"
#include "pb/common.pb.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "segcore/Collection.h"
#include "storage/ChunkManager.h"
#include "storage/FileManager.h"
#include "storage/InsertData.h"
#include "storage/PayloadReader.h"
#include "storage/ThreadPools.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "test_utils/DataGen.h"
#include "test_utils/indexbuilder_test_utils.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::segcore;

namespace milvus::test {

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
          bool nullable = false,
          bool has_lack_binlog_row_ = false,
          bool has_default_value_ = false>
void
test_run() {
    int64_t collection_id = 1;
    int64_t partition_id = 2;
    int64_t segment_id = 3;
    int64_t field_id = 101;
    int64_t index_build_id = 4000;
    int64_t index_version = 4000;
    int64_t lack_binlog_row = 100;

    auto field_meta = milvus::segcore::gen_field_meta(collection_id,
                                                      partition_id,
                                                      segment_id,
                                                      field_id,
                                                      dtype,
                                                      element_type,
                                                      nullable);
    auto index_meta =
        gen_index_meta(segment_id, field_id, index_build_id, index_version);

    if (has_default_value_) {
        auto default_value = field_meta.field_schema.mutable_default_value();
        if constexpr (std::is_same_v<int8_t, T> || std::is_same_v<int16_t, T> ||
                      std::is_same_v<int32_t, T>) {
            default_value->set_int_data(20);
        } else if constexpr (std::is_same_v<int64_t, T>) {
            default_value->set_long_data(20);
        } else if constexpr (std::is_same_v<float, T>) {
            default_value->set_float_data(20);
        } else if constexpr (std::is_same_v<double, T>) {
            default_value->set_double_data(20);
        }
    }

    std::string root_path = "/tmp/test-inverted-index/";
    auto storage_config = gen_local_storage_config(root_path);
    auto cm = storage::CreateChunkManager(storage_config);
    auto fs = storage::InitArrowFileSystem(storage_config);

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

    auto field_data = storage::CreateFieldData(dtype, DataType::NONE, nullable);
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
        field_data->FillFieldData(data.data(), valid_data_, data.size(), 0);
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

    storage::FileManagerContext ctx(field_meta, index_meta, cm, fs);
    std::vector<std::string> index_files;

    {
        Config config;
        config["index_type"] = milvus::index::INVERTED_INDEX_TYPE;
        config[INSERT_FILES_KEY] = std::vector<std::string>{log_path};
        config[INDEX_NUM_ROWS_KEY] = nb;
        if (has_lack_binlog_row_) {
            config[INDEX_NUM_ROWS_KEY] = nb + lack_binlog_row;
        }

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
        config[milvus::LOAD_PRIORITY] =
            milvus::proto::common::LoadPriority::HIGH;
        ctx.set_for_loading_index(true);
        auto index =
            index::IndexFactory::GetInstance().CreateIndex(index_info, ctx);
        index->Load(milvus::tracer::TraceContext{}, config);

        auto cnt = index->Count();
        if (has_lack_binlog_row_) {
            ASSERT_EQ(cnt, nb + lack_binlog_row);
        } else {
            ASSERT_EQ(cnt, nb);
        }

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
                size_t start = 0;
                if (has_lack_binlog_row_) {
                    for (int i = 0; i < lack_binlog_row; i++) {
                        if (!has_default_value_) {
                            ASSERT_EQ(bitset[i], false);
                        } else {
                            ASSERT_EQ(bitset[i], s.find(20) != s.end());
                        }
                    }
                    start += lack_binlog_row;
                }
                for (size_t i = start; i < bitset.size(); i++) {
                    if (nullable && !valid_data[i - start]) {
                        ASSERT_EQ(bitset[i], false);
                    } else {
                        ASSERT_EQ(bitset[i],
                                  s.find(data[i - start]) != s.end());
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
                size_t start = 0;
                if (has_lack_binlog_row_) {
                    for (int i = 0; i < lack_binlog_row; i++) {
                        if (!has_default_value_) {
                            ASSERT_EQ(bitset[i], false);
                        } else {
                            ASSERT_EQ(bitset[i], s.find(20) == s.end());
                        }
                    }
                    start += lack_binlog_row;
                }
                for (size_t i = start; i < bitset.size(); i++) {
                    if (nullable && !valid_data[i - start]) {
                        ASSERT_EQ(bitset[i], false);
                    } else {
                        ASSERT_NE(bitset[i],
                                  s.find(data[i - start]) != s.end());
                    }
                }
            }

            {
                auto bitset = real_index->IsNull();
                ASSERT_EQ(cnt, bitset.size());
                size_t start = 0;
                if (has_lack_binlog_row_) {
                    for (int i = 0; i < lack_binlog_row; i++) {
                        if (has_default_value_) {
                            ASSERT_EQ(bitset[i], false);
                        } else {
                            ASSERT_EQ(bitset[i], true);
                        }
                    }
                    start += lack_binlog_row;
                }
                for (size_t i = start; i < bitset.size(); i++) {
                    if (nullable && !valid_data[i - start]) {
                        ASSERT_EQ(bitset[i], true);
                    } else {
                        ASSERT_EQ(bitset[i], false);
                    }
                }
            }

            {
                auto bitset = real_index->IsNotNull();
                ASSERT_EQ(cnt, bitset.size());
                size_t start = 0;
                if (has_lack_binlog_row_) {
                    for (int i = 0; i < lack_binlog_row; i++) {
                        if (has_default_value_) {
                            ASSERT_EQ(bitset[i], true);
                        } else {
                            ASSERT_EQ(bitset[i], false);
                        }
                    }
                    start += lack_binlog_row;
                }

                for (size_t i = start; i < bitset.size(); i++) {
                    if (nullable && !valid_data[i - start]) {
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
                std::vector<std::tuple<T, OpType, RefFunc, bool>> test_cases{
                    {
                        20,
                        OpType::GreaterThan,
                        [&](int64_t i) -> bool { return data[i] > 20; },
                        false,
                    },
                    {
                        20,
                        OpType::GreaterEqual,
                        [&](int64_t i) -> bool { return data[i] >= 20; },
                        true,
                    },
                    {
                        20,
                        OpType::LessThan,
                        [&](int64_t i) -> bool { return data[i] < 20; },
                        false,
                    },
                    {
                        20,
                        OpType::LessEqual,
                        [&](int64_t i) -> bool { return data[i] <= 20; },
                        true,
                    },
                };
                for (const auto& [test_value, op, ref, default_value_res] :
                     test_cases) {
                    auto bitset = real_index->Range(test_value, op);
                    ASSERT_EQ(cnt, bitset.size());
                    size_t start = 0;
                    if (has_lack_binlog_row_) {
                        for (int i = 0; i < lack_binlog_row; i++) {
                            if (has_default_value_) {
                                ASSERT_EQ(bitset[i], default_value_res);
                            } else {
                                ASSERT_EQ(bitset[i], false);
                            }
                        }
                        start += lack_binlog_row;
                    }
                    for (size_t i = start; i < bitset.size(); i++) {
                        auto ans = bitset[i];
                        auto should = ref(i - start);
                        if (nullable && !valid_data[i - start]) {
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
                std::vector<std::tuple<T, bool, T, bool, RefFunc, bool>>
                    test_cases{
                        {
                            1,
                            false,
                            20,
                            false,
                            [&](int64_t i) -> bool {
                                return 1 < data[i] && data[i] < 20;
                            },
                            false,
                        },
                        {
                            1,
                            false,
                            20,
                            true,
                            [&](int64_t i) -> bool {
                                return 1 < data[i] && data[i] <= 20;
                            },
                            true,
                        },
                        {
                            1,
                            true,
                            20,
                            false,
                            [&](int64_t i) -> bool {
                                return 1 <= data[i] && data[i] < 20;
                            },
                            false,
                        },
                        {
                            1,
                            true,
                            20,
                            true,
                            [&](int64_t i) -> bool {
                                return 1 <= data[i] && data[i] <= 20;
                            },
                            true,
                        },
                    };
                for (const auto& [lb,
                                  lb_inclusive,
                                  ub,
                                  ub_inclusive,
                                  ref,
                                  default_value_res] : test_cases) {
                    auto bitset =
                        real_index->Range(lb, lb_inclusive, ub, ub_inclusive);
                    ASSERT_EQ(cnt, bitset.size());
                    size_t start = 0;
                    if (has_lack_binlog_row_) {
                        for (int i = 0; i < lack_binlog_row; i++) {
                            if (has_default_value_) {
                                ASSERT_EQ(bitset[i], default_value_res);
                            } else {
                                ASSERT_EQ(bitset[i], false);
                            }
                        }
                        start += lack_binlog_row;
                    }
                    for (size_t i = start; i < bitset.size(); i++) {
                        auto ans = bitset[i];
                        auto should = ref(i - start);
                        if (nullable && !valid_data[i - start]) {
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

template <bool nullable = false,
          bool has_lack_binlog_row_ = false,
          bool has_default_value_ = false>
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
    int64_t lack_binlog_row = 100;

    auto field_meta = milvus::segcore::gen_field_meta(collection_id,
                                                      partition_id,
                                                      segment_id,
                                                      field_id,
                                                      dtype,
                                                      DataType::NONE,
                                                      nullable);
    auto index_meta =
        gen_index_meta(segment_id, field_id, index_build_id, index_version);

    if (has_default_value_) {
        auto default_value = field_meta.field_schema.mutable_default_value();
        default_value->set_string_data("20");
    }

    std::string root_path = "/tmp/test-inverted-index/";
    auto storage_config = gen_local_storage_config(root_path);
    auto cm = storage::CreateChunkManager(storage_config);
    auto fs = storage::InitArrowFileSystem(storage_config);

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

    auto field_data = storage::CreateFieldData(dtype, DataType::NONE, nullable);
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
        field_data->FillFieldData(data.data(), valid_data_, data.size(), 0);
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

    storage::FileManagerContext ctx(field_meta, index_meta, cm, fs);
    std::vector<std::string> index_files;

    {
        Config config;
        config["index_type"] = milvus::index::INVERTED_INDEX_TYPE;
        config[INSERT_FILES_KEY] = std::vector<std::string>{log_path};
        config[INDEX_NUM_ROWS_KEY] = nb;
        if (has_lack_binlog_row_) {
            config[INDEX_NUM_ROWS_KEY] = nb + lack_binlog_row;
        }

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
        config[milvus::LOAD_PRIORITY] =
            milvus::proto::common::LoadPriority::HIGH;
        ctx.set_for_loading_index(true);
        auto index =
            index::IndexFactory::GetInstance().CreateIndex(index_info, ctx);
        index->Load(milvus::tracer::TraceContext{}, config);

        auto cnt = index->Count();
        if (has_lack_binlog_row_) {
            ASSERT_EQ(cnt, nb + lack_binlog_row);
        } else {
            ASSERT_EQ(cnt, nb);
        }

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
            size_t start = 0;
            if (has_lack_binlog_row_) {
                for (int i = 0; i < lack_binlog_row; i++) {
                    if (!has_default_value_) {
                        ASSERT_EQ(bitset[i], false);
                    } else {
                        ASSERT_EQ(bitset[i], s.find("20") != s.end());
                    }
                }
                start += lack_binlog_row;
            }
            for (size_t i = start; i < bitset.size(); i++) {
                if (nullable && !valid_data[i - start]) {
                    ASSERT_EQ(bitset[i], false);
                } else {
                    ASSERT_EQ(bitset[i], s.find(data[i - start]) != s.end());
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
            size_t start = 0;
            if (has_lack_binlog_row_) {
                for (int i = 0; i < lack_binlog_row; i++) {
                    if (!has_default_value_) {
                        ASSERT_EQ(bitset[i], false);
                    } else {
                        ASSERT_NE(bitset[i], s.find("20") != s.end());
                    }
                }
                start += lack_binlog_row;
            }
            for (size_t i = start; i < bitset.size(); i++) {
                if (nullable && !valid_data[i - start]) {
                    ASSERT_EQ(bitset[i], false);
                } else {
                    ASSERT_NE(bitset[i], s.find(data[i - start]) != s.end());
                }
            }
        }

        using RefFunc = std::function<bool(int64_t)>;

        {
            std::vector<std::tuple<T, OpType, RefFunc, bool>> test_cases{
                {
                    "20",
                    OpType::GreaterThan,
                    [&](int64_t i) -> bool { return data[i] > "20"; },
                    false,
                },
                {
                    "20",
                    OpType::GreaterEqual,
                    [&](int64_t i) -> bool { return data[i] >= "20"; },
                    true,
                },
                {
                    "20",
                    OpType::LessThan,
                    [&](int64_t i) -> bool { return data[i] < "20"; },
                    false,
                },
                {
                    "20",
                    OpType::LessEqual,
                    [&](int64_t i) -> bool { return data[i] <= "20"; },
                    true,
                },
            };
            for (const auto& [test_value, op, ref, default_value_res] :
                 test_cases) {
                auto bitset = real_index->Range(test_value, op);
                ASSERT_EQ(cnt, bitset.size());
                size_t start = 0;
                if (has_lack_binlog_row_) {
                    for (int i = 0; i < lack_binlog_row; i++) {
                        if (has_default_value_) {
                            ASSERT_EQ(bitset[i], default_value_res);
                        } else {
                            ASSERT_EQ(bitset[i], false);
                        }
                    }
                    start += lack_binlog_row;
                }
                for (size_t i = start; i < bitset.size(); i++) {
                    auto ans = bitset[i];
                    auto should = ref(i - start);
                    if (nullable && !valid_data[i - start]) {
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
            std::vector<std::tuple<T, bool, T, bool, RefFunc, bool>> test_cases{
                {
                    "1",
                    false,
                    "20",
                    false,
                    [&](int64_t i) -> bool {
                        return "1" < data[i] && data[i] < "20";
                    },
                    false,
                },
                {
                    "1",
                    false,
                    "20",
                    true,
                    [&](int64_t i) -> bool {
                        return "1" < data[i] && data[i] <= "20";
                    },
                    true,
                },
                {
                    "1",
                    true,
                    "20",
                    false,
                    [&](int64_t i) -> bool {
                        return "1" <= data[i] && data[i] < "20";
                    },
                    false,
                },
                {
                    "1",
                    true,
                    "20",
                    true,
                    [&](int64_t i) -> bool {
                        return "1" <= data[i] && data[i] <= "20";
                    },
                    true,
                },
            };
            for (const auto& [lb,
                              lb_inclusive,
                              ub,
                              ub_inclusive,
                              ref,
                              default_value_res] : test_cases) {
                auto bitset =
                    real_index->Range(lb, lb_inclusive, ub, ub_inclusive);
                ASSERT_EQ(cnt, bitset.size());
                size_t start = 0;
                if (has_lack_binlog_row_) {
                    for (int i = 0; i < lack_binlog_row; i++) {
                        if (has_default_value_) {
                            ASSERT_EQ(bitset[i], default_value_res);
                        } else {
                            ASSERT_EQ(bitset[i], false);
                        }
                    }
                    start += lack_binlog_row;
                }
                for (size_t i = start; i < bitset.size(); i++) {
                    auto ans = bitset[i];
                    auto should = ref(i - start);
                    if (nullable && !valid_data[i - start]) {
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
            size_t start = 0;
            if (has_lack_binlog_row_) {
                for (int i = 0; i < lack_binlog_row; i++) {
                    if (has_default_value_) {
                        ASSERT_EQ(bitset[i], boost::starts_with("20", prefix));
                    } else {
                        ASSERT_EQ(bitset[i], false);
                    }
                }
                start += lack_binlog_row;
            }
            for (size_t i = start; i < bitset.size(); i++) {
                auto should = boost::starts_with(data[i - start], prefix);
                if (nullable && !valid_data[i - start]) {
                    should = false;
                }
                ASSERT_EQ(bitset[i], should);
            }
        }

        {
            ASSERT_TRUE(real_index->SupportPatternQuery());
            auto prefix = data[0];
            auto bitset = real_index->PatternQuery(prefix + "(.|\n)*");
            ASSERT_EQ(cnt, bitset.size());
            size_t start = 0;
            if (has_lack_binlog_row_) {
                for (int i = 0; i < lack_binlog_row; i++) {
                    if (has_default_value_) {
                        ASSERT_EQ(bitset[i], boost::starts_with("20", prefix));
                    } else {
                        ASSERT_EQ(bitset[i], false);
                    }
                }
                start += lack_binlog_row;
            }
            for (size_t i = start; i < bitset.size(); i++) {
                auto should = boost::starts_with(data[i - start], prefix);
                if (nullable && !valid_data[i - start]) {
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

TEST(InvertedIndex, HasLackBinlogRows) {
    // lack binlog is null
    test_run<int8_t, DataType::INT8, DataType::NONE, true, true>();
    test_run<int16_t, DataType::INT16, DataType::NONE, true, true>();
    test_run<int32_t, DataType::INT32, DataType::NONE, true, true>();
    test_run<int64_t, DataType::INT64, DataType::NONE, true, true>();

    test_run<bool, DataType::BOOL, DataType::NONE, true, true>();

    test_run<float, DataType::FLOAT, DataType::NONE, true, true>();
    test_run<double, DataType::DOUBLE, DataType::NONE, true, true>();

    test_string<true, true>();

    // lack binlog is default_value
    test_run<int8_t, DataType::INT8, DataType::NONE, true, true, true>();
    test_run<int16_t, DataType::INT16, DataType::NONE, true, true, true>();
    test_run<int32_t, DataType::INT32, DataType::NONE, true, true, true>();
    test_run<int64_t, DataType::INT64, DataType::NONE, true, true, true>();

    test_run<bool, DataType::BOOL, DataType::NONE, true, true, true>();

    test_run<float, DataType::FLOAT, DataType::NONE, true, true, true>();
    test_run<double, DataType::DOUBLE, DataType::NONE, true, true, true>();

    test_string<true, true, true>();
}

// ============== Unified Pattern Matching Consistency Tests ==============
// These tests verify that ALL THREE execution paths produce identical results:
// 1. RE2 regex (RegexMatcher) - used for sealed segments without index
// 2. LikePatternMatcher - used for growing data (brute-force scan)
// 3. Tantivy index - used for sealed segments with inverted index
//
// This is critical for query correctness across different segment states.

namespace {

// Helper to build Tantivy index for testing
std::unique_ptr<index::InvertedIndexTantivy<std::string>>
BuildTantivyStringIndex(const std::vector<std::string>& data) {
    auto index = std::make_unique<index::InvertedIndexTantivy<std::string>>();
    index->BuildWithRawDataForUT(data.size(), data.data(), Config());
    return index;
}

// Verify all three matchers agree for a pattern against test data
void
VerifyPatternMatchConsistency(
    const std::string& pattern,
    const std::vector<std::string>& test_data,
    index::InvertedIndexTantivy<std::string>* tantivy_index,
    const std::string& test_category) {
    PatternMatchTranslator translator;
    auto regex_pattern = translator(pattern);

    RegexMatcher re2_matcher(regex_pattern);
    LikePatternMatcher like_matcher(pattern);
    auto tantivy_result =
        tantivy_index->PatternMatch(pattern, proto::plan::OpType::Match);

    for (size_t i = 0; i < test_data.size(); i++) {
        bool re2_result = re2_matcher(test_data[i]);
        bool like_result = like_matcher(test_data[i]);
        bool tantivy_bit = tantivy_result[i];

        EXPECT_EQ(re2_result, like_result)
            << test_category << " - RE2/LikePatternMatcher mismatch:\n"
            << "  pattern=\"" << pattern << "\"\n"
            << "  data=\"" << test_data[i] << "\" (len=" << test_data[i].size()
            << ")\n"
            << "  RE2=" << re2_result << ", Like=" << like_result;

        EXPECT_EQ(re2_result, tantivy_bit)
            << test_category << " - RE2/Tantivy mismatch:\n"
            << "  pattern=\"" << pattern << "\"\n"
            << "  data=\"" << test_data[i] << "\" (len=" << test_data[i].size()
            << ")\n"
            << "  RE2=" << re2_result << ", Tantivy=" << tantivy_bit;
    }
}

}  // namespace

// ============== Comprehensive Pattern Matching Consistency Test ==============
// This single test verifies consistency across ALL THREE execution paths:
// 1. RE2 regex (RegexMatcher) - sealed segments without index
// 2. LikePatternMatcher - growing data brute-force scan
// 3. Tantivy index - sealed segments with inverted index

TEST(PatternMatchConsistency, AllMatchersMustAgree) {
    // Comprehensive test data covering all scenarios
    std::vector<std::string> test_data = {
        // Basic strings
        "hello",
        "world",
        "hello world",
        "HELLO",
        "hello123",
        "123hello",
        "h3ll0",
        "test",
        "testing",
        "tested",
        "tester",
        "apple",
        "application",
        "apply",
        "banana",

        // Overlapping pattern test data
        "a",
        "aa",
        "aaa",
        "aaaa",
        "aaaaa",
        "aba",
        "abba",
        "aab",
        "baa",
        "abab",
        "ababab",
        "abc",
        "abcbc",
        "abcab",
        "xaax",
        "xaaax",
        "ab",
        "ba",

        // UTF-8 test data (2-byte, 3-byte, 4-byte)
        "café",                       // 2-byte UTF-8 (é)
        "\xE4\xBD\xA0\xE5\xA5\xBD",   // 你好 (3-byte UTF-8)
        "test\xE4\xBD\xA0test",       // Mixed ASCII and CJK
        "\xF0\x9F\x98\x80",           // 😀 (4-byte UTF-8)
        "emoji\xF0\x9F\x98\x80test",  // Mixed with emoji
        "a\xC3\xA9"
        "b",                 // aéb
        "\xC3\xA9\xC3\xA9",  // éé (consecutive 2-byte)
        "normal",

        // Special characters for escape tests
        "100%",
        "50%off",
        "file_name",
        "file_name.txt",
        "path\\to\\file",
        "%percent%",
        "_underscore_",
        "back\\slash",

        // Edge cases
        "",      // Empty string
        "   ",   // Whitespace
        "\t\n",  // Tab and newline
        "123",   // Numbers only
        "!@#$",  // Special chars
    };

    // Build Tantivy index with all test data
    auto tantivy_index = BuildTantivyStringIndex(test_data);

    // Comprehensive pattern list covering all scenarios
    std::vector<std::string> patterns = {
        // Basic patterns
        "hello",   // Exact match
        "hello%",  // Prefix match
        "%world",  // Suffix match
        "%llo%",   // Inner match
        "h_llo",   // Single char wildcard
        "h%o",     // Prefix + suffix with gap
        "%",       // Match all
        "test%",   // Prefix
        "%ing",    // Suffix
        "%est%",   // Inner

        // Overlapping patterns (key regression tests)
        "%aa%aa%",     // Two overlapping "aa"
        "%aa%aa%aa%",  // Three overlapping "aa"
        "%ab%ba%",     // Different overlapping segments
        "%ab%ab%",     // Same segment twice
        "%ab%ab%ab%",  // Same segment three times
        "a%aa",        // Prefix with repeated suffix
        "aa%a",        // Repeated prefix with suffix
        "%a%a%",       // Single char segments
        "%a%a%a%",     // Three single char segments
        "a%a",         // Simple overlapping

        // UTF-8 patterns
        "caf_",                // Underscore matches 2-byte UTF-8
        "%\xE4\xBD\xA0%",      // Contains 你
        "a_b",                 // Single UTF-8 char in middle
        "%\xF0\x9F\x98\x80%",  // Contains emoji
        "\xE4\xBD\xA0%",       // Starts with 你
        "%\xE5\xA5\xBD",       // Ends with 好

        // Escape sequences
        "100\\%",            // Literal %
        "%\\%",              // Ends with %
        "\\%%",              // Starts with %
        "file\\_name%",      // Literal _
        "%\\\\%",            // Contains backslash
        "\\%percent\\%",     // Literal % on both sides
        "\\_underscore\\_",  // Literal _ on both sides

        // Edge case patterns
        "_",    // Single char
        "__",   // Two chars
        "___",  // Three chars
        "%%",   // Multiple percent
        "_%",   // One char then anything
        "%_",   // Anything then one char
        "",     // Empty pattern
    };

    int total_tests = 0;
    int passed_tests = 0;

    for (const auto& pattern : patterns) {
        PatternMatchTranslator translator;
        auto regex_pattern = translator(pattern);

        RegexMatcher re2_matcher(regex_pattern);
        LikePatternMatcher like_matcher(pattern);
        auto tantivy_result =
            tantivy_index->PatternMatch(pattern, proto::plan::OpType::Match);

        for (size_t i = 0; i < test_data.size(); i++) {
            total_tests++;

            bool re2_result = re2_matcher(test_data[i]);
            bool like_result = like_matcher(test_data[i]);
            bool tantivy_bit = tantivy_result[i];

            // All three must agree
            bool all_agree =
                (re2_result == like_result) && (re2_result == tantivy_bit);

            if (all_agree) {
                passed_tests++;
            }

            EXPECT_EQ(re2_result, like_result)
                << "RE2/LikePatternMatcher MISMATCH:\n"
                << "  Pattern: \"" << pattern << "\"\n"
                << "  Data: \"" << test_data[i]
                << "\" (len=" << test_data[i].size() << ")\n"
                << "  RE2=" << re2_result << ", Like=" << like_result;

            EXPECT_EQ(re2_result, tantivy_bit)
                << "RE2/Tantivy MISMATCH:\n"
                << "  Pattern: \"" << pattern << "\"\n"
                << "  Data: \"" << test_data[i]
                << "\" (len=" << test_data[i].size() << ")\n"
                << "  RE2=" << re2_result << ", Tantivy=" << tantivy_bit;
        }
    }

    // Summary
    std::cout << "Pattern Match Consistency: " << passed_tests << "/"
              << total_tests << " tests passed across all three matchers\n";
}

// Test OpType-specific matching (PrefixMatch, PostfixMatch, InnerMatch)
TEST(PatternMatchConsistency, OpTypeMatchersMustAgree) {
    std::vector<std::string> test_data = {
        "hello",
        "hello world",
        "world hello",
        "say hello there",
        "helloworld",
        "worldhello",
        "HELLO",
        "Hello",
        "test",
        "testing",
        "pretest",
        "pretesting",
    };

    auto tantivy_index = BuildTantivyStringIndex(test_data);

    std::vector<std::string> search_terms = {"hello", "test", "world"};

    for (const auto& term : search_terms) {
        // Test PrefixMatch (equivalent to "term%")
        {
            std::string like_pattern = term + "%";
            PatternMatchTranslator translator;
            RegexMatcher re2_matcher(translator(like_pattern));
            LikePatternMatcher like_matcher(like_pattern);
            auto tantivy_result = tantivy_index->PatternMatch(
                term, proto::plan::OpType::PrefixMatch);

            for (size_t i = 0; i < test_data.size(); i++) {
                bool re2_result = re2_matcher(test_data[i]);
                bool like_result = like_matcher(test_data[i]);
                bool tantivy_bit = tantivy_result[i];

                EXPECT_EQ(re2_result, like_result)
                    << "PrefixMatch RE2/Like mismatch: term=\"" << term
                    << "\", data=\"" << test_data[i] << "\"";
                EXPECT_EQ(re2_result, tantivy_bit)
                    << "PrefixMatch RE2/Tantivy mismatch: term=\"" << term
                    << "\", data=\"" << test_data[i] << "\"";
            }
        }

        // Test PostfixMatch (equivalent to "%term")
        {
            std::string like_pattern = "%" + term;
            PatternMatchTranslator translator;
            RegexMatcher re2_matcher(translator(like_pattern));
            LikePatternMatcher like_matcher(like_pattern);
            auto tantivy_result = tantivy_index->PatternMatch(
                term, proto::plan::OpType::PostfixMatch);

            for (size_t i = 0; i < test_data.size(); i++) {
                bool re2_result = re2_matcher(test_data[i]);
                bool like_result = like_matcher(test_data[i]);
                bool tantivy_bit = tantivy_result[i];

                EXPECT_EQ(re2_result, like_result)
                    << "PostfixMatch RE2/Like mismatch: term=\"" << term
                    << "\", data=\"" << test_data[i] << "\"";
                EXPECT_EQ(re2_result, tantivy_bit)
                    << "PostfixMatch RE2/Tantivy mismatch: term=\"" << term
                    << "\", data=\"" << test_data[i] << "\"";
            }
        }

        // Test InnerMatch (equivalent to "%term%")
        {
            std::string like_pattern = "%" + term + "%";
            PatternMatchTranslator translator;
            RegexMatcher re2_matcher(translator(like_pattern));
            LikePatternMatcher like_matcher(like_pattern);
            auto tantivy_result = tantivy_index->PatternMatch(
                term, proto::plan::OpType::InnerMatch);

            for (size_t i = 0; i < test_data.size(); i++) {
                bool re2_result = re2_matcher(test_data[i]);
                bool like_result = like_matcher(test_data[i]);
                bool tantivy_bit = tantivy_result[i];

                EXPECT_EQ(re2_result, like_result)
                    << "InnerMatch RE2/Like mismatch: term=\"" << term
                    << "\", data=\"" << test_data[i] << "\"";
                EXPECT_EQ(re2_result, tantivy_bit)
                    << "InnerMatch RE2/Tantivy mismatch: term=\"" << term
                    << "\", data=\"" << test_data[i] << "\"";
            }
        }
    }
}

// Test NUL byte and special byte handling consistency
TEST(PatternMatchConsistency, SpecialByteHandling) {
    // Test data with NUL bytes and special characters
    std::vector<std::string> test_data;

    // String with embedded NUL byte
    std::string with_nul = "hello";
    with_nul += '\0';
    with_nul += "world";
    test_data.push_back(with_nul);

    // String with multiple NUL bytes
    std::string multi_nul = "a";
    multi_nul += '\0';
    multi_nul += '\0';
    multi_nul += "b";
    test_data.push_back(multi_nul);

    // Normal strings for comparison
    test_data.push_back("helloworld");
    test_data.push_back("hello world");
    test_data.push_back("ab");

    auto tantivy_index = BuildTantivyStringIndex(test_data);

    std::vector<std::string> patterns = {
        "hello%",
        "%world",
        "hello%world",
        "a%b",
        "a__b",
        "%",
    };

    for (const auto& pattern : patterns) {
        PatternMatchTranslator translator;
        auto regex_pattern = translator(pattern);

        RegexMatcher re2_matcher(regex_pattern);
        LikePatternMatcher like_matcher(pattern);
        auto tantivy_result =
            tantivy_index->PatternMatch(pattern, proto::plan::OpType::Match);

        for (size_t i = 0; i < test_data.size(); i++) {
            bool re2_result = re2_matcher(test_data[i]);
            bool like_result = like_matcher(test_data[i]);
            bool tantivy_bit = tantivy_result[i];

            EXPECT_EQ(re2_result, like_result)
                << "NUL byte test - RE2/Like mismatch: pattern=\"" << pattern
                << "\", data_len=" << test_data[i].size();
            EXPECT_EQ(re2_result, tantivy_bit)
                << "NUL byte test - RE2/Tantivy mismatch: pattern=\"" << pattern
                << "\", data_len=" << test_data[i].size();
        }
    }
}
