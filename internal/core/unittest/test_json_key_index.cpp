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
#include <functional>
#include <boost/filesystem.hpp>
#include <unordered_set>
#include <memory>

#include "common/Tracer.h"
#include "index/BitmapIndex.h"
#include "storage/Util.h"
#include "storage/InsertData.h"
#include "indexbuilder/IndexFactory.h"
#include "index/IndexFactory.h"
#include "test_utils/indexbuilder_test_utils.h"
#include "index/Meta.h"
#include "index/JsonKeyInvertedIndex.h"
#include "common/Json.h"
#include "common/Types.h"
using namespace milvus::index;
using namespace milvus::indexbuilder;
using namespace milvus;
using namespace milvus::index;

static std::vector<milvus::Json>
GenerateJsons(int size) {
    std::vector<Json> jsons;
    for (int i = 0; i < size; i++) {
        auto str = R"({"int":)" + std::to_string(random()) + R"(,"double":)" +
                   std::to_string(static_cast<double>(random())) +
                   R"(,"string":")" + std::to_string(random()) +
                   R"(","bool": true)" + R"(, "array": [1,2,3])" + "}";
        jsons.push_back(milvus::Json(simdjson::padded_string(str)));
    }
    return jsons;
}

class JsonKeyIndexTest : public testing::Test {
 protected:
    void
    Init(int64_t collection_id,
         int64_t partition_id,
         int64_t segment_id,
         int64_t field_id,
         int64_t index_build_id,
         int64_t index_version,
         int64_t size) {
        proto::schema::FieldSchema field_schema;
        field_schema.set_data_type(proto::schema::DataType::JSON);

        auto field_meta = storage::FieldDataMeta{
            collection_id, partition_id, segment_id, field_id, field_schema};
        auto index_meta = storage::IndexMeta{
            segment_id, field_id, index_build_id, index_version};

        data_ = std::move(GenerateJsons(size));
        auto field_data = storage::CreateFieldData(DataType::JSON);
        field_data->FillFieldData(data_.data(), data_.size());
        storage::InsertData insert_data(field_data);
        insert_data.SetFieldDataMeta(field_meta);
        insert_data.SetTimestamps(0, 100);

        auto serialized_bytes = insert_data.Serialize(storage::Remote);

        auto log_path = fmt::format("/{}/{}/{}/{}/{}/{}",
                                    "/tmp/test-jsonkey-index/",
                                    collection_id,
                                    partition_id,
                                    segment_id,
                                    field_id,
                                    0);
        chunk_manager_->Write(
            log_path, serialized_bytes.data(), serialized_bytes.size());

        storage::FileManagerContext ctx(field_meta, index_meta, chunk_manager_);
        std::vector<std::string> index_files;

        Config config;
        config["insert_files"] = std::vector<std::string>{log_path};

        auto build_index = std::make_shared<JsonKeyInvertedIndex>(ctx, false);
        build_index->Build(config);

        auto binary_set = build_index->Upload(config);
        for (const auto& [key, _] : binary_set.binary_map_) {
            index_files.push_back(key);
        }

        index::CreateIndexInfo index_info{};
        config["index_files"] = index_files;

        index_ = std::make_shared<JsonKeyInvertedIndex>(ctx, true);
        index_->Load(milvus::tracer::TraceContext{}, config);
    }

    virtual void
    SetParam() {
    }
    void
    SetUp() override {
        SetParam();

        type_ = DataType::JSON;
        int64_t collection_id = 1;
        int64_t partition_id = 2;
        int64_t segment_id = 3;
        int64_t field_id = 101;
        int64_t index_build_id = 1000;
        int64_t index_version = 10000;
        size_ = 10000;
        std::string root_path = "/tmp/test-jsonkey-index/";

        storage::StorageConfig storage_config;
        storage_config.storage_type = "local";
        storage_config.root_path = root_path;
        chunk_manager_ = storage::CreateChunkManager(storage_config);

        Init(collection_id,
             partition_id,
             segment_id,
             field_id,
             index_build_id,
             index_version,
             size_);
    }

    virtual ~JsonKeyIndexTest() override {
        boost::filesystem::remove_all(chunk_manager_->GetRootPath());
    }

 public:
    void
    TestTermInFunc() {
        {
            struct Testcase {
                std::vector<int64_t> term;
                std::vector<std::string> nested_path;
            };
            std::vector<Testcase> testcases{
                {{1, 2, 3, 4}, {"int"}},
                {{10, 100, 1000, 10000}, {"int"}},
                {{100, 10000, 9999, 444}, {"int"}},
                {{23, 42, 66, 17, 25}, {"int"}},
            };
            for (auto testcase : testcases) {
                auto check = [&](int64_t value) {
                    std::unordered_set<int64_t> term_set(testcase.term.begin(),
                                                         testcase.term.end());
                    return term_set.find(value) != term_set.end();
                };
                std::unordered_set<int64_t> term_set(testcase.term.begin(),
                                                     testcase.term.end());
                auto filter_func = [&term_set, this](uint32_t row_id,
                                                     uint16_t offset,
                                                     uint16_t size) {
                    auto val = this->data_[row_id].template at_pos<int64_t>(
                        offset, size);
                    if (val.second != "") {
                        return false;
                    }
                    return term_set.find((int64_t(val.first))) !=
                           term_set.end();
                };
                auto pointer = milvus::Json::pointer(testcase.nested_path);
                auto bitset = index_->FilterByPath(pointer, size_, filter_func);
                ASSERT_EQ(bitset.size(), size_);
                for (int i = 0; i < bitset.size(); ++i) {
                    auto val = data_[i].template at<int64_t>(pointer).value();
                    auto ans = bitset[i];
                    auto ref = check(val);
                    ASSERT_EQ(ans, ref);
                }
            }
        }
    }
    void
    TestUnaryRangeInFunc() {
        struct Testcase {
            int64_t val;
            std::vector<std::string> nested_path;
        };
        std::vector<Testcase> testcases{
            {10, {"int"}},
            {20, {"int"}},
            {30, {"int"}},
            {40, {"int"}},
        };
        std::vector<OpType> ops{
            OpType::Equal,
            OpType::NotEqual,
            OpType::GreaterThan,
            OpType::GreaterEqual,
            OpType::LessThan,
            OpType::LessEqual,
        };
        for (const auto& testcase : testcases) {
            auto check = [&](int64_t value) { return value == testcase.val; };
            std::function<bool(int64_t)> f = check;
            for (auto& op : ops) {
                switch (op) {
                    case OpType::Equal: {
                        f = [&](int64_t value) {
                            return value == testcase.val;
                        };
                        break;
                    }
                    case OpType::NotEqual: {
                        f = [&](int64_t value) {
                            return value != testcase.val;
                        };
                        break;
                    }
                    case OpType::GreaterEqual: {
                        f = [&](int64_t value) {
                            return value >= testcase.val;
                        };
                        break;
                    }
                    case OpType::GreaterThan: {
                        f = [&](int64_t value) { return value > testcase.val; };
                        break;
                    }
                    case OpType::LessEqual: {
                        f = [&](int64_t value) {
                            return value <= testcase.val;
                        };
                        break;
                    }
                    case OpType::LessThan: {
                        f = [&](int64_t value) { return value < testcase.val; };
                        break;
                    }
                    default: {
                        PanicInfo(Unsupported, "unsupported range node");
                    }
                }

                auto filter_func = [&op, &testcase, this](uint32_t row_id,
                                                          uint16_t offset,
                                                          uint16_t size) {
                    auto val = this->data_[row_id].template at_pos<int64_t>(
                        offset, size);
                    if (val.second != "") {
                        return false;
                    }
                    switch (op) {
                        case OpType::GreaterThan:
                            return int64_t(val.first) > testcase.val;
                        case OpType::GreaterEqual:
                            return int64_t(val.first) >= testcase.val;
                        case OpType::LessThan:
                            return int64_t(val.first) < testcase.val;
                        case OpType::LessEqual:
                            return int64_t(val.first) <= testcase.val;
                        case OpType::Equal:
                            return int64_t(val.first) == testcase.val;
                        case OpType::NotEqual:
                            return int64_t(val.first) != testcase.val;
                        default:
                            return false;
                    }
                };
                auto pointer = milvus::Json::pointer(testcase.nested_path);
                auto bitset = index_->FilterByPath(pointer, size_, filter_func);
                ASSERT_EQ(bitset.size(), size_);
                for (int i = 0; i < bitset.size(); ++i) {
                    auto ans = bitset[i];
                    auto val = data_[i].template at<int64_t>(pointer).value();
                    auto ref = f(val);
                    ASSERT_EQ(ans, ref);
                }
            }
        }
    }

    void
    TestBinaryRangeInFunc() {
        struct Testcase {
            bool lower_inclusive;
            bool upper_inclusive;
            int64_t lower;
            int64_t upper;
            std::vector<std::string> nested_path;
        };
        std::vector<Testcase> testcases{
            {true, false, 10, 20, {"int"}},
            {true, true, 20, 30, {"int"}},
            {false, true, 30, 40, {"int"}},
            {false, false, 40, 50, {"int"}},
        };
        for (const auto& testcase : testcases) {
            auto check = [&](int64_t value) {
                if (testcase.lower_inclusive && testcase.upper_inclusive) {
                    return testcase.lower <= value && value <= testcase.upper;
                } else if (testcase.lower_inclusive &&
                           !testcase.upper_inclusive) {
                    return testcase.lower <= value && value < testcase.upper;
                } else if (!testcase.lower_inclusive &&
                           testcase.upper_inclusive) {
                    return testcase.lower < value && value <= testcase.upper;
                } else {
                    return testcase.lower < value && value < testcase.upper;
                }
            };

            auto filter_func = [&testcase, this](uint32_t row_id,
                                                 uint16_t offset,
                                                 uint16_t size) {
                auto val =
                    this->data_[row_id].template at_pos<int64_t>(offset, size);
                if (val.second != "") {
                    return false;
                }
                if (testcase.lower_inclusive && testcase.upper_inclusive) {
                    return testcase.lower <= int64_t(val.first) &&
                           int64_t(val.first) <= testcase.upper;
                } else if (testcase.lower_inclusive &&
                           !testcase.upper_inclusive) {
                    return testcase.lower <= int64_t(val.first) &&
                           int64_t(val.first) < testcase.upper;
                } else if (!testcase.lower_inclusive &&
                           testcase.upper_inclusive) {
                    return testcase.lower < int64_t(val.first) &&
                           int64_t(val.first) <= testcase.upper;
                } else {
                    return testcase.lower < int64_t(val.first) &&
                           int64_t(val.first) < testcase.upper;
                }
            };
            auto pointer = milvus::Json::pointer(testcase.nested_path);
            auto bitset = index_->FilterByPath(pointer, size_, filter_func);
            ASSERT_EQ(bitset.size(), size_);
            for (int i = 0; i < bitset.size(); ++i) {
                auto ans = bitset[i];
                auto val = data_[i].template at<int64_t>(pointer).value();
                auto ref = check(val);
                ASSERT_EQ(ans, ref);
            }
        }
    }

    void
    TestExistInFunc() {
        struct Testcase {
            std::vector<std::string> nested_path;
        };
        std::vector<Testcase> testcases{
            {{"A"}},
            {{"int"}},
            {{"double"}},
            {{"B"}},
        };
        for (const auto& testcase : testcases) {
            auto pointer = milvus::Json::pointer(testcase.nested_path);
            auto filter_func = [&pointer, this](uint32_t row_id,
                                                uint16_t offset,
                                                uint16_t size) {
                return this->data_[row_id].exist(pointer);
            };

            auto bitset = index_->FilterByPath(pointer, size_, filter_func);
            ASSERT_EQ(bitset.size(), size_);
            for (int i = 0; i < bitset.size(); ++i) {
                auto ans = bitset[i];
                auto val = data_[i].exist(pointer);
                ASSERT_EQ(ans, val);
            }
        }
    }

    void
    TestJsonContainsAllFunc() {
        {
            std::vector<Testcase<int64_t>> testcases{
                {{1, 10}, {"int"}},
                {{10, 100}, {"int"}},
                {{100, 1000}, {"int"}},
                {{1000, 10}, {"int"}},
                {{2, 4, 6, 8, 10}, {"int"}},
                {{1, 2, 3, 4, 5}, {"int"}},
            };
            for (const auto& testcase : testcases) {
                auto check = [&](const std::vector<int64_t>& values) {
                    for (auto const& e : testcase.term) {
                        if (std::find(values.begin(), values.end(), e) ==
                            values.end()) {
                            return false;
                        }
                    }
                    return true;
                };
                auto pointer = milvus::Json::pointer(testcase.nested_path);
                std::unordered_set<int64_t> elements;
                for (auto const& element : testcase.term) {
                    elements.insert(element);
                }
                auto filter_func = [&elements, this](uint32_t row_id,
                                                     uint16_t offset,
                                                     uint16_t size) {
                    auto array = this->data_[row_id].array_at(offset, size);
                    std::unordered_set<int64_t> tmp_elements(elements);
                    for (auto&& it : array) {
                        auto val = it.template get<int64_t>();
                        if (val.error()) {
                            continue;
                        }
                        tmp_elements.erase(val.value());
                        if (tmp_elements.size() == 0) {
                            return true;
                        }
                    }
                    return tmp_elements.empty();
                };

                auto bitset = index_->FilterByPath(pointer, size_, filter_func);
                ASSERT_EQ(bitset.size(), size_);
                for (int i = 0; i < bitset.size(); ++i) {
                    auto ans = bitset[i];
                    auto array = data_[i].array_at(pointer);
                    std::vector<int64_t> res;
                    for (const auto& element : array) {
                        res.push_back(element.template get<int64_t>());
                    }
                    ASSERT_EQ(ans, check(res));
                }
            }
        }

        {
            std::vector<Testcase<bool>> bool_testcases{
                {{true, true}, {"bool"}}, {{false, false}, {"bool"}}};
            for (const auto& testcase : bool_testcases) {
                auto check = [&](const std::vector<bool>& values) {
                    for (auto const& e : testcase.term) {
                        if (std::find(values.begin(), values.end(), e) ==
                            values.end()) {
                            return false;
                        }
                    }
                    return true;
                };
                auto pointer = milvus::Json::pointer(testcase.nested_path);
                std::unordered_set<bool> elements;
                for (auto const& element : testcase.term) {
                    elements.insert(element);
                }
                auto filter_func = [&elements, this](uint32_t row_id,
                                                     uint16_t offset,
                                                     uint16_t size) {
                    auto array = this->data_[row_id].array_at(offset, size);
                    std::unordered_set<bool> tmp_elements(elements);
                    for (auto&& it : array) {
                        auto val = it.template get<bool>();
                        if (val.error()) {
                            continue;
                        }
                        tmp_elements.erase(val.value());
                        if (tmp_elements.size() == 0) {
                            return true;
                        }
                    }

                    return tmp_elements.empty();
                };

                auto bitset = index_->FilterByPath(pointer, size_, filter_func);
                ASSERT_EQ(bitset.size(), size_);
                for (int i = 0; i < bitset.size(); ++i) {
                    auto ans = bitset[i];
                    auto array = data_[i].array_at(pointer);
                    std::vector<bool> res;
                    for (const auto& element : array) {
                        res.push_back(element.template get<bool>());
                    }
                    ASSERT_EQ(ans, check(res));
                }
            }
        }
    }

    template <typename T>
    struct Testcase {
        std::vector<T> term;
        std::vector<std::string> nested_path;
        bool res;
    };

 public:
    std::shared_ptr<JsonKeyInvertedIndex> index_;
    DataType type_;
    size_t size_;
    std::vector<milvus::Json> data_;
    std::vector<std::string> json_col;
    std::shared_ptr<storage::ChunkManager> chunk_manager_;
};

TEST_F(JsonKeyIndexTest, CountFuncTest) {
    TestTermInFunc();
    TestUnaryRangeInFunc();
    TestBinaryRangeInFunc();
    TestExistInFunc();
}