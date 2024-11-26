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

using namespace milvus::index;
using namespace milvus::indexbuilder;
using namespace milvus;
using namespace milvus::index;

std::string
join(const std::vector<std::string>& vec, const std::string& delimiter) {
    std::ostringstream oss;
    for (size_t i = 0; i < vec.size(); ++i) {
        oss << vec[i];
        if (i != vec.size() - 1) {
            oss << delimiter;
        }
    }
    return oss.str();
}

// 1000 keys
static std::string
GenerateJson(int N) {
    std::vector<std::string> data(N);
    std::default_random_engine er(42);
    std::normal_distribution<> distr(0, 1);
    std::vector<std::string> keys;
    for (int i = 0; i < N; i++) {
        keys.push_back("keys" + std::to_string(i));
    }
    std::string json_string;
    std::vector<std::string> values(N);
    for (int i = 0; i < N; i++) {
        if (i % 7 == 0) {
            values[i] = std::to_string(er());
        } else if (i % 7 == 1) {
            values[i] = std::to_string(static_cast<double>(er()));
        } else if (i % 7 == 2) {
            values[i] = er() / 2 == 0 ? "true" : "false";
        } else if (i % 7 == 3) {
            values[i] = "\"xxxx" + std::to_string(i) + "\"";
        } else if (i % 7 == 4) {
            std::vector<std::string> intvec(10);
            for (int j = 0; j < 10; j++) {
                intvec[j] = std::to_string(i + j);
            }
            values[i] = "[" + join(intvec, ",") + "]";
        } else if (i % 7 == 5) {
            std::vector<std::string> doublevec(10);
            for (int j = 0; j < 10; j++) {
                doublevec[j] =
                    std::to_string(static_cast<double>(i + j + er()));
            }
            values[i] = "[" + join(doublevec, ",") + "]";
        } else if (i % 7 == 6) {
            std::vector<std::string> stringvec(10);
            for (int j = 0; j < 10; j++) {
                stringvec[j] = "\"xxx" + std::to_string(j) + "\"";
            }
            values[i] = "[" + join(stringvec, ",") + "]";
        }
    }
    json_string += "{";
    for (int i = 0; i < N - 1; i++) {
        json_string += R"(")" + keys[i] + R"(":)" + values[i] + R"(,)";
    }
    json_string += R"(")" + keys[N - 1] + R"(":)" + values[N - 1];
    json_string += "}";
    return json_string;
}

static std::vector<Json>
GenerateJsons(int size, int dim) {
    std::vector<Json> jsons;
    for (int i = 0; i < size; ++i) {
        std::cout << GenerateJson(dim) << std::endl;
        jsons.push_back(
            milvus::Json(simdjson::padded_string(GenerateJson(dim))));
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
         int64_t size,
         int64_t dim) {
        proto::schema::FieldSchema field_schema;
        field_schema.set_data_type(proto::schema::DataType::JSON);

        auto field_meta = storage::FieldDataMeta{
            collection_id, partition_id, segment_id, field_id, field_schema};
        auto index_meta = storage::IndexMeta{
            segment_id, field_id, index_build_id, index_version};

        data_ = std::move(GenerateJsons(size, dim));
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
        size_ = 10;
        dim_ = 10;
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
             size_,
             dim_);
    }

    virtual ~JsonKeyIndexTest() override {
        boost::filesystem::remove_all(chunk_manager_->GetRootPath());
    }

 public:
    void
    TestTermInFunc() {
        {
            std::vector<std::vector<std::string>> testcases{{"705894"}};
            for (auto testcase : testcases) {
                auto check = [&](std::string value) {
                    std::unordered_set<std::string> term_set(testcase.begin(),
                                                             testcase.end());
                    return term_set.find(value) != term_set.end();
                };
                std::unordered_set<std::string> term_set(testcase.begin(),
                                                         testcase.end());
                auto filter_func = [&term_set, this](uint32_t row_id,
                                                     uint16_t offset,
                                                     uint16_t size) {
                    auto val =
                        this->data_[row_id].template at_pos<std::string_view>(
                            offset, size);
                    if (val.second != "") {
                        return false;
                    }
                    return term_set.find((std::string(val.first))) !=
                           term_set.end();
                };
                auto bitset =
                    index_->FilterByPath("/keys0", size_, filter_func);

                ASSERT_EQ(bitset.size(), size_);
                for (int i = 0; i < bitset.size(); ++i) {
                    auto ans = bitset[i];
                    auto ref = check("705894");
                    ASSERT_EQ(ans, ref);
                }
            }
        }
        {
            std::vector<std::string> testcases{"true"};
            for (auto& value : testcases) {
                auto filter_func = [this, &value](uint32_t row_id,
                                                  uint16_t offset,
                                                  uint16_t size) {
                    auto val =
                        this->data_[row_id].template at_pos<std::string_view>(
                            offset, size);
                    if (val.second != "") {
                        return false;
                    }
                    return std::string(val.first) == value;
                };

                auto bitset =
                    index_->FilterByPath("/keys2", size_, filter_func);
                ASSERT_EQ(bitset.size(), size_);
                for (int i = 0; i < bitset.size(); ++i) {
                    auto ans = bitset[i];
                    auto ref = (value == "false");
                    ASSERT_EQ(ans, ref);
                }
            }
        }
    }
    void
    TestUnaryRangeInFunc() {
        std::vector<std::string> testcases{"10", "705894", "805894"};
        std::vector<OpType> ops{
            OpType::Equal,
            OpType::NotEqual,
            OpType::GreaterThan,
            OpType::GreaterEqual,
            OpType::LessThan,
            OpType::LessEqual,
        };
        for (const auto& testcase : testcases) {
            auto check = [&](std::string value) { return value == testcase; };
            std::function<bool(std::string)> f = check;
            for (auto& op : ops) {
                switch (op) {
                    case OpType::Equal: {
                        f = [&](std::string value) {
                            return value == testcase;
                        };
                        break;
                    }
                    case OpType::NotEqual: {
                        f = [&](std::string value) {
                            return value != testcase;
                        };
                        break;
                    }
                    case OpType::GreaterEqual: {
                        f = [&](std::string value) {
                            return value >= testcase;
                        };
                        break;
                    }
                    case OpType::GreaterThan: {
                        f = [&](std::string value) { return value > testcase; };
                        break;
                    }
                    case OpType::LessEqual: {
                        f = [&](std::string value) {
                            return value <= testcase;
                        };
                        break;
                    }
                    case OpType::LessThan: {
                        f = [&](std::string value) { return value < testcase; };
                        break;
                    }
                    default: {
                        PanicInfo(Unsupported, "unsupported range node");
                    }
                }

                auto filter_func = [&op, &testcase, this](uint32_t row_id,
                                                          uint16_t offset,
                                                          uint16_t size) {
                    auto val =
                        this->data_[row_id].template at_pos<std::string_view>(
                            offset, size);
                    if (val.second != "") {
                        return false;
                    }
                    switch (op) {
                        case OpType::GreaterThan:
                            return std::string(val.first) > testcase;
                        case OpType::GreaterEqual:
                            return std::string(val.first) >= testcase;
                        case OpType::LessThan:
                            return std::string(val.first) < testcase;
                        case OpType::LessEqual:
                            return std::string(val.first) <= testcase;
                        case OpType::Equal:
                            return std::string(val.first) == testcase;
                        case OpType::NotEqual:
                            return std::string(val.first) != testcase;
                        default:
                            return false;
                    }
                };
                auto bitset =
                    index_->FilterByPath("/keys0", size_, filter_func);
                ASSERT_EQ(bitset.size(), size_);
                for (int i = 0; i < bitset.size(); ++i) {
                    auto ans = bitset[i];
                    auto ref = f("705894");
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
            std::string lower;
            std::string upper;
        };
        std::vector<Testcase> testcases{
            {true, false, "10", "20"},
            {true, true, "20", "30"},
            {false, true, "30", "40"},
            {false, false, "40", "50"},
        };
        for (const auto& testcase : testcases) {
            auto check = [&](std::string value) {
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
                    this->data_[row_id].template at_pos<std::string_view>(
                        offset, size);
                if (val.second != "") {
                    return false;
                }
                if (testcase.lower_inclusive && testcase.upper_inclusive) {
                    return testcase.lower <= std::string(val.first) &&
                           std::string(val.first) <= testcase.upper;
                } else if (testcase.lower_inclusive &&
                           !testcase.upper_inclusive) {
                    return testcase.lower <= std::string(val.first) &&
                           std::string(val.first) < testcase.upper;
                } else if (!testcase.lower_inclusive &&
                           testcase.upper_inclusive) {
                    return testcase.lower < std::string(val.first) &&
                           std::string(val.first) <= testcase.upper;
                } else {
                    return testcase.lower < std::string(val.first) &&
                           std::string(val.first) < testcase.upper;
                }
            };
            auto bitset = index_->FilterByPath("/keys7", size_, filter_func);
            ASSERT_EQ(bitset.size(), size_);
            for (int i = 0; i < bitset.size(); ++i) {
                auto ans = bitset[i];
                auto ref = check("970724117");
                ASSERT_EQ(ans, ref);
            }
        }
    }

 public:
    std::shared_ptr<JsonKeyInvertedIndex> index_;
    DataType type_;
    size_t size_;
    size_t dim_;
    std::vector<milvus::Json> data_;
    std::shared_ptr<storage::ChunkManager> chunk_manager_;
};

TEST_F(JsonKeyIndexTest, CountFuncTest) {
    TestTermInFunc();
    TestUnaryRangeInFunc();
    TestBinaryRangeInFunc();
}