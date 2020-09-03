// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include <fiu-control.h>
#include <fiu/fiu-local.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <set>
#include <string>
#include <experimental/filesystem>

#include "db/DBProxy.h"
#include "db/utils.h"
#include "db/transcript/ScriptFile.h"
#include "db/transcript/ScriptCodec.h"
#include "db/transcript/ScriptRecorder.h"
#include "db/transcript/ScriptReplay.h"
#include "db/transcript/TranscriptProxy.h"

namespace {

using DBProxy = milvus::engine::DBProxy;
using VaribleData = milvus::engine::VaribleData;

using ScriptFile = milvus::engine::ScriptFile;
using ScriptCodec = milvus::engine::ScriptCodec;
using ScriptRecorder = milvus::engine::ScriptRecorder;
using ScriptReplay = milvus::engine::ScriptReplay;
using TranscriptProxy = milvus::engine::TranscriptProxy;

const char* COLLECTION_NAME = "wal_tbl";
const char* VECTOR_FIELD_NAME = "vector";
const char* INT_FIELD_NAME = "int";
const char* STRING_FIELD_NAME = "string";

void
CreateContext(CreateCollectionContext& context) {
    auto collection_schema = std::make_shared<Collection>(COLLECTION_NAME);
    context.collection = collection_schema;
    auto vector_field = std::make_shared<Field>(VECTOR_FIELD_NAME, 0, milvus::engine::DataType::VECTOR_FLOAT);
    auto int_field = std::make_shared<Field>(INT_FIELD_NAME, 0, milvus::engine::DataType::INT32);
    auto str_field = std::make_shared<Field>(STRING_FIELD_NAME, 0, milvus::engine::DataType::STRING);
    context.fields_schema[vector_field] = {};
    context.fields_schema[int_field] = {};
    context.fields_schema[str_field] = {};

    auto params = context.collection->GetParams();
    params[milvus::engine::PARAM_UID_AUTOGEN] = true;
    params[milvus::engine::PARAM_SEGMENT_ROW_COUNT] = 1000;
    context.collection->SetParams(params);
}

void
CreateChunk(DataChunkPtr& chunk, int64_t row_count) {
    chunk = std::make_shared<DataChunk>();
    chunk->count_ = row_count;
    {
        // int32 type field
        std::string field_name = INT_FIELD_NAME;
        auto bin = std::make_shared<BinaryData>();
        bin->data_.resize(chunk->count_ * sizeof(int32_t));
        int32_t* p = (int32_t*)(bin->data_.data());
        for (int64_t i = 0; i < chunk->count_; ++i) {
            p[i] = i;
        }
        chunk->fixed_fields_.insert(std::make_pair(field_name, bin));
    }
    {
        // vector type field
        int64_t dimension = 128;
        std::string field_name = VECTOR_FIELD_NAME;
        auto bin = std::make_shared<BinaryData>();
        bin->data_.resize(chunk->count_ * sizeof(float) * dimension);
        float* p = (float*)(bin->data_.data());
        for (int64_t i = 0; i < chunk->count_; ++i) {
            for (int64_t j = 0; j < dimension; ++j) {
                p[i * dimension + j] = 100 + i * j / 100.0;
            }
        }
        chunk->fixed_fields_.insert(std::make_pair(field_name, bin));
    }

    {
        // string type field
        std::string field_name = STRING_FIELD_NAME;
        auto bin = std::make_shared<VaribleData>();
        bin->data_.resize(chunk->count_);
        memset(bin->data_.data(), 1, chunk->count_);
        bin->offset_.resize(chunk->count_);
        for (int64_t i = 0; i < chunk->count_; ++i) {
            bin->offset_[i] = 1;
        }
        chunk->variable_fields_.insert(std::make_pair(field_name, bin));
    }
}

void
CreateQuery(milvus::query::QueryPtr& query) {
    query = std::make_shared<milvus::query::Query>();
    query->collection_id = COLLECTION_NAME;
    query->partitions = {"p1", "p2", "p3"};
    query->field_names = {"f1", "f2", "f3"};
    query->index_fields = {"a", "b", "c"};
    query->metric_types = {
        {"a", "IP"},
        {"b", "L2"},
    };

    // vector queries
    for (int i = 0; i < 3; ++i) {
        milvus::query::VectorQueryPtr vector_query = std::make_shared<milvus::query::VectorQuery>();
        vector_query->field_name = "f" + std::to_string(i);
        vector_query->metric_type = "L2";
        vector_query->extra_params["nlist"] = 16384;
        vector_query->topk = i + 10;
        vector_query->nq = i + 100;
        vector_query->boost = false;
        vector_query->query_vector.float_data = {0.1, 0.2, 0.3, 0.4};
        vector_query->query_vector.binary_data = {1, 2, 3, 4, 5, 6};
        query->vectors.insert(std::make_pair(std::to_string(i), vector_query));
    }

    // general query
    query->root = std::make_shared<milvus::query::GeneralQuery>();
    query->root->leaf = std::make_shared<milvus::query::LeafQuery>();
    query->root->leaf->vector_placeholder = "placeholder";
    query->root->leaf->query_boost = 0.6;
    query->root->leaf->term_query = std::make_shared<milvus::query::TermQuery>();
    query->root->leaf->term_query->json_obj["term"] = 1;
    query->root->leaf->range_query = std::make_shared<milvus::query::RangeQuery>();
    query->root->leaf->range_query->json_obj["range"] = 10.0;
    query->root->bin = std::make_shared<milvus::query::BinaryQuery>();
    query->root->bin->query_boost = 99.9;
    query->root->bin->relation = milvus::query::QueryRelation::R3;
    query->root->bin->left_query = std::make_shared<milvus::query::GeneralQuery>();
    query->root->bin->left_query->leaf = std::make_shared<milvus::query::LeafQuery>();
    query->root->bin->left_query->leaf->vector_placeholder = "holder";
    query->root->bin->right_query = std::make_shared<milvus::query::GeneralQuery>();
    query->root->bin->right_query->bin = std::make_shared<milvus::query::BinaryQuery>();
    query->root->bin->right_query->bin->query_boost = 33.3;
}

class DummyDB : public DBProxy {
 public:
    explicit DummyDB(const DBOptions& options)
        : DBProxy(nullptr, options) {
    }

    Status
    CreateCollection(const milvus::engine::snapshot::CreateCollectionContext& context) override {
        actions_record_.emplace_back(milvus::engine::ActionCreateCollection);
        return Status::OK();
    }

    Status
    DropCollection(const std::string& collection_name) override {
        actions_record_.emplace_back(milvus::engine::ActionDropCollection);
        return Status::OK();
    }

    Status
    HasCollection(const std::string& collection_name, bool& has_or_not) override {
        actions_record_.emplace_back(milvus::engine::ActionHasCollection);
        return Status::OK();
    }

    Status
    ListCollections(std::vector<std::string>& names) override {
        actions_record_.emplace_back(milvus::engine::ActionListCollections);
        return Status::OK();
    }

    Status
    GetCollectionInfo(const std::string& collection_name, milvus::engine::snapshot::CollectionPtr& collection,
                      milvus::engine::snapshot::FieldElementMappings& fields_schema) override {
        actions_record_.emplace_back(milvus::engine::ActionGetCollectionInfo);
        return Status::OK();
    }

    Status
    GetCollectionStats(const std::string& collection_name, milvus::json& collection_stats) override {
        actions_record_.emplace_back(milvus::engine::ActionGetCollectionStats);
        return Status::OK();
    }

    Status
    CountEntities(const std::string& collection_name, int64_t& row_count) override {
        actions_record_.emplace_back(milvus::engine::ActionCountEntities);
        return Status::OK();
    }

    Status
    CreatePartition(const std::string& collection_name, const std::string& partition_name) override {
        actions_record_.emplace_back(milvus::engine::ActionCreatePartition);
        return Status::OK();
    }

    Status
    DropPartition(const std::string& collection_name, const std::string& partition_name) override {
        actions_record_.emplace_back(milvus::engine::ActionDropPartition);
        return Status::OK();
    }

    Status
    HasPartition(const std::string& collection_name, const std::string& partition_tag, bool& exist) override {
        actions_record_.emplace_back(milvus::engine::ActionHasPartition);
        return Status::OK();
    }

    Status
    ListPartitions(const std::string& collection_name, std::vector<std::string>& partition_names) override {
        actions_record_.emplace_back(milvus::engine::ActionListPartitions);
        return Status::OK();
    }

    Status
    CreateIndex(const milvus::server::ContextPtr& context,
                const std::string& collection_name,
                const std::string& field_name,
                const milvus::engine::CollectionIndex& index) override {
        actions_record_.emplace_back(milvus::engine::ActionCreateIndex);
        return Status::OK();
    }

    Status
    DropIndex(const std::string& collection_name, const std::string& field_name) override {
        actions_record_.emplace_back(milvus::engine::ActionDropIndex);
        return Status::OK();
    }

    Status
    DescribeIndex(const std::string& collection_name,
                  const std::string& field_name,
                  milvus::engine::CollectionIndex& index) override {
        actions_record_.emplace_back(milvus::engine::ActionDescribeIndex);
        return Status::OK();
    }

    Status
    Insert(const std::string& collection_name, const std::string& partition_name, DataChunkPtr& data_chunk,
           idx_t op_id) override {
        actions_record_.emplace_back(milvus::engine::ActionInsert);
        return Status::OK();
    }

    Status
    GetEntityByID(const std::string& collection_name, const IDNumbers& id_array,
                  const std::vector<std::string>& field_names, std::vector<bool>& valid_row,
                  DataChunkPtr& data_chunk) override {
        actions_record_.emplace_back(milvus::engine::ActionGetEntityByID);
        return Status::OK();
    }

    Status
    DeleteEntityByID(const std::string& collection_name,
                     const milvus::engine::IDNumbers& entity_ids,
                     idx_t op_id) override {
        actions_record_.emplace_back(milvus::engine::ActionDeleteEntityByID);
        return Status::OK();
    }

    Status
    ListIDInSegment(const std::string& collection_name, int64_t segment_id, IDNumbers& entity_ids) override {
        actions_record_.emplace_back(milvus::engine::ActionListIDInSegment);
        return Status::OK();
    }

    Status
    Query(const milvus::server::ContextPtr& context,
          const milvus::query::QueryPtr& query_ptr,
          milvus::engine::QueryResultPtr& result) override {
        actions_record_.emplace_back(milvus::engine::ActionQuery);
        return Status::OK();
    }

    Status
    LoadCollection(const milvus::server::ContextPtr& context, const std::string& collection_name,
                   const std::vector<std::string>& field_names, bool force) override {
        actions_record_.emplace_back(milvus::engine::ActionLoadCollection);
        return Status::OK();
    }

    Status
    Flush(const std::string& collection_name) override {
        actions_record_.emplace_back(milvus::engine::ActionFlush);
        return Status::OK();
    }

    Status
    Flush() override {
        actions_record_.emplace_back(milvus::engine::ActionFlush);
        return Status::OK();
    }

    Status
    Compact(const milvus::server::ContextPtr& context, const std::string& collection_name, double threshold) override {
        actions_record_.emplace_back(milvus::engine::ActionCompact);
        return Status::OK();
    }

    const std::vector<std::string>& Actions() const {
        return actions_record_;
    }

 private:
    std::vector<std::string> actions_record_;
};

using DummyDBPtr = std::shared_ptr<DummyDB>;

} // namespace

TEST(TranscriptTest, CodecTest) {

    {
        milvus::json json_obj;
        std::string input = "action";
        ScriptCodec::EncodeAction(json_obj, input);

        std::string output;
        int64_t action_ts = 0;
        ScriptCodec::DecodeAction(json_obj, output, action_ts);
        ASSERT_EQ(input, output);
        ASSERT_GT(action_ts, 0);
    }

    {
        milvus::json json_obj;
        CreateCollectionContext input;
        CreateContext(input);
        ScriptCodec::Encode(json_obj, input);

        CreateCollectionContext output;
        ScriptCodec::Decode(json_obj, output);
        ASSERT_NE(output.collection, nullptr);
        ASSERT_EQ(output.collection->GetName(), input.collection->GetName());
        ASSERT_EQ(output.collection->GetParams(), input.collection->GetParams());
        ASSERT_EQ(output.fields_schema.size(), input.fields_schema.size());
    }

    {
        milvus::json json_obj;
        std::string input = "abc";
        ScriptCodec::EncodeCollectionName(json_obj, input);

        std::string output;
        ScriptCodec::DecodeCollectionName(json_obj, output);
        ASSERT_EQ(input, output);
    }

    {
        milvus::json json_obj;
        std::string input = "abc";
        ScriptCodec::EncodePartitionName(json_obj, input);

        std::string output;
        ScriptCodec::DecodePartitionName(json_obj, output);
        ASSERT_EQ(input, output);
    }

    {
        milvus::json json_obj;
        std::string input = "abc";
        ScriptCodec::EncodeFieldName(json_obj, input);

        std::string output;
        ScriptCodec::DecodeFieldName(json_obj, output);
        ASSERT_EQ(input, output);
    }

    {
        milvus::json json_obj;
        std::vector<std::string> input = {"abc", "cdf", "fjk"};
        ScriptCodec::EncodeFieldNames(json_obj, input);

        std::vector<std::string> output;
        ScriptCodec::DecodeFieldNames(json_obj, output);
        ASSERT_EQ(input, output);
    }

    {
        milvus::json json_obj;
        milvus::engine::CollectionIndex input;
        input.index_name_ = "a";
        input.index_type_ = "IVF";
        input.metric_name_ = "IP";
        input.extra_params_["NLIST"] = 1024;
        ScriptCodec::Encode(json_obj, input);

        milvus::engine::CollectionIndex output;
        ScriptCodec::Decode(json_obj, output);
        ASSERT_EQ(input.index_name_, output.index_name_);
        ASSERT_EQ(input.index_type_, output.index_type_);
        ASSERT_EQ(input.metric_name_, output.metric_name_);
        ASSERT_EQ(input.extra_params_, output.extra_params_);
    }

    {
        milvus::json json_obj;
        milvus::engine::DataChunkPtr input;
        CreateChunk(input, 10);
        ScriptCodec::Encode(json_obj, input);

        milvus::engine::DataChunkPtr output;
        ScriptCodec::Decode(json_obj, output);
        ASSERT_NE(output, nullptr);
        ASSERT_EQ(input->count_, output->count_);
        ASSERT_EQ(input->fixed_fields_.size(), output->fixed_fields_.size());
        ASSERT_EQ(input->variable_fields_.size(), output->variable_fields_.size());
        for (auto& pair : input->fixed_fields_) {
            auto& name = pair.first;
            auto& bin_2 = output->fixed_fields_[name];
            ASSERT_NE(bin_2, nullptr);
            auto& bin_1 = pair.second;
            ASSERT_NE(bin_1, nullptr);
            ASSERT_EQ(bin_1->data_, bin_2->data_);
        }
        for (auto& pair : input->variable_fields_) {
            auto& name = pair.first;
            auto& bin_2 = output->variable_fields_[name];
            ASSERT_NE(bin_2, nullptr);
            auto& bin_1 = pair.second;
            ASSERT_NE(bin_1, nullptr);
            ASSERT_EQ(bin_1->data_, bin_2->data_);
            ASSERT_EQ(bin_1->offset_, bin_2->offset_);
        }
    }

    {
        milvus::json json_obj;
        milvus::engine::IDNumbers input = {1, 3, 5, 7};
        ScriptCodec::Encode(json_obj, input);

        milvus::engine::IDNumbers output;
        ScriptCodec::Decode(json_obj, output);
        ASSERT_EQ(input, output);
    }

    {
        milvus::json json_obj;
        int64_t input = 5467;
        ScriptCodec::EncodeSegmentID(json_obj, input);

        int64_t output = 0;
        ScriptCodec::DecodeSegmentID(json_obj, output);
        ASSERT_EQ(input, output);
    }

    {
        milvus::json json_obj;
        milvus::query::QueryPtr input;
        CreateQuery(input);
        ScriptCodec::Encode(json_obj, input);

        milvus::query::QueryPtr output;
        ScriptCodec::Decode(json_obj, output);
        ASSERT_NE(output, nullptr);
        ASSERT_EQ(input->collection_id, output->collection_id);
        ASSERT_EQ(input->partitions, output->partitions);
        ASSERT_EQ(input->field_names, output->field_names);
        ASSERT_EQ(input->index_fields, output->index_fields);
        ASSERT_EQ(input->metric_types, output->metric_types);
        ASSERT_EQ(input->vectors.size(), output->vectors.size());

        // vector queries
        for (auto& pair : input->vectors) {
            ASSERT_GT(output->vectors.count(pair.first), 0);
            auto& query_1 = output->vectors[pair.first];
            auto& query_2 = pair.second;
            ASSERT_NE(query_1, nullptr);
            ASSERT_NE(query_2, nullptr);
            ASSERT_EQ(query_1->field_name, query_2->field_name);
            ASSERT_EQ(query_1->extra_params, query_2->extra_params);
            ASSERT_EQ(query_1->topk, query_2->topk);
            ASSERT_EQ(query_1->nq, query_2->nq);
            ASSERT_EQ(query_1->metric_type, query_2->metric_type);
            ASSERT_EQ(query_1->boost, query_2->boost);
            ASSERT_EQ(query_1->query_vector.float_data, query_2->query_vector.float_data);
            ASSERT_EQ(query_1->query_vector.binary_data, query_2->query_vector.binary_data);
        }

        // general query
        ASSERT_NE(output->root, nullptr);
        ASSERT_NE(output->root->leaf, nullptr);
        ASSERT_EQ(output->root->leaf->query_boost, input->root->leaf->query_boost);
        ASSERT_EQ(output->root->leaf->vector_placeholder, input->root->leaf->vector_placeholder);
        ASSERT_NE(output->root->leaf->term_query, nullptr);
        ASSERT_EQ(output->root->leaf->term_query->json_obj, input->root->leaf->term_query->json_obj);
        ASSERT_NE(output->root->leaf->range_query, nullptr);
        ASSERT_EQ(output->root->leaf->range_query->json_obj, input->root->leaf->range_query->json_obj);
        ASSERT_NE(output->root->bin, nullptr);
        ASSERT_EQ(output->root->bin->relation, input->root->bin->relation);
        ASSERT_EQ(output->root->bin->query_boost, input->root->bin->query_boost);
    }

    {
        milvus::json json_obj;
        double input = 2.5;
        ScriptCodec::EncodeThreshold(json_obj, input);

        double output = 0;
        ScriptCodec::DecodeThreshold(json_obj, output);
        ASSERT_EQ(input, output);
    }

    {
        milvus::json json_obj;
        bool input = true;
        ScriptCodec::EncodeForce(json_obj, input);

        bool output = false;
        ScriptCodec::DecodeForce(json_obj, output);
        ASSERT_EQ(input, output);
    }
}

TEST(TranscriptTest, FileTest) {
    std::string file_path = "/tmp/milvus_script_test.txt";
    std::experimental::filesystem::remove(file_path);
    int32_t repeat = 100;
    {
        ScriptFile file;
        file.OpenWrite(file_path);
        for (int32_t i = 0; i < repeat; ++i) {
            file.WriteLine(file_path);
        }

        ASSERT_TRUE(file.ExceedMaxSize(milvus::engine::MAX_SCRIPT_FILE_SIZE));
    }

    {
        ScriptFile file;
        file.OpenRead(file_path);

        int32_t count = 0;
        std::string line;
        while (file.ReadLine(line)) {
            ASSERT_EQ(line, file_path);
            count++;
        }
        ASSERT_EQ(count, repeat);
    }

}

TEST(TranscriptTest, ReplayTest) {
    DBOptions options;
    DummyDBPtr db = std::make_shared<DummyDB>(options);

    std::string transcript_path = "/tmp/milvus_transcript";
    ScriptRecorder recorder(transcript_path);

    // register action functions
    std::string collection_name = "collection";
    std::string partition_name = "partition";
    std::string field_name = "field";
    std::vector<std::string> actions;
    std::vector<std::function<void()>> functions;
    functions.emplace_back([&]() {
        milvus::engine::snapshot::CreateCollectionContext context;
        recorder.CreateCollection(context);
        actions.emplace_back(milvus::engine::ActionCreateCollection);
    });
    functions.emplace_back([&]() {
        recorder.DropCollection(collection_name);
        actions.emplace_back(milvus::engine::ActionDropCollection);
    });
    functions.emplace_back([&]() {
        bool has = false;
        recorder.HasCollection(collection_name, has);
        actions.emplace_back(milvus::engine::ActionHasCollection);
    });
    functions.emplace_back([&]() {
        std::vector<std::string> names;
        recorder.ListCollections(names);
        actions.emplace_back(milvus::engine::ActionListCollections);
    });
    functions.emplace_back([&]() {
        milvus::engine::snapshot::CollectionPtr collection;
        milvus::engine::snapshot::FieldElementMappings fields_schema;
        recorder.GetCollectionInfo(collection_name, collection, fields_schema);
        actions.emplace_back(milvus::engine::ActionGetCollectionInfo);
    });
    functions.emplace_back([&]() {
        milvus::json collection_stats;
        recorder.GetCollectionStats(collection_name, collection_stats);
        actions.emplace_back(milvus::engine::ActionGetCollectionStats);
    });
    functions.emplace_back([&]() {
        int64_t count = 0;
        recorder.CountEntities(collection_name, count);
        actions.emplace_back(milvus::engine::ActionCountEntities);
    });
    functions.emplace_back([&]() {
        recorder.CreatePartition(collection_name, partition_name);
        actions.emplace_back(milvus::engine::ActionCreatePartition);
    });
    functions.emplace_back([&]() {
        recorder.DropPartition(collection_name, partition_name);
        actions.emplace_back(milvus::engine::ActionDropPartition);
    });
    functions.emplace_back([&]() {
        bool has = false;
        recorder.HasPartition(collection_name, partition_name, has);
        actions.emplace_back(milvus::engine::ActionHasPartition);
    });
    functions.emplace_back([&]() {
        std::vector<std::string> partition_names;
        recorder.ListPartitions(collection_name, partition_names);
        actions.emplace_back(milvus::engine::ActionListPartitions);
    });
    functions.emplace_back([&]() {
        milvus::engine::CollectionIndex index;
        recorder.CreateIndex(nullptr, collection_name, field_name, index);
        actions.emplace_back(milvus::engine::ActionCreateIndex);
    });
    functions.emplace_back([&]() {
        recorder.DropIndex(collection_name, field_name);
        actions.emplace_back(milvus::engine::ActionDropIndex);
    });
    functions.emplace_back([&]() {
        milvus::engine::CollectionIndex index;
        index.index_type_ = "PQ";
        recorder.DescribeIndex(collection_name, field_name, index);
        actions.emplace_back(milvus::engine::ActionDescribeIndex);
    });
    functions.emplace_back([&]() {
        milvus::engine::DataChunkPtr chunk;
        recorder.Insert(collection_name, partition_name, chunk, 0);
        actions.emplace_back(milvus::engine::ActionInsert);
    });
    functions.emplace_back([&]() {
        IDNumbers id_array = {1, 2, 3};
        std::vector<std::string> field_names = {field_name};
        std::vector<bool> valid_row;
        DataChunkPtr data_chunk;
        recorder.GetEntityByID(collection_name, id_array, field_names, valid_row, data_chunk);
        actions.emplace_back(milvus::engine::ActionGetEntityByID);
    });
    functions.emplace_back([&]() {
        IDNumbers id_array = {1, 2, 3};
        recorder.DeleteEntityByID(collection_name, id_array, 0);
        actions.emplace_back(milvus::engine::ActionDeleteEntityByID);
    });
    functions.emplace_back([&]() {
        IDNumbers id_array;
        recorder.ListIDInSegment(collection_name, 1, id_array);
        actions.emplace_back(milvus::engine::ActionListIDInSegment);
    });
    functions.emplace_back([&]() {
        milvus::query::QueryPtr query_ptr;
        milvus::engine::QueryResultPtr result;
        recorder.Query(nullptr, query_ptr, result);
        actions.emplace_back(milvus::engine::ActionQuery);
    });
    functions.emplace_back([&]() {
        IDNumbers id_array = {1, 2, 3};
        std::vector<std::string> field_names = {field_name};
        recorder.LoadCollection(nullptr, collection_name, field_names, true);
        actions.emplace_back(milvus::engine::ActionLoadCollection);
    });
    functions.emplace_back([&]() {
        recorder.Flush(collection_name);
        actions.emplace_back(milvus::engine::ActionFlush);
    });
    functions.emplace_back([&]() {
        recorder.Flush();
        actions.emplace_back(milvus::engine::ActionFlush);
    });
    functions.emplace_back([&]() {
        recorder.Compact(nullptr, collection_name, 0.5);
        actions.emplace_back(milvus::engine::ActionCompact);
    });

    // random actions
    for (int32_t i = 0; i < 100; i++) {
        auto rand = lrand48();
        auto index = rand % functions.size();
        auto& function = functions.at(index);
        function();
    }

    // each action at least do one time
    for (size_t i = 0; i < functions.size(); ++i) {
        auto& function = functions.at(i);
        function();
    }

    // replay
    ScriptReplay replay;
    std::string script_path = recorder.GetScriptPath();
    auto status = replay.Replay(db, script_path);
    ASSERT_TRUE(status.ok());

    const std::vector<std::string>& record_actions = db->Actions();
    ASSERT_EQ(actions.size(), record_actions.size());
    ASSERT_EQ(actions, record_actions);

    std::experimental::filesystem::remove_all(transcript_path);
}

TEST(TranscriptTest, ProxyTest) {
    std::string test_path = "/tmp/milvus_test";
    DBOptions options;
    options.transcript_enable_ = true;
    options.meta_.path_ = test_path + "/db";
    DummyDBPtr db = std::make_shared<DummyDB>(options);
    TranscriptProxy proxy(db, options);
    proxy.Start();

    // register action functions
    std::string collection_name = "collection";
    std::string partition_name = "partition";
    std::string field_name = "field";
    std::vector<std::string> actions;
    std::vector<std::function<void()>> functions;
    functions.emplace_back([&]() {
    milvus::engine::snapshot::CreateCollectionContext context;
        proxy.CreateCollection(context);
        actions.emplace_back(milvus::engine::ActionCreateCollection);
    });
    functions.emplace_back([&]() {
        proxy.DropCollection(collection_name);
        actions.emplace_back(milvus::engine::ActionDropCollection);
    });
    functions.emplace_back([&]() {
    bool has = false;
        proxy.HasCollection(collection_name, has);
        actions.emplace_back(milvus::engine::ActionHasCollection);
    });
    functions.emplace_back([&]() {
        std::vector<std::string> names;
        proxy.ListCollections(names);
        actions.emplace_back(milvus::engine::ActionListCollections);
    });
    functions.emplace_back([&]() {
        milvus::engine::snapshot::CollectionPtr collection;
        milvus::engine::snapshot::FieldElementMappings fields_schema;
        proxy.GetCollectionInfo(collection_name, collection, fields_schema);
        actions.emplace_back(milvus::engine::ActionGetCollectionInfo);
    });
    functions.emplace_back([&]() {
        milvus::json collection_stats;
        proxy.GetCollectionStats(collection_name, collection_stats);
        actions.emplace_back(milvus::engine::ActionGetCollectionStats);
    });
    functions.emplace_back([&]() {
        int64_t count = 0;
        proxy.CountEntities(collection_name, count);
        actions.emplace_back(milvus::engine::ActionCountEntities);
    });
    functions.emplace_back([&]() {
        proxy.CreatePartition(collection_name, partition_name);
        actions.emplace_back(milvus::engine::ActionCreatePartition);
    });
    functions.emplace_back([&]() {
        proxy.DropPartition(collection_name, partition_name);
        actions.emplace_back(milvus::engine::ActionDropPartition);
    });
    functions.emplace_back([&]() {
        bool has = false;
        proxy.HasPartition(collection_name, partition_name, has);
        actions.emplace_back(milvus::engine::ActionHasPartition);
    });
    functions.emplace_back([&]() {
        std::vector<std::string> partition_names;
        proxy.ListPartitions(collection_name, partition_names);
        actions.emplace_back(milvus::engine::ActionListPartitions);
    });
    functions.emplace_back([&]() {
        milvus::engine::CollectionIndex index;
        proxy.CreateIndex(nullptr, collection_name, field_name, index);
        actions.emplace_back(milvus::engine::ActionCreateIndex);
    });
    functions.emplace_back([&]() {
        proxy.DropIndex(collection_name, field_name);
        actions.emplace_back(milvus::engine::ActionDropIndex);
    });
    functions.emplace_back([&]() {
        milvus::engine::CollectionIndex index;
        index.index_type_ = "PQ";
        proxy.DescribeIndex(collection_name, field_name, index);
        actions.emplace_back(milvus::engine::ActionDescribeIndex);
    });
    functions.emplace_back([&]() {
        milvus::engine::DataChunkPtr chunk;
        proxy.Insert(collection_name, partition_name, chunk, 0);
        actions.emplace_back(milvus::engine::ActionInsert);
    });
    functions.emplace_back([&]() {
        IDNumbers id_array = {1, 2, 3};
        std::vector<std::string> field_names = {field_name};
        std::vector<bool> valid_row;
        DataChunkPtr data_chunk;
        proxy.GetEntityByID(collection_name, id_array, field_names, valid_row, data_chunk);
        actions.emplace_back(milvus::engine::ActionGetEntityByID);
    });
    functions.emplace_back([&]() {
        IDNumbers id_array = {1, 2, 3};
        proxy.DeleteEntityByID(collection_name, id_array, 0);
        actions.emplace_back(milvus::engine::ActionDeleteEntityByID);
    });
    functions.emplace_back([&]() {
        IDNumbers id_array;
        proxy.ListIDInSegment(collection_name, 1, id_array);
        actions.emplace_back(milvus::engine::ActionListIDInSegment);
    });
    functions.emplace_back([&]() {
        milvus::query::QueryPtr query_ptr;
        milvus::engine::QueryResultPtr result;
        proxy.Query(nullptr, query_ptr, result);
        actions.emplace_back(milvus::engine::ActionQuery);
    });
    functions.emplace_back([&]() {
        IDNumbers id_array = {1, 2, 3};
        std::vector<std::string> field_names = {field_name};
        proxy.LoadCollection(nullptr, collection_name, field_names, true);
        actions.emplace_back(milvus::engine::ActionLoadCollection);
    });
    functions.emplace_back([&]() {
        proxy.Flush(collection_name);
        actions.emplace_back(milvus::engine::ActionFlush);
    });
    functions.emplace_back([&]() {
        proxy.Flush();
        actions.emplace_back(milvus::engine::ActionFlush);
    });
    functions.emplace_back([&]() {
        proxy.Compact(nullptr, collection_name, 0.5);
        actions.emplace_back(milvus::engine::ActionCompact);
    });

    // random actions
    for (int32_t i = 0; i < 100; i++) {
        auto rand = lrand48();
        auto index = rand % functions.size();
        auto& function = functions.at(index);
        function();
    }

    // each action at least do one time
    for (size_t i = 0; i < functions.size(); ++i) {
        auto& function = functions.at(i);
        function();
    }

    {
        const std::vector<std::string>& record_actions = db->Actions();
        ASSERT_EQ(actions.size(), record_actions.size());
    }

    // replay
    {
        options.replay_script_path_ = proxy.GetScriptRecorder()->GetScriptPath();
        proxy.Stop();
        DummyDBPtr db_replay = std::make_shared<DummyDB>(options);
        TranscriptProxy proxy_replay(db_replay, options);
        proxy_replay.Start();

        const std::vector<std::string>& record_actions = db_replay->Actions();
        ASSERT_EQ(actions.size(), record_actions.size());
        ASSERT_EQ(actions, record_actions);
        proxy_replay.Stop();
    }

    std::experimental::filesystem::remove_all(test_path);
}