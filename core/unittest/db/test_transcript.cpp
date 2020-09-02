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

#include "db/utils.h"
#include "db/transcript/ScriptFile.h"
#include "db/transcript/ScriptCodec.h"
#include "db/transcript/ScriptRecorder.h"
#include "db/transcript/ScriptReplay.h"

namespace {

using VaribleData = milvus::engine::VaribleData;

using ScriptFile = milvus::engine::ScriptFile;
using ScriptCodec = milvus::engine::ScriptCodec;
using ScriptRecorder = milvus::engine::ScriptRecorder;
using ScriptReplay = milvus::engine::ScriptReplay;

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
        ScriptCodec::Encode(json_obj, input);

        milvus::query::QueryPtr output;
        ScriptCodec::Decode(json_obj, output);
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
        while(file.ReadLine(line)) {
            ASSERT_EQ(line, file_path);
            count++;
        }
        ASSERT_EQ(count, repeat);
    }

}