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

#include "common/EasyAssert.h"
#include "pb/schema.pb.h"
#include "segcore/token_stream_c.h"
#include "segcore/tokenizer_c.h"
#include "segcore/map_c.h"

using Map = std::map<std::string, std::string>;

TEST(ValidateTextSchema, Default) {
    milvus::proto::schema::FieldSchema schema;
    std::vector<uint8_t> buffer(schema.ByteSizeLong());
    schema.SerializeToArray(buffer.data(), buffer.size());
    auto status = validate_text_schema(buffer.data(), buffer.size());
    ASSERT_EQ(milvus::ErrorCode::Success, status.error_code);
}

TEST(ValidateTextSchema, JieBa) {
    milvus::proto::schema::FieldSchema schema;
    {
        auto kv = schema.add_type_params();
        kv->set_key("analyzer_params");
        kv->set_value(R"({"tokenizer": "jieba"})");
    }

    std::vector<uint8_t> buffer(schema.ByteSizeLong());
    schema.SerializeToArray(buffer.data(), buffer.size());
    auto status = validate_text_schema(buffer.data(), buffer.size());
    ASSERT_EQ(milvus::ErrorCode::Success, status.error_code);
}

void
set_cmap(CMap m, const std::string& key, const std::string& value) {
    cmap_set(m, key.c_str(), key.length(), value.c_str(), value.length());
}

TEST(CTokenizer, Default) {
    auto m = create_cmap();
    set_cmap(m, "tokenizer", "default");

    CTokenizer tokenizer;
    {
        auto status = create_tokenizer(m, &tokenizer);
        ASSERT_EQ(milvus::ErrorCode::Success, status.error_code);
    }

    std::string text("football, basketball, swimming");
    auto token_stream =
        create_token_stream(tokenizer, text.c_str(), text.length());

    std::vector<std::string> refs{"football", "basketball", "swimming"};
    for (int i = 0; i < 3; i++) {
        ASSERT_TRUE(token_stream_advance(token_stream));
        auto token = token_stream_get_token(token_stream);
        ASSERT_EQ(refs[i], std::string(token));
        free_token(const_cast<char*>(token));
    }
    ASSERT_FALSE(token_stream_advance(token_stream));

    free_token_stream(token_stream);
    free_tokenizer(tokenizer);
    free_cmap(m);
}
