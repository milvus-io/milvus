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
#include <cstdint>
#include <cstdlib>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/EasyAssert.h"
#include "common/common_type_c.h"
#include "gtest/gtest.h"
#include "pb/common.pb.h"
#include "segcore/token_stream_c.h"
#include "segcore/tokenizer_c.h"

using Map = std::map<std::string, std::string>;

TEST(CTokenizer, Default) {
    auto analyzer_params = R"({"tokenizer": "standard"})";
    CTokenizer tokenizer;
    {
        auto status = create_tokenizer(analyzer_params, "", &tokenizer);
        ASSERT_EQ(milvus::ErrorCode::Success, status.error_code);
    }

    std::string text("football, basketball, swimming");
    CTokenStream token_stream;
    {
        auto status = create_token_stream(
            tokenizer, text.c_str(), text.length(), &token_stream);
        ASSERT_EQ(milvus::ErrorCode::Success, status.error_code);
    }

    std::vector<std::string> refs{"football", "basketball", "swimming"};
    std::vector<std::int64_t> offsets{0, 10, 22};
    for (int i = 0; i < 3; i++) {
        ASSERT_TRUE(token_stream_advance(token_stream));
        auto token = token_stream_get_token(token_stream);
        ASSERT_EQ(refs[i], std::string(token));
        free_token(const_cast<char*>(token));
    }
    ASSERT_FALSE(token_stream_advance(token_stream));

    free_token_stream(token_stream);

    {
        auto status = create_token_stream(
            tokenizer, text.c_str(), text.length(), &token_stream);
        ASSERT_EQ(milvus::ErrorCode::Success, status.error_code);
    }

    for (int i = 0; i < 3; i++) {
        ASSERT_TRUE(token_stream_advance(token_stream));
        auto token = token_stream_get_detailed_token(token_stream);
        ASSERT_EQ(refs[i], std::string(token.token));
        ASSERT_EQ(offsets[i], token.start_offset);

        free_token(const_cast<char*>(token.token));
    }
    ASSERT_FALSE(token_stream_advance(token_stream));
    free_token_stream(token_stream);

    free_tokenizer(tokenizer);
}

TEST(CTokenizer, BM25ErrorClassification) {
    CTokenizer tokenizer = nullptr;
    auto status =
        create_tokenizer(R"({"tokenizer":"whitespace"})", "", &tokenizer);
    ASSERT_EQ(milvus::Success, status.error_code);
    std::unique_ptr<void, decltype(&free_tokenizer)> guard(tokenizer,
                                                           free_tokenizer);
    auto check_failure = [&](const uint8_t* text,
                             uint64_t size,
                             const uint64_t* offsets,
                             uint64_t rows,
                             milvus::ErrorCode expected) {
        CBM25Batch output{};
        auto result =
            batch_tokenize_bm25(tokenizer, text, size, offsets, rows, &output);
        // Free even an unexpected partial result before recording failures.
        std::unique_ptr<void, decltype(&free_bm25_batch)> batch_guard(
            output.handle, free_bm25_batch);
        EXPECT_EQ(expected, result.error_code);
        EXPECT_EQ(nullptr, output.data);
        EXPECT_EQ(0, output.data_size);
        EXPECT_EQ(nullptr, output.offsets);
        EXPECT_EQ(nullptr, output.handle);
        free(const_cast<char*>(result.error_msg));
    };

    // The first row succeeds before malformed user text in the second row.
    // Call the C API directly: Go's UTF-8 check would mask the Rust error path.
    const uint8_t invalid_text[] = {'o', 'k', 0xff};
    const uint64_t text_offsets[] = {0, 2, 3};
    check_failure(invalid_text,
                  sizeof(invalid_text),
                  text_offsets,
                  2,
                  milvus::InvalidParameter);

    // Offsets and buffer pointers are produced internally, not user input.
    const uint64_t invalid_offsets[] = {0, 1};
    check_failure(invalid_text,
                  sizeof(invalid_text),
                  invalid_offsets,
                  1,
                  milvus::UnexpectedError);
    check_failure(nullptr, 3, text_offsets, 2, milvus::UnexpectedError);
    free_bm25_batch(nullptr);
}
