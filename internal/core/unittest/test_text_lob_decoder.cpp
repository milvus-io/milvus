// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
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

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/c/bridge.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "milvus-storage/lob_column/lob_reference.h"
#include "storage/loon_ffi/text_lob_decoder_c.h"

TEST(TextLOBDecoder, DecodesInlineNullAndEmptyWithoutChangingReferences) {
    arrow::BinaryBuilder builder;
    auto first = milvus_storage::lob_column::EncodeInlineText("alpha");
    ASSERT_TRUE(builder.Append(first.data(), first.size()).ok());
    ASSERT_TRUE(builder.AppendNull().ok());
    auto empty = milvus_storage::lob_column::EncodeInlineText("");
    ASSERT_TRUE(builder.Append(empty.data(), empty.size()).ok());

    std::shared_ptr<arrow::Array> refs;
    ASSERT_TRUE(builder.Finish(&refs).ok());
    auto original = std::static_pointer_cast<arrow::BinaryArray>(refs);

    CStorageConfig config{};
    config.storage_type = "local";
    config.root_path = "/";
    CTextLOBDecoder decoder = nullptr;
    auto created = NewTextLOBDecoder(
        105, "/tmp/milvus-text-lob-test/lobs/105", config, &decoder);
    ASSERT_EQ(created.error_code, 0);

    ArrowArray exported_refs{};
    ArrowSchema exported_schema{};
    ASSERT_TRUE(
        arrow::ExportArray(*refs, &exported_refs, &exported_schema).ok());
    ArrowArray decoded{};
    auto status = DecodeTextLOB(decoder, &exported_refs, &decoded);
    ASSERT_EQ(status.error_code, 0);
    if (exported_schema.release != nullptr) {
        exported_schema.release(&exported_schema);
    }

    auto imported = arrow::ImportArray(&decoded, arrow::utf8());
    ASSERT_TRUE(imported.ok()) << imported.status().ToString();
    auto values =
        std::static_pointer_cast<arrow::StringArray>(imported.ValueOrDie());
    ASSERT_EQ(values->length(), 3);
    EXPECT_EQ(values->GetString(0), "alpha");
    EXPECT_TRUE(values->IsNull(1));
    EXPECT_EQ(values->GetString(2), "");
    EXPECT_EQ(original->GetString(0), std::string(first.begin(), first.end()));
    EXPECT_EQ(original->GetString(2), std::string(empty.begin(), empty.end()));

    auto closed = CloseTextLOBDecoder(decoder);
    EXPECT_EQ(closed.error_code, 0);
    EXPECT_EQ(values->GetString(0), "alpha");
}
