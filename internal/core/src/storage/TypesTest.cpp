// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include "storage/Types.h"

namespace milvus::storage {
namespace {

consteval bool
DefaultIndexMetaHasSafeValues() {
    IndexMeta index_meta;
    return index_meta.segment_id == 0 && index_meta.field_id == 0 &&
           index_meta.build_id == 0 && index_meta.index_version == 0 &&
           index_meta.key.empty() && index_meta.field_name.empty() &&
           index_meta.field_type == DataType::NONE && index_meta.dim == 0 &&
           !index_meta.index_non_encoding &&
           index_meta.index_store_path_version ==
               milvus::proto::index::IndexStorePathVersion::
                   INDEX_STORE_PATH_VERSION_BUILD_ROOTED;
}

static_assert(DefaultIndexMetaHasSafeValues());

TEST(StorageTypesTest, DefaultIndexMetaHasSafeValues) {
    IndexMeta index_meta;

    EXPECT_EQ(index_meta.segment_id, 0);
    EXPECT_EQ(index_meta.field_id, 0);
    EXPECT_EQ(index_meta.build_id, 0);
    EXPECT_EQ(index_meta.index_version, 0);
    EXPECT_TRUE(index_meta.key.empty());
    EXPECT_TRUE(index_meta.field_name.empty());
    EXPECT_EQ(index_meta.field_type, DataType::NONE);
    EXPECT_EQ(index_meta.dim, 0);
    EXPECT_FALSE(index_meta.index_non_encoding);
    EXPECT_EQ(index_meta.index_store_path_version,
              milvus::proto::index::IndexStorePathVersion::
                  INDEX_STORE_PATH_VERSION_BUILD_ROOTED);
}

}  // namespace
}  // namespace milvus::storage
