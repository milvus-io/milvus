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

#include <gtest/gtest.h>

#include <cstdlib>
#include <memory>
#include <string>
#include <utility>

#include "clustering/analyze_c.h"
#include "indexbuilder/index_c.h"
#include "pb/clustering.pb.h"
#include "pb/index_cgo_msg.pb.h"
#include "storage/PluginLoader.h"
#include "test_utils/Constants.h"

namespace milvus::storage {
namespace {

class RecordingCipherPlugin : public plugin::ICipherPlugin {
 public:
    void
    Update(int64_t ez_id,
           int64_t collection_id,
           const std::string& key) override {
        update_count++;
        updated_ez_id = ez_id;
        updated_collection_id = collection_id;
        updated_key = key;
    }

    std::pair<std::shared_ptr<plugin::IEncryptor>, std::string>
    GetEncryptor(int64_t, int64_t) const override {
        return {};
    }

    std::shared_ptr<plugin::IDecryptor>
    GetDecryptor(int64_t, int64_t, const std::string&) const override {
        return nullptr;
    }

    int update_count = 0;
    int64_t updated_ez_id = 0;
    int64_t updated_collection_id = 0;
    std::string updated_key;
};

void
ExpectMissingCipherPlugin(CStatus status) {
    ASSERT_NE(status.error_code, Success);
    ASSERT_NE(status.error_msg, nullptr);
    EXPECT_NE(std::string(status.error_msg).find("failed to get cipher plugin"),
              std::string::npos);
    free(const_cast<char*>(status.error_msg));
}

std::string
BuildIndexInfoWithPluginContext() {
    proto::indexcgo::BuildIndexInfo build_index_info;
    build_index_info.mutable_field_schema()->set_data_type(
        proto::schema::DataType::Int64);

    auto storage_config = build_index_info.mutable_storage_config();
    storage_config->set_root_path(TestLocalPath);
    storage_config->set_storage_type("local");

    auto plugin_context = build_index_info.mutable_storage_plugin_context();
    plugin_context->set_encryption_zone_id(17);
    plugin_context->set_collection_id(23);
    plugin_context->set_encryption_key("unsafe-key");
    return build_index_info.SerializeAsString();
}

TEST(CipherPluginContextTest, AnalyzeUsesContextWhenPresent) {
    proto::clustering::AnalyzeInfo analyze_info;
    analyze_info.mutable_field_schema()->set_data_type(
        proto::schema::DataType::Int64);
    auto storage_config = analyze_info.mutable_storage_config();
    storage_config->set_root_path(TestLocalPath);
    storage_config->set_storage_type("local");
    auto serialized = analyze_info.SerializeAsString();

    CAnalyze analyze = nullptr;
    auto status = Analyze(&analyze,
                          reinterpret_cast<const uint8_t*>(serialized.data()),
                          serialized.size(),
                          nullptr);
    ASSERT_NE(status.error_code, Success);
    ASSERT_NE(status.error_msg, nullptr);
    EXPECT_NE(std::string(status.error_msg).find("invalid data type"),
              std::string::npos);
    free(const_cast<char*>(status.error_msg));

    CPluginContext plugin_context{17, 23, "unsafe-key"};
    status = Analyze(&analyze,
                     reinterpret_cast<const uint8_t*>(serialized.data()),
                     serialized.size(),
                     &plugin_context);
    ExpectMissingCipherPlugin(status);
    EXPECT_EQ(analyze, nullptr);
}

TEST(CipherPluginContextTest, IndexApisUseContextWhenPresent) {
    auto serialized = BuildIndexInfoWithPluginContext();
    auto data = reinterpret_cast<const uint8_t*>(serialized.data());

    CIndex index = nullptr;
    ExpectMissingCipherPlugin(CreateIndex(&index, data, serialized.size()));
    EXPECT_EQ(index, nullptr);

    ExpectMissingCipherPlugin(
        BuildJsonKeyIndex(nullptr, data, serialized.size()));
    ExpectMissingCipherPlugin(BuildTextIndex(nullptr, data, serialized.size()));
}

TEST(CipherPluginContextTest, RegistersPluginAndReturnsOnlyIdentifiers) {
    constexpr int64_t ez_id = 17;
    constexpr int64_t collection_id = 23;
    char key[] = "unsafe-key";

    auto cipher_plugin = std::make_shared<RecordingCipherPlugin>();
    auto context = PluginLoader::GetInstance().registerCipherPluginContext(
        ez_id, collection_id, std::string(key), cipher_plugin);

    key[0] = 'X';
    EXPECT_EQ(cipher_plugin->update_count, 1);
    EXPECT_EQ(cipher_plugin->updated_ez_id, ez_id);
    EXPECT_EQ(cipher_plugin->updated_collection_id, collection_id);
    EXPECT_EQ(cipher_plugin->updated_key, "unsafe-key");

    ASSERT_NE(context, nullptr);
    EXPECT_EQ(context->ez_id, ez_id);
    EXPECT_EQ(context->collection_id, collection_id);
    EXPECT_EQ(context->key, nullptr);
}

TEST(CipherPluginContextTest, RejectsMissingCipherPlugin) {
    EXPECT_ANY_THROW(PluginLoader::GetInstance().registerCipherPluginContext(
        17, 23, "unsafe-key", std::shared_ptr<plugin::ICipherPlugin>()));
}

}  // namespace
}  // namespace milvus::storage
