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

#include <optional>
#include <random>
#include <string>
#include <vector>
#include "common/EasyAssert.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "storage/RemoteChunkManagerSingleton.h"
#include "storage/Util.h"
#include "storage/storage_c.h"

using namespace std;
using namespace milvus;
using namespace milvus::storage;

string rootPath = "files";
string bucketName = "a-bucket";

CStorageConfig
get_azure_storage_config() {
    auto endpoint = "core.windows.net";
    auto accessKey = "devstoreaccount1";
    auto accessValue =
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
        "K1SZFPTOtr/KBHBeksoGMGw==";

    return CStorageConfig{
        endpoint,
        bucketName.c_str(),
        accessKey,
        accessValue,
        rootPath.c_str(),
        "remote",
        "azure",
        "",
        "error",
        "",
        false,
        "",
        false,
        false,
        30000,
        "",
    };
}

class StorageTest : public testing::Test {
 public:
    StorageTest() {
    }
    ~StorageTest() {
    }
    virtual void
    SetUp() {
    }
};

TEST_F(StorageTest, InitLocalChunkManagerSingleton) {
    auto status = InitLocalChunkManagerSingleton("tmp");
    EXPECT_EQ(status.error_code, Success);
}

TEST_F(StorageTest, GetLocalUsedSize) {
    int64_t size = 0;
    auto lcm = LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    EXPECT_EQ(lcm->GetRootPath(), "/tmp/milvus/local_data/");
    string test_dir = lcm->GetRootPath() + "tmp";
    string test_file = test_dir + "/test.txt";

    auto status = GetLocalUsedSize(test_dir.c_str(), &size);
    EXPECT_EQ(status.error_code, Success);
    EXPECT_EQ(size, 0);
    lcm->CreateDir(test_dir);
    lcm->CreateFile(test_file);
    uint8_t data[5] = {0x17, 0x32, 0x45, 0x34, 0x23};
    lcm->Write(test_file, data, sizeof(data));
    status = GetLocalUsedSize(test_dir.c_str(), &size);
    EXPECT_EQ(status.error_code, Success);
    EXPECT_EQ(size, 5);
    lcm->RemoveDir(test_dir);
}

TEST_F(StorageTest, InitRemoteChunkManagerSingleton) {
    CStorageConfig storageConfig = get_azure_storage_config();
    auto status = InitRemoteChunkManagerSingleton(storageConfig);
    EXPECT_STREQ(status.error_msg, "");
    EXPECT_EQ(status.error_code, Success);
    auto rcm =
        RemoteChunkManagerSingleton::GetInstance().GetRemoteChunkManager();
    EXPECT_EQ(rcm->GetRootPath(), "/tmp/milvus/remote_data");
}

TEST_F(StorageTest, CleanRemoteChunkManagerSingleton) {
    CleanRemoteChunkManagerSingleton();
}

class StorageUtilTest : public testing::Test {
 public:
    StorageUtilTest() = default;
    ~StorageUtilTest() {
    }
    void
    SetUp() override {
    }
};

TEST_F(StorageUtilTest, CreateArrowScalarFromDefaultValue) {
    {
        FieldMeta field_without_defval(
            FieldName("f"), FieldId(100), DataType::INT64, false, std::nullopt);
        ASSERT_ANY_THROW(
            CreateArrowScalarFromDefaultValue(field_without_defval));
    }
    {
        DefaultValueType default_value;
        default_value.set_int_data(10);
        FieldMeta int_field(FieldName("f"),
                            FieldId(100),
                            DataType::INT32,
                            false,
                            default_value);
        auto scalar = CreateArrowScalarFromDefaultValue(int_field);
        ASSERT_TRUE(scalar->Equals(*arrow::MakeScalar(int32_t(10))));
    }
    {
        DefaultValueType default_value;
        default_value.set_long_data(10);
        FieldMeta long_field(FieldName("f"),
                             FieldId(100),
                             DataType::INT64,
                             false,
                             default_value);
        auto scalar = CreateArrowScalarFromDefaultValue(long_field);
        ASSERT_TRUE(scalar->Equals(*arrow::MakeScalar(int64_t(10))));
    }
    {
        DefaultValueType default_value;
        default_value.set_float_data(1.0f);
        FieldMeta float_field(FieldName("f"),
                              FieldId(100),
                              DataType::FLOAT,
                              false,
                              default_value);
        auto scalar = CreateArrowScalarFromDefaultValue(float_field);
        ASSERT_TRUE(scalar->ApproxEquals(*arrow::MakeScalar(1.0f)));
    }
    {
        DefaultValueType default_value;
        default_value.set_double_data(1.0f);
        FieldMeta double_field(FieldName("f"),
                               FieldId(100),
                               DataType::DOUBLE,
                               false,
                               default_value);
        auto scalar = CreateArrowScalarFromDefaultValue(double_field);
        ASSERT_TRUE(scalar->ApproxEquals(arrow::DoubleScalar(1.0f)));
    }
    {
        DefaultValueType default_value;
        default_value.set_bool_data(true);
        FieldMeta bool_field(
            FieldName("f"), FieldId(100), DataType::BOOL, false, default_value);
        auto scalar = CreateArrowScalarFromDefaultValue(bool_field);
        ASSERT_TRUE(scalar->Equals(*arrow::MakeScalar(true)));
    }
    {
        DefaultValueType default_value;
        default_value.set_string_data("bar");
        FieldMeta varchar_field(FieldName("f"),
                                FieldId(100),
                                DataType::VARCHAR,
                                false,
                                default_value);
        auto scalar = CreateArrowScalarFromDefaultValue(varchar_field);
        ASSERT_TRUE(scalar->Equals(*arrow::MakeScalar("bar")));
    }
    {
        FieldMeta unsupport_field(
            FieldName("f"), FieldId(100), DataType::JSON, false, std::nullopt);
        ASSERT_ANY_THROW(CreateArrowScalarFromDefaultValue(unsupport_field));
    }
}
