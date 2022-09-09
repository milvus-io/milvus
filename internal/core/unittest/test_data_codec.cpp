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

#include "storage/DataCodec.h"
#include "storage/InsertData.h"
#include "storage/IndexData.h"
#include "common/Consts.h"
#include "utils/Json.h"

using namespace milvus;

TEST(storage, InsertDataFloat) {
    std::vector<float> data = {1, 2, 3, 4, 5};
    storage::Payload payload{storage::DataType::FLOAT, reinterpret_cast<const uint8_t*>(data.data()), int(data.size())};
    auto field_data = std::make_shared<storage::FieldData>(payload);

    storage::InsertData insert_data(field_data);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    auto new_insert_data = storage::DeserializeFileData(reinterpret_cast<const uint8_t*>(serialized_bytes.data()),
                                                        serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(), std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetPayload();
    ASSERT_EQ(new_payload->data_type, storage::DataType::FLOAT);
    ASSERT_EQ(new_payload->rows, data.size());
    std::vector<float> new_data(data.size());
    memcpy(new_data.data(), new_payload->raw_data, new_payload->rows * sizeof(float));
    ASSERT_EQ(data, new_data);
}

TEST(storage, InsertDataVectorFloat) {
    std::vector<float> data = {1, 2, 3, 4, 5, 6, 7, 8};
    int DIM = 2;
    storage::Payload payload{storage::DataType::VECTOR_FLOAT, reinterpret_cast<const uint8_t*>(data.data()),
                             int(data.size()) / DIM, DIM};
    auto field_data = std::make_shared<storage::FieldData>(payload);

    storage::InsertData insert_data(field_data);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    auto new_insert_data = storage::DeserializeFileData(reinterpret_cast<const uint8_t*>(serialized_bytes.data()),
                                                        serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(), std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetPayload();
    ASSERT_EQ(new_payload->data_type, storage::DataType::VECTOR_FLOAT);
    ASSERT_EQ(new_payload->rows, data.size() / DIM);
    std::vector<float> new_data(data.size());
    memcpy(new_data.data(), new_payload->raw_data, new_payload->rows * sizeof(float) * DIM);
    ASSERT_EQ(data, new_data);
}

TEST(storage, LocalInsertDataVectorFloat) {
    std::vector<float> data = {1, 2, 3, 4, 5, 6, 7, 8};
    int DIM = 2;
    storage::Payload payload{storage::DataType::VECTOR_FLOAT, reinterpret_cast<const uint8_t*>(data.data()),
                             int(data.size()) / DIM, DIM};
    auto field_data = std::make_shared<storage::FieldData>(payload);

    storage::InsertData insert_data(field_data);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::LocalDisk);
    auto new_insert_data =
        storage::DeserializeLocalInsertFileData(reinterpret_cast<const uint8_t*>(serialized_bytes.data()),
                                                serialized_bytes.size(), storage::DataType::VECTOR_FLOAT);
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    auto new_payload = new_insert_data->GetPayload();
    ASSERT_EQ(new_payload->data_type, storage::DataType::VECTOR_FLOAT);
    ASSERT_EQ(new_payload->rows, data.size() / DIM);
    std::vector<float> new_data(data.size());
    memcpy(new_data.data(), new_payload->raw_data, new_payload->rows * sizeof(float) * DIM);
    ASSERT_EQ(data, new_data);
}

TEST(storage, LocalIndexData) {
    std::vector<uint8_t> data = {1, 2, 3, 4, 5, 6, 7, 8};
    storage::Payload payload{storage::DataType::INT8, reinterpret_cast<const uint8_t*>(data.data()), int(data.size())};
    auto field_data = std::make_shared<storage::FieldData>(payload);
    storage::IndexData indexData_data(field_data);
    auto serialized_bytes = indexData_data.Serialize(storage::StorageType::LocalDisk);

    auto new_index_data = storage::DeserializeLocalIndexFileData(
        reinterpret_cast<const uint8_t*>(serialized_bytes.data()), serialized_bytes.size());
    ASSERT_EQ(new_index_data->GetCodecType(), storage::IndexDataType);
    auto new_payload = new_index_data->GetPayload();
    ASSERT_EQ(new_payload->data_type, storage::DataType::INT8);
    ASSERT_EQ(new_payload->rows, data.size());
    std::vector<uint8_t> new_data(data.size());
    memcpy(new_data.data(), new_payload->raw_data, new_payload->rows * sizeof(uint8_t));
    ASSERT_EQ(data, new_data);
}
