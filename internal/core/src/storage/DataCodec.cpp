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

#include "storage/DataCodec.h"
#include <memory>
#include "storage/Event.h"
#include "log/Log.h"
#include "storage/Util.h"
#include "storage/InsertData.h"
#include "storage/IndexData.h"
#include "storage/BinlogReader.h"
#include "storage/PluginLoader.h"
#include "common/EasyAssert.h"
#include "common/Consts.h"

namespace milvus::storage {

std::unique_ptr<DataCodec>
DeserializeFileData(const std::shared_ptr<uint8_t[]> input_data,
                    int64_t length,
                    bool is_field_data) {
    auto buff_to_keep = input_data;  // ref += 1
    auto reader =
        std::make_shared<BinlogReader>(buff_to_keep, length);  //ref += 1
    ReadMediumType(reader);

    DescriptorEvent descriptor_event(reader);
    auto data_type =
        static_cast<DataType>(descriptor_event.event_data.fix_part.data_type);
    auto& extras = descriptor_event.event_data.extras;
    bool nullable = (extras.find(NULLABLE) != extras.end())
                        ? std::any_cast<bool>(extras[NULLABLE])
                        : false;
    auto descriptor_fix_part = descriptor_event.event_data.fix_part;
    FieldDataMeta data_meta{descriptor_fix_part.collection_id,
                            descriptor_fix_part.partition_id,
                            descriptor_fix_part.segment_id,
                            descriptor_fix_part.field_id};

    auto edek = descriptor_event.GetEdekFromExtra();
    if (edek.length() > 0) {
        auto cipherPlugin = PluginLoader::GetInstance().getCipherPlugin();
        AssertInfo(cipherPlugin != nullptr,
                   "cipher plugin missing for an encrypted file");

        int64_t ez_id = descriptor_event.GetEZFromExtra();
        AssertInfo(ez_id != -1, "ez_id meta not exist for a encrypted file");
        auto decryptor = cipherPlugin->GetDecryptor(
            ez_id, descriptor_fix_part.collection_id, edek);

        auto left_size = length - descriptor_event.event_header.next_position_;
        LOG_INFO(
            "start decrypting data, ez_id: {}, collection_id: {}, total "
            "length: {}, descriptor_length: {}, cipher text length: {}",
            ez_id,
            descriptor_fix_part.collection_id,
            length,
            descriptor_event.event_header.next_position_,
            left_size);

        AssertInfo(left_size > 0, "cipher text length is 0");
        std::string cipher_str;
        cipher_str.resize(left_size);  // allocate enough space for size bytes

        auto err =
            reader->Read(left_size, reinterpret_cast<void*>(cipher_str.data()));
        AssertInfo(err.ok(), "Read binlog failed, err = {}", err.what());

        auto decrypted_str = decryptor->Decrypt(cipher_str);
        LOG_INFO(
            "cipher plugin decrypted data: cipher text length: {}, plain text "
            "length: {}",
            left_size,
            decrypted_str.size());

        auto decrypted_ptr =
            std::shared_ptr<uint8_t[]>(new uint8_t[decrypted_str.size()],
                                       [](uint8_t* ptr) { delete[] ptr; });
        memcpy(decrypted_ptr.get(), decrypted_str.data(), decrypted_str.size());
        buff_to_keep = decrypted_ptr;

        reader =
            std::make_shared<BinlogReader>(buff_to_keep, decrypted_str.size());
    }

    EventHeader header(reader);
    auto event_data_length = header.event_length_ - GetEventHeaderSize(header);
    switch (header.event_type_) {
        case EventType::InsertEvent: {
            auto insert_event_data = InsertEventData(
                reader, event_data_length, data_type, nullable, is_field_data);

            std::unique_ptr<InsertData> insert_data;
            insert_data =
                std::make_unique<InsertData>(insert_event_data.payload_reader);
            insert_data->SetFieldDataMeta(data_meta);
            insert_data->SetTimestamps(insert_event_data.start_timestamp,
                                       insert_event_data.end_timestamp);
            // DataCodec must keep the input_data alive for zero-copy usage,
            // otherwise segmentation violation will occur
            insert_data->SetData(buff_to_keep);
            return insert_data;
        }
        case EventType::IndexFileEvent: {
            auto index_event_data =
                IndexEventData(reader, event_data_length, data_type, nullable);

            if (index_event_data.payload_reader->get_payload_datatype() ==
                DataType::STRING) {
                AssertInfo(index_event_data.payload_reader->has_field_data(),
                           "old index having no field_data");
                auto field_data =
                    index_event_data.payload_reader->get_field_data();
                AssertInfo(field_data->get_data_type() == DataType::STRING,
                           "wrong index type in index binlog file");
                AssertInfo(
                    field_data->get_num_rows() == 1,
                    "wrong length of string num in old index binlog file");
                auto new_field_data =
                    CreateFieldData(DataType::INT8, DataType::NONE, nullable);
                new_field_data->FillFieldData(
                    (*static_cast<const std::string*>(field_data->RawValue(0)))
                        .c_str(),
                    field_data->Size());
                index_event_data.payload_reader =
                    std::make_shared<PayloadReader>(new_field_data);
            }
            auto index_data =
                std::make_unique<IndexData>(index_event_data.payload_reader);
            index_data->SetFieldDataMeta(data_meta);
            IndexMeta index_meta;
            index_meta.segment_id = data_meta.segment_id;
            index_meta.field_id = data_meta.field_id;
            auto& extras = descriptor_event.event_data.extras;
            AssertInfo(extras.find(INDEX_BUILD_ID_KEY) != extras.end(),
                       "index build id not exist");
            index_meta.build_id = std::stol(
                std::any_cast<std::string>(extras[INDEX_BUILD_ID_KEY]));
            index_data->set_index_meta(index_meta);
            index_data->SetTimestamps(index_event_data.start_timestamp,
                                      index_event_data.end_timestamp);
            // DataCodec must keep the input_data alive for zero-copy usage,
            // otherwise segmentation violation will occur
            index_data->SetData(buff_to_keep);
            return index_data;
        }
        default:
            ThrowInfo(
                DataFormatBroken,
                fmt::format("unsupported event type {}", header.event_type_));
    }
}

}  // namespace milvus::storage
