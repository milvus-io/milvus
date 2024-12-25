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
#include "storage/Event.h"
#include "storage/Util.h"
#include "storage/InsertData.h"
#include "storage/IndexData.h"
#include "storage/BinlogReader.h"
#include "common/EasyAssert.h"
#include "common/Consts.h"

namespace milvus::storage {

// deserialize remote insert and index file
std::unique_ptr<DataCodec>
DeserializeRemoteFileData(BinlogReaderPtr reader, bool is_field_data) {
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
    EventHeader header(reader);
    switch (header.event_type_) {
        case EventType::InsertEvent: {
            auto event_data_length =
                header.event_length_ - GetEventHeaderSize(header);
            auto insert_event_data = InsertEventData(
                reader, event_data_length, data_type, nullable, is_field_data);

            std::unique_ptr<InsertData> insert_data;
            if (is_field_data) {
                insert_data =
                    std::make_unique<InsertData>(insert_event_data.field_data);
            } else {
                insert_data = std::make_unique<InsertData>(
                    insert_event_data.payload_reader);
            }
            insert_data->SetFieldDataMeta(data_meta);
            insert_data->SetTimestamps(insert_event_data.start_timestamp,
                                       insert_event_data.end_timestamp);
            return insert_data;
        }
        case EventType::IndexFileEvent: {
            auto event_data_length =
                header.event_length_ - GetEventHeaderSize(header);
            auto index_event_data =
                IndexEventData(reader, event_data_length, data_type, nullable);
            auto field_data = index_event_data.field_data;
            // for compatible with golang indexcode.Serialize, which set dataType to String
            if (data_type == DataType::STRING) {
                AssertInfo(field_data->get_data_type() == DataType::STRING,
                           "wrong index type in index binlog file");
                AssertInfo(
                    field_data->get_num_rows() == 1,
                    "wrong length of string num in old index binlog file");
                auto new_field_data = CreateFieldData(DataType::INT8, nullable);
                new_field_data->FillFieldData(
                    (*static_cast<const std::string*>(field_data->RawValue(0)))
                        .c_str(),
                    field_data->Size());
                field_data = new_field_data;
            }

            auto index_data = std::make_unique<IndexData>(field_data);
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
            return index_data;
        }
        default:
            PanicInfo(
                DataFormatBroken,
                fmt::format("unsupported event type {}", header.event_type_));
    }
}

// For now, no file header in file data
std::unique_ptr<DataCodec>
DeserializeLocalFileData(BinlogReaderPtr reader) {
    PanicInfo(NotImplemented, "not supported");
}

std::unique_ptr<DataCodec>
DeserializeFileData(const std::shared_ptr<uint8_t[]> input_data,
                    int64_t length,
                    bool is_field_data) {
    auto binlog_reader = std::make_shared<BinlogReader>(input_data, length);
    auto medium_type = ReadMediumType(binlog_reader);
    std::unique_ptr<DataCodec> res;
    switch (medium_type) {
        case StorageType::Remote: {
            res = DeserializeRemoteFileData(binlog_reader, is_field_data);
            break;
        }
        case StorageType::LocalDisk: {
            res = DeserializeLocalFileData(binlog_reader);
            break;
        }
        default:
            PanicInfo(DataFormatBroken,
                      fmt::format("unsupported medium type {}", medium_type));
    }
    return res;
}

}  // namespace milvus::storage
