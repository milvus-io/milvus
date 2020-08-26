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

#include "db/wal/WalOperationCodec.h"
#include "utils/Log.h"

#include <memory>
#include <utility>

namespace milvus {
namespace engine {

Status
WalOperationCodec::WriteInsertOperation(const WalFilePtr& file, const std::string& partition_name,
                                        const DataChunkPtr& chunk, idx_t op_id) {
    if (file == nullptr || !file->IsOpened() || chunk == nullptr) {
        return Status(DB_ERROR, "Invalid input for write insert operation");
    }

    try {
        // calculate total bytes, it must equal to total_bytes
        int64_t calculate_total_bytes = 0;
        calculate_total_bytes += sizeof(int32_t);        // operation type
        calculate_total_bytes += sizeof(idx_t);          // operation id
        calculate_total_bytes += sizeof(int64_t);        // calculated total bytes
        calculate_total_bytes += sizeof(int32_t);        // partition name length
        calculate_total_bytes += partition_name.size();  // partition name
        calculate_total_bytes += sizeof(int64_t);        // chunk entity count
        calculate_total_bytes += sizeof(int32_t);        // fixed field count
        for (auto& pair : chunk->fixed_fields_) {
            calculate_total_bytes += sizeof(int32_t);    // field name length
            calculate_total_bytes += pair.first.size();  // field name

            calculate_total_bytes += sizeof(int64_t);            // data size
            calculate_total_bytes += pair.second->data_.size();  // data
        }
        calculate_total_bytes += sizeof(idx_t);  // operation id again

        int64_t total_bytes = 0;
        // write operation type
        int32_t type = WalOperationType::INSERT_ENTITY;
        total_bytes += file->Write<int32_t>(&type);

        // write operation id
        total_bytes += file->Write<idx_t>(&op_id);

        // write calculated total bytes
        total_bytes += file->Write<int64_t>(&calculate_total_bytes);

        // write partition name
        int32_t part_name_length = partition_name.size();
        total_bytes += file->Write<int32_t>(&part_name_length);
        if (part_name_length > 0) {
            total_bytes += file->Write(partition_name.data(), part_name_length);
        }

        // write chunk entity count
        total_bytes += file->Write<int64_t>(&(chunk->count_));

        // write fixed data
        int32_t field_count = chunk->fixed_fields_.size();
        total_bytes += file->Write<int32_t>(&field_count);
        for (auto& pair : chunk->fixed_fields_) {
            if (pair.second == nullptr) {
                continue;
            }

            int32_t field_name_length = pair.first.size();
            total_bytes += file->Write<int32_t>(&field_name_length);
            total_bytes += file->Write(pair.first.data(), field_name_length);

            int64_t data_size = pair.second->data_.size();
            total_bytes += file->Write<int64_t>(&data_size);
            total_bytes += file->Write(pair.second->data_.data(), data_size);
        }

        // TODO: write variable data

        // write operation id again
        // Note: makesure operation id is written at end, so that wal cleanup thread know which file can be deleted
        total_bytes += file->Write<idx_t>(&op_id);

        // flush to system buffer
        file->Flush();

        if (total_bytes != calculate_total_bytes) {
            LOG_ENGINE_ERROR_ << "wal serialize(insert) bytes " << total_bytes << " not equal "
                              << calculate_total_bytes;
        }
    } catch (std::exception& ex) {
        std::string msg = "Failed to write insert operation, reason: " + std::string(ex.what());
        return Status(DB_ERROR, msg);
    }

    return Status::OK();
}

Status
WalOperationCodec::WriteDeleteOperation(const WalFilePtr& file, const IDNumbers& entity_ids, idx_t op_id) {
    if (file == nullptr || !file->IsOpened() || entity_ids.empty()) {
        return Status(DB_ERROR, "Invalid input for write delete operation");
    }

    try {
        // calculate total bytes, it must equal to total_bytes
        int64_t calculate_total_bytes = 0;
        calculate_total_bytes += sizeof(int32_t);                    // operation type
        calculate_total_bytes += sizeof(idx_t);                      // operation id
        calculate_total_bytes += sizeof(int64_t);                    // calculated total bytes
        calculate_total_bytes += sizeof(int64_t);                    // id count
        calculate_total_bytes += entity_ids.size() * sizeof(idx_t);  // ids
        calculate_total_bytes += sizeof(idx_t);                      // operation id again

        int64_t total_bytes = 0;
        // write operation type
        int32_t type = WalOperationType::DELETE_ENTITY;
        total_bytes += file->Write<int32_t>(&type);

        // write operation id
        total_bytes += file->Write<idx_t>(&op_id);

        // write calculated total bytes
        total_bytes += file->Write<int64_t>(&calculate_total_bytes);

        // write entity ids
        int64_t id_count = entity_ids.size();
        total_bytes += file->Write<int64_t>(&id_count);

        total_bytes += file->Write(entity_ids.data(), id_count * sizeof(idx_t));

        // write operation id again
        // Note: makesure operation id is written at end, so that wal cleanup thread know which file can be deleted
        total_bytes += file->Write<idx_t>(&op_id);

        // flush to system buffer
        file->Flush();

        if (total_bytes != calculate_total_bytes) {
            LOG_ENGINE_ERROR_ << "wal serialize(delete) bytes " << total_bytes << " not equal "
                              << calculate_total_bytes;
        }
    } catch (std::exception& ex) {
        std::string msg = "Failed to write insert operation, reason: " + std::string(ex.what());
        return Status(DB_ERROR, msg);
    }

    return Status::OK();
}

Status
WalOperationCodec::IterateOperation(const WalFilePtr& file, WalOperationPtr& operation, idx_t from_op_id) {
    if (file == nullptr || !file->IsOpened()) {
        return Status(DB_ERROR, "Invalid input iterate wal operation");
    }

    // read operation type
    int32_t type = WalOperationType::INVALID;
    int64_t read_bytes = file->Read<int32_t>(&type);
    if (read_bytes <= 0) {
        return Status(DB_ERROR, "End of file");
    }

    // read operation id
    idx_t op_id = 0;
    read_bytes = file->Read<idx_t>(&op_id);
    if (read_bytes <= 0) {
        return Status(DB_ERROR, "End of file");
    }

    // read total bytes
    int64_t total_bytes = 0;
    read_bytes = file->Read<int64_t>(&total_bytes);
    if (read_bytes <= 0) {
        return Status(DB_ERROR, "End of file");
    }

    // if the operation id is less/equal than from_op_id, skip this operation
    if (op_id <= from_op_id) {
        int64_t offset = total_bytes - sizeof(int32_t) - sizeof(idx_t) - sizeof(int64_t);
        file->SeekForward(offset);
        return Status::OK();
    }

    if (type == WalOperationType::INSERT_ENTITY) {
        // read partition name
        int32_t part_name_length = 0;
        std::string partition_name;
        file->Read<int32_t>(&part_name_length);
        if (part_name_length > 0) {
            read_bytes = file->ReadStr(partition_name, part_name_length);
            if (read_bytes <= 0) {
                return Status(DB_ERROR, "End of file");
            }
        }

        // read chunk entity countint64_t total_bytes = 0;
        DataChunkPtr chunk = std::make_shared<DataChunk>();
        read_bytes = file->Read<int64_t>(&(chunk->count_));
        if (read_bytes <= 0) {
            return Status(DB_ERROR, "End of file");
        }

        // read fixed data
        int32_t field_count = 0;
        read_bytes = file->Read<int32_t>(&field_count);
        if (read_bytes <= 0) {
            return Status(DB_ERROR, "End of file");
        }

        for (int32_t i = 0; i < field_count; i++) {
            int32_t field_name_length = 0;
            read_bytes = file->Read<int32_t>(&field_name_length);
            if (read_bytes <= 0) {
                return Status(DB_ERROR, "End of file");
            }

            // field name
            std::string field_name;
            read_bytes = file->ReadStr(field_name, field_name_length);
            if (read_bytes <= 0) {
                return Status(DB_ERROR, "End of file");
            }

            // binary data
            int64_t data_size = 0;
            read_bytes = file->Read<int64_t>(&data_size);
            if (read_bytes <= 0) {
                return Status(DB_ERROR, "End of file");
            }

            BinaryDataPtr data = std::make_shared<BinaryData>();
            data->data_.resize(data_size);
            read_bytes = file->Read(data->data_.data(), data_size);
            if (read_bytes <= 0) {
                return Status(DB_ERROR, "End of file");
            }

            chunk->fixed_fields_.insert(std::make_pair(field_name, data));
        }

        InsertEntityOperationPtr insert_op = std::make_shared<InsertEntityOperation>();
        insert_op->partition_name = partition_name;
        insert_op->data_chunk_ = chunk;
        operation = insert_op;
    } else if (type == WalOperationType::DELETE_ENTITY) {
        // read entity ids
        int64_t id_count = 0;
        read_bytes = file->Read<int64_t>(&id_count);
        if (read_bytes <= 0) {
            return Status(DB_ERROR, "End of file");
        }

        IDNumbers ids;
        ids.resize(id_count);
        read_bytes = file->Read(ids.data(), id_count * sizeof(idx_t));
        if (read_bytes <= 0) {
            return Status(DB_ERROR, "End of file");
        }

        DeleteEntityOperationPtr delete_op = std::make_shared<DeleteEntityOperation>();
        delete_op->entity_ids_.swap(ids);
        operation = delete_op;
    }

    read_bytes = file->Read<idx_t>(&op_id);
    if (read_bytes <= 0) {
        return Status(DB_ERROR, "End of file");
    }

    operation->SetID(op_id);

    return Status::OK();
}

}  // namespace engine
}  // namespace milvus
