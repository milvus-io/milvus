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

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "db/Utils.h"
#include "db/attr/InstanceStructuredIndex.h"
#include "db/meta/FilesHolder.h"
#include "segment/SegmentReader.h"
#include "segment/SegmentWriter.h"

namespace milvus {
namespace Attr {

Status
InstanceStructuredIndex::CreateStructuredIndex(const std::string& collection_id, const engine::meta::MetaPtr meta_ptr) {
    std::vector<int> file_types = {
        milvus::engine::meta::SegmentSchema::RAW,
        milvus::engine::meta::SegmentSchema::TO_INDEX,
    };
    engine::meta::FilesHolder files_holder;
    auto status = meta_ptr->FilesByType(collection_id, file_types, files_holder);
    if (!status.ok()) {
        return status;
    }

    engine::meta::CollectionSchema collection_schema;
    engine::meta::hybrid::FieldsSchema fields_schema;
    collection_schema.collection_id_ = collection_id;
    status = meta_ptr->DescribeHybridCollection(collection_schema, fields_schema);
    if (!status.ok()) {
        return Status::OK();
    }

    for (auto& segment_schema : files_holder.HoldFiles()) {
        std::string segment_dir;
        engine::utils::GetParentPath(segment_schema.location_, segment_dir);
        auto segment_reader_ptr = std::make_shared<segment::SegmentReader>(segment_dir);
        segment::SegmentPtr segment_ptr;
        segment_reader_ptr->GetSegment(segment_ptr);
        status = segment_reader_ptr->Load();

        if (!status.ok()) {
            return status;
        }

        std::unordered_map<std::string, std::vector<uint8_t>> attr_datas;
        std::unordered_map<std::string, int64_t> attr_sizes;
        std::unordered_map<std::string, engine::meta::hybrid::DataType> attr_types;
        std::vector<std::string> field_names;

        for (auto& field_schema : fields_schema.fields_schema_) {
            if (field_schema.field_type_ != (int32_t)engine::meta::hybrid::DataType::VECTOR_FLOAT) {
                attr_types.insert(
                    std::make_pair(field_schema.field_name_, (engine::meta::hybrid::DataType)field_schema.field_type_));
                field_names.emplace_back(field_schema.field_name_);
            }
        }

        auto attrs = segment_ptr->attrs_ptr_->attrs;

        auto attr_it = attrs.begin();
        for (; attr_it != attrs.end(); attr_it++) {
            if (attr_it->second->GetCount() != 0) {
                attr_datas.insert(std::make_pair(attr_it->first, attr_it->second->GetMutableData()));
                attr_sizes.insert(std::make_pair(attr_it->first, attr_it->second->GetCount()));
            }
        }

        std::unordered_map<std::string, knowhere::IndexPtr> attr_indexes;
        status = GenStructuredIndex(collection_id, field_names, attr_types, attr_datas, attr_sizes, attr_indexes);
        if (!status.ok()) {
            return status;
        }

        status = SerializeStructuredIndex(segment_schema, attr_indexes, attr_sizes, attr_types);
        if (!status.ok()) {
            return status;
        }
    }
    return Status::OK();
}

Status
InstanceStructuredIndex::GenStructuredIndex(
    const std::string& collection_id, const std::vector<std::string>& field_names,
    const std::unordered_map<std::string, engine::meta::hybrid::DataType>& attr_types,
    const std::unordered_map<std::string, std::vector<uint8_t>>& attr_datas,
    std::unordered_map<std::string, int64_t>& attr_sizes,
    std::unordered_map<std::string, knowhere::IndexPtr>& attr_indexes) {
    if (attr_sizes.empty() || attr_datas.empty()) {
        return Status{SERVER_UNEXPECTED_ERROR, "attributes data is null when generate structured index"};
    }

    for (auto& field_name : field_names) {
        knowhere::IndexPtr index_ptr = nullptr;
        switch (attr_types.at(field_name)) {
            case engine::meta::hybrid::DataType::INT8: {
                auto attr_size = attr_sizes.at(field_name);
                std::vector<int8_t> attr_data(attr_size);
                memcpy(attr_data.data(), attr_datas.at(field_name).data(), attr_size);

                auto int8_index_ptr = std::make_shared<knowhere::StructuredIndexSort<int8_t>>(
                    (size_t)attr_size, reinterpret_cast<const signed char*>(attr_data.data()));
                index_ptr = std::static_pointer_cast<knowhere::Index>(int8_index_ptr);

                attr_indexes.insert(std::make_pair(field_name, index_ptr));
                attr_sizes.at(field_name) *= sizeof(int8_t);
                break;
            }
            case engine::meta::hybrid::DataType::INT16: {
                auto attr_size = attr_sizes.at(field_name);
                std::vector<int16_t> attr_data(attr_size);
                memcpy(attr_data.data(), attr_datas.at(field_name).data(), attr_size);

                auto int16_index_ptr = std::make_shared<knowhere::StructuredIndexSort<int16_t>>(
                    (size_t)attr_size, reinterpret_cast<const int16_t*>(attr_data.data()));
                index_ptr = std::static_pointer_cast<knowhere::Index>(int16_index_ptr);

                attr_indexes.insert(std::make_pair(field_name, index_ptr));
                attr_sizes.at(field_name) *= sizeof(int16_t);
                break;
            }
            case engine::meta::hybrid::DataType::INT32: {
                auto attr_size = attr_sizes.at(field_name);
                std::vector<int32_t> attr_data(attr_size);
                memcpy(attr_data.data(), attr_datas.at(field_name).data(), attr_size);

                auto int32_index_ptr = std::make_shared<knowhere::StructuredIndexSort<int32_t>>(
                    (size_t)attr_size, reinterpret_cast<const int32_t*>(attr_data.data()));
                index_ptr = std::static_pointer_cast<knowhere::Index>(int32_index_ptr);

                attr_indexes.insert(std::make_pair(field_name, index_ptr));
                attr_sizes.at(field_name) *= sizeof(int32_t);
                break;
            }
            case engine::meta::hybrid::DataType::INT64: {
                auto attr_size = attr_sizes.at(field_name);
                std::vector<int64_t> attr_data(attr_size);
                memcpy(attr_data.data(), attr_datas.at(field_name).data(), attr_size);

                auto int64_index_ptr = std::make_shared<knowhere::StructuredIndexSort<int64_t>>(
                    (size_t)attr_size, reinterpret_cast<const int64_t*>(attr_data.data()));
                index_ptr = std::static_pointer_cast<knowhere::Index>(int64_index_ptr);

                attr_indexes.insert(std::make_pair(field_name, index_ptr));
                attr_sizes.at(field_name) *= sizeof(int64_t);
                break;
            }
            case engine::meta::hybrid::DataType::FLOAT: {
                auto attr_size = attr_sizes.at(field_name);
                std::vector<float> attr_data(attr_size);
                memcpy(attr_data.data(), attr_datas.at(field_name).data(), attr_size);

                auto float_index_ptr = std::make_shared<knowhere::StructuredIndexSort<float>>(
                    (size_t)attr_size, reinterpret_cast<const float*>(attr_data.data()));
                index_ptr = std::static_pointer_cast<knowhere::Index>(float_index_ptr);

                attr_indexes.insert(std::make_pair(field_name, index_ptr));
                attr_sizes.at(field_name) *= sizeof(float);
                break;
            }
            case engine::meta::hybrid::DataType::DOUBLE: {
                auto attr_size = attr_sizes.at(field_name);
                std::vector<double> attr_data(attr_size);
                memcpy(attr_data.data(), attr_datas.at(field_name).data(), attr_size);

                auto double_index_ptr = std::make_shared<knowhere::StructuredIndexSort<double>>(
                    (size_t)attr_size, reinterpret_cast<const double*>(attr_data.data()));
                index_ptr = std::static_pointer_cast<knowhere::Index>(double_index_ptr);

                attr_indexes.insert(std::make_pair(field_name, index_ptr));
                attr_sizes.at(field_name) *= sizeof(double);
                break;
            }
            default: {}
        }
    }

#if 0
    {
        std::unordered_map<std::string, engine::meta::hybrid::DataType> attr_type;
        engine::meta::CollectionSchema collection_schema;
        engine::meta::hybrid::FieldsSchema fields_schema;
        collection_schema.collection_id_ = collection_id;
        status = meta_ptr_->DescribeHybridCollection(collection_schema, fields_schema);
        if (!status.ok()) {
            return status;
        }

        if (field_names.empty()) {
            for (auto& schema : fields_schema.fields_schema_) {
                field_names.emplace_back(schema.collection_id_);
            }
        }

        for (auto& schema : fields_schema.fields_schema_) {
            attr_type.insert(std::make_pair(schema.field_name_, (engine::meta::hybrid::DataType)schema.field_type_));
        }

        meta::FilesHolder files_holder;
        meta_ptr_->FilesToIndex(files_holder);

        milvus::engine::meta::SegmentsSchema& to_index_files = files_holder.HoldFiles();
        status = index_failed_checker_.IgnoreFailedIndexFiles(to_index_files);
        if (!status.ok()) {
            return status;
        }

        status = SerializeStructuredIndex(to_index_files, attr_type, field_names);
        if (!status.ok()) {
            return status;
        }
    }
#endif
    return Status::OK();
}

Status
InstanceStructuredIndex::SerializeStructuredIndex(
    const engine::meta::SegmentSchema& segment_schema,
    const std::unordered_map<std::string, knowhere::IndexPtr>& attr_indexes,
    const std::unordered_map<std::string, int64_t>& attr_sizes,
    const std::unordered_map<std::string, engine::meta::hybrid::DataType>& attr_types) {
    auto status = Status::OK();

    std::string segment_dir;
    engine::utils::GetParentPath(segment_schema.location_, segment_dir);
    auto segment_writer_ptr = std::make_shared<segment::SegmentWriter>(segment_dir);
    status = segment_writer_ptr->SetAttrsIndex(attr_indexes, attr_sizes, attr_types);
    if (!status.ok()) {
        return status;
    }
    status = segment_writer_ptr->WriteAttrsIndex();
    if (!status.ok()) {
        return status;
    }

    return status;
}

}  // namespace Attr
}  // namespace milvus
