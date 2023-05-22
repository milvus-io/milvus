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

#include "segcore/segment_c.h"

#include "common/CGoHelper.h"
#include "common/LoadInfo.h"
#include "common/Types.h"
#include "common/Tracer.h"
#include "common/type_c.h"
#include "google/protobuf/text_format.h"
#include "index/IndexInfo.h"
#include "log/Log.h"
#include "segcore/Collection.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SegmentSealedImpl.h"
#include "segcore/SegcoreConfig.h"

//////////////////////////////    common interfaces    //////////////////////////////
CSegmentInterface
NewSegment(CCollection collection, SegmentType seg_type, int64_t segment_id) {
    auto col = static_cast<milvus::segcore::Collection*>(collection);

    std::unique_ptr<milvus::segcore::SegmentInterface> segment;
    switch (seg_type) {
        case Growing: {
            auto seg = milvus::segcore::CreateGrowingSegment(
                col->get_schema(), col->GetIndexMeta(), segment_id);
            segment = std::move(seg);
            break;
        }
        case Sealed:
        case Indexing:
            segment = milvus::segcore::CreateSealedSegment(col->get_schema(),
                                                           segment_id);
            break;
        default:
            LOG_SEGCORE_ERROR_ << "invalid segment type "
                               << static_cast<int32_t>(seg_type);
            break;
    }

    return segment.release();
}

void
DeleteSegment(CSegmentInterface c_segment) {
    auto s = static_cast<milvus::segcore::SegmentInterface*>(c_segment);
    delete s;
}

void
DeleteSearchResult(CSearchResult search_result) {
    auto res = static_cast<milvus::SearchResult*>(search_result);
    delete res;
}

CStatus
Search(CSegmentInterface c_segment,
       CSearchPlan c_plan,
       CPlaceholderGroup c_placeholder_group,
       CTraceContext c_trace,
       uint64_t timestamp,
       CSearchResult* result) {
    try {
        auto segment = (milvus::segcore::SegmentInterface*)c_segment;
        auto plan = (milvus::query::Plan*)c_plan;
        auto phg_ptr = reinterpret_cast<const milvus::query::PlaceholderGroup*>(
            c_placeholder_group);
        auto ctx = milvus::tracer::TraceContext{
            c_trace.traceID, c_trace.spanID, c_trace.flag};

        auto span = milvus::tracer::StartSpan("SegcoreSearch", &ctx);

        auto search_result = segment->Search(plan, phg_ptr, timestamp);
        if (!milvus::PositivelyRelated(
                plan->plan_node_->search_info_.metric_type_)) {
            for (auto& dis : search_result->distances_) {
                dis *= -1;
            }
        }
        *result = search_result.release();

        span->End();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
Retrieve(CSegmentInterface c_segment,
         CRetrievePlan c_plan,
         CTraceContext c_trace,
         uint64_t timestamp,
         CRetrieveResult* result) {
    try {
        auto segment =
            static_cast<const milvus::segcore::SegmentInterface*>(c_segment);
        auto plan = static_cast<const milvus::query::RetrievePlan*>(c_plan);

        auto ctx = milvus::tracer::TraceContext{
            c_trace.traceID, c_trace.spanID, c_trace.flag};
        auto span = milvus::tracer::StartSpan("SegcoreRetrieve", &ctx);

        auto retrieve_result = segment->Retrieve(plan, timestamp);

        *result = retrieve_result.release();

        span->End();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

int64_t
GetMemoryUsageInBytes(CSegmentInterface c_segment) {
    auto segment = static_cast<milvus::segcore::SegmentInterface*>(c_segment);
    auto mem_size = segment->GetMemoryUsageInBytes();
    return mem_size;
}

int64_t
GetRowCount(CSegmentInterface c_segment) {
    auto segment = static_cast<milvus::segcore::SegmentInterface*>(c_segment);
    auto row_count = segment->get_row_count();
    return row_count;
}

// TODO: segmentInterface implement get_deleted_count()
int64_t
GetDeletedCount(CSegmentInterface c_segment) {
    auto segment =
        reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
    auto deleted_count = segment->get_deleted_count();
    return deleted_count;
}

int64_t
GetRealCount(CSegmentInterface c_segment) {
    // not accurate, pk may exist in deleted record and not in insert record.
    // return GetRowCount(c_segment) - GetDeletedCount(c_segment);
    auto segment =
        reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
    return segment->get_real_count();
}

bool
HasRawData(CSegmentInterface c_segment, int64_t field_id) {
    auto segment =
        reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
    return segment->HasRawData(field_id);
}

//////////////////////////////    interfaces for growing segment    //////////////////////////////
CStatus
Insert(CSegmentInterface c_segment,
       int64_t reserved_offset,
       int64_t size,
       const int64_t* row_ids,
       const uint64_t* timestamps,
       const uint8_t* data_info,
       const uint64_t data_info_len) {
    try {
        auto segment = static_cast<milvus::segcore::SegmentGrowing*>(c_segment);
        auto insert_data = std::make_unique<milvus::InsertData>();
        auto suc = insert_data->ParseFromArray(data_info, data_info_len);
        AssertInfo(suc, "failed to parse insert data from records");

        segment->Insert(
            reserved_offset, size, row_ids, timestamps, insert_data.get());
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
PreInsert(CSegmentInterface c_segment, int64_t size, int64_t* offset) {
    try {
        auto segment = static_cast<milvus::segcore::SegmentGrowing*>(c_segment);
        *offset = segment->PreInsert(size);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
Delete(CSegmentInterface c_segment,
       int64_t reserved_offset,
       int64_t size,
       const uint8_t* ids,
       const uint64_t ids_size,
       const uint64_t* timestamps) {
    auto segment = static_cast<milvus::segcore::SegmentInterface*>(c_segment);
    auto pks = std::make_unique<milvus::proto::schema::IDs>();
    auto suc = pks->ParseFromArray(ids, ids_size);
    AssertInfo(suc, "failed to parse pks from ids");
    try {
        auto res =
            segment->Delete(reserved_offset, size, pks.get(), timestamps);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

int64_t
PreDelete(CSegmentInterface c_segment, int64_t size) {
    auto segment = static_cast<milvus::segcore::SegmentInterface*>(c_segment);

    return segment->PreDelete(size);
}

//////////////////////////////    interfaces for sealed segment    //////////////////////////////
CStatus
LoadFieldData(CSegmentInterface c_segment,
              CLoadFieldDataInfo load_field_data_info) {
    try {
        auto segment_interface =
            reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        auto segment =
            dynamic_cast<milvus::segcore::SegmentSealed*>(segment_interface);
        AssertInfo(segment != nullptr, "segment conversion failed");
        auto field_data = std::make_unique<milvus::DataArray>();
        auto suc = field_data->ParseFromArray(load_field_data_info.blob,
                                              load_field_data_info.blob_size);
        AssertInfo(suc, "unmarshal field data string failed");
        auto load_info = LoadFieldDataInfo{load_field_data_info.field_id,
                                           field_data.get(),
                                           load_field_data_info.row_count,
                                           load_field_data_info.mmap_dir_path};
        segment->LoadFieldData(load_info);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
LoadDeletedRecord(CSegmentInterface c_segment,
                  CLoadDeletedRecordInfo deleted_record_info) {
    try {
        auto segment_interface =
            reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        AssertInfo(segment_interface != nullptr, "segment conversion failed");
        auto pks = std::make_unique<milvus::proto::schema::IDs>();
        auto suc = pks->ParseFromArray(deleted_record_info.primary_keys,
                                       deleted_record_info.primary_keys_size);
        AssertInfo(suc, "unmarshal field data string failed");
        auto load_info = LoadDeletedRecordInfo{deleted_record_info.timestamps,
                                               pks.get(),
                                               deleted_record_info.row_count};
        segment_interface->LoadDeletedRecord(load_info);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
UpdateSealedSegmentIndex(CSegmentInterface c_segment,
                         CLoadIndexInfo c_load_index_info) {
    try {
        auto segment_interface =
            reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        auto segment =
            dynamic_cast<milvus::segcore::SegmentSealed*>(segment_interface);
        AssertInfo(segment != nullptr, "segment conversion failed");
        auto load_index_info =
            static_cast<milvus::segcore::LoadIndexInfo*>(c_load_index_info);
        segment->LoadIndex(*load_index_info);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
DropFieldData(CSegmentInterface c_segment, int64_t field_id) {
    try {
        auto segment_interface =
            reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        auto segment =
            dynamic_cast<milvus::segcore::SegmentSealed*>(segment_interface);
        AssertInfo(segment != nullptr, "segment conversion failed");
        segment->DropFieldData(milvus::FieldId(field_id));
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
DropSealedSegmentIndex(CSegmentInterface c_segment, int64_t field_id) {
    try {
        auto segment_interface =
            reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        auto segment =
            dynamic_cast<milvus::segcore::SegmentSealed*>(segment_interface);
        AssertInfo(segment != nullptr, "segment conversion failed");
        segment->DropIndex(milvus::FieldId(field_id));
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

//////////////////////////////    interfaces for RetrieveResult    //////////////////////////////
void
DeleteRetrieveResult(CRetrieveResult retrieve_result) {
    auto res = reinterpret_cast<milvus::RetrieveResult*>(retrieve_result);
    delete res;
}

bool
RetrieveResultIsCount(CRetrieveResult retrieve_result) {
    auto res = reinterpret_cast<milvus::RetrieveResult*>(retrieve_result);
    return res->is_count_;
}

int64_t
GetRetrieveResultRowCount(CRetrieveResult retrieve_result) {
    auto result = reinterpret_cast<milvus::RetrieveResult*>(retrieve_result);
    return result->result_offsets_.size();
}

CStatus
GetRetrieveResultOffsets(CRetrieveResult retrieve_result,
                         int64_t* dest,
                         int64_t size) {
    try {
        auto result =
            reinterpret_cast<milvus::RetrieveResult*>(retrieve_result);
        int64_t* offset_ptr = result->result_offsets_.data();
        AssertInfo(size == result->result_offsets_.size(),
                   "offset size not match");
        std::copy(offset_ptr, offset_ptr + size, dest);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

int64_t
GetRetrieveResultFieldSize(CRetrieveResult retrieve_result) {
    auto result = reinterpret_cast<milvus::RetrieveResult*>(retrieve_result);
    return result->field_data_.size();
}

bool
RetrieveResultHasIds(CRetrieveResult retrieve_result) {
    auto result = reinterpret_cast<milvus::RetrieveResult*>(retrieve_result);
    return result->ids_ != nullptr;
}

CDataType
GetRetrieveResultPkType(CRetrieveResult retrieve_result) {
    auto result = reinterpret_cast<milvus::RetrieveResult*>(retrieve_result);
    auto type = result->pk_type_;
    // Only Int64 or Varchar for PK
    if (type == milvus::DataType::INT64) {
        return CDataType::Int64;
    } else {
        return CDataType::VarChar;
    }
}

CStatus
GetRetrieveResultPkDataForInt(CRetrieveResult retrieve_result,
                              int64_t* data,
                              int64_t size) {
    try {
        auto result =
            reinterpret_cast<milvus::RetrieveResult*>(retrieve_result);
        const auto* ids = result->ids_.get();
        AssertInfo(ids != nullptr && ids->has_int_id(), "ids has no int data");
        auto long_array = ids->int_id();
        AssertInfo(long_array.data_size() == size, "ids size not match");
        const int64_t* src_data_ptr = long_array.data().data();
        std::copy(src_data_ptr, src_data_ptr + size, data);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
GetRetrieveResultPkDataForString(CRetrieveResult retrieve_result,
                                 char** data,
                                 int64_t size) {
    try {
        auto result =
            reinterpret_cast<milvus::RetrieveResult*>(retrieve_result);
        const auto* ids = result->ids_.get();
        AssertInfo(ids != nullptr && ids->has_str_id(),
                   "ids has no string data");
        auto str_array = ids->str_id();
        AssertInfo(str_array.data_size() == size, "ids size not match");
        for (size_t i = 0; i < size; ++i) {
            data[i] = strdup(str_array.data(i).c_str());
        }
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CDataType
ConvertSchemaPbTypeToCType(::milvus::proto::schema::DataType type) {
    switch (type) {
        case milvus::proto::schema::DataType::Bool:
            return CDataType::Bool;
        case milvus::proto::schema::DataType::Int8:
            return CDataType::Int8;
        case milvus::proto::schema::DataType::Int16:
            return CDataType::Int16;
        case milvus::proto::schema::DataType::Int32:
            return CDataType::Int32;
        case milvus::proto::schema::DataType::Int64:
            return CDataType::Int64;
        case milvus::proto::schema::DataType::Float:
            return CDataType::Float;
        case milvus::proto::schema::DataType::Double:
            return CDataType::Double;
        case milvus::proto::schema::DataType::String:
            return CDataType::String;
        case milvus::proto::schema::DataType::VarChar:
            return CDataType::VarChar;
        case milvus::proto::schema::DataType::Array:
            return CDataType::Array;
        case milvus::proto::schema::DataType::JSON:
            return CDataType::JSON;
        case milvus::proto::schema::DataType::BinaryVector:
            return CDataType::BinaryVector;
        case milvus::proto::schema::DataType::FloatVector:
            return CDataType::FloatVector;
        default:
            return CDataType::None;
    }
}

CStatus
GetRetrieveResultFieldMeta(CRetrieveResult retrieve_result,
                           int64_t index,
                           CFieldMeta* meta) {
    try {
        auto result =
            reinterpret_cast<milvus::RetrieveResult*>(retrieve_result);
        AssertInfo(index <= result->field_data_.size(),
                   "retrieve result field index out of range");
        const auto* field = result->field_data_[index].get();
        meta->field_id = field->field_id();
        if (!field->field_name().empty()) {
            strcpy(meta->field_name, field->field_name().c_str());
        }
        meta->field_type = ConvertSchemaPbTypeToCType(field->type());
        if (meta->field_type == CDataType::FloatVector ||
            meta->field_type == CDataType::BinaryVector) {
            meta->dim = field->vectors().dim();
        }
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
GetRetrieveResultFieldDataForBool(CRetrieveResult retrieve_result,
                                  int64_t index,
                                  bool* data,
                                  int64_t size) {
    try {
        auto result =
            reinterpret_cast<milvus::RetrieveResult*>(retrieve_result);
        AssertInfo(index <= result->field_data_.size(),
                   "retrieve result field index out of range");
        const auto* field = result->field_data_[index].get();
        auto bool_array = field->scalars().bool_data();
        AssertInfo(bool_array.data_size() == size,
                   "bool field data size not match");
        const bool* src_data_ptr = bool_array.data().data();
        std::copy(src_data_ptr, src_data_ptr + size, data);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
GetRetrieveResultFieldDataForInt(CRetrieveResult retrieve_result,
                                 int64_t index,
                                 int32_t* data,
                                 int64_t size) {
    try {
        auto result =
            reinterpret_cast<milvus::RetrieveResult*>(retrieve_result);
        AssertInfo(index <= result->field_data_.size(),
                   "retrieve result field index out of range");
        const auto* field = result->field_data_[index].get();
        auto int_array = field->scalars().int_data();
        AssertInfo(int_array.data_size() == size,
                   "int field data size not match");
        const int32_t* src_data_ptr = int_array.data().data();
        std::copy(src_data_ptr, src_data_ptr + size, data);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
GetRetrieveResultFieldDataForLong(CRetrieveResult retrieve_result,
                                  int64_t index,
                                  int64_t* data,
                                  int64_t size) {
    try {
        auto result =
            reinterpret_cast<milvus::RetrieveResult*>(retrieve_result);
        AssertInfo(index <= result->field_data_.size(),
                   "retrieve result field index out of range");
        const auto* field = result->field_data_[index].get();
        auto long_array = field->scalars().long_data();
        AssertInfo(long_array.data_size() == size,
                   "long field data size not match");
        const int64_t* src_data_ptr = long_array.data().data();
        std::copy(src_data_ptr, src_data_ptr + size, data);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
GetRetrieveResultFieldDataForFloat(CRetrieveResult retrieve_result,
                                   int64_t index,
                                   float* data,
                                   int64_t size) {
    try {
        auto result =
            reinterpret_cast<milvus::RetrieveResult*>(retrieve_result);
        AssertInfo(index <= result->field_data_.size(),
                   "retrieve result field index out of range");
        const auto* field = result->field_data_[index].get();
        auto float_array = field->scalars().float_data();
        AssertInfo(float_array.data_size() == size,
                   "float field data size not match");
        const float* src_data_ptr = float_array.data().data();
        std::copy(src_data_ptr, src_data_ptr + size, data);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
GetRetrieveResultFieldDataForDouble(CRetrieveResult retrieve_result,
                                    int64_t index,
                                    double* data,
                                    int64_t size) {
    try {
        auto result =
            reinterpret_cast<milvus::RetrieveResult*>(retrieve_result);
        AssertInfo(index <= result->field_data_.size(),
                   "retrieve result field index out of range");
        const auto* field = result->field_data_[index].get();
        auto double_array = field->scalars().double_data();
        AssertInfo(double_array.data_size() == size,
                   "double field data size not match");
        const double* src_data_ptr = double_array.data().data();
        std::copy(src_data_ptr, src_data_ptr + size, data);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
GetRetrieveResultFieldDataForVarChar(CRetrieveResult retrieve_result,
                                     int64_t index,
                                     char** data,
                                     int64_t size) {
    try {
        auto result =
            reinterpret_cast<milvus::RetrieveResult*>(retrieve_result);
        AssertInfo(index <= result->field_data_.size(),
                   "retrieve result field index out of range");
        const auto* field = result->field_data_[index].get();
        auto string_array = field->scalars().string_data();
        AssertInfo(string_array.data_size() == size,
                   "string field data size not match");
        for (size_t i = 0; i < size; ++i) {
            data[i] = strdup(string_array.data(i).c_str());
        }
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
GetRetrieveResultFieldDataForJson(CRetrieveResult retrieve_result,
                                  int64_t index,
                                  char** data,
                                  int64_t size) {
    try {
        auto result =
            reinterpret_cast<milvus::RetrieveResult*>(retrieve_result);
        AssertInfo(index <= result->field_data_.size(),
                   "retrieve result field index out of range");
        const auto* field = result->field_data_[index].get();
        auto json_array = field->scalars().json_data();
        AssertInfo(json_array.data_size() == size,
                   "json field data size not match");
        for (size_t i = 0; i < size; ++i) {
            data[i] = strdup(json_array.data(i).c_str());
        }
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
GetRetrieveResultFieldDataForFloatVector(CRetrieveResult retrieve_result,
                                         int64_t index,
                                         float* data,
                                         int64_t dim,
                                         int64_t size) {
    try {
        auto result =
            reinterpret_cast<milvus::RetrieveResult*>(retrieve_result);
        AssertInfo(index <= result->field_data_.size(),
                   "retrieve result field index out of range");
        const auto* field = result->field_data_[index].get();
        auto float_array = field->vectors().float_vector();
        AssertInfo(float_array.data_size() == size * dim,
                   "float vector field size not match");
        const float* src_data_ptr = float_array.data().data();
        std::copy(src_data_ptr, src_data_ptr + size * dim, data);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
GetRetrieveResultFieldDataForBinaryVector(CRetrieveResult retrieve_result,
                                          int64_t index,
                                          char* data,
                                          int64_t dim,
                                          int64_t size) {
    try {
        auto result =
            reinterpret_cast<milvus::RetrieveResult*>(retrieve_result);
        AssertInfo(index <= result->field_data_.size(),
                   "retrieve result field index out of range");
        const auto* field = result->field_data_[index].get();
        auto byte_array = field->vectors().binary_vector();
        AssertInfo(byte_array.length() == size * dim / 8, "");
        std::copy(byte_array.data(), byte_array.data() + size * dim / 8, data);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}