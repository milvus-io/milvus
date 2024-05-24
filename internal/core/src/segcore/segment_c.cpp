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

#include <memory>
#include <limits>

#include "common/FieldData.h"
#include "common/LoadInfo.h"
#include "common/Types.h"
#include "common/Tracer.h"
#include "common/type_c.h"
#include "google/protobuf/text_format.h"
#include "log/Log.h"
#include "mmap/Types.h"
#include "segcore/Collection.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SegmentSealedImpl.h"
#include "segcore/Utils.h"
#include "storage/Util.h"
#include "storage/space.h"

//////////////////////////////    common interfaces    //////////////////////////////
CStatus
NewSegment(CCollection collection,
           SegmentType seg_type,
           int64_t segment_id,
           CSegmentInterface* newSegment) {
    try {
        auto col = static_cast<milvus::segcore::Collection*>(collection);

        std::unique_ptr<milvus::segcore::SegmentInterface> segment;
        switch (seg_type) {
            case Growing: {
                auto seg = milvus::segcore::CreateGrowingSegment(
                    col->get_schema(), col->get_index_meta(), segment_id);
                segment = std::move(seg);
                break;
            }
            case Sealed:
            case Indexing:
                segment = milvus::segcore::CreateSealedSegment(
                    col->get_schema(), col->get_index_meta(), segment_id);
                break;
            default:
                PanicInfo(milvus::UnexpectedError,
                          "invalid segment type: {}",
                          seg_type);
        }

        *newSegment = segment.release();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

void
DeleteSegment(CSegmentInterface c_segment) {
    auto s = static_cast<milvus::segcore::SegmentInterface*>(c_segment);
    delete s;
}

void
ClearSegmentData(CSegmentInterface c_segment) {
    auto s = static_cast<milvus::segcore::SegmentSealedImpl*>(c_segment);
    s->ClearData();
}

void
DeleteSearchResult(CSearchResult search_result) {
    auto res = static_cast<milvus::SearchResult*>(search_result);
    delete res;
}

CStatus
Search(CTraceContext c_trace,
       CSegmentInterface c_segment,
       CSearchPlan c_plan,
       CPlaceholderGroup c_placeholder_group,
       uint64_t timestamp,
       CSearchResult* result) {
    try {
        auto segment = (milvus::segcore::SegmentInterface*)c_segment;
        auto plan = (milvus::query::Plan*)c_plan;
        auto phg_ptr = reinterpret_cast<const milvus::query::PlaceholderGroup*>(
            c_placeholder_group);

        // save trace context into search_info
        auto& trace_ctx = plan->plan_node_->search_info_.trace_ctx_;
        trace_ctx.traceID = c_trace.traceID;
        trace_ctx.spanID = c_trace.spanID;
        trace_ctx.traceFlags = c_trace.traceFlags;

        auto span = milvus::tracer::StartSpan("SegCoreSearch", &trace_ctx);
        milvus::tracer::SetRootSpan(span);

        auto search_result = segment->Search(plan, phg_ptr, timestamp);
        if (!milvus::PositivelyRelated(
                plan->plan_node_->search_info_.metric_type_)) {
            for (auto& dis : search_result->distances_) {
                dis *= -1;
            }
        }
        *result = search_result.release();
        span->End();
        milvus::tracer::CloseRootSpan();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

void
DeleteRetrieveResult(CRetrieveResult* retrieve_result) {
    std::free(const_cast<void*>(retrieve_result->proto_blob));
}

CStatus
Retrieve(CTraceContext c_trace,
         CSegmentInterface c_segment,
         CRetrievePlan c_plan,
         uint64_t timestamp,
         CRetrieveResult* result,
         int64_t limit_size,
         bool ignore_non_pk) {
    try {
        auto segment =
            static_cast<milvus::segcore::SegmentInterface*>(c_segment);
        auto plan = static_cast<const milvus::query::RetrievePlan*>(c_plan);

        auto trace_ctx = milvus::tracer::TraceContext{
            c_trace.traceID, c_trace.spanID, c_trace.traceFlags};
        milvus::tracer::AutoSpan span("SegCoreRetrieve", &trace_ctx, true);

        auto retrieve_result = segment->Retrieve(
            &trace_ctx, plan, timestamp, limit_size, ignore_non_pk);

        auto size = retrieve_result->ByteSizeLong();
        std::unique_ptr<uint8_t[]> buffer(new uint8_t[size]);
        retrieve_result->SerializePartialToArray(buffer.get(), size);

        result->proto_blob = buffer.release();
        result->proto_size = size;

        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
RetrieveByOffsets(CTraceContext c_trace,
                  CSegmentInterface c_segment,
                  CRetrievePlan c_plan,
                  CRetrieveResult* result,
                  int64_t* offsets,
                  int64_t len) {
    try {
        auto segment =
            static_cast<milvus::segcore::SegmentInterface*>(c_segment);
        auto plan = static_cast<const milvus::query::RetrievePlan*>(c_plan);

        auto trace_ctx = milvus::tracer::TraceContext{
            c_trace.traceID, c_trace.spanID, c_trace.traceFlags};
        milvus::tracer::AutoSpan span(
            "SegCoreRetrieveByOffsets", &trace_ctx, true);

        auto retrieve_result =
            segment->Retrieve(&trace_ctx, plan, offsets, len);

        auto size = retrieve_result->ByteSizeLong();
        std::unique_ptr<uint8_t[]> buffer(new uint8_t[size]);
        retrieve_result->SerializePartialToArray(buffer.get(), size);

        result->proto_blob = buffer.release();
        result->proto_size = size;

        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
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
        AssertInfo(data_info_len < std::numeric_limits<int>::max(),
                   "insert data length ({}) exceeds max int",
                   data_info_len);
        auto segment = static_cast<milvus::segcore::SegmentGrowing*>(c_segment);
        auto insert_record_proto =
            std::make_unique<milvus::InsertRecordProto>();
        auto suc =
            insert_record_proto->ParseFromArray(data_info, data_info_len);
        AssertInfo(suc, "failed to parse insert data from records");

        segment->Insert(reserved_offset,
                        size,
                        row_ids,
                        timestamps,
                        insert_record_proto.get());
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
PreInsert(CSegmentInterface c_segment, int64_t size, int64_t* offset) {
    try {
        auto segment = static_cast<milvus::segcore::SegmentGrowing*>(c_segment);
        *offset = segment->PreInsert(size);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
Delete(CSegmentInterface c_segment,
       int64_t reserved_offset,  // deprecated
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
        return milvus::FailureCStatus(&e);
    }
}

//////////////////////////////    interfaces for sealed segment    //////////////////////////////
CStatus
LoadFieldData(CSegmentInterface c_segment,
              CLoadFieldDataInfo c_load_field_data_info) {
    try {
        auto segment =
            reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        AssertInfo(segment != nullptr, "segment conversion failed");
        auto load_info = (LoadFieldDataInfo*)c_load_field_data_info;
        segment->LoadFieldData(*load_info);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
LoadFieldDataV2(CSegmentInterface c_segment,
                CLoadFieldDataInfo c_load_field_data_info) {
    try {
        auto segment =
            reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        AssertInfo(segment != nullptr, "segment conversion failed");
        auto load_info = (LoadFieldDataInfo*)c_load_field_data_info;
        segment->LoadFieldDataV2(*load_info);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}
// just for test
CStatus
LoadFieldRawData(CSegmentInterface c_segment,
                 int64_t field_id,
                 const void* data,
                 int64_t row_count) {
    try {
        auto segment_interface =
            reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        auto segment =
            dynamic_cast<milvus::segcore::SegmentSealed*>(segment_interface);
        AssertInfo(segment != nullptr, "segment conversion failed");
        milvus::DataType data_type;
        int64_t dim = 1;
        if (milvus::SystemProperty::Instance().IsSystem(
                milvus::FieldId(field_id))) {
            data_type = milvus::DataType::INT64;
        } else {
            auto field_meta = segment->get_schema()[milvus::FieldId(field_id)];
            data_type = field_meta.get_data_type();

            if (milvus::IsVectorDataType(data_type) &&
                !milvus::IsSparseFloatVectorDataType(data_type)) {
                dim = field_meta.get_dim();
            }
        }
        auto field_data = milvus::storage::CreateFieldData(data_type, dim);
        field_data->FillFieldData(data, row_count);
        milvus::FieldDataChannelPtr channel =
            std::make_shared<milvus::FieldDataChannel>();
        channel->push(field_data);
        channel->close();
        auto field_data_info = milvus::FieldDataInfo(
            field_id, static_cast<size_t>(row_count), channel);
        segment->LoadFieldData(milvus::FieldId(field_id), field_data_info);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
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
        return milvus::FailureCStatus(&e);
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
        return milvus::FailureCStatus(&e);
    }
}

CStatus
UpdateFieldRawDataSize(CSegmentInterface c_segment,
                       int64_t field_id,
                       int64_t num_rows,
                       int64_t field_data_size) {
    try {
        auto segment_interface =
            reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        AssertInfo(segment_interface != nullptr, "segment conversion failed");
        segment_interface->set_field_avg_size(
            milvus::FieldId(field_id), num_rows, field_data_size);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
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
        return milvus::FailureCStatus(&e);
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
        return milvus::FailureCStatus(&e);
    }
}

CStatus
AddFieldDataInfoForSealed(CSegmentInterface c_segment,
                          CLoadFieldDataInfo c_load_field_data_info) {
    try {
        auto segment_interface =
            reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        auto segment =
            dynamic_cast<milvus::segcore::SegmentSealed*>(segment_interface);
        AssertInfo(segment != nullptr, "segment conversion failed");
        auto load_info =
            static_cast<LoadFieldDataInfo*>(c_load_field_data_info);
        segment->AddFieldDataInfoForSealed(*load_info);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(milvus::UnexpectedError, e.what());
    }
}

CStatus
WarmupChunkCache(CSegmentInterface c_segment, int64_t field_id) {
    try {
        auto segment_interface =
            reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        auto segment =
            dynamic_cast<milvus::segcore::SegmentSealed*>(segment_interface);
        AssertInfo(segment != nullptr, "segment conversion failed");
        segment->WarmupChunkCache(milvus::FieldId(field_id));
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(milvus::UnexpectedError, e.what());
    }
}

void
RemoveFieldFile(CSegmentInterface c_segment, int64_t field_id) {
    auto segment =
        reinterpret_cast<milvus::segcore::SegmentSealedImpl*>(c_segment);
    segment->RemoveFieldFile(milvus::FieldId(field_id));
}
