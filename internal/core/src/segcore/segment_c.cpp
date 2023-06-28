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

#include "common/CGoHelper.h"
#include "common/LoadInfo.h"
#include "common/Types.h"
#include "common/Tracer.h"
#include "common/type_c.h"
#include "google/protobuf/text_format.h"
#include "log/Log.h"
#include "segcore/Collection.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SegmentSealedImpl.h"
#include "storage/FieldData.h"
#include "storage/Util.h"
#include "mmap/Types.h"

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

void
DeleteRetrieveResult(CRetrieveResult* retrieve_result) {
    std::free(const_cast<void*>(retrieve_result->proto_blob));
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

        auto size = retrieve_result->ByteSizeLong();
        void* buffer = malloc(size);
        retrieve_result->SerializePartialToArray(buffer, size);

        result->proto_blob = buffer;
        result->proto_size = size;

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
        return milvus::FailureCStatus(UnexpectedError, e.what());
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
        return milvus::FailureCStatus(UnexpectedError, e.what());
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

            if (milvus::datatype_is_vector(data_type)) {
                dim = field_meta.get_dim();
            }
        }
        auto field_data = milvus::storage::CreateFieldData(data_type, dim);
        field_data->FillFieldData(data, row_count);
        milvus::storage::FieldDataChannelPtr channel =
            std::make_shared<milvus::storage::FieldDataChannel>();
        channel->push(field_data);
        channel->close();
        auto field_data_info = milvus::FieldDataInfo(
            field_id, static_cast<size_t>(row_count), channel);
        segment->LoadFieldData(milvus::FieldId(field_id), field_data_info);
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
