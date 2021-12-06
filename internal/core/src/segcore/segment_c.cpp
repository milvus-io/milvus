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

#include "common/CGoHelper.h"
#include "common/LoadInfo.h"
#include "common/Types.h"
#include "common/type_c.h"
#include "log/Log.h"

#include "segcore/Collection.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SegmentSealedImpl.h"
#include "segcore/SimilarityCorelation.h"
#include "segcore/segment_c.h"

//////////////////////////////    common interfaces    //////////////////////////////
CSegmentInterface
NewSegment(CCollection collection, SegmentType seg_type) {
    auto col = (milvus::segcore::Collection*)collection;

    std::unique_ptr<milvus::segcore::SegmentInterface> segment;
    switch (seg_type) {
        case Growing:
            segment = milvus::segcore::CreateGrowingSegment(col->get_schema());
            break;
        case Sealed:
        case Indexing:
            segment = milvus::segcore::CreateSealedSegment(col->get_schema());
            break;
        default:
            LOG_SEGCORE_ERROR_ << "invalid segment type " << (int32_t)seg_type;
            break;
    }

    return (void*)segment.release();
}

void
DeleteSegment(CSegmentInterface c_segment) {
    // TODO: use dynamic cast, and return c status
    auto s = (milvus::segcore::SegmentInterface*)c_segment;
    delete s;
}

void
DeleteSearchResult(CSearchResult search_result) {
    auto res = (milvus::SearchResult*)search_result;
    delete res;
}

CStatus
Search(CSegmentInterface c_segment,
       CSearchPlan c_plan,
       CPlaceholderGroup c_placeholder_group,
       uint64_t timestamp,
       CSearchResult* result) {
    try {
        auto segment = (milvus::segcore::SegmentInterface*)c_segment;
        auto plan = (milvus::query::Plan*)c_plan;
        auto phg_ptr = reinterpret_cast<const milvus::query::PlaceholderGroup*>(c_placeholder_group);
        auto search_result = segment->Search(plan, *phg_ptr, timestamp);
        if (!milvus::segcore::PositivelyRelated(plan->plan_node_->search_info_.metric_type_)) {
            for (auto& dis : search_result->distances_) {
                dis *= -1;
            }
        }
        *result = search_result.release();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

void
DeleteRetrieveResult(CRetrieveResult* retrieve_result) {
    std::free((void*)(retrieve_result->proto_blob));
}

CStatus
Retrieve(CSegmentInterface c_segment, CRetrievePlan c_plan, uint64_t timestamp, CRetrieveResult* result) {
    try {
        auto segment = (const milvus::segcore::SegmentInterface*)c_segment;
        auto plan = (const milvus::query::RetrievePlan*)c_plan;
        auto retrieve_result = segment->Retrieve(plan, timestamp);

        auto size = retrieve_result->ByteSize();
        void* buffer = malloc(size);
        retrieve_result->SerializePartialToArray(buffer, size);

        result->proto_blob = buffer;
        result->proto_size = size;
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

int64_t
GetMemoryUsageInBytes(CSegmentInterface c_segment) {
    auto segment = (milvus::segcore::SegmentInterface*)c_segment;
    auto mem_size = segment->GetMemoryUsageInBytes();
    return mem_size;
}

int64_t
GetRowCount(CSegmentInterface c_segment) {
    auto segment = (milvus::segcore::SegmentInterface*)c_segment;
    auto row_count = segment->get_row_count();
    return row_count;
}

// TODO: segmentInterface implement get_deleted_count()
int64_t
GetDeletedCount(CSegmentInterface c_segment) {
    auto segment = (milvus::segcore::SegmentGrowing*)c_segment;
    auto deleted_count = segment->get_deleted_count();
    return deleted_count;
}

//////////////////////////////    interfaces for growing segment    //////////////////////////////
CStatus
Insert(CSegmentInterface c_segment,
       int64_t reserved_offset,
       int64_t size,
       const int64_t* row_ids,
       const uint64_t* timestamps,
       void* raw_data,
       int sizeof_per_row,
       int64_t count) {
    try {
        auto segment = (milvus::segcore::SegmentGrowing*)c_segment;
        milvus::segcore::RowBasedRawData dataChunk{};

        dataChunk.raw_data = raw_data;
        dataChunk.sizeof_per_row = sizeof_per_row;
        dataChunk.count = count;
        segment->Insert(reserved_offset, size, row_ids, timestamps, dataChunk);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
PreInsert(CSegmentInterface c_segment, int64_t size, int64_t* offset) {
    try {
        auto segment = (milvus::segcore::SegmentGrowing*)c_segment;
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
       const int64_t* row_ids,
       const uint64_t* timestamps) {
    auto segment = (milvus::segcore::SegmentInterface*)c_segment;

    try {
        auto res = segment->Delete(reserved_offset, size, row_ids, timestamps);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

int64_t
PreDelete(CSegmentInterface c_segment, int64_t size) {
    auto segment = (milvus::segcore::SegmentInterface*)c_segment;

    return segment->PreDelete(size);
}

//////////////////////////////    interfaces for sealed segment    //////////////////////////////
CStatus
LoadFieldData(CSegmentInterface c_segment, CLoadFieldDataInfo load_field_data_info) {
    try {
        auto segment_interface = reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        auto segment = dynamic_cast<milvus::segcore::SegmentSealed*>(segment_interface);
        AssertInfo(segment != nullptr, "segment conversion failed");
        auto load_info =
            LoadFieldDataInfo{load_field_data_info.field_id, load_field_data_info.blob, load_field_data_info.row_count};
        segment->LoadFieldData(load_info);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
LoadDeletedRecord(CSegmentInterface c_segment, CLoadDeletedRecordInfo deleted_record_info) {
    try {
        auto segment_interface = reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        auto segment = dynamic_cast<milvus::segcore::SegmentSealed*>(segment_interface);
        AssertInfo(segment != nullptr, "segment conversion failed");
        auto load_info = LoadDeletedRecordInfo{deleted_record_info.timestamps, deleted_record_info.primary_keys,
                                               deleted_record_info.row_count};
        segment->LoadDeletedRecord(load_info);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
UpdateSealedSegmentIndex(CSegmentInterface c_segment, CLoadIndexInfo c_load_index_info) {
    try {
        auto segment_interface = reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        auto segment = dynamic_cast<milvus::segcore::SegmentSealed*>(segment_interface);
        AssertInfo(segment != nullptr, "segment conversion failed");
        auto load_index_info = (LoadIndexInfo*)c_load_index_info;
        segment->LoadIndex(*load_index_info);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
DropFieldData(CSegmentInterface c_segment, int64_t field_id) {
    try {
        auto segment_interface = reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        auto segment = dynamic_cast<milvus::segcore::SegmentSealed*>(segment_interface);
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
        auto segment_interface = reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        auto segment = dynamic_cast<milvus::segcore::SegmentSealed*>(segment_interface);
        AssertInfo(segment != nullptr, "segment conversion failed");
        segment->DropIndex(milvus::FieldId(field_id));
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}
