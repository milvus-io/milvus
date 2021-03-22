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

#include <cstring>
#include <cstdint>

#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentSealed.h"
#include "segcore/Collection.h"
#include "segcore/segment_c.h"
#include "common/LoadInfo.h"
#include "common/type_c.h"
#include <knowhere/index/vector_index/VecIndex.h>
#include <knowhere/index/vector_index/adapter/VectorAdapter.h>
#include "common/Types.h"

//////////////////////////////    common interfaces    //////////////////////////////
CSegmentInterface
NewSegment(CCollection collection, uint64_t segment_id, SegmentType seg_type) {
    auto col = (milvus::segcore::Collection*)collection;

    std::unique_ptr<milvus::segcore::SegmentInterface> segment;
    switch (seg_type) {
        case Invalid:
            std::cout << "invalid segment type" << std::endl;
            break;
        case Growing:
            segment = milvus::segcore::CreateGrowingSegment(col->get_schema());
            break;
        case Sealed:
        case Indexing:
            segment = milvus::segcore::CreateSealedSegment(col->get_schema());
            break;
        default:
            std::cout << "invalid segment type" << std::endl;
    }

    // std::cout << "create segment " << segment_id << std::endl;
    return (void*)segment.release();
}

void
DeleteSegment(CSegmentInterface c_segment) {
    // TODO: use dynamic cast, and return c status
    auto s = (milvus::segcore::SegmentInterface*)c_segment;

    // std::cout << "delete segment " << std::endl;
    delete s;
}

void
DeleteQueryResult(CQueryResult query_result) {
    auto res = (milvus::QueryResult*)query_result;
    delete res;
}

CStatus
Search(CSegmentInterface c_segment,
       CPlan c_plan,
       CPlaceholderGroup* c_placeholder_groups,
       uint64_t* timestamps,
       int num_groups,
       CQueryResult* result) {
    auto status = CStatus();
    auto query_result = std::make_unique<milvus::QueryResult>();
    try {
        auto segment = (milvus::segcore::SegmentInterface*)c_segment;
        auto plan = (milvus::query::Plan*)c_plan;
        std::vector<const milvus::query::PlaceholderGroup*> placeholder_groups;
        for (int i = 0; i < num_groups; ++i) {
            placeholder_groups.push_back((const milvus::query::PlaceholderGroup*)c_placeholder_groups[i]);
        }
        *query_result = segment->Search(plan, placeholder_groups.data(), timestamps, num_groups);
        if (plan->plan_node_->query_info_.metric_type_ != milvus::MetricType::METRIC_INNER_PRODUCT) {
            for (auto& dis : query_result->result_distances_) {
                dis *= -1;
            }
        }
        *result = query_result.release();
        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedException;
        status.error_msg = strdup(e.what());
    }

    // result_ids and result_distances have been allocated memory in goLang,
    // so we don't need to malloc here.
    // memcpy(result_ids, query_result.result_ids_.data(), query_result.get_row_count() * sizeof(long int));
    // memcpy(result_distances, query_result.result_distances_.data(), query_result.get_row_count() * sizeof(float));

    return status;
}

CStatus
FillTargetEntry(CSegmentInterface c_segment, CPlan c_plan, CQueryResult c_result) {
    auto segment = (milvus::segcore::SegmentInterface*)c_segment;
    auto plan = (milvus::query::Plan*)c_plan;
    auto result = (milvus::QueryResult*)c_result;

    auto status = CStatus();
    try {
        segment->FillTargetEntry(plan, *result);
        status.error_code = Success;
        status.error_msg = "";
    } catch (std::runtime_error& e) {
        status.error_code = UnexpectedException;
        status.error_msg = strdup(e.what());
    }
    return status;
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

        auto res = segment->Insert(reserved_offset, size, row_ids, timestamps, dataChunk);

        auto status = CStatus();
        status.error_code = Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        auto status = CStatus();
        status.error_code = UnexpectedException;
        status.error_msg = strdup(e.what());
        return status;
    }
}

CStatus
PreInsert(CSegmentInterface c_segment, int64_t size, int64_t* offset) {
    try {
        auto segment = (milvus::segcore::SegmentGrowing*)c_segment;
        *offset = segment->PreInsert(size);
        auto status = CStatus();
        status.error_code = Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        auto status = CStatus();
        status.error_code = UnexpectedException;
        status.error_msg = strdup(e.what());
        return status;
    }
}

CStatus
Delete(CSegmentInterface c_segment,
       int64_t reserved_offset,
       int64_t size,
       const int64_t* row_ids,
       const uint64_t* timestamps) {
    auto segment = (milvus::segcore::SegmentGrowing*)c_segment;

    try {
        auto res = segment->Delete(reserved_offset, size, row_ids, timestamps);

        auto status = CStatus();
        status.error_code = Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        auto status = CStatus();
        status.error_code = UnexpectedException;
        status.error_msg = strdup(e.what());
        return status;
    }
}

int64_t
PreDelete(CSegmentInterface c_segment, int64_t size) {
    auto segment = (milvus::segcore::SegmentGrowing*)c_segment;

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
        auto status = CStatus();
        status.error_code = Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        auto status = CStatus();
        status.error_code = UnexpectedException;
        status.error_msg = strdup(e.what());
        return status;
    }
}

CStatus
UpdateSealedSegmentIndex(CSegmentInterface c_segment, CLoadIndexInfo c_load_index_info) {
    auto status = CStatus();
    try {
        auto segment_interface = reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        auto segment = dynamic_cast<milvus::segcore::SegmentSealed*>(segment_interface);
        AssertInfo(segment != nullptr, "segment conversion failed");
        auto load_index_info = (LoadIndexInfo*)c_load_index_info;
        segment->LoadIndex(*load_index_info);
        status.error_code = Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        status.error_code = UnexpectedException;
        status.error_msg = strdup(e.what());
        return status;
    }
}

CStatus
DropFieldData(CSegmentInterface c_segment, int64_t field_id) {
    try {
        auto segment_interface = reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        auto segment = dynamic_cast<milvus::segcore::SegmentSealed*>(segment_interface);
        AssertInfo(segment != nullptr, "segment conversion failed");
        segment->DropFieldData(milvus::FieldId(field_id));
        auto status = CStatus();
        status.error_code = Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        auto status = CStatus();
        status.error_code = UnexpectedException;
        status.error_msg = strdup(e.what());
        return status;
    }
}

CStatus
DropSealedSegmentIndex(CSegmentInterface c_segment, int64_t field_id) {
    auto status = CStatus();
    try {
        auto segment_interface = reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        auto segment = dynamic_cast<milvus::segcore::SegmentSealed*>(segment_interface);
        AssertInfo(segment != nullptr, "segment conversion failed");
        segment->DropIndex(milvus::FieldId(field_id));
        status.error_code = Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        status.error_code = UnexpectedException;
        status.error_msg = strdup(e.what());
        return status;
    }
}

//////////////////////////////    deprecated interfaces    //////////////////////////////
CStatus
UpdateSegmentIndex(CSegmentInterface c_segment, CLoadIndexInfo c_load_index_info) {
    auto status = CStatus();
    try {
        auto segment_interface = reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        auto segment = dynamic_cast<milvus::segcore::SegmentGrowing*>(segment_interface);
        AssertInfo(segment != nullptr, "segment conversion failed");
        auto load_index_info = (LoadIndexInfo*)c_load_index_info;
        auto res = segment->LoadIndexing(*load_index_info);
        status.error_code = res.code();
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        status.error_code = UnexpectedException;
        status.error_msg = strdup(e.what());
        return status;
    }
}

int
Close(CSegmentInterface c_segment) {
    auto segment = (milvus::segcore::SegmentGrowing*)c_segment;
    auto status = segment->Close();
    return status.code();
}

int
BuildIndex(CCollection c_collection, CSegmentInterface c_segment) {
    PanicInfo("unimplemented");
}

bool
IsOpened(CSegmentInterface c_segment) {
    auto segment = (milvus::segcore::SegmentGrowing*)c_segment;
    auto status = segment->get_state();
    return status == milvus::segcore::SegmentGrowing::SegmentState::Open;
}
