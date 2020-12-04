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

#include "segcore/SegmentBase.h"
#include "segcore/Collection.h"
#include "segcore/segment_c.h"
#include <knowhere/index/vector_index/VecIndex.h>
#include <knowhere/index/vector_index/adapter/VectorAdapter.h>
#include <knowhere/index/vector_index/VecIndexFactory.h>
#include <cstdint>
#include <boost/concept_check.hpp>

CSegmentBase
NewSegment(CCollection collection, uint64_t segment_id) {
    auto col = (milvus::segcore::Collection*)collection;

    auto segment = milvus::segcore::CreateSegment(col->get_schema());

    // TODO: delete print
    std::cout << "create segment " << segment_id << std::endl;
    return (void*)segment.release();
}

void
DeleteSegment(CSegmentBase segment) {
    auto s = (milvus::segcore::SegmentBase*)segment;

    // TODO: delete print
    std::cout << "delete segment " << std::endl;
    delete s;
}

void
DeleteQueryResult(CQueryResult query_result) {
    auto res = (milvus::QueryResult*)query_result;
    delete res;
}

//////////////////////////////////////////////////////////////////

CStatus
Insert(CSegmentBase c_segment,
       int64_t reserved_offset,
       int64_t size,
       const int64_t* row_ids,
       const uint64_t* timestamps,
       void* raw_data,
       int sizeof_per_row,
       int64_t count) {
    auto segment = (milvus::segcore::SegmentBase*)c_segment;
    milvus::segcore::RowBasedRawData dataChunk{};

    dataChunk.raw_data = raw_data;
    dataChunk.sizeof_per_row = sizeof_per_row;
    dataChunk.count = count;

    try {
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

    // TODO: delete print
    // std::cout << "do segment insert, sizeof_per_row = " << sizeof_per_row << std::endl;
}

int64_t
PreInsert(CSegmentBase c_segment, int64_t size) {
    auto segment = (milvus::segcore::SegmentBase*)c_segment;

    // TODO: delete print
    // std::cout << "PreInsert segment " << std::endl;
    return segment->PreInsert(size);
}

CStatus
Delete(
    CSegmentBase c_segment, int64_t reserved_offset, int64_t size, const int64_t* row_ids, const uint64_t* timestamps) {
    auto segment = (milvus::segcore::SegmentBase*)c_segment;

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
PreDelete(CSegmentBase c_segment, int64_t size) {
    auto segment = (milvus::segcore::SegmentBase*)c_segment;

    // TODO: delete print
    // std::cout << "PreDelete segment " << std::endl;
    return segment->PreDelete(size);
}

CStatus
Search(CSegmentBase c_segment,
       CPlan c_plan,
       CPlaceholderGroup* c_placeholder_groups,
       uint64_t* timestamps,
       int num_groups,
       CQueryResult* result) {
    auto segment = (milvus::segcore::SegmentBase*)c_segment;
    auto plan = (milvus::query::Plan*)c_plan;
    std::vector<const milvus::query::PlaceholderGroup*> placeholder_groups;
    for (int i = 0; i < num_groups; ++i) {
        placeholder_groups.push_back((const milvus::query::PlaceholderGroup*)c_placeholder_groups[i]);
    }

    auto query_result = std::make_unique<milvus::QueryResult>();

    auto status = CStatus();
    try {
        auto res = segment->Search(plan, placeholder_groups.data(), timestamps, num_groups, *query_result);
        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedException;
        status.error_msg = strdup(e.what());
    }
    *result = query_result.release();

    // result_ids and result_distances have been allocated memory in goLang,
    // so we don't need to malloc here.
    // memcpy(result_ids, query_result.result_ids_.data(), query_result.get_row_count() * sizeof(long int));
    // memcpy(result_distances, query_result.result_distances_.data(), query_result.get_row_count() * sizeof(float));

    return status;
}

CStatus
FillTargetEntry(CSegmentBase c_segment, CPlan c_plan, CQueryResult c_result) {
    auto segment = (milvus::segcore::SegmentBase*)c_segment;
    auto plan = (milvus::query::Plan*)c_plan;
    auto result = (milvus::engine::QueryResult*)c_result;

    auto status = CStatus();
    try {
        auto res = segment->FillTargetEntry(plan, *result);
        status.error_code = Success;
        status.error_msg = "";
    } catch (std::runtime_error& e) {
        status.error_code = UnexpectedException;
        status.error_msg = strdup(e.what());
    }
    return status;
}

//////////////////////////////////////////////////////////////////

int
Close(CSegmentBase c_segment) {
    auto segment = (milvus::segcore::SegmentBase*)c_segment;
    auto status = segment->Close();
    return status.code();
}

int
BuildIndex(CCollection c_collection, CSegmentBase c_segment) {
    auto collection = (milvus::segcore::Collection*)c_collection;
    auto segment = (milvus::segcore::SegmentBase*)c_segment;

    auto status = segment->BuildIndex(collection->get_index());
    return status.code();
}

bool
IsOpened(CSegmentBase c_segment) {
    auto segment = (milvus::segcore::SegmentBase*)c_segment;
    auto status = segment->get_state();
    return status == milvus::segcore::SegmentBase::SegmentState::Open;
}

int64_t
GetMemoryUsageInBytes(CSegmentBase c_segment) {
    auto segment = (milvus::segcore::SegmentBase*)c_segment;
    auto mem_size = segment->GetMemoryUsageInBytes();
    return mem_size;
}

//////////////////////////////////////////////////////////////////

int64_t
GetRowCount(CSegmentBase c_segment) {
    auto segment = (milvus::segcore::SegmentBase*)c_segment;
    auto row_count = segment->get_row_count();
    return row_count;
}

int64_t
GetDeletedCount(CSegmentBase c_segment) {
    auto segment = (milvus::segcore::SegmentBase*)c_segment;
    auto deleted_count = segment->get_deleted_count();
    return deleted_count;
}
