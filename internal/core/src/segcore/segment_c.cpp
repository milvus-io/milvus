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
#include "google/protobuf/text_format.h"

//////////////////////////////    common interfaces    //////////////////////////////
CStatus
NewSegment(CCollection collection, SegmentType seg_type, int64_t segment_id, CSegmentInterface* c_segment) {
    try {
        auto col = reinterpret_cast<milvus::segcore::Collection*>(collection);

        std::unique_ptr<milvus::segcore::SegmentInterface> segment;
        switch (seg_type) {
            case Growing: {
                auto seg = milvus::segcore::CreateGrowingSegment(col->get_schema(), segment_id);
                seg->disable_small_index();
                segment = std::move(seg);
                break;
            }
            case Sealed:
            case Indexing:
                segment = milvus::segcore::CreateSealedSegment(col->get_schema(), segment_id);
                break;
            default:
                AssertInfo(false, "invalid segment type " + std::to_string(seg_type));
                break;
        }
        *c_segment = segment.release();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
DeleteSegment(CSegmentInterface c_segment) {
    try {
        auto s = reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        delete s;
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
DeleteSearchResult(CSearchResult search_result) {
    try {
        auto res = reinterpret_cast<milvus::SearchResult*>(search_result);
        delete res;
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
Search(CSegmentInterface c_segment,
       CSearchPlan c_plan,
       CPlaceholderGroup c_placeholder_group,
       uint64_t timestamp,
       CSearchResult* result,
       int64_t segment_id) {
    try {
        auto segment = reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        auto plan = reinterpret_cast<milvus::query::Plan*>(c_plan);
        auto phg_ptr = reinterpret_cast<const milvus::query::PlaceholderGroup*>(c_placeholder_group);
        auto search_result = segment->Search(plan, phg_ptr, timestamp);
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

CStatus
DeleteRetrieveResult(CRetrieveResult* retrieve_result) {
    try {
        std::free(const_cast<void*>(retrieve_result->proto_blob));
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
Retrieve(CSegmentInterface c_segment, CRetrievePlan c_plan, uint64_t timestamp, CRetrieveResult* result) {
    try {
        auto segment = reinterpret_cast<const milvus::segcore::SegmentInterface*>(c_segment);
        auto plan = reinterpret_cast<const milvus::query::RetrievePlan*>(c_plan);
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

CStatus
GetMemoryUsageInBytes(CSegmentInterface c_segment, int64_t* mem_size) {
    try {
        auto segment = reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        *mem_size = segment->GetMemoryUsageInBytes();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
GetRowCount(CSegmentInterface c_segment, int64_t* row_count) {
    try {
        auto segment = reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        *row_count = segment->get_row_count();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
GetDeletedCount(CSegmentInterface c_segment, int64_t* deleted_count) {
    try {
        auto segment = reinterpret_cast<milvus::segcore::SegmentGrowing*>(c_segment);
        *deleted_count = segment->get_deleted_count();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
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
        auto segment = reinterpret_cast<milvus::segcore::SegmentGrowing*>(c_segment);
        auto insert_data = std::make_unique<milvus::InsertData>();
        auto suc = insert_data->ParseFromArray(data_info, data_info_len);
        AssertInfo(suc, "failed to parse insert data from records");

        segment->Insert(reserved_offset, size, row_ids, timestamps, insert_data.get());
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
PreInsert(CSegmentInterface c_segment, int64_t size, int64_t* offset) {
    try {
        auto segment = reinterpret_cast<milvus::segcore::SegmentGrowing*>(c_segment);
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
    try {
        auto segment = reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        auto pks = std::make_unique<milvus::proto::schema::IDs>();
        auto suc = pks->ParseFromArray(ids, ids_size);
        AssertInfo(suc, "failed to parse pks from ids");
        auto res = segment->Delete(reserved_offset, size, pks.get(), timestamps);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
PreDelete(CSegmentInterface c_segment, int64_t size, int64_t* offset) {
    try {
        auto segment = reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);

        *offset = segment->PreDelete(size);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

//////////////////////////////    interfaces for sealed segment    //////////////////////////////
CStatus
LoadFieldData(CSegmentInterface c_segment, CLoadFieldDataInfo load_field_data_info) {
    try {
        auto segment_interface = reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        auto segment = dynamic_cast<milvus::segcore::SegmentSealed*>(segment_interface);
        AssertInfo(segment != nullptr, "segment conversion failed");
        auto field_data = std::make_unique<milvus::DataArray>();
        auto suc = field_data->ParseFromArray(load_field_data_info.blob, load_field_data_info.blob_size);
        AssertInfo(suc, "unmarshal field data string failed");
        auto load_info =
            LoadFieldDataInfo{load_field_data_info.field_id, field_data.get(), load_field_data_info.row_count};
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
        AssertInfo(segment_interface != nullptr, "segment conversion failed");
        auto pks = std::make_unique<milvus::proto::schema::IDs>();
        auto suc = pks->ParseFromArray(deleted_record_info.primary_keys, deleted_record_info.primary_keys_size);
        AssertInfo(suc, "unmarshal field data string failed");
        auto load_info =
            LoadDeletedRecordInfo{deleted_record_info.timestamps, pks.get(), deleted_record_info.row_count};
        segment_interface->LoadDeletedRecord(load_info);
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
        auto load_index_info = reinterpret_cast<LoadIndexInfo*>(c_load_index_info);
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
