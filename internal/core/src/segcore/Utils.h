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

#include <unordered_map>
#include <exception>
#include <memory>
#include <stdexcept>
#include <cstdlib>
#include <string>
#include <utility>
#include <vector>

#include "common/QueryResult.h"
#include "common/Types.h"
#include "segcore/DeletedRecord.h"
#include "segcore/InsertRecord.h"
#include "index/Index.h"
#include "storage/FieldData.h"

namespace milvus::segcore {

void
ParsePksFromFieldData(std::vector<PkType>& pks, const DataArray& data);

void
ParsePksFromFieldData(DataType data_type,
                      std::vector<PkType>& pks,
                      const std::vector<storage::FieldDataPtr>& datas);

void
ParsePksFromIDs(std::vector<PkType>& pks,
                DataType data_type,
                const IdArray& data);

int64_t
GetSizeOfIdArray(const IdArray& data);

// Note: this is temporary solution.
// modify bulk script implement to make process more clear
std::unique_ptr<DataArray>
CreateScalarDataArray(int64_t count, const FieldMeta& field_meta);

std::unique_ptr<DataArray>
CreateVectorDataArray(int64_t count, const FieldMeta& field_meta);

std::unique_ptr<DataArray>
CreateScalarDataArrayFrom(const void* data_raw,
                          int64_t count,
                          const FieldMeta& field_meta);

std::unique_ptr<DataArray>
CreateVectorDataArrayFrom(const void* data_raw,
                          int64_t count,
                          const FieldMeta& field_meta);

std::unique_ptr<DataArray>
CreateDataArrayFrom(const void* data_raw,
                    int64_t count,
                    const FieldMeta& field_meta);

// TODO remove merge dataArray, instead fill target entity when get data slice
std::unique_ptr<DataArray>
MergeDataArray(
    std::vector<std::pair<milvus::SearchResult*, int64_t>>& result_offsets,
    const FieldMeta& field_meta);

template <bool is_sealed>
std::shared_ptr<DeletedRecord::TmpBitmap>
get_deleted_bitmap(int64_t del_barrier,
                   int64_t insert_barrier,
                   DeletedRecord& delete_record,
                   const InsertRecord<is_sealed>& insert_record,
                   Timestamp query_timestamp) {
    // if insert_barrier and del_barrier have not changed, use cache data directly
    bool hit_cache = false;
    int64_t old_del_barrier = 0;
    auto current = delete_record.clone_lru_entry(
        insert_barrier, del_barrier, old_del_barrier, hit_cache);
    if (hit_cache) {
        return current;
    }

    auto bitmap = current->bitmap_ptr;

    int64_t start, end;
    if (del_barrier < old_del_barrier) {
        // in this case, ts of delete record[current_del_barrier : old_del_barrier] > query_timestamp
        // so these deletion records do not take effect in query/search
        // so bitmap corresponding to those pks in delete record[current_del_barrier:old_del_barrier] will be reset to 0
        // for example, current_del_barrier = 2, query_time = 120, the bitmap will be reset to [0, 1, 1, 0, 0, 0, 0, 0]
        start = del_barrier;
        end = old_del_barrier;
    } else {
        // the cache is not enough, so update bitmap using new pks in delete record[old_del_barrier:current_del_barrier]
        // for example, current_del_barrier = 4, query_time = 300, bitmap will be updated to [0, 1, 1, 0, 1, 1, 0, 0]
        start = old_del_barrier;
        end = del_barrier;
    }

    // Avoid invalid calculations when there are a lot of repeated delete pks
    std::unordered_map<PkType, Timestamp> delete_timestamps;
    for (auto del_index = start; del_index < end; ++del_index) {
        auto pk = delete_record.pks()[del_index];
        auto timestamp = delete_record.timestamps()[del_index];

        delete_timestamps[pk] = timestamp > delete_timestamps[pk]
                                    ? timestamp
                                    : delete_timestamps[pk];
    }

    for (auto& [pk, timestamp] : delete_timestamps) {
        auto segOffsets = insert_record.search_pk(pk, insert_barrier);
        for (auto offset : segOffsets) {
            int64_t insert_row_offset = offset.get();

            // The deletion record do not take effect in search/query,
            // and reset bitmap to 0
            if (timestamp > query_timestamp) {
                bitmap->reset(insert_row_offset);
                continue;
            }
            // Insert after delete with same pk, delete will not task effect on this insert record,
            // and reset bitmap to 0
            if (insert_record.timestamps_[insert_row_offset] >= timestamp) {
                bitmap->reset(insert_row_offset);
                continue;
            }
            // insert data corresponding to the insert_row_offset will be ignored in search/query
            bitmap->set(insert_row_offset);
        }
    }

    delete_record.insert_lru_entry(current);
    return current;
}

std::unique_ptr<DataArray>
ReverseDataFromIndex(const index::IndexBase* index,
                     const int64_t* seg_offsets,
                     int64_t count,
                     const FieldMeta& field_meta);

void
LoadFieldDatasFromRemote(std::vector<std::string>& remote_files,
                         storage::FieldDataChannelPtr channel);

std::vector<storage::FieldDataPtr>
CollectFieldDataChannel(storage::FieldDataChannelPtr& channel);

}  // namespace milvus::segcore
