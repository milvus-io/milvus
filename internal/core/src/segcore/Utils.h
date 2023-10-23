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
#include "mmap/Column.h"
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

int64_t
GetRawDataSizeOfDataArray(const DataArray* data,
                          const FieldMeta& field_meta,
                          int64_t num_rows);

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

std::unique_ptr<DataArray>
ReverseDataFromIndex(const index::IndexBase* index,
                     const int64_t* seg_offsets,
                     int64_t count,
                     const FieldMeta& field_meta);

void
LoadFieldDatasFromRemote(std::vector<std::string>& remote_files,
                         storage::FieldDataChannelPtr channel);

template <bool is_sealed>
std::vector<std::pair<Timestamp, int64_t>>
LookupPrimaryKeys(const InsertRecord<is_sealed>& insert_record,
                  const std::vector<PkType>& pks,
                  const Timestamp* tss) {
    std::vector<std::pair<Timestamp, int64_t>> delete_records;
    delete_records.reserve(pks.size());

    for (int i = 0; i < pks.size(); i++) {
        auto pk_offsets = insert_record.search_pk(pks[i], tss[i]);
        if (pk_offsets.empty()) {
            continue;
        }

        delete_records.emplace_back(tss[i], pk_offsets.back().get());
    }

    return delete_records;
}

/**
 * Returns an index pointing to the first element in the range [first, last) such that `value < element` is true
 * (i.e. that is strictly greater than value), or last if no such element is found.
 *
 * @param timestamps
 * @param first
 * @param last
 * @param value
 * @return The index of answer, last will be returned if no timestamp is bigger than the value.
 */
int64_t
upper_bound(const ConcurrentVector<Timestamp>& timestamps,
            int64_t first,
            int64_t last,
            Timestamp value);
}  // namespace milvus::segcore
