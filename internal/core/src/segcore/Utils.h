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

#include "common/FieldData.h"
#include "common/QueryResult.h"
// #include "common/Schema.h"
#include "common/Types.h"
#include "index/Index.h"
#include "log/Log.h"
#include "segcore/DeletedRecord.h"
#include "segcore/InsertRecord.h"

namespace milvus::segcore {

void
ParsePksFromFieldData(std::vector<PkType>& pks, const DataArray& data);

void
ParsePksFromFieldData(DataType data_type,
                      std::vector<PkType>& pks,
                      const std::vector<FieldDataPtr>& datas);

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
                          const void* valid_data,
                          int64_t count,
                          const FieldMeta& field_meta);

std::unique_ptr<DataArray>
CreateVectorDataArrayFrom(const void* data_raw,
                          int64_t count,
                          const FieldMeta& field_meta);

std::unique_ptr<DataArray>
CreateDataArrayFrom(const void* data_raw,
                    const void* valid_data,
                    int64_t count,
                    const FieldMeta& field_meta);

// TODO remove merge dataArray, instead fill target entity when get data slice
struct MergeBase {
 private:
    std::map<FieldId, std::unique_ptr<milvus::DataArray>>* output_fields_data_;
    size_t offset_;

 public:
    MergeBase() {
    }

    MergeBase(std::map<FieldId, std::unique_ptr<milvus::DataArray>>*
                  output_fields_data,
              size_t offset)
        : output_fields_data_(output_fields_data), offset_(offset) {
    }

    size_t
    getOffset() const {
        return offset_;
    }

    milvus::DataArray*
    get_field_data(FieldId fieldId) const {
        return (*output_fields_data_)[fieldId].get();
    }
};

std::unique_ptr<DataArray>
MergeDataArray(std::vector<MergeBase>& merge_bases,
               const FieldMeta& field_meta);

std::unique_ptr<DataArray>
ReverseDataFromIndex(const index::IndexBase* index,
                     const int64_t* seg_offsets,
                     int64_t count,
                     const FieldMeta& field_meta);

void
LoadArrowReaderFromRemote(const std::vector<std::string>& remote_files,
                          std::shared_ptr<ArrowReaderChannel> channel);

void
LoadFieldDatasFromRemote(const std::vector<std::string>& remote_files,
                         FieldDataChannelPtr channel);
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
