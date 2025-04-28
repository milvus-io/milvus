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

#include <string>
#include <vector>

#include "cachinglayer/Translator.h"
#include "cachinglayer/Utils.h"
#include "common/Schema.h"
#include "segcore/InsertRecord.h"
#include "segcore/ChunkedSegmentSealedImpl.h"

namespace milvus::segcore::storagev1translator {

class InsertRecordTranslator : public milvus::cachinglayer::Translator<
                                   milvus::segcore::InsertRecord<true>> {
 public:
    InsertRecordTranslator(int64_t segment_id,
                           DataType data_type,
                           FieldDataInfo field_data_info,
                           SchemaPtr schema,
                           bool is_sorted_by_pk,
                           std::vector<std::string> insert_files,
                           ChunkedSegmentSealedImpl* chunked_segment);

    size_t
    num_cells() const override;
    milvus::cachinglayer::cid_t
    cell_id_of(milvus::cachinglayer::uid_t uid) const override;
    milvus::cachinglayer::ResourceUsage
    estimated_byte_size_of_cell(milvus::cachinglayer::cid_t cid) const override;
    const std::string&
    key() const override;

    // each calling of this will trigger a new download.
    std::vector<std::pair<milvus::cachinglayer::cid_t,
                          std::unique_ptr<milvus::segcore::InsertRecord<true>>>>
    get_cells(const std::vector<milvus::cachinglayer::cid_t>& cids) override;

    // InsertRecord does not have meta.
    milvus::cachinglayer::Meta*
    meta() override {
        return &meta_;
    }

 private:
    std::unique_ptr<milvus::segcore::InsertRecord<true>>
    load_column_in_memory() const;

    int64_t segment_id_;
    std::string key_;
    DataType data_type_;
    FieldDataInfo field_data_info_;
    std::vector<std::string> insert_files_;
    mutable size_t estimated_byte_size_of_cell_;
    SchemaPtr schema_;
    bool is_sorted_by_pk_;
    ChunkedSegmentSealedImpl* chunked_segment_;
    milvus::cachinglayer::Meta meta_;
};

}  // namespace milvus::segcore::storagev1translator
