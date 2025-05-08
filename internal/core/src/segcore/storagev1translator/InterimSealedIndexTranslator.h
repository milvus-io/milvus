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

#include <cstdint>
#include "cachinglayer/Translator.h"
#include "index/Index.h"
#include "segcore/ChunkedSegmentSealedImpl.h"

namespace milvus::segcore::storagev1translator {

class InterimSealedIndexTranslator
    : public milvus::cachinglayer::Translator<milvus::index::IndexBase> {
 public:
    InterimSealedIndexTranslator(
        std::shared_ptr<ChunkedColumnInterface> vec_data,
        std::string segment_id,
        std::string field_id,
        knowhere::IndexType index_type,
        knowhere::MetricType metric_type,
        knowhere::Json build_config,
        int64_t dim,
        bool is_sparse);
    size_t
    num_cells() const override;
    milvus::cachinglayer::cid_t
    cell_id_of(milvus::cachinglayer::uid_t uid) const override;
    milvus::cachinglayer::ResourceUsage
    estimated_byte_size_of_cell(milvus::cachinglayer::cid_t cid) const override;
    const std::string&
    key() const override;
    std::vector<std::pair<milvus::cachinglayer::cid_t,
                          std::unique_ptr<milvus::index::IndexBase>>>
    get_cells(const std::vector<milvus::cachinglayer::cid_t>& cids) override;
    Meta*
    meta() override;

 private:
    std::shared_ptr<ChunkedColumnInterface> vec_data_;
    std::string segment_id_;
    std::string field_id_;
    knowhere::IndexType index_type_;
    knowhere::MetricType metric_type_;
    knowhere::Json build_config_;
    int64_t dim_;
    bool is_sparse_;
    std::string index_key_;
    milvus::cachinglayer::Meta meta_;
};

}  // namespace milvus::segcore::storagev1translator
