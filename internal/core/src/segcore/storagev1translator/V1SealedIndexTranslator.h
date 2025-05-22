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

// This translator is used for AppendIndex only
// However, AppendIndex is not called in Milvus, but just used in unit test
// This is because the AppendIndexV2 need to communicate with some outside components
// So this translator is not used in Milvus actually, it should be removed when AppendIndex is removed
class V1SealedIndexTranslator : public Translator<milvus::index::IndexBase> {
 public:
    V1SealedIndexTranslator(LoadIndexInfo* load_index_info,
                            knowhere::BinarySet* binary_set);
    ~V1SealedIndexTranslator() override = default;
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
    struct IndexLoadInfo {
        bool enable_mmap;
        std::string mmap_dir_path;
        DataType field_type;
        std::map<std::string, std::string> index_params;
        std::vector<std::string> index_files;
        int64_t index_size;
        int64_t index_engine_version;
        int64_t index_id;
        int64_t collection_id;
        int64_t partition_id;
        int64_t segment_id;
        int64_t field_id;
        int64_t index_build_id;
        int64_t index_version;
    };
    std::unique_ptr<milvus::index::IndexBase>
    LoadVecIndex();

    std::unique_ptr<milvus::index::IndexBase>
    LoadScalarIndex();

    std::string key_;
    milvus::cachinglayer::Meta meta_;
    IndexLoadInfo index_load_info_;
    knowhere::BinarySet* binary_set_;
};

}  // namespace milvus::segcore::storagev1translator
