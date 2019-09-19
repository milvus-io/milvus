// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


#include <sstream>
#include <SPTAG/AnnService/inc/Server/QueryParser.h>
#include <SPTAG/AnnService/inc/Core/VectorSet.h>
#include <SPTAG/AnnService/inc/Core/Common.h>


#undef mkdir

#include "IndexKDT.h"
#include "knowhere/index/vector_index/utils/Definitions.h"
//#include "knowhere/index/preprocessor/normalize.h"
#include "knowhere/index/vector_index/utils/KDTParameterMgr.h"
#include "knowhere/adapter/SptagAdapter.h"
#include "knowhere/common/Exception.h"


namespace zilliz {
namespace knowhere {

BinarySet
CPUKDTRNG::Serialize() {
    std::vector<void *> index_blobs;
    std::vector<int64_t> index_len;
    index_ptr_->SaveIndexToMemory(index_blobs, index_len);
    BinarySet binary_set;

    auto sample = std::make_shared<uint8_t>();
    sample.reset(static_cast<uint8_t *>(index_blobs[0]));
    auto tree = std::make_shared<uint8_t>();
    tree.reset(static_cast<uint8_t *>(index_blobs[1]));
    auto graph = std::make_shared<uint8_t>();
    graph.reset(static_cast<uint8_t *>(index_blobs[2]));
    auto metadata = std::make_shared<uint8_t>();
    metadata.reset(static_cast<uint8_t *>(index_blobs[3]));

    binary_set.Append("samples", sample, index_len[0]);
    binary_set.Append("tree", tree, index_len[1]);
    binary_set.Append("graph", graph, index_len[2]);
    binary_set.Append("metadata", metadata, index_len[3]);
    return binary_set;
}

void
CPUKDTRNG::Load(const BinarySet &binary_set) {
    std::vector<void *> index_blobs;

    auto samples = binary_set.GetByName("samples");
    index_blobs.push_back(samples->data.get());

    auto tree = binary_set.GetByName("tree");
    index_blobs.push_back(tree->data.get());

    auto graph = binary_set.GetByName("graph");
    index_blobs.push_back(graph->data.get());

    auto metadata = binary_set.GetByName("metadata");
    index_blobs.push_back(metadata->data.get());

    index_ptr_->LoadIndexFromMemory(index_blobs);
}

//PreprocessorPtr
//CPUKDTRNG::BuildPreprocessor(const DatasetPtr &dataset, const Config &config) {
//    return std::make_shared<NormalizePreprocessor>();
//}

IndexModelPtr
CPUKDTRNG::Train(const DatasetPtr &origin, const Config &train_config) {
    SetParameters(train_config);
    DatasetPtr dataset = origin->Clone();

    //if (index_ptr_->GetDistCalcMethod() == SPTAG::DistCalcMethod::Cosine
    //    && preprocessor_) {
    //    preprocessor_->Preprocess(dataset);
    //}

    auto vectorset = ConvertToVectorSet(dataset);
    auto metaset = ConvertToMetadataSet(dataset);
    index_ptr_->BuildIndex(vectorset, metaset);

    // TODO: return IndexModelPtr
    return nullptr;
}

void
CPUKDTRNG::Add(const DatasetPtr &origin, const Config &add_config) {
    SetParameters(add_config);
    DatasetPtr dataset = origin->Clone();

    //if (index_ptr_->GetDistCalcMethod() == SPTAG::DistCalcMethod::Cosine
    //    && preprocessor_) {
    //    preprocessor_->Preprocess(dataset);
    //}

    auto vectorset = ConvertToVectorSet(dataset);
    auto metaset = ConvertToMetadataSet(dataset);
    index_ptr_->AddIndex(vectorset, metaset);
}

void
CPUKDTRNG::SetParameters(const Config &config) {
    for (auto &para : KDTParameterMgr::GetInstance().GetKDTParameters()) {
        auto value = config.get_with_default(para.first, para.second);
        index_ptr_->SetParameter(para.first, value);
    }
}

DatasetPtr
CPUKDTRNG::Search(const DatasetPtr &dataset, const Config &config) {
    SetParameters(config);
    auto tensor = dataset->tensor()[0];
    auto p = (float *) tensor->raw_mutable_data();
    for (auto i = 0; i < 10; ++i) {
        for (auto j = 0; j < 10; ++j) {
            std::cout << p[i * 10 + j] << " ";
        }
        std::cout << std::endl;
    }
    std::vector<SPTAG::QueryResult> query_results = ConvertToQueryResult(dataset, config);

#pragma omp parallel for
    for (auto i = 0; i < query_results.size(); ++i) {
        auto target = (float *) query_results[i].GetTarget();
        std::cout << target[0] << ", " << target[1] << ", " << target[2] << std::endl;
        index_ptr_->SearchIndex(query_results[i]);
    }

    return ConvertToDataset(query_results);
}

int64_t CPUKDTRNG::Count() {
    index_ptr_->GetNumSamples();
}
int64_t CPUKDTRNG::Dimension() {
    index_ptr_->GetFeatureDim();
}

VectorIndexPtr CPUKDTRNG::Clone() {
    KNOWHERE_THROW_MSG("not support");
}

void CPUKDTRNG::Seal() {
    // do nothing
}

// TODO(linxj):
BinarySet
CPUKDTRNGIndexModel::Serialize() {}

void
CPUKDTRNGIndexModel::Load(const BinarySet &binary) {}

} // namespace knowhere
} // namespace zilliz
