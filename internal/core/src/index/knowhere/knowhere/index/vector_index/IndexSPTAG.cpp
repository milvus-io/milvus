// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include <SPTAG/AnnService/inc/Core/Common.h>
#include <SPTAG/AnnService/inc/Core/VectorSet.h>
#include <SPTAG/AnnService/inc/Server/QueryParser.h>

#include <array>
#include <sstream>
#include <vector>

#undef mkdir

#include "knowhere/common/Exception.h"
#include "knowhere/index/vector_index/IndexSPTAG.h"
#include "knowhere/index/vector_index/adapter/SptagAdapter.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "knowhere/index/vector_index/helpers/SPTAGParameterMgr.h"

namespace milvus {
namespace knowhere {

CPUSPTAGRNG::CPUSPTAGRNG(const std::string& IndexType) {
    if (IndexType == "KDT") {
        index_ptr_ = SPTAG::VectorIndex::CreateInstance(SPTAG::IndexAlgoType::KDT, SPTAG::VectorValueType::Float);
        index_ptr_->SetParameter("DistCalcMethod", "L2");
        index_type_ = IndexEnum::INDEX_SPTAG_KDT_RNT;
    } else {
        index_ptr_ = SPTAG::VectorIndex::CreateInstance(SPTAG::IndexAlgoType::BKT, SPTAG::VectorValueType::Float);
        index_ptr_->SetParameter("DistCalcMethod", "L2");
        index_type_ = IndexEnum::INDEX_SPTAG_BKT_RNT;
    }
}

BinarySet
CPUSPTAGRNG::Serialize(const Config& config) {
    std::string index_config;
    std::vector<SPTAG::ByteArray> index_blobs;

    std::shared_ptr<std::vector<std::uint64_t>> buffersize = index_ptr_->CalculateBufferSize();
    std::vector<char*> res(buffersize->size() + 1);
    for (uint64_t i = 1; i < res.size(); i++) {
        res[i] = new char[buffersize->at(i - 1)];
        auto ptr = &res[i][0];
        index_blobs.emplace_back(SPTAG::ByteArray((std::uint8_t*)ptr, buffersize->at(i - 1), false));
    }

    index_ptr_->SaveIndex(index_config, index_blobs);

    size_t length = index_config.length();
    char* cstr = new char[length];
    snprintf(cstr, length, "%s", index_config.c_str());

    BinarySet binary_set;
    std::shared_ptr<uint8_t[]> sample;
    sample.reset(static_cast<uint8_t*>(index_blobs[0].Data()));
    std::shared_ptr<uint8_t[]> tree;
    tree.reset(static_cast<uint8_t*>(index_blobs[1].Data()));
    std::shared_ptr<uint8_t[]> graph;
    graph.reset(static_cast<uint8_t*>(index_blobs[2].Data()));
    std::shared_ptr<uint8_t[]> deleteid;
    deleteid.reset(static_cast<uint8_t*>(index_blobs[3].Data()));
    std::shared_ptr<uint8_t[]> metadata1;
    metadata1.reset(static_cast<uint8_t*>(index_blobs[4].Data()));
    std::shared_ptr<uint8_t[]> metadata2;
    metadata2.reset(static_cast<uint8_t*>(index_blobs[5].Data()));
    std::shared_ptr<uint8_t[]> x_cfg;
    x_cfg.reset(static_cast<uint8_t*>((void*)cstr));

    binary_set.Append("samples", sample, index_blobs[0].Length());
    binary_set.Append("tree", tree, index_blobs[1].Length());
    binary_set.Append("deleteid", deleteid, index_blobs[3].Length());
    binary_set.Append("metadata1", metadata1, index_blobs[4].Length());
    binary_set.Append("metadata2", metadata2, index_blobs[5].Length());
    binary_set.Append("config", x_cfg, length);
    binary_set.Append("graph", graph, index_blobs[2].Length());

    if (config.contains(INDEX_FILE_SLICE_SIZE_IN_MEGABYTE)) {
        Disassemble(config[INDEX_FILE_SLICE_SIZE_IN_MEGABYTE].get<int64_t>() * 1024 * 1024, binary_set);
    }
    return binary_set;
}

void
CPUSPTAGRNG::Load(const BinarySet& binary_set) {
    Assemble(const_cast<BinarySet&>(binary_set));
    std::string index_config;
    std::vector<SPTAG::ByteArray> index_blobs;

    auto samples = binary_set.GetByName("samples");
    index_blobs.push_back(SPTAG::ByteArray(samples->data.get(), samples->size, false));

    auto tree = binary_set.GetByName("tree");
    index_blobs.push_back(SPTAG::ByteArray(tree->data.get(), tree->size, false));

    auto graph = binary_set.GetByName("graph");
    index_blobs.push_back(SPTAG::ByteArray(graph->data.get(), graph->size, false));

    auto deleteid = binary_set.GetByName("deleteid");
    index_blobs.push_back(SPTAG::ByteArray(deleteid->data.get(), deleteid->size, false));

    auto metadata1 = binary_set.GetByName("metadata1");
    index_blobs.push_back(SPTAG::ByteArray(CopyBinary(metadata1), metadata1->size, true));

    auto metadata2 = binary_set.GetByName("metadata2");
    index_blobs.push_back(SPTAG::ByteArray(metadata2->data.get(), metadata2->size, false));

    auto config = binary_set.GetByName("config");
    index_config = reinterpret_cast<char*>(config->data.get());

    index_ptr_->LoadIndex(index_config, index_blobs);
}

void
CPUSPTAGRNG::BuildAll(const DatasetPtr& origin, const Config& train_config) {
    SetParameters(train_config);

    DatasetPtr dataset = origin;

    auto vectorset = ConvertToVectorSet(dataset);
    auto metaset = ConvertToMetadataSet(dataset);
    index_ptr_->BuildIndex(vectorset, metaset);
}

void
CPUSPTAGRNG::SetParameters(const Config& config) {
#define Assign(param_name, str_name) \
    index_ptr_->SetParameter(str_name, std::to_string(build_cfg[param_name].get<int64_t>()))

    if (index_type_ == IndexEnum::INDEX_SPTAG_KDT_RNT) {
        auto build_cfg = SPTAGParameterMgr::GetInstance().GetKDTParameters();

        Assign("kdtnumber", "KDTNumber");
        Assign("numtopdimensionkdtsplit", "NumTopDimensionKDTSplit");
        Assign("samples", "Samples");
        Assign("tptnumber", "TPTNumber");
        Assign("tptleafsize", "TPTLeafSize");
        Assign("numtopdimensiontptsplit", "NumTopDimensionTPTSplit");
        Assign("neighborhoodsize", "NeighborhoodSize");
        Assign("graphneighborhoodscale", "GraphNeighborhoodScale");
        Assign("graphcefscale", "GraphCEFScale");
        Assign("refineiterations", "RefineIterations");
        Assign("cef", "CEF");
        Assign("maxcheckforrefinegraph", "MaxCheckForRefineGraph");
        Assign("numofthreads", "NumberOfThreads");
        Assign("maxcheck", "MaxCheck");
        Assign("thresholdofnumberofcontinuousnobetterpropagation", "ThresholdOfNumberOfContinuousNoBetterPropagation");
        Assign("numberofinitialdynamicpivots", "NumberOfInitialDynamicPivots");
        Assign("numberofotherdynamicpivots", "NumberOfOtherDynamicPivots");
    } else {
        auto build_cfg = SPTAGParameterMgr::GetInstance().GetBKTParameters();

        Assign("bktnumber", "BKTNumber");
        Assign("bktkmeansk", "BKTKMeansK");
        Assign("bktleafsize", "BKTLeafSize");
        Assign("samples", "Samples");
        Assign("tptnumber", "TPTNumber");
        Assign("tptleafsize", "TPTLeafSize");
        Assign("numtopdimensiontptsplit", "NumTopDimensionTPTSplit");
        Assign("neighborhoodsize", "NeighborhoodSize");
        Assign("graphneighborhoodscale", "GraphNeighborhoodScale");
        Assign("graphcefscale", "GraphCEFScale");
        Assign("refineiterations", "RefineIterations");
        Assign("cef", "CEF");
        Assign("maxcheckforrefinegraph", "MaxCheckForRefineGraph");
        Assign("numofthreads", "NumberOfThreads");
        Assign("maxcheck", "MaxCheck");
        Assign("thresholdofnumberofcontinuousnobetterpropagation", "ThresholdOfNumberOfContinuousNoBetterPropagation");
        Assign("numberofinitialdynamicpivots", "NumberOfInitialDynamicPivots");
        Assign("numberofotherdynamicpivots", "NumberOfOtherDynamicPivots");
    }
}

DatasetPtr
CPUSPTAGRNG::Query(const DatasetPtr& dataset_ptr, const Config& config, const faiss::BitsetView bitset) {
    SetParameters(config);

    float* p_data = (float*)dataset_ptr->Get<const void*>(meta::TENSOR);
    for (auto i = 0; i < 10; ++i) {
        for (auto j = 0; j < 10; ++j) {
            std::cout << p_data[i * 10 + j] << " ";
        }
        std::cout << std::endl;
    }
    std::vector<SPTAG::QueryResult> query_results = ConvertToQueryResult(dataset_ptr, config);

#pragma omp parallel for
    for (auto i = 0; i < query_results.size(); ++i) {
        auto target = (float*)query_results[i].GetTarget();
        std::cout << target[0] << ", " << target[1] << ", " << target[2] << std::endl;
        index_ptr_->SearchIndex(query_results[i]);
    }

    return ConvertToDataset(query_results, uids_);
}

int64_t
CPUSPTAGRNG::Count() {
    if (!index_ptr_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    return index_ptr_->GetNumSamples();
}

int64_t
CPUSPTAGRNG::Dim() {
    if (!index_ptr_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    return index_ptr_->GetFeatureDim();
}

void
CPUSPTAGRNG::UpdateIndexSize() {
    if (!index_ptr_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    index_size_ = index_ptr_->GetIndexSize();
}

// void
// CPUSPTAGRNG::Add(const DatasetPtr& origin, const Config& add_config) {
//     SetParameters(add_config);
//     DatasetPtr dataset = origin->Clone();

//     // if (index_ptr_->GetDistCalcMethod() == SPTAG::DistCalcMethod::Cosine
//     //    && preprocessor_) {
//     //    preprocessor_->Preprocess(dataset);
//     //}

//     auto vectorset = ConvertToVectorSet(dataset);
//     auto metaset = ConvertToMetadataSet(dataset);
//     index_ptr_->AddIndex(vectorset, metaset);
// }

}  // namespace knowhere
}  // namespace milvus
