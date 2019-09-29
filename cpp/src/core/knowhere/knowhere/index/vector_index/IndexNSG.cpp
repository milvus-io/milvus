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

#include "knowhere/index/vector_index/IndexNSG.h"
#include "knowhere/adapter/VectorAdapter.h"
#include "knowhere/common/Exception.h"
#include "knowhere/common/Timer.h"
#include "knowhere/index/vector_index/IndexGPUIVF.h"
#include "knowhere/index/vector_index/IndexIDMAP.h"
#include "knowhere/index/vector_index/IndexIVF.h"
#include "knowhere/index/vector_index/nsg/NSG.h"
#include "knowhere/index/vector_index/nsg/NSGIO.h"

namespace knowhere {

BinarySet
NSG::Serialize() {
    if (!index_ || !index_->is_trained) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }

    try {
        algo::NsgIndex* index = index_.get();

        MemoryIOWriter writer;
        algo::write_index(index, writer);
        auto data = std::make_shared<uint8_t>();
        data.reset(writer.data_);

        BinarySet res_set;
        res_set.Append("NSG", data, writer.total);
        return res_set;
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

void
NSG::Load(const BinarySet& index_binary) {
    try {
        auto binary = index_binary.GetByName("NSG");

        MemoryIOReader reader;
        reader.total = binary->size;
        reader.data_ = binary->data.get();

        auto index = algo::read_index(reader);
        index_.reset(index);
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

DatasetPtr
NSG::Search(const DatasetPtr& dataset, const Config& config) {
    auto build_cfg = std::dynamic_pointer_cast<NSGCfg>(config);
    if (build_cfg != nullptr) {
        build_cfg->CheckValid();  // throw exception
    }

    if (!index_ || !index_->is_trained) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }

    GETTENSOR(dataset)

    auto elems = rows * build_cfg->k;
    auto res_ids = (int64_t*)malloc(sizeof(int64_t) * elems);
    auto res_dis = (float*)malloc(sizeof(float) * elems);

    algo::SearchParams s_params;
    s_params.search_length = build_cfg->search_length;
    index_->Search((float*)p_data, rows, dim, build_cfg->k, res_dis, res_ids, s_params);

    auto id_buf = MakeMutableBufferSmart((uint8_t*)res_ids, sizeof(int64_t) * elems);
    auto dist_buf = MakeMutableBufferSmart((uint8_t*)res_dis, sizeof(float) * elems);

    std::vector<BufferPtr> id_bufs{nullptr, id_buf};
    std::vector<BufferPtr> dist_bufs{nullptr, dist_buf};

    auto int64_type = std::make_shared<arrow::Int64Type>();
    auto float_type = std::make_shared<arrow::FloatType>();

    auto id_array_data = arrow::ArrayData::Make(int64_type, elems, id_bufs);
    auto dist_array_data = arrow::ArrayData::Make(float_type, elems, dist_bufs);

    auto ids = std::make_shared<NumericArray<arrow::Int64Type>>(id_array_data);
    auto dists = std::make_shared<NumericArray<arrow::FloatType>>(dist_array_data);
    std::vector<ArrayPtr> array{ids, dists};

    return std::make_shared<Dataset>(array, nullptr);
}

IndexModelPtr
NSG::Train(const DatasetPtr& dataset, const Config& config) {
    auto build_cfg = std::dynamic_pointer_cast<NSGCfg>(config);
    if (build_cfg != nullptr) {
        build_cfg->CheckValid();  // throw exception
    }

    if (build_cfg->metric_type != METRICTYPE::L2) {
        KNOWHERE_THROW_MSG("NSG not support this kind of metric type");
    }

    // TODO(linxj): dev IndexFactory, support more IndexType
    auto preprocess_index = std::make_shared<GPUIVF>(build_cfg->gpu_id);
    auto model = preprocess_index->Train(dataset, config);
    preprocess_index->set_index_model(model);
    preprocess_index->AddWithoutIds(dataset, config);

    Graph knng;
    preprocess_index->GenGraph(build_cfg->knng, knng, dataset, config);

    algo::BuildParams b_params;
    b_params.candidate_pool_size = build_cfg->candidate_pool_size;
    b_params.out_degree = build_cfg->out_degree;
    b_params.search_length = build_cfg->search_length;

    GETTENSOR(dataset)
    auto array = dataset->array()[0];
    auto p_ids = array->data()->GetValues<int64_t>(1, 0);

    index_ = std::make_shared<algo::NsgIndex>(dim, rows);
    index_->SetKnnGraph(knng);
    index_->Build_with_ids(rows, (float*)p_data, (int64_t*)p_ids, b_params);
    return nullptr;  // TODO(linxj): support serialize
}

void
NSG::Add(const DatasetPtr& dataset, const Config& config) {
    // do nothing
}

int64_t
NSG::Count() {
    return index_->ntotal;
}

int64_t
NSG::Dimension() {
    return index_->dimension;
}

VectorIndexPtr
NSG::Clone() {
    KNOWHERE_THROW_MSG("not support");
}

void
NSG::Seal() {
    // do nothing
}

}  // namespace knowhere
