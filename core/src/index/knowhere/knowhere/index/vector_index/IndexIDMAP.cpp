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

#include <faiss/IndexFlat.h>
#include <faiss/MetaIndexes.h>
#include <faiss/gpu/GpuCloner.h>
#include <faiss/index_factory.h>
#include <faiss/index_io.h>

#include <vector>

#include "knowhere/adapter/VectorAdapter.h"
#include "knowhere/common/Exception.h"
#include "knowhere/index/vector_index/IndexIDMAP.h"
#include "knowhere/index/vector_index/helpers/FaissIO.h"

namespace knowhere {

BinarySet
IDMAP::Serialize() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }

    std::lock_guard<std::mutex> lk(mutex_);
    return SerializeImpl();
}

void
IDMAP::Load(const BinarySet& index_binary) {
    std::lock_guard<std::mutex> lk(mutex_);
    LoadImpl(index_binary);
}

DatasetPtr
IDMAP::Search(const DatasetPtr& dataset, const Config& config) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }

    config->CheckValid();
    // auto metric_type = config["metric_type"].as_string() == "L2" ?
    //                   faiss::METRIC_L2 : faiss::METRIC_INNER_PRODUCT;
    // index_->metric_type = metric_type;

    GETTENSOR(dataset)

    auto elems = rows * config->k;
    auto res_ids = (int64_t*)malloc(sizeof(int64_t) * elems);
    auto res_dis = (float*)malloc(sizeof(float) * elems);

    search_impl(rows, (float*)p_data, config->k, res_dis, res_ids, Config());

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

void
IDMAP::search_impl(int64_t n, const float* data, int64_t k, float* distances, int64_t* labels, const Config& cfg) {
    index_->search(n, (float*)data, k, distances, labels);
}

void
IDMAP::Add(const DatasetPtr& dataset, const Config& config) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }

    std::lock_guard<std::mutex> lk(mutex_);
    GETTENSOR(dataset)

    // TODO: magic here.
    auto array = dataset->array()[0];
    auto p_ids = array->data()->GetValues<int64_t>(1, 0);

    index_->add_with_ids(rows, (float*)p_data, p_ids);
}

int64_t
IDMAP::Count() {
    return index_->ntotal;
}

int64_t
IDMAP::Dimension() {
    return index_->d;
}

// TODO(linxj): return const pointer
float*
IDMAP::GetRawVectors() {
    try {
        auto file_index = dynamic_cast<faiss::IndexIDMap*>(index_.get());
        auto flat_index = dynamic_cast<faiss::IndexFlat*>(file_index->index);
        return flat_index->xb.data();
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

// TODO(linxj): return const pointer
int64_t*
IDMAP::GetRawIds() {
    try {
        auto file_index = dynamic_cast<faiss::IndexIDMap*>(index_.get());
        return file_index->id_map.data();
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

const char* type = "IDMap,Flat";

void
IDMAP::Train(const Config& config) {
    config->CheckValid();

    auto index = faiss::index_factory(config->d, type, GetMetricType(config->metric_type));
    index_.reset(index);
}

VectorIndexPtr
IDMAP::Clone() {
    std::lock_guard<std::mutex> lk(mutex_);

    auto clone_index = faiss::clone_index(index_.get());
    std::shared_ptr<faiss::Index> new_index;
    new_index.reset(clone_index);
    return std::make_shared<IDMAP>(new_index);
}

VectorIndexPtr
IDMAP::CopyCpuToGpu(const int64_t& device_id, const Config& config) {
    if (auto res = FaissGpuResourceMgr::GetInstance().GetRes(device_id)) {
        ResScope rs(res, device_id, false);
        auto gpu_index = faiss::gpu::index_cpu_to_gpu(res->faiss_res.get(), device_id, index_.get());

        std::shared_ptr<faiss::Index> device_index;
        device_index.reset(gpu_index);
        return std::make_shared<GPUIDMAP>(device_index, device_id, res);
    } else {
        KNOWHERE_THROW_MSG("CopyCpuToGpu Error, can't get gpu_resource");
    }
}

void
IDMAP::Seal() {
    // do nothing
}

VectorIndexPtr
GPUIDMAP::CopyGpuToCpu(const Config& config) {
    std::lock_guard<std::mutex> lk(mutex_);

    faiss::Index* device_index = index_.get();
    faiss::Index* host_index = faiss::gpu::index_gpu_to_cpu(device_index);

    std::shared_ptr<faiss::Index> new_index;
    new_index.reset(host_index);
    return std::make_shared<IDMAP>(new_index);
}

VectorIndexPtr
GPUIDMAP::Clone() {
    auto cpu_idx = CopyGpuToCpu(Config());

    if (auto idmap = std::dynamic_pointer_cast<IDMAP>(cpu_idx)) {
        return idmap->CopyCpuToGpu(gpu_id_, Config());
    } else {
        KNOWHERE_THROW_MSG("IndexType not Support GpuClone");
    }
}

BinarySet
GPUIDMAP::SerializeImpl() {
    try {
        MemoryIOWriter writer;
        {
            faiss::Index* index = index_.get();
            faiss::Index* host_index = faiss::gpu::index_gpu_to_cpu(index);

            faiss::write_index(host_index, &writer);
            delete host_index;
        }
        auto data = std::make_shared<uint8_t>();
        data.reset(writer.data_);

        BinarySet res_set;
        res_set.Append("IVF", data, writer.rp);

        return res_set;
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

void
GPUIDMAP::LoadImpl(const BinarySet& index_binary) {
    auto binary = index_binary.GetByName("IVF");
    MemoryIOReader reader;
    {
        reader.total = binary->size;
        reader.data_ = binary->data.get();

        faiss::Index* index = faiss::read_index(&reader);

        if (auto res = FaissGpuResourceMgr::GetInstance().GetRes(gpu_id_)) {
            ResScope rs(res, gpu_id_, false);
            auto device_index = faiss::gpu::index_cpu_to_gpu(res->faiss_res.get(), gpu_id_, index);
            index_.reset(device_index);
            res_ = res;
        } else {
            KNOWHERE_THROW_MSG("Load error, can't get gpu resource");
        }

        delete index;
    }
}

VectorIndexPtr
GPUIDMAP::CopyGpuToGpu(const int64_t& device_id, const Config& config) {
    auto cpu_index = CopyGpuToCpu(config);
    return std::static_pointer_cast<IDMAP>(cpu_index)->CopyCpuToGpu(device_id, config);
}

float*
GPUIDMAP::GetRawVectors() {
    KNOWHERE_THROW_MSG("Not support");
}

int64_t*
GPUIDMAP::GetRawIds() {
    KNOWHERE_THROW_MSG("Not support");
}

void
GPUIDMAP::search_impl(int64_t n, const float* data, int64_t k, float* distances, int64_t* labels, const Config& cfg) {
    ResScope rs(res_, gpu_id_);
    index_->search(n, (float*)data, k, distances, labels);
}

}  // namespace knowhere
