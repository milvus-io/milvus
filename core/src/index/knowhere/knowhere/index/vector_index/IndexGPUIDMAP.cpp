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

#include "knowhere/index/vector_index/IndexGPUIDMAP.h"

#include <faiss/AutoTune.h>
#include <faiss/IndexFlat.h>
#include <faiss/MetaIndexes.h>
#include <faiss/index_io.h>

#ifdef MILVUS_GPU_VERSION

#include <faiss/gpu/GpuCloner.h>

#endif

#include "knowhere/adapter/VectorAdapter.h"
#include "knowhere/common/Exception.h"
#include "knowhere/index/vector_index/IndexIDMAP.h"
#include "knowhere/index/vector_index/helpers/FaissIO.h"

namespace knowhere {

VectorIndexPtr
GPUIDMAP::CopyGpuToCpu(const Config& config) {
    std::lock_guard<std::mutex> lk(mutex_);

    faiss::Index* device_index = index_.get();
    faiss::Index* host_index = faiss::gpu::index_gpu_to_cpu(device_index);

    std::shared_ptr<faiss::Index> new_index;
    new_index.reset(host_index);
    return std::make_shared<IDMAP>(new_index);
}

// VectorIndexPtr
// GPUIDMAP::Clone() {
//    auto cpu_idx = CopyGpuToCpu(Config());
//
//    if (auto idmap = std::dynamic_pointer_cast<IDMAP>(cpu_idx)) {
//        return idmap->CopyCpuToGpu(gpu_id_, Config());
//    } else {
//        KNOWHERE_THROW_MSG("IndexType not Support GpuClone");
//    }
//}

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

void
GPUIDMAP::GenGraph(const float* data, const int64_t& k, Graph& graph, const Config& config) {
    int64_t K = k + 1;
    auto ntotal = Count();

    size_t dim = config->d;
    auto batch_size = 1000;
    auto tail_batch_size = ntotal % batch_size;
    auto batch_search_count = ntotal / batch_size;
    auto total_search_count = tail_batch_size == 0 ? batch_search_count : batch_search_count + 1;

    std::vector<float> res_dis(K * batch_size);
    graph.resize(ntotal);
    Graph res_vec(total_search_count);
    for (int i = 0; i < total_search_count; ++i) {
        auto b_size = (i == (total_search_count - 1)) && tail_batch_size != 0 ? tail_batch_size : batch_size;

        auto& res = res_vec[i];
        res.resize(K * b_size);

        auto xq = data + batch_size * dim * i;
        search_impl(b_size, (float*)xq, K, res_dis.data(), res.data(), config);

        for (int j = 0; j < b_size; ++j) {
            auto& node = graph[batch_size * i + j];
            node.resize(k);
            auto start_pos = j * K + 1;
            for (int m = 0, cursor = start_pos; m < k && cursor < start_pos + k; ++m, ++cursor) {
                node[m] = res[cursor];
            }
        }
    }
}

}  // namespace knowhere
