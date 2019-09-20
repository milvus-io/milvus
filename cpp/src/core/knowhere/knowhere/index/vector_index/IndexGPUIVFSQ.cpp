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


#include <faiss/gpu/GpuAutoTune.h>

#include "knowhere/adapter/VectorAdapter.h"
#include "knowhere/common/Exception.h"
#include "IndexGPUIVFSQ.h"
#include "IndexIVFSQ.h"


namespace zilliz {
namespace knowhere {

    IndexModelPtr GPUIVFSQ::Train(const DatasetPtr &dataset, const Config &config) {
        auto nlist = config["nlist"].as<size_t>();
        auto nbits = config["nbits"].as<size_t>(); // TODO(linxj):  gpu only support SQ4 SQ8 SQ16
        gpu_id_ = config.get_with_default("gpu_id", gpu_id_);
        auto metric_type = config["metric_type"].as_string() == "L2" ?
                           faiss::METRIC_L2 : faiss::METRIC_INNER_PRODUCT;

        GETTENSOR(dataset)

        std::stringstream index_type;
        index_type << "IVF" << nlist << "," << "SQ" << nbits;
        auto build_index = faiss::index_factory(dim, index_type.str().c_str(), metric_type);

        auto temp_resource = FaissGpuResourceMgr::GetInstance().GetRes(gpu_id_);
        if (temp_resource != nullptr) {
            ResScope rs(temp_resource, gpu_id_, true);
            auto device_index = faiss::gpu::index_cpu_to_gpu(temp_resource->faiss_res.get(), gpu_id_, build_index);
            device_index->train(rows, (float *) p_data);

            std::shared_ptr<faiss::Index> host_index = nullptr;
            host_index.reset(faiss::gpu::index_gpu_to_cpu(device_index));

            delete device_index;
            delete build_index;

            return std::make_shared<IVFIndexModel>(host_index);
        } else {
            KNOWHERE_THROW_MSG("Build IVFSQ can't get gpu resource");
        }
    }

    VectorIndexPtr GPUIVFSQ::CopyGpuToCpu(const Config &config) {
        std::lock_guard<std::mutex> lk(mutex_);

        faiss::Index *device_index = index_.get();
        faiss::Index *host_index = faiss::gpu::index_gpu_to_cpu(device_index);

        std::shared_ptr<faiss::Index> new_index;
        new_index.reset(host_index);
        return std::make_shared<IVFSQ>(new_index);
    }

} // knowhere
} // zilliz

