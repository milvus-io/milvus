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

#include <map>
#include <memory>
#include <mutex>
#include <utility>

#include <faiss/gpu/StandardGpuResources.h>

#include "utils/BlockingQueue.h"

namespace milvus {
namespace knowhere {

struct Resource {
    explicit Resource(std::shared_ptr<faiss::gpu::StandardGpuResources>& r) : faiss_res(r) {
        static int64_t global_id = 0;
        id = global_id++;
    }

    std::shared_ptr<faiss::gpu::StandardGpuResources> faiss_res;
    int64_t id;
    std::mutex mutex;
};
using ResPtr = std::shared_ptr<Resource>;
using ResWPtr = std::weak_ptr<Resource>;

class FaissGpuResourceMgr {
 public:
    friend class ResScope;
    using ResBQ = BlockingQueue<ResPtr>;

 public:
    struct DeviceParams {
        int64_t temp_mem_size = 0;
        int64_t pinned_mem_size = 0;
        int64_t resource_num = 2;
    };

 public:
    static FaissGpuResourceMgr&
    GetInstance();

    // Free gpu resource, avoid cudaGetDevice error when deallocate.
    // this func should be invoke before main return
    void
    Free();

    void
    AllocateTempMem(ResPtr& resource, const int64_t device_id, const int64_t size);

    void
    InitDevice(int64_t device_id, int64_t pin_mem_size = 0, int64_t temp_mem_size = 0, int64_t res_num = 2);

    void
    InitResource();

    // allocate gpu memory invoke by build or copy_to_gpu
    ResPtr
    GetRes(const int64_t device_id, const int64_t alloc_size = 0);

    void
    MoveToIdle(const int64_t device_id, const ResPtr& res);

    void
    Dump();

 protected:
    bool initialized_ = false;
    std::mutex init_mutex_;

    std::map<int64_t, std::unique_ptr<std::mutex>> mutex_cache_;
    std::map<int64_t, DeviceParams> devices_params_;
    std::map<int64_t, ResBQ> idle_map_;
};

class ResScope {
 public:
    ResScope(ResPtr& res, const int64_t device_id, const bool isown)
        : resource(res), device_id(device_id), move(true), own(isown) {
        Lock();
    }

    ResScope(ResWPtr& res, const int64_t device_id, const bool isown)
        : resource(res), device_id(device_id), move(true), own(isown) {
        Lock();
    }

    // specif for search
    // get the ownership of gpuresource and gpu
    ResScope(ResWPtr& res, const int64_t device_id) : device_id(device_id), move(false), own(true) {
        resource = res.lock();
        Lock();
    }

    void
    Lock() {
        if (own)
            FaissGpuResourceMgr::GetInstance().mutex_cache_[device_id]->lock();
        resource->mutex.lock();
    }

    ~ResScope() {
        if (own)
            FaissGpuResourceMgr::GetInstance().mutex_cache_[device_id]->unlock();
        if (move)
            FaissGpuResourceMgr::GetInstance().MoveToIdle(device_id, resource);
        resource->mutex.unlock();
    }

 private:
    ResPtr resource;  // hold resource until deconstruct
    int64_t device_id;
    bool move = true;
    bool own = false;
};

}  // namespace knowhere
}  // namespace milvus
