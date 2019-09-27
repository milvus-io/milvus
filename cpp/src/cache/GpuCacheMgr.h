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


#include "CacheMgr.h"
#include "DataObj.h"

#include <unordered_map>
#include <memory>
#include <string>

namespace zilliz {
namespace milvus {
namespace cache {

class GpuCacheMgr;
using GpuCacheMgrPtr = std::shared_ptr<GpuCacheMgr>;

class GpuCacheMgr : public CacheMgr<DataObjPtr> {
 public:
    GpuCacheMgr();

    static GpuCacheMgr *GetInstance(uint64_t gpu_id);

    engine::VecIndexPtr GetIndex(const std::string &key);

 private:
    static std::mutex mutex_;
    static std::unordered_map<uint64_t, GpuCacheMgrPtr> instance_;
};

} // namespace cache
} // namespace milvus
} // namespace zilliz
