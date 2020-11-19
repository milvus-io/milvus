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

#pragma once

#include <memory>
#include <string>

#include "cache/CacheMgr.h"
#include "cache/DataObj.h"
#include "metrics/Prometheus.h"
#include "value/config/ConfigMgr.h"

namespace milvus {
namespace cache {

class CpuCacheMgr : public CacheMgr<DataObjPtr>, public ConfigObserver {
 public:
    static CpuCacheMgr&
    GetInstance();

 private:
    CpuCacheMgr();

    ~CpuCacheMgr();

 public:
    DataObjPtr
    GetItem(const std::string& key) override;

 public:
    void
    ConfigUpdate(const std::string& name) override;

 private:
    /* metrics */
    template <typename T>
    using Family = prometheus::Family<T>;
    using Counter = prometheus::Counter;

    /* cache_gets_total */
    Family<Counter>& cache_gets_total_family_ =
        prometheus::BuildCounter().Name("cache_gets_total").Help("cache_gets_total").Register(prometheus.registry());
    Counter& cache_gets_total_counter_ = cache_gets_total_family_.Add({});

    /* cache_hits_total */
    Family<Counter>& cache_hits_total_family_ =
        prometheus::BuildCounter().Name("cache_hits_total").Help("cache_hits_total").Register(prometheus.registry());
    Counter& cache_hits_total_counter_ = cache_hits_total_family_.Add({});
};

}  // namespace cache
}  // namespace milvus
