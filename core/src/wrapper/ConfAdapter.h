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

#pragma once

#include "VecIndex.h"
#include "knowhere/common/Config.h"

#include <memory>

namespace milvus {
namespace engine {

// TODO(linxj): remove later, replace with real metaconf
constexpr int64_t TEMPMETA_DEFAULT_VALUE = -1;
struct TempMetaConf {
    int64_t size = TEMPMETA_DEFAULT_VALUE;
    int64_t nlist = TEMPMETA_DEFAULT_VALUE;
    int64_t dim = TEMPMETA_DEFAULT_VALUE;
    int64_t gpu_id = TEMPMETA_DEFAULT_VALUE;
    int64_t k = TEMPMETA_DEFAULT_VALUE;
    int64_t nprobe = TEMPMETA_DEFAULT_VALUE;
    int64_t search_length = TEMPMETA_DEFAULT_VALUE;
    knowhere::METRICTYPE metric_type = knowhere::DEFAULT_TYPE;
};

class ConfAdapter {
 public:
    virtual knowhere::Config
    Match(const TempMetaConf& metaconf);

    virtual knowhere::Config
    MatchSearch(const TempMetaConf& metaconf, const IndexType& type);

 protected:
    static void
    MatchBase(knowhere::Config conf, knowhere::METRICTYPE defalut_metric = knowhere::METRICTYPE::L2);
};

using ConfAdapterPtr = std::shared_ptr<ConfAdapter>;

class IVFConfAdapter : public ConfAdapter {
 public:
    knowhere::Config
    Match(const TempMetaConf& metaconf) override;

    knowhere::Config
    MatchSearch(const TempMetaConf& metaconf, const IndexType& type) override;

 protected:
    static int64_t
    MatchNlist(const int64_t& size, const int64_t& nlist, const int64_t& per_nlist);
};

class IVFSQConfAdapter : public IVFConfAdapter {
 public:
    knowhere::Config
    Match(const TempMetaConf& metaconf) override;
};

class IVFPQConfAdapter : public IVFConfAdapter {
 public:
    knowhere::Config
    Match(const TempMetaConf& metaconf) override;

    knowhere::Config
    MatchSearch(const TempMetaConf& metaconf, const IndexType& type) override;

 protected:
    static int64_t
    MatchNlist(const int64_t& size, const int64_t& nlist);
};

class NSGConfAdapter : public IVFConfAdapter {
 public:
    knowhere::Config
    Match(const TempMetaConf& metaconf) override;

    knowhere::Config
    MatchSearch(const TempMetaConf& metaconf, const IndexType& type) final;
};

class SPTAGKDTConfAdapter : public ConfAdapter {
 public:
    knowhere::Config
    Match(const TempMetaConf& metaconf) override;

    knowhere::Config
    MatchSearch(const TempMetaConf& metaconf, const IndexType& type) override;
};

class SPTAGBKTConfAdapter : public ConfAdapter {
 public:
    knowhere::Config
    Match(const TempMetaConf& metaconf) override;

    knowhere::Config
    MatchSearch(const TempMetaConf& metaconf, const IndexType& type) override;
};

class BinIDMAPConfAdapter : public ConfAdapter {
 public:
    knowhere::Config
    Match(const TempMetaConf& metaconf) override;
};

class BinIVFConfAdapter : public IVFConfAdapter {
 public:
    knowhere::Config
    Match(const TempMetaConf& metaconf) override;
};

}  // namespace engine
}  // namespace milvus
