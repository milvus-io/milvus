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

#include "config/Config.h"
#include "config/handler/ConfigHandler.h"

namespace milvus {
namespace server {

class EngineConfigHandler : virtual public ConfigHandler {
 public:
    EngineConfigHandler();

    virtual ~EngineConfigHandler();

 protected:
    virtual void
    OnUseBlasThresholdChanged(int64_t threshold) {
    }

    virtual void
    OnSearchCombineMaxNqChanged(int64_t nq) {
        search_combine_nq_ = nq;
    }

 protected:
    void
    AddUseBlasThresholdListener();

    void
    RemoveUseBlasThresholdListener();

    void
    AddSearchCombineMaxNqListener();

    void
    RemoveSearchCombineMaxNqListener();

 protected:
    int64_t use_blas_threshold_ = std::stoll(CONFIG_ENGINE_USE_BLAS_THRESHOLD_DEFAULT);
    int64_t search_combine_nq_ = std::stoll(CONFIG_ENGINE_SEARCH_COMBINE_MAX_NQ_DEFAULT);
};

}  // namespace server
}  // namespace milvus
