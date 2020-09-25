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

#include "config/handler/EngineConfigHandler.h"

#include <string>

namespace milvus {
namespace server {

EngineConfigHandler::EngineConfigHandler() {
    auto& config = Config::GetInstance();
    config.GetEngineConfigUseBlasThreshold(use_blas_threshold_);
    config.GetEngineSearchCombineMaxNq(search_combine_nq_);
}

EngineConfigHandler::~EngineConfigHandler() {
    RemoveUseBlasThresholdListener();
    RemoveSearchCombineMaxNqListener();
}

//////////////////////////// Listener methods //////////////////////////////////
void
EngineConfigHandler::AddUseBlasThresholdListener() {
    ConfigCallBackF lambda = [this](const std::string& value) -> Status {
        auto& config = server::Config::GetInstance();
        auto status = config.GetEngineConfigUseBlasThreshold(use_blas_threshold_);
        if (status.ok()) {
            OnUseBlasThresholdChanged(use_blas_threshold_);
        }

        return status;
    };

    auto& config = Config::GetInstance();
    config.RegisterCallBack(CONFIG_ENGINE, CONFIG_ENGINE_USE_BLAS_THRESHOLD, identity_, lambda);
}

void
EngineConfigHandler::RemoveUseBlasThresholdListener() {
    auto& config = Config::GetInstance();
    config.CancelCallBack(CONFIG_ENGINE, CONFIG_ENGINE_USE_BLAS_THRESHOLD, identity_);
}

void
EngineConfigHandler::AddSearchCombineMaxNqListener() {
    ConfigCallBackF lambda = [this](const std::string& value) -> Status {
        auto& config = server::Config::GetInstance();
        auto status = config.GetEngineSearchCombineMaxNq(search_combine_nq_);
        if (status.ok()) {
            OnSearchCombineMaxNqChanged(search_combine_nq_);
        }

        return status;
    };

    auto& config = Config::GetInstance();
    config.RegisterCallBack(CONFIG_ENGINE, CONFIG_ENGINE_SEARCH_COMBINE_MAX_NQ, identity_, lambda);
}

void
EngineConfigHandler::RemoveSearchCombineMaxNqListener() {
    auto& config = Config::GetInstance();
    config.CancelCallBack(CONFIG_ENGINE, CONFIG_ENGINE_SEARCH_COMBINE_MAX_NQ, identity_);
}

}  // namespace server
}  // namespace milvus
