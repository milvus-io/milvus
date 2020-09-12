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
#include <utility>
#include <vector>

#include <SPTAG/AnnService/inc/Core/Common.h>
#include "IndexParameter.h"
#include "knowhere/common/Config.h"

namespace milvus {
namespace knowhere {

class SPTAGParameterMgr {
 public:
    const Config&
    GetKDTParameters();

    const Config&
    GetBKTParameters();

 public:
    static SPTAGParameterMgr&
    GetInstance() {
        static SPTAGParameterMgr instance;
        return instance;
    }

    SPTAGParameterMgr(const SPTAGParameterMgr&) = delete;

    SPTAGParameterMgr&
    operator=(const SPTAGParameterMgr&) = delete;

 private:
    SPTAGParameterMgr();

 private:
    Config kdt_config_;
    Config bkt_config_;
};

}  // namespace knowhere
}  // namespace milvus
