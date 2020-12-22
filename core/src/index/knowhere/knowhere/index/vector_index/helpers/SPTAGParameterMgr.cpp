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

#include "knowhere/index/vector_index/helpers/SPTAGParameterMgr.h"

namespace milvus {
namespace knowhere {

const Config&
SPTAGParameterMgr::GetKDTParameters() {
    return kdt_config_;
}

const Config&
SPTAGParameterMgr::GetBKTParameters() {
    return bkt_config_;
}

SPTAGParameterMgr::SPTAGParameterMgr() {
    kdt_config_["kdtnumber"] = 1;
    kdt_config_["numtopdimensionkdtsplit"] = 5;
    kdt_config_["samples"] = 100;
    kdt_config_["tptnumber"] = 1;
    kdt_config_["tptleafsize"] = 2000;
    kdt_config_["numtopdimensiontptsplit"] = 5;
    kdt_config_["neighborhoodsize"] = 32;
    kdt_config_["graphneighborhoodscale"] = 2;
    kdt_config_["graphcefscale"] = 2;
    kdt_config_["refineiterations"] = 0;
    kdt_config_["cef"] = 1000;
    kdt_config_["maxcheckforrefinegraph"] = 10000;
    kdt_config_["numofthreads"] = 1;
    kdt_config_["maxcheck"] = 8192;
    kdt_config_["thresholdofnumberofcontinuousnobetterpropagation"] = 3;
    kdt_config_["numberofinitialdynamicpivots"] = 50;
    kdt_config_["numberofotherdynamicpivots"] = 4;

    bkt_config_["bktnumber"] = 1;
    bkt_config_["bktkmeansk"] = 32;
    bkt_config_["bktleafsize"] = 8;
    bkt_config_["samples"] = 100;
    bkt_config_["tptnumber"] = 1;
    bkt_config_["tptleafsize"] = 2000;
    bkt_config_["numtopdimensiontptsplit"] = 5;
    bkt_config_["neighborhoodsize"] = 32;
    bkt_config_["graphneighborhoodscale"] = 2;
    bkt_config_["graphcefscale"] = 2;
    bkt_config_["refineiterations"] = 0;
    bkt_config_["cef"] = 1000;
    bkt_config_["maxcheckforrefinegraph"] = 10000;
    bkt_config_["numofthreads"] = 1;
    bkt_config_["maxcheck"] = 8192;
    bkt_config_["thresholdofnumberofcontinuousnobetterpropagation"] = 3;
    bkt_config_["numberofinitialdynamicpivots"] = 50;
    bkt_config_["numberofotherdynamicpivots"] = 4;
}

}  // namespace knowhere
}  // namespace milvus
