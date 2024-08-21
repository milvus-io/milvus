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

#include "segcore/vector_index_c.h"

#include "knowhere/utils.h"
#include "index/Meta.h"
#include "index/IndexFactory.h"

int
GetIndexListSize() {
    return knowhere::IndexFactory::Instance().GetIndexFeatures().size();
}

void
GetIndexFeatures(void* index_key_list, uint64_t* index_feature_list) {
    auto features = knowhere::IndexFactory::Instance().GetIndexFeatures();
    int idx = 0;

    const char** index_keys = (const char**)index_key_list;
    uint64_t* index_features = (uint64_t*)index_feature_list;
    for (auto it = features.begin(); it != features.end(); ++it) {
        index_keys[idx] = it->first.c_str();
        index_features[idx] = it->second;
        idx++;
    }
}

