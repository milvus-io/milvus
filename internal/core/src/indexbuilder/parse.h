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

#include <string>
#include <knowhere/common/Config.h>
#include "pb/index_cgo_msg.pb.h"

namespace milvus::indexbuilder {
namespace indexcgo = milvus::proto::indexcgo;

template <typename T>
void
ParseConfig(knowhere::Config& conf,
            const std::string& key,
            std::function<T(std::string)> fn,
            std::optional<T> default_v) {
    if (!conf.contains(key)) {
        if (default_v.has_value()) {
            conf[key] = default_v.value();
        }
    } else {
        auto value = conf[key];
        conf[key] = fn(value);
    }
}

void
AddToConfig(knowhere::Config& conf, const indexcgo::IndexParams& params) {
    for (auto i = 0; i < params.params_size(); i++) {
        auto key = params.params(i).key();
        auto value = params.params(i).value();
        conf[key] = value;
    }
}

void
AddToConfig(knowhere::Config& conf, const indexcgo::TypeParams& params) {
    for (auto i = 0; i < params.params_size(); i++) {
        auto key = params.params(i).key();
        auto value = params.params(i).value();
        conf[key] = value;
    }
}
}  // namespace milvus::indexbuilder
