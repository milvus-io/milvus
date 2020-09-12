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

#include <memory>
#include "knowhere/common/Config.h"

namespace milvus {
namespace knowhere {

struct Quantizer {
    virtual ~Quantizer() = default;

    int64_t size = -1;
};
using QuantizerPtr = std::shared_ptr<Quantizer>;

// struct QuantizerCfg : Cfg {
//     int64_t mode = -1;  // 0: all data, 1: copy quantizer, 2: copy data
// };
// using QuantizerConfig = std::shared_ptr<QuantizerCfg>;

}  // namespace knowhere
}  // namespace milvus
