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

namespace milvus {
namespace knowhere {
namespace impl {

struct Distance {
    virtual ~Distance() = default;
    virtual float
    Compare(const float* a, const float* b, unsigned size) const = 0;
};

struct DistanceL2 : public Distance {
    float
    Compare(const float* a, const float* b, unsigned size) const override;
};

struct DistanceIP : public Distance {
    float
    Compare(const float* a, const float* b, unsigned size) const override;
};

}  // namespace impl
}  // namespace knowhere
}  // namespace milvus
