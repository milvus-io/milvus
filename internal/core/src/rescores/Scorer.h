// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "expr/ITypeExpr.h"

namespace milvus::rescores {
class Scorer {
 public:
    virtual expr::TypedExprPtr
    filter() = 0;

    virtual float
    rescore(float old_score) = 0;

    virtual float
    weight() = 0;
};

class WeightScorer : public Scorer {
 public:
    WeightScorer(expr::TypedExprPtr filter, float weight)
        : filter_(std::move(filter)), weight_(weight){};

    expr::TypedExprPtr
    filter() override {
        return filter_;
    }

    float
    rescore(float old_score) override {
        return old_score * weight_;
    }

    float
    weight() override{
        return weight_;
    }

 private:
    expr::TypedExprPtr filter_;
    float weight_;
};
}  // namespace milvus::rescores