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


#include <memory>

#include "knowhere/common/config.h"
#include "knowhere/common/dataset.h"
#include "knowhere/index/index.h"
#include "knowhere/index/preprocessor/preprocessor.h"


namespace zilliz {
namespace knowhere {

class VectorIndex;
using VectorIndexPtr = std::shared_ptr<VectorIndex>;


class VectorIndex : public Index {
 public:
    virtual PreprocessorPtr
    BuildPreprocessor(const DatasetPtr &dataset, const Config &config) { return nullptr; }

    virtual IndexModelPtr
    Train(const DatasetPtr &dataset, const Config &config) { return nullptr; }

    virtual void
    Add(const DatasetPtr &dataset, const Config &config) = 0;

    virtual void
    Seal() = 0;

    virtual VectorIndexPtr
    Clone() = 0;

    virtual int64_t
    Count() = 0;

    virtual int64_t
    Dimension() = 0;
};



} // namespace knowhere
} // namespace zilliz
