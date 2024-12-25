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
#include "common/Types.h"

namespace milvus {
namespace expr {
class FunctionSignature {
 protected:
    FunctionSignature(DataType returnType,
                      std::vector<DataType>&& argumentTypes,
                      bool variableArity)
        : returnType_(returnType),
          argumentTypes_(std::move(argumentTypes)),
          variableArity_(variableArity) {
    }

 private:
    const DataType returnType_;
    const std::vector<DataType> argumentTypes_;
    const bool variableArity_;
};

using FunctionSignaturePtr = std::shared_ptr<FunctionSignature>;

class AggregateFunctionSignature : public FunctionSignature {
 public:
    AggregateFunctionSignature(DataType returnType,
                               std::vector<DataType>&& argumentTypes,
                               DataType intermediateType,
                               bool variableArity)
        : FunctionSignature(
              returnType, std::move(argumentTypes), variableArity),
          intermediateType_(intermediateType) {
    }

    const DataType&
    intermediateType() const {
        return intermediateType_;
    }

 private:
    DataType intermediateType_;
};

using AggregateFunctionSignaturePtr =
    std::shared_ptr<AggregateFunctionSignature>;

class AggregateFunctionSignatureBuilder {
 public:
    AggregateFunctionSignatureBuilder&
    returnType(DataType returnType) {
        returnType_.emplace(returnType);
        return *this;
    }

    AggregateFunctionSignatureBuilder&
    intermediateType(DataType interType) {
        intermediateType_.emplace(interType);
        return *this;
    }

    AggregateFunctionSignatureBuilder&
    argumentType(DataType argType) {
        argumentTypes_.emplace_back(argType);
        return *this;
    }

    AggregateFunctionSignatureBuilder&
    variableArity() {
        variableArity_ = true;
        return *this;
    }

    inline std::shared_ptr<AggregateFunctionSignature>
    build() {
        AssertInfo(returnType_.has_value(),
                   "Must specify returnType for aggregation");
        AssertInfo(intermediateType_.has_value(),
                   "Must specify intermediateType for aggregation");
        return std::make_shared<AggregateFunctionSignature>(
            returnType_.value(),
            std::move(argumentTypes_),
            intermediateType_.value(),
            variableArity_);
    }

 private:
    std::optional<DataType> returnType_;
    std::optional<DataType> intermediateType_;
    std::vector<DataType> argumentTypes_;
    bool variableArity_{false};
};
}  // namespace expr
}  // namespace milvus