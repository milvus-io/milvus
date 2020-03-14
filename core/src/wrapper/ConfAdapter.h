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

#include "VecIndex.h"
#include "utils/Json.h"

namespace milvus {
namespace engine {

class ConfAdapter {
 public:
    virtual bool
    CheckTrain(milvus::json& oricfg);

    virtual bool
    CheckSearch(milvus::json& oricfg, const IndexType& type);

    //  todo(linxj): refactor in next release.
    //
    //  virtual bool
    //  CheckTrain(milvus::json&, IndexMode&) = 0;
    //
    //  virtual bool
    //  CheckSearch(milvus::json&, const IndexType&, IndexMode&) = 0;
};
using ConfAdapterPtr = std::shared_ptr<ConfAdapter>;

class IVFConfAdapter : public ConfAdapter {
 public:
    bool
    CheckTrain(milvus::json& oricfg) override;

    bool
    CheckSearch(milvus::json& oricfg, const IndexType& type) override;
};

class IVFSQConfAdapter : public IVFConfAdapter {
 public:
    bool
    CheckTrain(milvus::json& oricfg) override;
};

class IVFPQConfAdapter : public IVFConfAdapter {
 public:
    bool
    CheckTrain(milvus::json& oricfg) override;
};

class NSGConfAdapter : public IVFConfAdapter {
 public:
    bool
    CheckTrain(milvus::json& oricfg) override;

    bool
    CheckSearch(milvus::json& oricfg, const IndexType& type) override;
};

class BinIDMAPConfAdapter : public ConfAdapter {
 public:
    bool
    CheckTrain(milvus::json& oricfg) override;
};

class BinIVFConfAdapter : public IVFConfAdapter {
 public:
    bool
    CheckTrain(milvus::json& oricfg) override;
};

class HNSWConfAdapter : public ConfAdapter {
 public:
    bool
    CheckTrain(milvus::json& oricfg) override;

    bool
    CheckSearch(milvus::json& oricfg, const IndexType& type) override;
};

}  // namespace engine
}  // namespace milvus
