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

#include <algorithm>
#include <memory>
#include <vector>

#include "knowhere/common/Config.h"
#include "knowhere/index/IndexType.h"

namespace milvus {
namespace knowhere {

class ConfAdapter {
 public:
    virtual bool
    CheckTrain(Config& oricfg, const IndexMode mode);

    virtual bool
    CheckSearch(Config& oricfg, const IndexType type, const IndexMode mode);
};
using ConfAdapterPtr = std::shared_ptr<ConfAdapter>;

class IVFConfAdapter : public ConfAdapter {
 public:
    bool
    CheckTrain(Config& oricfg, const IndexMode mode) override;

    bool
    CheckSearch(Config& oricfg, const IndexType type, const IndexMode mode) override;
};

class IVFSQConfAdapter : public IVFConfAdapter {
 public:
    bool
    CheckTrain(Config& oricfg, const IndexMode mode) override;
};

class IVFPQConfAdapter : public IVFConfAdapter {
 public:
    bool
    CheckTrain(Config& oricfg, const IndexMode mode) override;

    static bool
    CheckPQParams(int64_t dimension, int64_t m, int64_t nbits, IndexMode& mode);

    static bool
    CheckGPUPQParams(int64_t dimension, int64_t m, int64_t nbits);

    static bool
    CheckCPUPQParams(int64_t dimension, int64_t m);
};

class IVFHNSWConfAdapter : public ConfAdapter {
 public:
    bool
    CheckTrain(Config& oricfg, const IndexMode mode) override;

    bool
    CheckSearch(Config& oricfg, const IndexType type, const IndexMode mode) override;
};

class NSGConfAdapter : public IVFConfAdapter {
 public:
    bool
    CheckTrain(Config& oricfg, const IndexMode mode) override;

    bool
    CheckSearch(Config& oricfg, const IndexType type, const IndexMode mode) override;
};

class BinIDMAPConfAdapter : public ConfAdapter {
 public:
    bool
    CheckTrain(Config& oricfg, const IndexMode mode) override;
};

class BinIVFConfAdapter : public IVFConfAdapter {
 public:
    bool
    CheckTrain(Config& oricfg, const IndexMode mode) override;
};

class HNSWConfAdapter : public ConfAdapter {
 public:
    bool
    CheckTrain(Config& oricfg, const IndexMode mode) override;

    bool
    CheckSearch(Config& oricfg, const IndexType type, const IndexMode mode) override;
};

class ANNOYConfAdapter : public ConfAdapter {
 public:
    bool
    CheckTrain(Config& oricfg, const IndexMode mode) override;

    bool
    CheckSearch(Config& oricfg, const IndexType type, const IndexMode mode) override;
};

class RHNSWFlatConfAdapter : public ConfAdapter {
 public:
    bool
    CheckTrain(Config& oricfg, const IndexMode mode) override;

    bool
    CheckSearch(Config& oricfg, const IndexType type, const IndexMode mode) override;
};

class RHNSWPQConfAdapter : public ConfAdapter {
 public:
    bool
    CheckTrain(Config& oricfg, const IndexMode mode) override;

    bool
    CheckSearch(Config& oricfg, const IndexType type, const IndexMode mode) override;
};

class RHNSWSQConfAdapter : public ConfAdapter {
 public:
    bool
    CheckTrain(Config& oricfg, const IndexMode mode) override;

    bool
    CheckSearch(Config& oricfg, const IndexType type, const IndexMode mode) override;
};

class NGTPANNGConfAdapter : public ConfAdapter {
 public:
    bool
    CheckTrain(Config& oricfg, const IndexMode mode) override;

    bool
    CheckSearch(Config& oricfg, const IndexType type, const IndexMode mode) override;
};

class NGTONNGConfAdapter : public ConfAdapter {
 public:
    bool
    CheckTrain(Config& oricfg, const IndexMode mode) override;

    bool
    CheckSearch(Config& oricfg, const IndexType type, const IndexMode mode) override;
};

}  // namespace knowhere
}  // namespace milvus
