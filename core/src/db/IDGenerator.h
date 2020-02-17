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

#include "Types.h"

#include <cstddef>
#include <vector>

namespace milvus {
namespace engine {

class IDGenerator {
 public:
    virtual IDNumber
    GetNextIDNumber() = 0;

    virtual void
    GetNextIDNumbers(size_t n, IDNumbers& ids) = 0;

    virtual ~IDGenerator() = 0;
};  // IDGenerator

class SimpleIDGenerator : public IDGenerator {
 public:
    ~SimpleIDGenerator() override = default;

    IDNumber
    GetNextIDNumber() override;

    void
    GetNextIDNumbers(size_t n, IDNumbers& ids) override;

 private:
    void
    NextIDNumbers(size_t n, IDNumbers& ids);

    static constexpr size_t MAX_IDS_PER_MICRO = 1000;
};  // SimpleIDGenerator

}  // namespace engine
}  // namespace milvus
