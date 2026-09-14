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

#include <cstdint>
#include <functional>
#include <memory>

namespace milvus::detail {

// Transitional binding between expression-specific SkipIndex predicates and
// Column-internal physical planning. Column callers only pass this opaque
// object through Scan/Take options; Cell ids never leave the backend read
// implementation. A future expression-valued filter can replace the bound
// callback without changing Scan/Take result contracts.
class ColumnFilter final {
 public:
    enum class MetricsSource {
        PreloadedStatistics,
        LoadedPayload,
    };

    using PhysicalCellPredicate = std::function<bool(int64_t)>;

    ColumnFilter(MetricsSource source, PhysicalCellPredicate predicate)
        : source_(source), predicate_(std::move(predicate)) {
    }

    MetricsSource
    Source() const {
        return source_;
    }

    bool
    CanSkipPhysicalCell(int64_t cell_id) const {
        return predicate_ && predicate_(cell_id);
    }

 private:
    MetricsSource source_;
    PhysicalCellPredicate predicate_;
};

using ColumnFilterPtr = std::shared_ptr<const ColumnFilter>;

}  // namespace milvus::detail
