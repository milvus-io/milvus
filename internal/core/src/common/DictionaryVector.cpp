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

#include "common/DictionaryVector.h"
#include "mmap/ChunkedColumnInterface.h"

namespace milvus {

VectorPtr
TryMakeDictionaryVector(OpContext* op_ctx,
                        DataType type,
                        std::shared_ptr<ChunkedColumnInterface> column,
                        std::shared_ptr<const std::vector<int64_t>> offsets) {
    if (!column) {
        return nullptr;
    }
    // TEXT/GEOMETRY/JSON and nested values retain their existing extraction
    // paths. In particular, raw TEXT may require large-object resolution.
    switch (type) {
        case DataType::BOOL:
        case DataType::INT8:
        case DataType::INT16:
        case DataType::INT32:
        case DataType::INT64:
        case DataType::TIMESTAMPTZ:
        case DataType::FLOAT:
        case DataType::DOUBLE:
        case DataType::STRING:
        case DataType::VARCHAR:
            break;
        default:
            return nullptr;
    }
    AssertInfo(offsets != nullptr, "Dictionary selection cannot be null");
    return DispatchVectorType(type, [&]<typename T>() -> VectorPtr {
        if constexpr (std::is_arithmetic_v<T> ||
                      std::is_same_v<T, std::string>) {
            TakeOptions options;
            options.offsets =
                OffsetView::From(offsets->data(), offsets->size());
            options.target_type = TargetTypeOf<VectorValue<T>>();
            // CurrentCell would invalidate previously returned string views
            // when reads cross cells. Batch pins survive all logical reads.
            options.pin_policy = TakePinPolicy::Batch;
            auto take = column->Take(op_ctx, options);
            if (!take) {
                return nullptr;
            }
            return VectorPtr(new DictionaryVector<T>(
                type, std::move(column), std::move(offsets), std::move(take)));
        } else {
            return nullptr;
        }
    });
}

}  // namespace milvus
