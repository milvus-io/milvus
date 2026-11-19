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

#include "common/Vector.h"
#include "mmap/ChunkedColumnScanCommon.h"

namespace milvus {

class ChunkedColumnInterface;
struct OpContext;

// Returns nullptr only for unsupported columns/types. Storage errors propagate.
// Offsets are immutable and may be shared by all columns in a projected batch.
// Reads must complete within the supplied OpContext's lifetime.
VectorPtr
TryMakeDictionaryVector(OpContext* op_ctx,
                        DataType type,
                        std::shared_ptr<ChunkedColumnInterface> column,
                        std::shared_ptr<const std::vector<int64_t>> offsets);

// A read-only scalar selection over a storage column. TakeResult already maps
// logical rows to source cells, so there is no second indices indirection here.
// Raw results borrow pinned payload; decoding backends retain their owned result.
template <typename T>
class DictionaryVector final : public SimpleVector<T> {
 public:
    using typename SimpleVector<T>::TValueAt;
    static_assert(std::is_arithmetic_v<T> || std::is_same_v<T, std::string>);

    TValueAt
    ValueAt(size_t index) const override {
        AssertInfo(index < this->length_, "ValueAt index out of range");
        // SimpleVector's contract requires a valid row. This factory never
        // supplies a data filter or candidate mask, so values cannot be skipped.
        return *values_[index].value;
    }

    bool
    ValidAt(size_t index) const override {
        return take_->IsValid(index);
    }

    void
    resize(vector_size_t size, bool = true) override {
        AssertInfo(size == this->length_,
                   "Cannot resize an immutable dictionary selection");
    }

    VectorPtr
    cloneEmpty(vector_size_t) const override {
        ThrowInfo(Unsupported,
                  "DictionaryVector cannot allocate output storage");
    }

 private:
    friend VectorPtr
    TryMakeDictionaryVector(OpContext*,
                            DataType,
                            std::shared_ptr<ChunkedColumnInterface>,
                            std::shared_ptr<const std::vector<int64_t>>);

    DictionaryVector(DataType type,
                     std::shared_ptr<ChunkedColumnInterface> column,
                     std::shared_ptr<const std::vector<int64_t>> offsets,
                     TakeResultPtr take)
        : SimpleVector<T>(type, offsets->size()),
          column_(std::move(column)),
          offsets_(std::move(offsets)),
          take_(std::move(take)),
          values_(take_->template Access<TValueAt>()) {
        AssertInfo(values_.Size() == this->length_,
                   "Dictionary result size must match its selection");
    }

    // Declaration order keeps the source and borrowed offsets alive until the
    // TakeResult (including its Batch pins) has been destroyed.
    std::shared_ptr<ChunkedColumnInterface> column_;
    std::shared_ptr<const std::vector<int64_t>> offsets_;
    TakeResultPtr take_;
    TakeResult::TypedAccessor<TValueAt> values_;
};

}  // namespace milvus
