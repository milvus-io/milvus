// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <atomic>
#include <utility>

#include "index/contracts/query/IIndexReaderBase.h"
#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/IScalarPredicateReader.h"
#include "index/contracts/query/IScalarValueReader.h"

namespace milvus {

// Observe consumer value reads while retaining the real backend's predicates,
// null semantics, capability metadata, and resource ownership.
template <typename T>
class CountingScalarReader final : public index::IIndexReaderBase,
                                   public index::IScalarPredicateReader<T>,
                                   public index::IScalarValueReader<T>,
                                   public index::INullReader {
 public:
    CountingScalarReader(index::IIndexReaderBasePtr reader,
                         std::atomic<int64_t>& lookup_calls)
        : reader_(std::move(reader)),
          predicates_(dynamic_cast<const index::IScalarPredicateReader<T>&>(
              *reader_)),
          values_(dynamic_cast<const index::IScalarValueReader<T>&>(*reader_)),
          nulls_(dynamic_cast<const index::INullReader&>(*reader_)),
          lookup_calls_(lookup_calls) {
    }

    index::ReaderCaps
    Caps() const override {
        return reader_->Caps();
    }

    index::Domain
    CoordDomain() const override {
        return reader_->CoordDomain();
    }

    int64_t
    Count() const override {
        return reader_->Count();
    }

    DataType
    ValueType() const override {
        return reader_->ValueType();
    }

    int64_t
    MemoryUsage() const override {
        return reader_->MemoryUsage();
    }

    cachinglayer::ResourceUsage
    CellByteSize() const override {
        return reader_->CellByteSize();
    }

    TargetBitmap
    In(size_t count, const T* values) const override {
        return predicates_.In(count, values);
    }

    TargetBitmap
    NotIn(size_t count, const T* values) const override {
        return predicates_.NotIn(count, values);
    }

    TargetBitmap
    Range(const T& value, index::CompareOp op) const override {
        return predicates_.Range(value, op);
    }

    TargetBitmap
    Range(const T& lo, bool lo_inc, const T& hi, bool hi_inc) const override {
        return predicates_.Range(lo, lo_inc, hi, hi_inc);
    }

    TargetBitmap
    IsNull() const override {
        return nulls_.IsNull();
    }

    TargetBitmap
    IsNotNull() const override {
        return nulls_.IsNotNull();
    }

    std::optional<index::owned_t<T>>
    Lookup(int64_t offset) const override {
        ++lookup_calls_;
        return values_.Lookup(offset);
    }

    void
    Gather(const int64_t* offsets,
           int64_t count,
           const std::function<void(int64_t, const T*, bool)>& out)
        const override {
        lookup_calls_ += count;
        values_.Gather(offsets, count, out);
    }

 private:
    index::IIndexReaderBasePtr reader_;
    const index::IScalarPredicateReader<T>& predicates_;
    const index::IScalarValueReader<T>& values_;
    const index::INullReader& nulls_;
    std::atomic<int64_t>& lookup_calls_;
};

}  // namespace milvus
