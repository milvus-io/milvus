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

#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include "common/JsonCastType.h"
#include "common/Types.h"
#include "index/contracts/query/IIndexReaderBase.h"

// Path-addressed JSON predicate interfaces. This mixin routes a path and cast
// type to a scalar query interface; it does not add new predicate semantics.
// JSON shredding is a column layout managed separately by segcore/json_stats.

namespace milvus::index {

// A separate resolution handle is required because implementations produce two
// different lifetimes behind the same path-routing contract. A flat JSON index
// creates a temporary path-bound reader, which this handle must own. A typed
// path index already owns its child reader, so this handle only borrows it.
// Neither form extends the lifetime of the field-level reader, parent engine,
// or cache cell; the caller must keep the parent cache pin alive while using
// the result.
class JsonResolvedReader {
 public:
    JsonResolvedReader() = default;

    JsonResolvedReader(const JsonResolvedReader&) = delete;
    JsonResolvedReader&
    operator=(const JsonResolvedReader&) = delete;

    JsonResolvedReader(JsonResolvedReader&& other) noexcept
        : owned_(std::move(other.owned_)), reader_(other.reader_) {
        if (owned_ != nullptr) {
            reader_ = owned_.get();
        }
        other.reader_ = nullptr;
    }

    JsonResolvedReader&
    operator=(JsonResolvedReader&& other) noexcept {
        if (this != &other) {
            owned_ = std::move(other.owned_);
            reader_ = owned_ != nullptr ? owned_.get() : other.reader_;
            other.reader_ = nullptr;
        }
        return *this;
    }

    static JsonResolvedReader
    Owned(std::unique_ptr<const IIndexReaderBase> reader) noexcept {
        JsonResolvedReader result;
        result.owned_ = std::move(reader);
        result.reader_ = result.owned_.get();
        return result;
    }

    static JsonResolvedReader
    Borrowed(const IIndexReaderBase* reader) noexcept {
        JsonResolvedReader result;
        result.reader_ = reader;
        return result;
    }

    const IIndexReaderBase*
    get() const noexcept {
        return reader_;
    }

    const IIndexReaderBase*
    operator->() const noexcept {
        return reader_;
    }

    explicit operator bool() const noexcept {
        return reader_ != nullptr;
    }

 private:
    std::unique_ptr<const IIndexReaderBase> owned_;
    const IIndexReaderBase* reader_{nullptr};
};

class IJsonIndexReader {
 public:
    virtual ~IJsonIndexReader() = default;

    // Resolve a supported path/cast pair to a predicate reader. The result may own
    // a temporary interface view or borrow a child; in either case keep the parent
    // reader pinned while using it. Cast to the query interface once after resolve.
    // An empty result means this index cannot serve the path/cast pair; choosing a
    // raw or shredded-column fallback belongs to the consumer.
    virtual JsonResolvedReader
    Resolve(std::string_view path, JsonCastType cast_type) const = 0;

    // Path existence within the shapes this index supports, independent of
    // whether a value is comparable under any particular cast. Call only when
    // CastTypesOf(path) is nonempty. A supported path absent from all rows
    // returns an all-zero bitmap; that is distinct from an unsupported path
    // shape. Typed comparability is exposed by INullReader on the resolved
    // reader.
    virtual TargetBitmap
    Exists(std::string_view path) const = 0;

    // Supported cast vocabulary for this path shape. Empty means unsupported,
    // not that the path happens to be absent from the indexed rows. Check this
    // before invoking Exists().
    virtual std::vector<JsonCastType>
    CastTypesOf(std::string_view path) const = 0;
};

// A per-path cast index uses an ordinary IScalarPredicateReader<T>, registered
// under a field/path key. JSON value extraction belongs before the builder;
// the resulting scalar reader needs no JSON-specific query interface.

}  // namespace milvus::index
