// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <string>
#include <utility>

#include "cachinglayer/Translator.h"
#include "common/Types.h"
#include "common/OpContext.h"
#include "index/contracts/Registry.h"

namespace milvus::segcore::storagev1translator {

// Each load owns its effective priority without mutating a shared caller context.
// Storage counters are returned even when loading throws.
class IndexLoadContext {
 public:
    IndexLoadContext(milvus::OpContext* parent, int32_t configured_priority)
        : parent_(parent) {
        ctx.runtime_load_priority = configured_priority;
        if (parent_ != nullptr) {
            ctx.cancellation_token = parent_->cancellation_token;
            ctx.runtime_load_priority =
                parent_->runtime_load_priority.value_or(configured_priority);
            ctx.coload_fields = parent_->coload_fields;
            ctx.pinned_segment_state = parent_->pinned_segment_state;
            ctx.pinned_state_owner = parent_->pinned_state_owner;
            ctx.trace_context = parent_->trace_context;
            ctx.trace_span = parent_->trace_span;
        }
    }

    ~IndexLoadContext() {
        if (parent_ != nullptr) {
            parent_->storage_usage.scanned_cold_bytes.fetch_add(
                ctx.storage_usage.scanned_cold_bytes.load());
            parent_->storage_usage.scanned_total_bytes.fetch_add(
                ctx.storage_usage.scanned_total_bytes.load());
        }
    }

    milvus::OpContext ctx;

 private:
    milvus::OpContext* parent_;
};

// Common cache translator contract for readers selected through the index
// family registry. Loading, accounting, and cache metadata remain translator
// specific.
class IndexReaderTranslator
    : public cachinglayer::Translator<index::IIndexReaderBase> {
 public:
    const index::IndexFamily&
    Family() const noexcept;

    DataType
    ValueType() const noexcept;

    const index::ReaderCaps&
    Caps() const noexcept;

 protected:
    void
    SetReaderContract(index::IndexFamily family,
                      DataType value_type,
                      index::ReaderCaps caps) {
        family_ = std::move(family);
        value_type_ = value_type;
        caps_ = std::move(caps);
    }

 private:
    index::IndexFamily family_;
    DataType value_type_{DataType::NONE};
    index::ReaderCaps caps_;
};

// Root under which index loaders stage local files, with or without mmap.
// QueryNode supplies localStorage.path; an empty root resolves to it.
std::string
LocalStagingRoot(const std::string& configured_root = {});

}  // namespace milvus::segcore::storagev1translator
