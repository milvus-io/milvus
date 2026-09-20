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

#include "segcore/indexing/IndexInventory.h"

#include <utility>

#include "cachinglayer/Utils.h"
#include "common/EasyAssert.h"

namespace milvus::segcore {

IndexInventory::RootSlot
IndexInventory::Register(Entry entry) {
    AssertInfo(entry.slot != nullptr,
               "cannot register a null index cache slot for field {}",
               entry.meta.key.field_id.get());
    auto key = entry.meta.key;
    const auto field_id = key.field_id;
    auto it = entries_.find(key);
    if (it == entries_.end()) {
        entries_.emplace(std::move(key), std::move(entry));
        RefreshCapability(field_id);
        return nullptr;
    }
    auto retired = std::move(it->second.slot);
    it->second = std::move(entry);
    RefreshCapability(field_id);
    return retired;
}

IndexInventory::RootSlot
IndexInventory::Drop(const IndexKey& key) {
    auto it = entries_.find(key);
    if (it == entries_.end()) {
        return nullptr;
    }
    auto retired = std::move(it->second.slot);
    const auto field_id = key.field_id;
    entries_.erase(it);
    RefreshCapability(field_id);
    return retired;
}

FieldIndexCapability
IndexInventory::Capability(FieldId field_id) const {
    const auto it = capabilities_.find(field_id);
    return it == capabilities_.end() ? FieldIndexCapability(field_id)
                                     : it->second;
}

void
IndexInventory::RefreshCapability(FieldId field_id) {
    std::vector<IndexCapabilityEntry> matches;
    matches.reserve(entries_.size());
    for (const auto& [key, entry] : entries_) {
        if (key.field_id == field_id) {
            matches.push_back(entry.meta);
        }
    }
    if (matches.empty()) {
        capabilities_.erase(field_id);
        return;
    }
    capabilities_.insert_or_assign(
        field_id, FieldIndexCapability(field_id, std::move(matches)));
}

std::vector<IndexCapabilityEntry>
IndexInventory::Entries() const {
    std::vector<IndexCapabilityEntry> result;
    result.reserve(entries_.size());
    for (const auto& [_, entry] : entries_) {
        result.push_back(entry.meta);
    }
    return result;
}

std::vector<IndexInventory::RootSlot>
IndexInventory::Slots() const {
    std::vector<RootSlot> result;
    result.reserve(entries_.size());
    for (const auto& [_, entry] : entries_) {
        result.push_back(entry.slot);
    }
    return result;
}

IndexPin
IndexInventory::PinIndex(milvus::OpContext* op_ctx,
                         const IndexKey& key) const {
    const auto it = entries_.find(key);
    if (it == entries_.end()) {
        return {};
    }

    const auto& entry = it->second;
    auto accessor =
        milvus::cachinglayer::SemiInlineGet(entry.slot->PinCells(op_ctx, {0}));
    auto* reader = accessor->get_cell_of(0);
    AssertInfo(reader != nullptr,
               "index cache slot returned a null reader for field {}",
               key.field_id.get());
    AssertInfo(SameCaps(entry.meta.caps, reader->Caps()),
               "index capability metadata disagrees with the opened reader "
               "for field {}",
               key.field_id.get());
    return IndexPin(std::move(accessor), reader);
}

}  // namespace milvus::segcore
