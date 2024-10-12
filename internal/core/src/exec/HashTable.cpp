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

#include "HashTable.h"
#include <memory>
#include "common/SimdUtil.h"
#include "exec/operator/OperatorUtils.h"

namespace milvus {
namespace exec {

void
populateLookupRows(const TargetBitmapView& activeRows,
                   std::vector<vector_size_t>& lookupRows) {
    if (activeRows.all()) {
        std::iota(lookupRows.begin(), lookupRows.end(), 0);
    } else {
        auto start = -1;
        lookupRows.clear();
        lookupRows.reserve(activeRows.count());
        do {
            auto next_active = activeRows.find_next(start);
            if (!next_active.has_value())
                break;
            auto next_active_row = next_active.value();
            lookupRows.emplace_back(next_active_row);
            start = next_active_row;
        } while (true);
    }
}

void
BaseHashTable::prepareForGroupProbe(HashLookup& lookup,
                                    const RowVectorPtr& input,
                                    TargetBitmap& activeRows,
                                    bool ignoreNullKeys) {
    auto& hashers = lookup.hashers_;
    int numKeys = hashers.size();
    // set up column vector to each column
    for (auto i = 0; i < numKeys; i++) {
        auto& hasher = hashers[i];
        auto column_idx = hasher->ChannelIndex();
        ColumnVectorPtr column_ptr =
            std::dynamic_pointer_cast<ColumnVector>(input->child(column_idx));
        AssertInfo(column_ptr != nullptr,
                   "Failed to get column vector from row vector input");
        hashers[i]->setColumnData(column_ptr);
        // deselect null values
        if (ignoreNullKeys) {
            int64_t length = column_ptr->size();
            TargetBitmapView valid_bits_view(column_ptr->GetValidRawData(),
                                             length);
            activeRows &= valid_bits_view;
        }
    }
    lookup.reset(activeRows.size());

    const auto mode = hashMode();
    for (auto i = 0; i < hashers.size(); i++) {
        if (mode == BaseHashTable::HashMode::kHash) {
            TargetBitmapView tmp_views(activeRows);
            hashers[i]->hash(i > 0, tmp_views, lookup.hashes_);
        } else {
            PanicInfo(
                milvus::OpTypeInvalid,
                "Not support target hashMode, only support kHash for now");
        }
    }
    TargetBitmapView active_views(activeRows);
    populateLookupRows(active_views, lookup.rows_);
}

class ProbeState {
 public:
    enum class Operation { kProbe, kInsert, kErase };
    // Special tag for an erased entry. This counts as occupied for probe and as
    // empty for insert. If a tag word with empties gets an erase, we make the
    // erased tag empty. If the tag word getting the erase has no empties, the
    // erase is marked with a tombstone. A probe always stops with a tag word with
    // empties. Adding an empty to a tag word with no empties would break probes
    // that needed to skip this tag word. This is standard practice for open
    // addressing hash tables. F14 has more sophistication in this but we do not
    // need it here since erase is very rare except spilling and is not expected
    // to change the load factor by much in the expected uses.
    //static constexpr uint8_t kTombstoneTag = 0x7f;
    static constexpr uint8_t kEmptyTag = 0x00;
    static constexpr int32_t kFullMask = 0xffff;

    int32_t
    row() const {
        return row_;
    }

    template <typename Table>
    inline void
    preProbe(const Table& table, uint64_t hash, int32_t row) {
        row_ = row;
        bucketOffset_ = table.bucketOffset(hash);
        const auto tag = BaseHashTable::hashTag(hash);
        wantedTags_ = BaseHashTable::TagVector::broadcast(tag);
        group_ = nullptr;
        __builtin_prefetch(reinterpret_cast<uint8_t*>(table.table_) +
                           bucketOffset_);
    }

    template <Operation op = Operation::kInsert, typename Table>
    inline void
    firstProbe(const Table& table, int32_t firstKey) {
        tagsInTable_ = BaseHashTable::loadTags(
            reinterpret_cast<uint8_t*>(table.table_), bucketOffset_);
        hits_ = milvus::toBitMask(tagsInTable_ == wantedTags_);
        if (hits_) {
            loadNextHit<op>(table, firstKey);
        }
    }

    template <Operation op, typename Compare, typename Insert, typename Table>
    inline char*
    fullProbe(Table& table,
              int32_t firstKey,
              Compare compare,
              Insert insert,
              bool extraCheck) {
        AssertInfo(op == Operation::kInsert,
                   "Only support insert operation for group cases");
        if (group_ && compare(group_, row_)) {
            return group_;
        }

        if (extraCheck) {
            tagsInTable_ = table.loadTags(bucketOffset_);
            hits_ = milvus::toBitMask(tagsInTable_ == wantedTags_);
        }

        const auto kEmptyGroup = BaseHashTable::TagVector::broadcast(0);
        for (int64_t numProbedBuckets = 0;
             numProbedBuckets < table.numBuckets();
             ++numProbedBuckets) {
            while (hits_ > 0) {
                loadNextHit<op>(table, firstKey);
                if (compare(group_, row_)) {
                    return group_;
                }
            }

            uint16_t empty =
                milvus::toBitMask(tagsInTable_ == kEmptyGroup) & kFullMask;
            // if there are still empty slot available, try to insert into existing empty slot or tombstone slot
            if (empty > 0) {
                auto pos = milvus::bits::getAndClearLastSetBit(empty);
                return insert(row_, bucketOffset_ + pos);
            }
            bucketOffset_ = table.nextBucketOffset(bucketOffset_);
            tagsInTable_ = table.loadTags(bucketOffset_);
            hits_ = milvus::toBitMask(tagsInTable_ == wantedTags_);
        }
        PanicInfo(UnexpectedError,
                  "Slots in hash table is not enough for hash operation, fail "
                  "the request");
    }

 private:
    static constexpr uint8_t kNotSet = 0xff;
    template <Operation op, typename Table>
    inline void
    loadNextHit(Table& table, int32_t firstKey) {
        const int32_t hit = milvus::bits::getAndClearLastSetBit(hits_);
        group_ = table.row(bucketOffset_, hit);
        __builtin_prefetch(group_ + firstKey);
    }

    char* group_;
    BaseHashTable::TagVector wantedTags_;
    BaseHashTable::TagVector tagsInTable_;
    int32_t row_;
    int64_t bucketOffset_;
    BaseHashTable::MaskType hits_;
    //uint8_t indexInTags_ = kNotSet;
};

template <bool ignoreNullKeys>
void
HashTable<ignoreNullKeys>::allocateTables(uint64_t size) {
    AssertInfo(milvus::bits::isPowerOfTwo(size),
               "Size:{} for allocating tables must be a power of two",
               size);
    AssertInfo(size > 0,
               "Size:{} for allocating tables must be larger than zero",
               size);
    capacity_ = size;
    const uint64_t byteSize = capacity_ * tableSlotSize();
    AssertInfo(byteSize % kBucketSize == 0,
               "byteSize:{} for hashTable must be a multiple of kBucketSize:{}",
               byteSize,
               kBucketSize);
    numBuckets_ = byteSize / kBucketSize;
    sizeMask_ = byteSize - 1;
    sizeBits_ = __builtin_popcountll(sizeMask_);
    bucketOffsetMask_ = sizeMask_ & ~(kBucketSize - 1);
    // The total size is 8 bytes per slot, in groups of 16 slots with 16 bytes of
    // tags and 16 * 6 bytes of pointers and a padding of 16 bytes to round up the
    // cache line.
    // TODO support memory pool here to avoid OOM
    table_ = new char*[capacity_];
    memset(table_, 0, capacity_ * sizeof(char*));
}

template <bool ignoreNullKeys>
void
HashTable<ignoreNullKeys>::checkSize(int32_t numNew) {
    AssertInfo(capacity_ == 0 || capacity_ > numDistinct_,
               "capacity_ {}, numDistinct {}",
               capacity_,
               numDistinct_);
    if (table_ == nullptr || capacity_ == 0) {
        const auto newSize = newHashTableEntriesNumber(numDistinct_, numNew);
        allocateTables(newSize);
    }
}

template <bool ignoreNullKeys>
bool
HashTable<ignoreNullKeys>::compareKeys(const char* group,
                                       milvus::exec::HashLookup& lookup,
                                       milvus::vector_size_t row) {
    int32_t numKeys = lookup.hashers_.size();
    int32_t i = 0;
    do {
        auto& hasher = lookup.hashers_[i];
        if (!rows_->equals<!ignoreNullKeys>(
                group, rows()->columnAt(i), hasher->columnData(), row)) {
            return false;
        }
    } while (++i < numKeys);
    return true;
}

template <bool ignoreNullKeys>
void
HashTable<ignoreNullKeys>::storeKeys(milvus::exec::HashLookup& lookup,
                                     milvus::vector_size_t row) {
    for (int32_t i = 0; i < hashers_.size(); i++) {
        auto& hasher = hashers_[i];
        rows_->store(hasher->columnData(), row, lookup.hits_[row], i);
    }
}

template <bool ignoreNullKeys>
void
HashTable<ignoreNullKeys>::storeRowPointer(uint64_t index,
                                           uint64_t hash,
                                           char* row) {
    const int64_t bktOffset = bucketOffset(index);
    auto* bucket = bucketAt(bktOffset);
    const auto slotIndex = index & (sizeof(TagVector) - 1);
    bucket->setTag(slotIndex, hashTag(hash));
    bucket->setPointer(slotIndex, row);
}

template <bool ignoreNullKeys>
char*
HashTable<ignoreNullKeys>::insertEntry(milvus::exec::HashLookup& lookup,
                                       uint64_t index,
                                       milvus::vector_size_t row) {
    char* group = rows_->newRow();
    lookup.hits_[row] = group;
    storeKeys(lookup, row);
    storeRowPointer(index, lookup.hashes_[row], group);
    numDistinct_++;
    lookup.newGroups_.push_back(row);
    return group;
}

template <bool ignoreNullKeys>
FOLLY_ALWAYS_INLINE void
HashTable<ignoreNullKeys>::fullProbe(HashLookup& lookup,
                                     ProbeState& state,
                                     bool extraCheck) {
    constexpr ProbeState::Operation op = ProbeState::Operation::kInsert;
    lookup.hits_[state.row()] = state.fullProbe<op>(
        *this,
        0,
        [&](char* group, int32_t row) {
            return compareKeys(group, lookup, row);
        },
        [&](int32_t row, uint64_t index) {
            return insertEntry(lookup, index, row);
        },
        extraCheck);
}

template <bool ignoreNullKeys>
void
HashTable<ignoreNullKeys>::groupProbe(milvus::exec::HashLookup& lookup) {
    AssertInfo(hashMode_ == HashMode::kHash, "Only support kHash mode for now");
    checkSize(lookup.group_limit_);
    ProbeState state;
    int32_t numProbes = lookup.rows_.size();
    auto rows = lookup.rows_.data();
    for (int32_t probeIdx = 0; probeIdx < numProbes; probeIdx++) {
        int32_t row = rows[probeIdx];
        state.preProbe(*this, lookup.hashes_[row], row);
        state.firstProbe<ProbeState::Operation::kInsert>(*this, 0);
        fullProbe(lookup, state, false);
        if (lookup.group_enough()) {
            // group count has reached group limit, exit early
            break;
        }
    }
}

template <bool ignoreNullKeys>
void
HashTable<ignoreNullKeys>::setHashMode(HashMode mode, int32_t numNew) {
    // TODO set hash mode kArray/kHash/kNormalizedKey
}

template <bool nullable>
void
HashTable<nullable>::clear(bool freeTable) {
    if (table_) {
        delete[] table_;
        table_ = nullptr;
    }
    rows_->clear();
    numDistinct_ = 0;
}

template class HashTable<true>;
template class HashTable<false>;
}  // namespace exec
}  // namespace milvus
