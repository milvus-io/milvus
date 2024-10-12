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
#include <vector>
#include <memory>

#include "VectorHasher.h"
#include "exec/operator/query-agg/RowContainer.h"
#include "xsimd/xsimd.hpp"
#include "common/BitUtil.h"

namespace milvus {
namespace exec {

struct HashLookup {
    explicit HashLookup(
        const std::vector<std::unique_ptr<VectorHasher>>& hashers,
        int64_t group_limit)
        : hashers_(hashers), group_limit_(group_limit) {
    }

    void
    reset(vector_size_t size) {
        rows_.resize(size);
        hashes_.resize(size);
        hits_.resize(size);
        newGroups_.clear();
    }

    /// One entry per group-by
    const std::vector<std::unique_ptr<VectorHasher>>& hashers_;

    /// Set of row numbers of row to probe.
    std::vector<vector_size_t> rows_;

    /// Hashes or value IDs for rows in 'rows'. Not aligned with 'rows'. Index is
    /// the row number.
    std::vector<uint64_t> hashes_;

    /// Contains one entry for each row in 'rows'. Index is the row number.
    /// For groupProbe, a pointer to an existing or new row with matching grouping
    /// keys.
    std::vector<char*> hits_;

    /// For groupProbe, row numbers for which a new entry was inserted (didn't
    /// exist before the groupProbe). Empty for joinProbe.
    std::vector<vector_size_t> newGroups_;

    int64_t group_limit_{0};

    inline bool
    group_enough() {
        return newGroups_.size() >= group_limit_;
    }
};

class BaseHashTable {
 public:
#if XSIMD_WITH_SSE2
    using TagVector = xsimd::batch<uint8_t, xsimd::sse2>;
#elif XSIMD_WITH_NEON
    using TagVector = xsimd::batch<uint8_t, xsimd::neon>;
#endif
    using MaskType = uint16_t;

    enum class HashMode { kHash, kArray, kNormalizedKey };

    explicit BaseHashTable(std::vector<std::unique_ptr<VectorHasher>>&& hashers)
        : hashers_(std::move(hashers)) {
    }

    virtual ~BaseHashTable() = default;

    RowContainer*
    rows() const {
        return rows_.get();
    }

    /// Extracts a 7 bit tag from a hash number. The high bit is always set.
    static uint8_t
    hashTag(uint64_t hash) {
        // This is likely all 0 for small key types (<= 32 bits).  Not an issue
        // because small types have a range that makes them normalized key cases.
        // If there are multiple small type keys, they are mixed which makes them a
        // 64 bit hash.  Normalized keys are mixed before being used as hash
        // numbers.
        return static_cast<uint8_t>(hash >> 38) | 0x80;
    }

    static FOLLY_ALWAYS_INLINE size_t
    tableSlotSize() {
        // Each slot is 8 bytes.
        return sizeof(void*);
    }

    static TagVector
    loadTags(uint8_t* tags, int64_t tagIndex) {
        auto src = tags + tagIndex;
#if XSIMD_WITH_SSE2
        return TagVector(
            _mm_loadu_si128(reinterpret_cast<__m128i const*>(src)));
#elif XSIMD_WITH_NEON
        return TagVector(vld1q_u8(src));
#endif
    }

    const std::vector<std::unique_ptr<VectorHasher>>&
    hashers() const {
        return hashers_;
    }

    /// Returns the hash mode. This is needed for the caller to calculate
    /// the hash numbers using the appropriate method of the
    /// VectorHashers of 'this'.
    virtual HashMode
    hashMode() const = 0;

    virtual void
    setHashMode(HashMode mode, int32_t numNew) = 0;

    /// Disables use of array or normalized key hash modes.
    void
    forceGenericHashMode() {
        setHashMode(HashMode::kHash, 0);
    }

    /// Populates 'hashes' and 'rows' fields in 'lookup' in preparation for
    /// 'groupProbe' call. Rehashes the table if necessary. Uses lookup.hashes to
    /// decode grouping keys from 'input'. If 'ignoreNullKeys_' is true, updates
    /// 'rows' to remove entries with null grouping keys. After this call, 'rows'
    /// may have no entries selected.
    void
    prepareForGroupProbe(HashLookup& lookup,
                         const RowVectorPtr& input,
                         TargetBitmap& activeRows,
                         bool nullableKeys);

    /// Finds or creates a group for each key in 'lookup'. The keys are
    /// returned in 'lookup.hits'.
    virtual void
    groupProbe(HashLookup& lookup) = 0;

    virtual void
    clear(bool freeTable = false) = 0;

 protected:
    std::vector<std::unique_ptr<VectorHasher>> hashers_;
    std::unique_ptr<RowContainer> rows_;
};

class ProbeState;

template <bool ignoreNullKeys>
class HashTable : public BaseHashTable {
 public:
    HashTable(std::vector<std::unique_ptr<VectorHasher>>&& hashers,
              const std::vector<Accumulator>& accumulators)
        : BaseHashTable(std::move(hashers)) {
        std::vector<DataType> keyTypes;
        for (auto& hasher : hashers_) {
            keyTypes.push_back(hasher->ChannelDataType());
        }
        hashMode_ = HashMode::kHash;
        rows_ = std::make_unique<RowContainer>(
            keyTypes, accumulators, ignoreNullKeys);
    };

    ~HashTable() override {
    }

    void
    setHashMode(HashMode mode, int32_t numNew) override;

    void
    groupProbe(HashLookup& lookup) override;

    // The table in non-kArray mode has a power of two number of buckets each with
    // 16 slots. Each slot has a 1 byte tag (a field of hash number) and a 48 bit
    // pointer. All the tags are in a 16 byte SIMD word followed by the 6 byte
    // pointers. There are 16 bytes of padding at the end to make the bucket
    // occupy exactly two (64 bytes) cache lines.
    class Bucket {
     public:
        uint8_t
        tagAt(int32_t slotIndex) {
            return reinterpret_cast<uint8_t*>(&tags_)[slotIndex];
        }

        char*
        pointerAt(int32_t slotIndex) {
            return reinterpret_cast<char*>(
                *reinterpret_cast<uintptr_t*>(
                    &pointers_[kPointerSize * slotIndex]) &
                kPointerMask);
        }

        void
        setTag(int32_t slotIndex, uint8_t tag) {
            reinterpret_cast<uint8_t*>(&tags_)[slotIndex] = tag;
        }

        void
        setPointer(int32_t slotIndex, void* pointer) {
            auto* const slot = reinterpret_cast<uintptr_t*>(
                &pointers_[slotIndex * kPointerSize]);
            *slot =
                (*slot & ~kPointerMask) | reinterpret_cast<uintptr_t>(pointer);
        }

     private:
        static constexpr uint8_t kPointerSignificantBits = 48;
        static constexpr uint64_t kPointerMask =
            milvus::bits::lowMask(kPointerSignificantBits);
        static constexpr int32_t kPointerSize = kPointerSignificantBits / 8;

        TagVector tags_;
        char pointers_[sizeof(TagVector) * kPointerSize];
        char padding_[16];
    };
    static_assert(sizeof(Bucket) == 128);
    static constexpr uint64_t kBucketSize = sizeof(Bucket);

    Bucket*
    bucketAt(int64_t offset) const {
        //AssertInfo(offset&(kBucketSize-1)==0, "Invalid offset:{} and kBucketSize:{}", offset, kBucketSize);
        return reinterpret_cast<Bucket*>(reinterpret_cast<char*>(table_) +
                                         offset);
    }

    int64_t
    bucketOffset(uint64_t hash) const {
        return hash & bucketOffsetMask_;
    }

    int64_t
    nextBucketOffset(int64_t bucketOffset) const {
        //AssertInfo(bucketOffset&(kBucketSize - 1) == 0, "Invalid bucketOffset:{} for nextBucketOffset", bucketOffset);
        AssertInfo(bucketOffset < sizeMask_,
                   "BucketOffset:{} must be less than sizeMask_:{} for "
                   "nextBucketOffset",
                   bucketOffset,
                   sizeMask_);
        return sizeMask_ & (bucketOffset + kBucketSize);
    }

    bool
    compareKeys(const char* group, HashLookup& lookup, vector_size_t row);

    char*
    row(int64_t bucketOffset, int32_t slotIndex) const {
        return bucketAt(bucketOffset)->pointerAt(slotIndex);
    }

    int64_t
    numBuckets() const {
        return numBuckets_;
    }

    TagVector
    loadTags(int64_t bucketOffset) const {
        return BaseHashTable::loadTags(reinterpret_cast<uint8_t*>(table_),
                                       bucketOffset);
    }

    char*
    insertEntry(HashLookup& lookup, uint64_t index, vector_size_t row);

    void
    storeKeys(HashLookup& lookup, vector_size_t row);

    void
    storeRowPointer(uint64_t index, uint64_t hash, char* row);

    // Allocates new tables for tags and payload pointers. The size must
    // a power of 2.
    void
    allocateTables(uint64_t size);

    void
    fullProbe(HashLookup& lookup, ProbeState& state, bool extraCheck);

    void
    clear(bool freeTable = false) override;

    void
    checkSize(int32_t numNew);

    // Returns the number of entries after which the table gets rehashed.
    static uint64_t
    rehashSize(int64_t size) {
        // This implements the F14 load factor: Resize if less than 1/8 unoccupied.
        return size - (size / 8);
    }

    uint64_t
    rehashSize() const {
        return rehashSize(capacity_);
    }

    static uint64_t
    newHashTableEntriesNumber(uint64_t numDistinct, uint64_t numNew) {
        auto numNewEntries =
            std::max((uint64_t)2048,
                     milvus::bits::nextPowerOfTwo(numNew * 2 + numDistinct));
        const auto newNumDistinct = numDistinct + numNew;
        if (newNumDistinct > rehashSize(numNewEntries)) {
            numNewEntries *= 2;
        }
        return numNewEntries;
    }

 private:
    HashMode hashMode_ = HashMode::kHash;
    int64_t bucketOffsetMask_{0};
    int64_t numBuckets_{0};
    int64_t numDistinct_{0};

    // Number of slots across all buckets.
    int64_t capacity_{0};
    // Mask for extracting low bits of hash number for use as byte offsets into
    // the table. This is set to 'capacity_ * sizeof(void*) - 1'.
    int64_t sizeMask_{0};
    int8_t sizeBits_;

    int64_t numRehashes_{0};
    char** table_ = nullptr;

    HashMode
    hashMode() const override {
        return hashMode_;
    }
    friend class ProbeState;
};

}  // namespace exec
}  // namespace milvus