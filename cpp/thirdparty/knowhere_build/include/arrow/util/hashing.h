// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Private header, not to be exported

#pragma once

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/hash-util.h"
#include "arrow/util/macros.h"
#include "arrow/util/string_view.h"

namespace arrow {
namespace internal {

// XXX would it help to have a 32-bit hash value on large datasets?
typedef uint64_t hash_t;

// Notes about the choice of a hash function.
// - xxHash64 is extremely fast on large enough data
// - for small- to medium-sized data, there are better choices
//   (see comprehensive benchmarks results at
//    https://aras-p.info/blog/2016/08/09/More-Hash-Function-Tests/)
// - for very small fixed-size data (<= 16 bytes, e.g. Decimal128), it is
//   beneficial to define specialized hash functions
// - while xxHash and others have good statistical properties, we can relax those
//   a bit if it helps performance (especially if the hash table implementation
//   has a good collision resolution strategy)

template <uint64_t AlgNum>
inline hash_t ComputeStringHash(const void* data, int64_t length);

template <typename Scalar, uint64_t AlgNum>
struct ScalarHelperBase {
  static bool CompareScalars(Scalar u, Scalar v) { return u == v; }

  static hash_t ComputeHash(const Scalar& value) {
    // Generic hash computation for scalars.  Simply apply the string hash
    // to the bit representation of the value.

    // XXX in the case of FP values, we'd like equal values to have the same hash,
    // even if they have different bit representations...
    return ComputeStringHash<AlgNum>(&value, sizeof(value));
  }
};

template <typename Scalar, uint64_t AlgNum = 0, typename Enable = void>
struct ScalarHelper : public ScalarHelperBase<Scalar, AlgNum> {};

template <typename Scalar, uint64_t AlgNum>
struct ScalarHelper<Scalar, AlgNum,
                    typename std::enable_if<std::is_integral<Scalar>::value>::type>
    : public ScalarHelperBase<Scalar, AlgNum> {
  // ScalarHelper specialization for integers

  static hash_t ComputeHash(const Scalar& value) {
    // Faster hash computation for integers.

    // Two of xxhash's prime multipliers (which are chosen for their
    // bit dispersion properties)
    static constexpr uint64_t multipliers[] = {11400714785074694791ULL,
                                               14029467366897019727ULL};

    // Multiplying by the prime number mixes the low bits into the high bits,
    // then byte-swapping (which is a single CPU instruction) allows the
    // combined high and low bits to participate in the initial hash table index.
    auto h = static_cast<hash_t>(value);
    return BitUtil::ByteSwap(multipliers[AlgNum] * h);
  }
};

template <typename Scalar, uint64_t AlgNum>
struct ScalarHelper<
    Scalar, AlgNum,
    typename std::enable_if<std::is_same<util::string_view, Scalar>::value>::type>
    : public ScalarHelperBase<Scalar, AlgNum> {
  // ScalarHelper specialization for util::string_view

  static hash_t ComputeHash(const util::string_view& value) {
    return ComputeStringHash<AlgNum>(value.data(), static_cast<int64_t>(value.size()));
  }
};

template <typename Scalar, uint64_t AlgNum>
struct ScalarHelper<Scalar, AlgNum,
                    typename std::enable_if<std::is_floating_point<Scalar>::value>::type>
    : public ScalarHelperBase<Scalar, AlgNum> {
  // ScalarHelper specialization for reals

  static bool CompareScalars(Scalar u, Scalar v) {
    if (std::isnan(u)) {
      // XXX should we do a bit-precise comparison?
      return std::isnan(v);
    }
    return u == v;
  }
};

template <uint64_t AlgNum = 0>
hash_t ComputeStringHash(const void* data, int64_t length) {
  if (ARROW_PREDICT_TRUE(length <= 16)) {
    // Specialize for small hash strings, as they are quite common as
    // hash table keys.
    auto p = reinterpret_cast<const uint8_t*>(data);
    auto n = static_cast<uint32_t>(length);
    if (n <= 8) {
      if (n <= 3) {
        if (n == 0) {
          return 1U;
        }
        uint32_t x = (n << 24) ^ (p[0] << 16) ^ (p[n / 2] << 8) ^ p[n - 1];
        return ScalarHelper<uint32_t, AlgNum>::ComputeHash(x);
      }
      // 4 <= length <= 8
      // We can read the string as two overlapping 32-bit ints, apply
      // different hash functions to each of them in parallel, then XOR
      // the results
      uint32_t x, y;
      hash_t hx, hy;
      // XXX those are unaligned accesses.  Should we have a facility for that?
      x = *reinterpret_cast<const uint32_t*>(p + n - 4);
      y = *reinterpret_cast<const uint32_t*>(p);
      hx = ScalarHelper<uint32_t, AlgNum>::ComputeHash(x);
      hy = ScalarHelper<uint32_t, AlgNum ^ 1>::ComputeHash(y);
      return n ^ hx ^ hy;
    }
    // 8 <= length <= 16
    // Apply the same principle as above
    uint64_t x, y;
    hash_t hx, hy;
    x = *reinterpret_cast<const uint64_t*>(p + n - 8);
    y = *reinterpret_cast<const uint64_t*>(p);
    hx = ScalarHelper<uint64_t, AlgNum>::ComputeHash(x);
    hy = ScalarHelper<uint64_t, AlgNum ^ 1>::ComputeHash(y);
    return n ^ hx ^ hy;
  }

  if (HashUtil::have_hardware_crc32) {
    // DoubleCrcHash is faster that Murmur2.
    auto h = HashUtil::DoubleCrcHash(data, static_cast<int32_t>(length), AlgNum);
    return ScalarHelper<uint64_t, AlgNum>::ComputeHash(h);
  } else {
    // Fall back on 64-bit Murmur2 for longer strings.
    // It has decent speed for medium-sized strings.  There may be faster
    // hashes on long strings such as xxHash, but that may not matter much
    // for the typical length distribution of hash keys.
    return HashUtil::MurmurHash2_64(data, static_cast<int>(length), AlgNum);
  }
}

// XXX add a HashEq<ArrowType> struct with both hash and compare functions?

// ----------------------------------------------------------------------
// An open-addressing insert-only hash table (no deletes)

template <typename Payload>
class HashTable {
 public:
  static constexpr hash_t kSentinel = 0ULL;

  struct Entry {
    hash_t h;
    Payload payload;

    // An entry is valid if the hash is different from the sentinel value
    operator bool() const { return h != kSentinel; }
  };

  explicit HashTable(uint64_t capacity) {
    // Presize for at least 8 elements
    capacity = std::max(capacity, static_cast<uint64_t>(8U));
    size_ = BitUtil::NextPower2(capacity * 4U);
    size_mask_ = size_ - 1;
    n_filled_ = 0;
    // This will zero out hash entries, marking them empty
    entries_.resize(size_);
  }

  // Lookup with non-linear probing
  // cmp_func should have signature bool(const Payload*).
  // Return a (Entry*, found) pair.
  template <typename CmpFunc>
  std::pair<Entry*, bool> Lookup(hash_t h, CmpFunc&& cmp_func) {
    auto p = Lookup<DoCompare, CmpFunc>(h, entries_.data(), size_mask_,
                                        std::forward<CmpFunc>(cmp_func));
    return {&entries_[p.first], p.second};
  }

  template <typename CmpFunc>
  std::pair<const Entry*, bool> Lookup(hash_t h, CmpFunc&& cmp_func) const {
    auto p = Lookup<DoCompare, CmpFunc>(h, entries_.data(), size_mask_,
                                        std::forward<CmpFunc>(cmp_func));
    return {&entries_[p.first], p.second};
  }

  void Insert(Entry* entry, hash_t h, const Payload& payload) {
    // Ensure entry is empty before inserting
    assert(!*entry);
    entry->h = FixHash(h);
    entry->payload = payload;
    ++n_filled_;
    if (NeedUpsizing()) {
      // Resizing is expensive, avoid doing it too often
      Upsize(size_ * 4);
    }
  }

  uint64_t size() const { return n_filled_; }

  // Visit all non-empty entries in the table
  // The visit_func should have signature void(const Entry*)
  template <typename VisitFunc>
  void VisitEntries(VisitFunc&& visit_func) const {
    for (const auto& entry : entries_) {
      if (entry) {
        visit_func(&entry);
      }
    }
  }

 protected:
  // NoCompare is for when the value is known not to exist in the table
  enum CompareKind { DoCompare, NoCompare };

  // The workhorse lookup function
  template <CompareKind CKind, typename CmpFunc>
  std::pair<uint64_t, bool> Lookup(hash_t h, const Entry* entries, uint64_t size_mask,
                                   CmpFunc&& cmp_func) const {
    static constexpr uint8_t perturb_shift = 5;

    uint64_t index, perturb;
    const Entry* entry;

    h = FixHash(h);
    index = h & size_mask;
    perturb = (h >> perturb_shift) + 1U;

    while (true) {
      entry = &entries[index];
      if (CompareEntry<CKind, CmpFunc>(h, entry, std::forward<CmpFunc>(cmp_func))) {
        // Found
        return {index, true};
      }
      if (entry->h == 0U) {
        // Empty slot
        return {index, false};
      }

      // Perturbation logic inspired from CPython's set / dict object.
      // The goal is that all 64 bits of the unmasked hash value eventually
      // participate in the probing sequence, to minimize clustering.
      index = (index + perturb) & size_mask;
      perturb = (perturb >> perturb_shift) + 1U;
    }
  }

  template <CompareKind CKind, typename CmpFunc>
  bool CompareEntry(hash_t h, const Entry* entry, CmpFunc&& cmp_func) const {
    if (CKind == NoCompare) {
      return false;
    } else {
      return entry->h == h && cmp_func(&entry->payload);
    }
  }

  bool NeedUpsizing() const {
    // Keep the load factor <= 1/2
    return n_filled_ * 2U >= size_;
  }

  void Upsize(uint64_t new_size) {
    assert(new_size > size_);
    uint64_t new_mask = new_size - 1;
    assert((new_size & new_mask) == 0);  // it's a power of two

    std::vector<Entry> new_entries(new_size);
    for (auto& entry : entries_) {
      if (entry) {
        // Dummy compare function (will not be called)
        auto cmp_func = [](const Payload*) { return false; };
        // Non-empty slot, move into new
        auto p = Lookup<NoCompare>(entry.h, new_entries.data(), new_mask, cmp_func);
        assert(!p.second);  // shouldn't have found a matching entry
        new_entries[p.first] = entry;
      }
    }
    std::swap(entries_, new_entries);
    size_ = new_size;
    size_mask_ = new_mask;
  }

  hash_t FixHash(hash_t h) const { return (h == kSentinel) ? 42U : h; }

  uint64_t size_;
  uint64_t size_mask_;
  uint64_t n_filled_;
  std::vector<Entry> entries_;
};

// XXX typedef memo_index_t int32_t ?

constexpr int32_t kKeyNotFound = -1;

// ----------------------------------------------------------------------
// A base class for memoization table.

class MemoTable {
 public:
  virtual ~MemoTable() = default;

  virtual int32_t size() const = 0;
};

// ----------------------------------------------------------------------
// A memoization table for memory-cheap scalar values.

// The memoization table remembers and allows to look up the insertion
// index for each key.

template <typename Scalar, template <class> class HashTableTemplateType = HashTable>
class ScalarMemoTable : public MemoTable {
 public:
  explicit ScalarMemoTable(int64_t entries = 0)
      : hash_table_(static_cast<uint64_t>(entries)) {}

  int32_t Get(const Scalar& value) const {
    auto cmp_func = [value](const Payload* payload) -> bool {
      return ScalarHelper<Scalar, 0>::CompareScalars(payload->value, value);
    };
    hash_t h = ComputeHash(value);
    auto p = hash_table_.Lookup(h, cmp_func);
    if (p.second) {
      return p.first->payload.memo_index;
    } else {
      return kKeyNotFound;
    }
  }

  template <typename Func1, typename Func2>
  int32_t GetOrInsert(const Scalar& value, Func1&& on_found, Func2&& on_not_found) {
    auto cmp_func = [value](const Payload* payload) -> bool {
      return ScalarHelper<Scalar, 0>::CompareScalars(value, payload->value);
    };
    hash_t h = ComputeHash(value);
    auto p = hash_table_.Lookup(h, cmp_func);
    int32_t memo_index;
    if (p.second) {
      memo_index = p.first->payload.memo_index;
      on_found(memo_index);
    } else {
      memo_index = size();
      hash_table_.Insert(p.first, h, {value, memo_index});
      on_not_found(memo_index);
    }
    return memo_index;
  }

  int32_t GetOrInsert(const Scalar& value) {
    return GetOrInsert(value, [](int32_t i) {}, [](int32_t i) {});
  }

  int32_t GetNull() const { return null_index_; }

  template <typename Func1, typename Func2>
  int32_t GetOrInsertNull(Func1&& on_found, Func2&& on_not_found) {
    int32_t memo_index = GetNull();
    if (memo_index != kKeyNotFound) {
      on_found(memo_index);
    } else {
      null_index_ = memo_index = size();
      on_not_found(memo_index);
    }
    return memo_index;
  }

  int32_t GetOrInsertNull() {
    return GetOrInsertNull([](int32_t i) {}, [](int32_t i) {});
  }

  // The number of entries in the memo table +1 if null was added.
  // (which is also 1 + the largest memo index)
  int32_t size() const override {
    return static_cast<int32_t>(hash_table_.size()) + (GetNull() != kKeyNotFound);
  }

  // Copy values starting from index `start` into `out_data`
  void CopyValues(int32_t start, Scalar* out_data) const {
    hash_table_.VisitEntries([=](const HashTableEntry* entry) {
      int32_t index = entry->payload.memo_index - start;
      if (index >= 0) {
        out_data[index] = entry->payload.value;
      }
    });
  }

  void CopyValues(Scalar* out_data) const { CopyValues(0, out_data); }

 protected:
  struct Payload {
    Scalar value;
    int32_t memo_index;
  };

  using HashTableType = HashTableTemplateType<Payload>;
  using HashTableEntry = typename HashTableType::Entry;
  HashTableType hash_table_;
  int32_t null_index_ = kKeyNotFound;

  hash_t ComputeHash(const Scalar& value) const {
    return ScalarHelper<Scalar, 0>::ComputeHash(value);
  }
};

// ----------------------------------------------------------------------
// A memoization table for small scalar values, using direct indexing

template <typename Scalar, typename Enable = void>
struct SmallScalarTraits {};

template <>
struct SmallScalarTraits<bool> {
  static constexpr int32_t cardinality = 2;

  static uint32_t AsIndex(bool value) { return value ? 1 : 0; }
};

template <typename Scalar>
struct SmallScalarTraits<Scalar,
                         typename std::enable_if<std::is_integral<Scalar>::value>::type> {
  using Unsigned = typename std::make_unsigned<Scalar>::type;

  static constexpr int32_t cardinality = 1U + std::numeric_limits<Unsigned>::max();

  static uint32_t AsIndex(Scalar value) { return static_cast<Unsigned>(value); }
};

template <typename Scalar, template <class> class HashTableTemplateType = HashTable>
class SmallScalarMemoTable : public MemoTable {
 public:
  explicit SmallScalarMemoTable(int64_t entries = 0) {
    std::fill(value_to_index_, value_to_index_ + cardinality + 1, kKeyNotFound);
    index_to_value_.reserve(cardinality);
  }

  int32_t Get(const Scalar value) const {
    auto value_index = AsIndex(value);
    return value_to_index_[value_index];
  }

  template <typename Func1, typename Func2>
  int32_t GetOrInsert(const Scalar value, Func1&& on_found, Func2&& on_not_found) {
    auto value_index = AsIndex(value);
    auto memo_index = value_to_index_[value_index];
    if (memo_index == kKeyNotFound) {
      memo_index = static_cast<int32_t>(index_to_value_.size());
      index_to_value_.push_back(value);
      value_to_index_[value_index] = memo_index;
      assert(memo_index < cardinality + 1);
      on_not_found(memo_index);
    } else {
      on_found(memo_index);
    }
    return memo_index;
  }

  int32_t GetOrInsert(const Scalar value) {
    return GetOrInsert(value, [](int32_t i) {}, [](int32_t i) {});
  }

  int32_t GetNull() const { return value_to_index_[cardinality]; }

  template <typename Func1, typename Func2>
  int32_t GetOrInsertNull(Func1&& on_found, Func2&& on_not_found) {
    auto memo_index = GetNull();
    if (memo_index == kKeyNotFound) {
      memo_index = value_to_index_[cardinality] = size();
      index_to_value_.push_back(0);
      on_not_found(memo_index);
    } else {
      on_found(memo_index);
    }
    return memo_index;
  }

  int32_t GetOrInsertNull() {
    return GetOrInsertNull([](int32_t i) {}, [](int32_t i) {});
  }

  // The number of entries in the memo table
  // (which is also 1 + the largest memo index)
  int32_t size() const override { return static_cast<int32_t>(index_to_value_.size()); }

  // Copy values starting from index `start` into `out_data`
  void CopyValues(int32_t start, Scalar* out_data) const {
    DCHECK_GE(start, 0);
    DCHECK_LE(static_cast<size_t>(start), index_to_value_.size());
    int64_t offset = start * static_cast<int32_t>(sizeof(Scalar));
    memcpy(out_data, index_to_value_.data() + offset, (size() - start) * sizeof(Scalar));
  }

  void CopyValues(Scalar* out_data) const { CopyValues(0, out_data); }

  const std::vector<Scalar>& values() const { return index_to_value_; }

 protected:
  static constexpr auto cardinality = SmallScalarTraits<Scalar>::cardinality;
  static_assert(cardinality <= 256, "cardinality too large for direct-addressed table");

  uint32_t AsIndex(Scalar value) const {
    return SmallScalarTraits<Scalar>::AsIndex(value);
  }

  // The last index is reserved for the null element.
  int32_t value_to_index_[cardinality + 1];
  std::vector<Scalar> index_to_value_;
};

// ----------------------------------------------------------------------
// A memoization table for variable-sized binary data.

class BinaryMemoTable : public MemoTable {
 public:
  explicit BinaryMemoTable(int64_t entries = 0, int64_t values_size = -1)
      : hash_table_(static_cast<uint64_t>(entries)) {
    offsets_.reserve(entries + 1);
    offsets_.push_back(0);
    if (values_size == -1) {
      values_.reserve(entries * 4);  // A conservative heuristic
    } else {
      values_.reserve(values_size);
    }
  }

  int32_t Get(const void* data, int32_t length) const {
    hash_t h = ComputeStringHash<0>(data, length);
    auto p = Lookup(h, data, length);
    if (p.second) {
      return p.first->payload.memo_index;
    } else {
      return kKeyNotFound;
    }
  }

  int32_t Get(const std::string& value) const {
    return Get(value.data(), static_cast<int32_t>(value.length()));
  }

  int32_t Get(const util::string_view& value) const {
    return Get(value.data(), static_cast<int32_t>(value.length()));
  }

  template <typename Func1, typename Func2>
  int32_t GetOrInsert(const void* data, int32_t length, Func1&& on_found,
                      Func2&& on_not_found) {
    hash_t h = ComputeStringHash<0>(data, length);
    auto p = Lookup(h, data, length);
    int32_t memo_index;
    if (p.second) {
      memo_index = p.first->payload.memo_index;
      on_found(memo_index);
    } else {
      memo_index = size();
      // Insert offset
      auto offset = static_cast<int32_t>(values_.size());
      assert(offsets_.size() == static_cast<uint32_t>(memo_index + 1));
      assert(offsets_[memo_index] == offset);
      offsets_.push_back(offset + length);
      // Insert string value
      values_.append(static_cast<const char*>(data), length);
      // Insert hash entry
      hash_table_.Insert(const_cast<HashTableEntry*>(p.first), h, {memo_index});

      on_not_found(memo_index);
    }
    return memo_index;
  }

  template <typename Func1, typename Func2>
  int32_t GetOrInsert(const util::string_view& value, Func1&& on_found,
                      Func2&& on_not_found) {
    return GetOrInsert(value.data(), static_cast<int32_t>(value.length()),
                       std::forward<Func1>(on_found), std::forward<Func2>(on_not_found));
  }

  int32_t GetOrInsert(const void* data, int32_t length) {
    return GetOrInsert(data, length, [](int32_t i) {}, [](int32_t i) {});
  }

  int32_t GetOrInsert(const util::string_view& value) {
    return GetOrInsert(value.data(), static_cast<int32_t>(value.length()));
  }

  int32_t GetOrInsert(const std::string& value) {
    return GetOrInsert(value.data(), static_cast<int32_t>(value.length()));
  }

  int32_t GetNull() const { return null_index_; }

  template <typename Func1, typename Func2>
  int32_t GetOrInsertNull(Func1&& on_found, Func2&& on_not_found) {
    auto memo_index = GetNull();
    if (memo_index == kKeyNotFound) {
      memo_index = null_index_ = size();
      auto offset = static_cast<int32_t>(values_.size());
      // Only the offset array needs to be updated.
      offsets_.push_back(offset);

      on_not_found(memo_index);
    } else {
      on_found(memo_index);
    }
    return memo_index;
  }

  int32_t GetOrInsertNull() {
    return GetOrInsertNull([](int32_t i) {}, [](int32_t i) {});
  }

  // The number of entries in the memo table
  // (which is also 1 + the largest memo index)
  int32_t size() const override {
    return static_cast<int32_t>(hash_table_.size() + (GetNull() != kKeyNotFound));
  }

  int32_t values_size() const { return static_cast<int32_t>(values_.size()); }

  const uint8_t* values_data() const {
    return reinterpret_cast<const uint8_t*>(values_.data());
  }

  // Copy (n + 1) offsets starting from index `start` into `out_data`
  template <class Offset>
  void CopyOffsets(int32_t start, Offset* out_data) const {
    auto delta = offsets_[start];
    for (uint32_t i = start; i < offsets_.size(); ++i) {
      auto adjusted_offset = offsets_[i] - delta;
      auto cast_offset = static_cast<Offset>(adjusted_offset);
      assert(static_cast<int32_t>(cast_offset) == adjusted_offset);  // avoid truncation
      *out_data++ = cast_offset;
    }
  }

  template <class Offset>
  void CopyOffsets(Offset* out_data) const {
    CopyOffsets(0, out_data);
  }

  // Copy values starting from index `start` into `out_data`
  void CopyValues(int32_t start, uint8_t* out_data) const {
    CopyValues(start, -1, out_data);
  }

  // Same as above, but check output size in debug mode
  void CopyValues(int32_t start, int64_t out_size, uint8_t* out_data) const {
    int32_t offset = offsets_[start];
    auto length = values_.size() - static_cast<size_t>(offset);
    if (out_size != -1) {
      assert(static_cast<int64_t>(length) == out_size);
    }
    memcpy(out_data, values_.data() + offset, length);
  }

  void CopyValues(uint8_t* out_data) const { CopyValues(0, -1, out_data); }

  void CopyValues(int64_t out_size, uint8_t* out_data) const {
    CopyValues(0, out_size, out_data);
  }

  void CopyFixedWidthValues(int32_t start, int32_t width_size, int64_t out_size,
                            uint8_t* out_data) const {
    // This method exists to cope with the fact that the BinaryMemoTable does
    // not know the fixed width when inserting the null value. The data
    // buffer hold a zero length string for the null value (if found).
    //
    // Thus, the method will properly inject an empty value of the proper width
    // in the output buffer.

    int32_t null_index = GetNull();
    if (null_index < start) {
      // Nothing to skip, proceed as usual.
      CopyValues(start, out_size, out_data);
      return;
    }

    int32_t left_offset = offsets_[start];

    // Ensure that the data length is exactly missing width_size bytes to fit
    // in the expected output (n_values * width_size).
#ifndef NDEBUG
    int64_t data_length = values_.size() - static_cast<size_t>(left_offset);
    assert(data_length + width_size == out_size);
#endif

    auto in_data = values_.data() + left_offset;
    // The null use 0-length in the data, slice the data in 2 and skip by
    // width_size in out_data. [part_1][width_size][part_2]
    auto null_data_offset = offsets_[null_index];
    auto left_size = null_data_offset - left_offset;
    if (left_size > 0) {
      memcpy(out_data, in_data + left_offset, left_size);
    }

    auto right_size = values_.size() - static_cast<size_t>(null_data_offset);
    if (right_size > 0) {
      // skip the null fixed size value.
      auto out_offset = left_size + width_size;
      assert(out_data + out_offset + right_size == out_data + out_size);
      memcpy(out_data + out_offset, in_data + null_data_offset, right_size);
    }
  }

  // Visit the stored values in insertion order.
  // The visitor function should have the signature `void(util::string_view)`
  // or `void(const util::string_view&)`.
  template <typename VisitFunc>
  void VisitValues(int32_t start, VisitFunc&& visit) const {
    for (uint32_t i = start; i < offsets_.size() - 1; ++i) {
      visit(
          util::string_view(values_.data() + offsets_[i], offsets_[i + 1] - offsets_[i]));
    }
  }

 protected:
  struct Payload {
    int32_t memo_index;
  };

  using HashTableType = HashTable<Payload>;
  using HashTableEntry = typename HashTable<Payload>::Entry;
  HashTableType hash_table_;

  std::vector<int32_t> offsets_;
  std::string values_;

  int32_t null_index_ = kKeyNotFound;

  std::pair<const HashTableEntry*, bool> Lookup(hash_t h, const void* data,
                                                int32_t length) const {
    auto cmp_func = [=](const Payload* payload) {
      int32_t start, stop;
      start = offsets_[payload->memo_index];
      stop = offsets_[payload->memo_index + 1];
      return length == stop - start && memcmp(data, values_.data() + start, length) == 0;
    };
    return hash_table_.Lookup(h, cmp_func);
  }
};

template <typename T, typename Enable = void>
struct HashTraits {};

template <>
struct HashTraits<BooleanType> {
  using MemoTableType = SmallScalarMemoTable<bool>;
};

template <typename T>
struct HashTraits<T, enable_if_8bit_int<T>> {
  using c_type = typename T::c_type;
  using MemoTableType = SmallScalarMemoTable<typename T::c_type>;
};

template <typename T>
struct HashTraits<
    T, typename std::enable_if<has_c_type<T>::value && !is_8bit_int<T>::value>::type> {
  using c_type = typename T::c_type;
  using MemoTableType = ScalarMemoTable<c_type, HashTable>;
};

template <typename T>
struct HashTraits<T, enable_if_binary<T>> {
  using MemoTableType = BinaryMemoTable;
};

template <typename T>
struct HashTraits<T, enable_if_fixed_size_binary<T>> {
  using MemoTableType = BinaryMemoTable;
};

template <typename MemoTableType>
static inline Status ComputeNullBitmap(MemoryPool* pool, const MemoTableType& memo_table,
                                       int64_t start_offset, int64_t* null_count,
                                       std::shared_ptr<Buffer>* null_bitmap) {
  int64_t dict_length = static_cast<int64_t>(memo_table.size()) - start_offset;
  int64_t null_index = memo_table.GetNull();

  *null_count = 0;
  *null_bitmap = nullptr;

  if (null_index != kKeyNotFound && null_index >= start_offset) {
    null_index -= start_offset;
    *null_count = 1;
    RETURN_NOT_OK(internal::BitmapAllButOne(pool, dict_length, null_index, null_bitmap));
  }

  return Status::OK();
}

template <typename T, typename Enable = void>
struct DictionaryTraits {};

template <>
struct DictionaryTraits<BooleanType> {
  using T = BooleanType;
  using MemoTableType = typename HashTraits<T>::MemoTableType;

  static Status GetDictionaryArrayData(MemoryPool* pool,
                                       const std::shared_ptr<DataType>& type,
                                       const MemoTableType& memo_table,
                                       int64_t start_offset,
                                       std::shared_ptr<ArrayData>* out) {
    if (start_offset < 0) {
      return Status::Invalid("invalid start_offset ", start_offset);
    }

    BooleanBuilder builder(pool);
    const auto& bool_values = memo_table.values();
    const auto null_index = memo_table.GetNull();

    // Will iterate up to 3 times.
    for (int64_t i = start_offset; i < memo_table.size(); i++) {
      RETURN_NOT_OK(i == null_index ? builder.AppendNull()
                                    : builder.Append(bool_values[i]));
    }

    return builder.FinishInternal(out);
  }
};  // namespace internal

template <typename T>
struct DictionaryTraits<T, enable_if_has_c_type<T>> {
  using c_type = typename T::c_type;
  using MemoTableType = typename HashTraits<T>::MemoTableType;

  static Status GetDictionaryArrayData(MemoryPool* pool,
                                       const std::shared_ptr<DataType>& type,
                                       const MemoTableType& memo_table,
                                       int64_t start_offset,
                                       std::shared_ptr<ArrayData>* out) {
    std::shared_ptr<Buffer> dict_buffer;
    auto dict_length = static_cast<int64_t>(memo_table.size()) - start_offset;
    // This makes a copy, but we assume a dictionary array is usually small
    // compared to the size of the dictionary-using array.
    // (also, copying the dictionary values is cheap compared to the cost
    //  of building the memo table)
    RETURN_NOT_OK(
        AllocateBuffer(pool, TypeTraits<T>::bytes_required(dict_length), &dict_buffer));
    memo_table.CopyValues(static_cast<int32_t>(start_offset),
                          reinterpret_cast<c_type*>(dict_buffer->mutable_data()));

    int64_t null_count = 0;
    std::shared_ptr<Buffer> null_bitmap = nullptr;
    RETURN_NOT_OK(
        ComputeNullBitmap(pool, memo_table, start_offset, &null_count, &null_bitmap));

    *out = ArrayData::Make(type, dict_length, {null_bitmap, dict_buffer}, null_count);
    return Status::OK();
  }
};

template <typename T>
struct DictionaryTraits<T, enable_if_binary<T>> {
  using MemoTableType = typename HashTraits<T>::MemoTableType;

  static Status GetDictionaryArrayData(MemoryPool* pool,
                                       const std::shared_ptr<DataType>& type,
                                       const MemoTableType& memo_table,
                                       int64_t start_offset,
                                       std::shared_ptr<ArrayData>* out) {
    std::shared_ptr<Buffer> dict_offsets;
    std::shared_ptr<Buffer> dict_data;

    // Create the offsets buffer
    auto dict_length = static_cast<int64_t>(memo_table.size() - start_offset);
    RETURN_NOT_OK(AllocateBuffer(
        pool, TypeTraits<Int32Type>::bytes_required(dict_length + 1), &dict_offsets));
    auto raw_offsets = reinterpret_cast<int32_t*>(dict_offsets->mutable_data());
    memo_table.CopyOffsets(static_cast<int32_t>(start_offset), raw_offsets);

    // Create the data buffer
    DCHECK_EQ(raw_offsets[0], 0);
    RETURN_NOT_OK(AllocateBuffer(pool, raw_offsets[dict_length], &dict_data));
    memo_table.CopyValues(static_cast<int32_t>(start_offset), dict_data->size(),
                          dict_data->mutable_data());

    int64_t null_count = 0;
    std::shared_ptr<Buffer> null_bitmap = nullptr;
    RETURN_NOT_OK(
        ComputeNullBitmap(pool, memo_table, start_offset, &null_count, &null_bitmap));

    *out = ArrayData::Make(type, dict_length, {null_bitmap, dict_offsets, dict_data},
                           null_count);

    return Status::OK();
  }
};

template <typename T>
struct DictionaryTraits<T, enable_if_fixed_size_binary<T>> {
  using MemoTableType = typename HashTraits<T>::MemoTableType;

  static Status GetDictionaryArrayData(MemoryPool* pool,
                                       const std::shared_ptr<DataType>& type,
                                       const MemoTableType& memo_table,
                                       int64_t start_offset,
                                       std::shared_ptr<ArrayData>* out) {
    const T& concrete_type = internal::checked_cast<const T&>(*type);
    std::shared_ptr<Buffer> dict_data;

    // Create the data buffer
    auto dict_length = static_cast<int64_t>(memo_table.size() - start_offset);
    auto width_length = concrete_type.byte_width();
    auto data_length = dict_length * width_length;
    RETURN_NOT_OK(AllocateBuffer(pool, data_length, &dict_data));
    auto data = dict_data->mutable_data();

    memo_table.CopyFixedWidthValues(static_cast<int32_t>(start_offset), width_length,
                                    data_length, data);

    int64_t null_count = 0;
    std::shared_ptr<Buffer> null_bitmap = nullptr;
    RETURN_NOT_OK(
        ComputeNullBitmap(pool, memo_table, start_offset, &null_count, &null_bitmap));

    *out = ArrayData::Make(type, dict_length, {null_bitmap, dict_data}, null_count);
    return Status::OK();
  }
};

}  // namespace internal
}  // namespace arrow
