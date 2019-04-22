//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "table/full_filter_block.h"

#ifdef ROCKSDB_MALLOC_USABLE_SIZE
#ifdef OS_FREEBSD
#include <malloc_np.h>
#else
#include <malloc.h>
#endif
#endif

#include "monitoring/perf_context_imp.h"
#include "port/port.h"
#include "rocksdb/filter_policy.h"
#include "util/coding.h"

namespace rocksdb {

FullFilterBlockBuilder::FullFilterBlockBuilder(
    const SliceTransform* prefix_extractor, bool whole_key_filtering,
    FilterBitsBuilder* filter_bits_builder)
    : prefix_extractor_(prefix_extractor),
      whole_key_filtering_(whole_key_filtering),
      last_whole_key_recorded_(false),
      last_prefix_recorded_(false),
      num_added_(0) {
  assert(filter_bits_builder != nullptr);
  filter_bits_builder_.reset(filter_bits_builder);
}

void FullFilterBlockBuilder::Add(const Slice& key) {
  const bool add_prefix = prefix_extractor_ && prefix_extractor_->InDomain(key);
  if (whole_key_filtering_) {
    if (!add_prefix) {
      AddKey(key);
    } else {
      // if both whole_key and prefix are added to bloom then we will have whole
      // key and prefix addition being interleaved and thus cannot rely on the
      // bits builder to properly detect the duplicates by comparing with the
      // last item.
      Slice last_whole_key = Slice(last_whole_key_str_);
      if (!last_whole_key_recorded_ || last_whole_key.compare(key) != 0) {
        AddKey(key);
        last_whole_key_recorded_ = true;
        last_whole_key_str_.assign(key.data(), key.size());
      }
    }
  }
  if (add_prefix) {
    AddPrefix(key);
  }
}

// Add key to filter if needed
inline void FullFilterBlockBuilder::AddKey(const Slice& key) {
  filter_bits_builder_->AddKey(key);
  num_added_++;
}

// Add prefix to filter if needed
inline void FullFilterBlockBuilder::AddPrefix(const Slice& key) {
  Slice prefix = prefix_extractor_->Transform(key);
  if (whole_key_filtering_) {
    // if both whole_key and prefix are added to bloom then we will have whole
    // key and prefix addition being interleaved and thus cannot rely on the
    // bits builder to properly detect the duplicates by comparing with the last
    // item.
    Slice last_prefix = Slice(last_prefix_str_);
    if (!last_prefix_recorded_ || last_prefix.compare(prefix) != 0) {
      AddKey(prefix);
      last_prefix_recorded_ = true;
      last_prefix_str_.assign(prefix.data(), prefix.size());
    }
  } else {
    AddKey(prefix);
  }
}

void FullFilterBlockBuilder::Reset() {
  last_whole_key_recorded_ = false;
  last_prefix_recorded_ = false;
}

Slice FullFilterBlockBuilder::Finish(const BlockHandle& /*tmp*/,
                                     Status* status) {
  Reset();
  // In this impl we ignore BlockHandle
  *status = Status::OK();
  if (num_added_ != 0) {
    num_added_ = 0;
    return filter_bits_builder_->Finish(&filter_data_);
  }
  return Slice();
}

FullFilterBlockReader::FullFilterBlockReader(
    const SliceTransform* prefix_extractor, bool _whole_key_filtering,
    const Slice& contents, FilterBitsReader* filter_bits_reader,
    Statistics* stats)
    : FilterBlockReader(contents.size(), stats, _whole_key_filtering),
      prefix_extractor_(prefix_extractor),
      contents_(contents) {
  assert(filter_bits_reader != nullptr);
  filter_bits_reader_.reset(filter_bits_reader);
  if (prefix_extractor_ != nullptr) {
    full_length_enabled_ =
        prefix_extractor_->FullLengthEnabled(&prefix_extractor_full_length_);
  }
}

FullFilterBlockReader::FullFilterBlockReader(
    const SliceTransform* prefix_extractor, bool _whole_key_filtering,
    BlockContents&& contents, FilterBitsReader* filter_bits_reader,
    Statistics* stats)
    : FullFilterBlockReader(prefix_extractor, _whole_key_filtering,
                            contents.data, filter_bits_reader, stats) {
  block_contents_ = std::move(contents);
}

bool FullFilterBlockReader::KeyMayMatch(
    const Slice& key, const SliceTransform* /*prefix_extractor*/,
    uint64_t block_offset, const bool /*no_io*/,
    const Slice* const /*const_ikey_ptr*/) {
#ifdef NDEBUG
  (void)block_offset;
#endif
  assert(block_offset == kNotValid);
  if (!whole_key_filtering_) {
    return true;
  }
  return MayMatch(key);
}

bool FullFilterBlockReader::PrefixMayMatch(
    const Slice& prefix, const SliceTransform* /* prefix_extractor */,
    uint64_t block_offset, const bool /*no_io*/,
    const Slice* const /*const_ikey_ptr*/) {
#ifdef NDEBUG
  (void)block_offset;
#endif
  assert(block_offset == kNotValid);
  return MayMatch(prefix);
}

bool FullFilterBlockReader::MayMatch(const Slice& entry) {
  if (contents_.size() != 0)  {
    if (filter_bits_reader_->MayMatch(entry)) {
      PERF_COUNTER_ADD(bloom_sst_hit_count, 1);
      return true;
    } else {
      PERF_COUNTER_ADD(bloom_sst_miss_count, 1);
      return false;
    }
  }
  return true;  // remain the same with block_based filter
}

size_t FullFilterBlockReader::ApproximateMemoryUsage() const {
  size_t usage = block_contents_.usable_size();
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
  usage += malloc_usable_size((void*)this);
  usage += malloc_usable_size(filter_bits_reader_.get());
#else
  usage += sizeof(*this);
  usage += sizeof(*filter_bits_reader_.get());
#endif  // ROCKSDB_MALLOC_USABLE_SIZE
  return usage;
}

bool FullFilterBlockReader::RangeMayExist(const Slice* iterate_upper_bound,
    const Slice& user_key, const SliceTransform* prefix_extractor,
    const Comparator* comparator, const Slice* const const_ikey_ptr,
    bool* filter_checked, bool need_upper_bound_check) {
  if (!prefix_extractor || !prefix_extractor->InDomain(user_key)) {
    *filter_checked = false;
    return true;
  }
  Slice prefix = prefix_extractor->Transform(user_key);
  if (need_upper_bound_check &&
      !IsFilterCompatible(iterate_upper_bound, prefix, comparator)) {
    *filter_checked = false;
    return true;
  } else {
    *filter_checked = true;
    return PrefixMayMatch(prefix, prefix_extractor, kNotValid, false,
                          const_ikey_ptr);
  }
}

bool FullFilterBlockReader::IsFilterCompatible(
    const Slice* iterate_upper_bound, const Slice& prefix,
    const Comparator* comparator) {
  // Try to reuse the bloom filter in the SST table if prefix_extractor in
  // mutable_cf_options has changed. If range [user_key, upper_bound) all
  // share the same prefix then we may still be able to use the bloom filter.
  if (iterate_upper_bound != nullptr && prefix_extractor_) {
    if (!prefix_extractor_->InDomain(*iterate_upper_bound)) {
      return false;
    }
    Slice upper_bound_xform =
        prefix_extractor_->Transform(*iterate_upper_bound);
    // first check if user_key and upper_bound all share the same prefix
    if (!comparator->Equal(prefix, upper_bound_xform)) {
      // second check if user_key's prefix is the immediate predecessor of
      // upper_bound and have the same length. If so, we know for sure all
      // keys in the range [user_key, upper_bound) share the same prefix.
      // Also need to make sure upper_bound are full length to ensure
      // correctness
      if (!full_length_enabled_ ||
          iterate_upper_bound->size() != prefix_extractor_full_length_ ||
          !comparator->IsSameLengthImmediateSuccessor(prefix,
                                                      *iterate_upper_bound)) {
        return false;
      }
    }
    return true;
  } else {
    return false;
  }
}

}  // namespace rocksdb
