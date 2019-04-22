// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

namespace rocksdb {
// A comparator to be used in std::set
struct SetComparator {
  explicit SetComparator() : user_comparator_(BytewiseComparator()) {}
  explicit SetComparator(const Comparator* user_comparator)
      : user_comparator_(user_comparator ? user_comparator
                                         : BytewiseComparator()) {}
  bool operator()(const Slice& lhs, const Slice& rhs) const {
    return user_comparator_->Compare(lhs, rhs) < 0;
  }

 private:
  const Comparator* user_comparator_;
};
}  // namespace rocksdb
