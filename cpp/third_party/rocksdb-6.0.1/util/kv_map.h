//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <map>
#include <string>

#include "rocksdb/comparator.h"
#include "rocksdb/slice.h"
#include "util/coding.h"
#include "util/murmurhash.h"

namespace rocksdb {
namespace stl_wrappers {

struct LessOfComparator {
  explicit LessOfComparator(const Comparator* c = BytewiseComparator())
      : cmp(c) {}

  bool operator()(const std::string& a, const std::string& b) const {
    return cmp->Compare(Slice(a), Slice(b)) < 0;
  }
  bool operator()(const Slice& a, const Slice& b) const {
    return cmp->Compare(a, b) < 0;
  }

  const Comparator* cmp;
};

typedef std::map<std::string, std::string, LessOfComparator> KVMap;
}
}
