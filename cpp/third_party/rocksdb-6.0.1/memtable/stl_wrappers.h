//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <map>
#include <string>

#include "rocksdb/comparator.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/slice.h"
#include "util/coding.h"
#include "util/murmurhash.h"

namespace rocksdb {
namespace stl_wrappers {

class Base {
 protected:
  const MemTableRep::KeyComparator& compare_;
  explicit Base(const MemTableRep::KeyComparator& compare)
      : compare_(compare) {}
};

struct Compare : private Base {
  explicit Compare(const MemTableRep::KeyComparator& compare) : Base(compare) {}
  inline bool operator()(const char* a, const char* b) const {
    return compare_(a, b) < 0;
  }
};

}
}
