//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <algorithm>
#include <memory>
#include <string>
#include "rocksdb/env.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"
#include "util/coding.h"
#include "utilities/merge_operators.h"

namespace rocksdb {

// A 'model' merge operator that XORs two (same sized) array of bytes.
// Implemented as an AssociativeMergeOperator for simplicity and example.
class BytesXOROperator : public AssociativeMergeOperator {
 public:
  // XORs the two array of bytes one byte at a time and stores the result
  // in new_value. len is the number of xored bytes, and the length of new_value
  virtual bool Merge(const Slice& key,
                     const Slice* existing_value,
                     const Slice& value,
                     std::string* new_value,
                     Logger* logger) const override;

  virtual const char* Name() const override {
    return "BytesXOR";
  }

  void XOR(const Slice* existing_value, const Slice& value,
          std::string* new_value) const;
};

}  // namespace rocksdb
