//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include "rocksdb/merge_operator.h"

#include <stdio.h>

#include <memory>
#include <string>

namespace rocksdb {

class MergeOperators {
 public:
  static std::shared_ptr<MergeOperator> CreatePutOperator();
  static std::shared_ptr<MergeOperator> CreateDeprecatedPutOperator();
  static std::shared_ptr<MergeOperator> CreateUInt64AddOperator();
  static std::shared_ptr<MergeOperator> CreateStringAppendOperator();
  static std::shared_ptr<MergeOperator> CreateStringAppendOperator(char delim_char);
  static std::shared_ptr<MergeOperator> CreateStringAppendTESTOperator();
  static std::shared_ptr<MergeOperator> CreateMaxOperator();
  static std::shared_ptr<MergeOperator> CreateBytesXOROperator();

  // Will return a different merge operator depending on the string.
  // TODO: Hook the "name" up to the actual Name() of the MergeOperators?
  static std::shared_ptr<MergeOperator> CreateFromStringId(
      const std::string& name) {
    if (name == "put") {
      return CreatePutOperator();
    } else if (name == "put_v1") {
      return CreateDeprecatedPutOperator();
    } else if ( name == "uint64add") {
      return CreateUInt64AddOperator();
    } else if (name == "stringappend") {
      return CreateStringAppendOperator();
    } else if (name == "stringappendtest") {
      return CreateStringAppendTESTOperator();
    } else if (name == "max") {
      return CreateMaxOperator();
    } else if (name == "bytesxor") {
      return CreateBytesXOROperator();
    } else {
      // Empty or unknown, just return nullptr
      return nullptr;
    }
  }
};

}  // namespace rocksdb
