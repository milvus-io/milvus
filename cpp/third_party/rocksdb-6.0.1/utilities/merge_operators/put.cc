//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <memory>
#include "rocksdb/slice.h"
#include "rocksdb/merge_operator.h"
#include "utilities/merge_operators.h"

using namespace rocksdb;

namespace { // anonymous namespace

// A merge operator that mimics Put semantics
// Since this merge-operator will not be used in production,
// it is implemented as a non-associative merge operator to illustrate the
// new interface and for testing purposes. (That is, we inherit from
// the MergeOperator class rather than the AssociativeMergeOperator
// which would be simpler in this case).
//
// From the client-perspective, semantics are the same.
class PutOperator : public MergeOperator {
 public:
  bool FullMerge(const Slice& /*key*/, const Slice* /*existing_value*/,
                 const std::deque<std::string>& operand_sequence,
                 std::string* new_value, Logger* /*logger*/) const override {
    // Put basically only looks at the current/latest value
    assert(!operand_sequence.empty());
    assert(new_value != nullptr);
    new_value->assign(operand_sequence.back());
    return true;
  }

  bool PartialMerge(const Slice& /*key*/, const Slice& /*left_operand*/,
                    const Slice& right_operand, std::string* new_value,
                    Logger* /*logger*/) const override {
    new_value->assign(right_operand.data(), right_operand.size());
    return true;
  }

  using MergeOperator::PartialMergeMulti;
  bool PartialMergeMulti(const Slice& /*key*/,
                         const std::deque<Slice>& operand_list,
                         std::string* new_value,
                         Logger* /*logger*/) const override {
    new_value->assign(operand_list.back().data(), operand_list.back().size());
    return true;
  }

  const char* Name() const override { return "PutOperator"; }
};

class PutOperatorV2 : public PutOperator {
  bool FullMerge(const Slice& /*key*/, const Slice* /*existing_value*/,
                 const std::deque<std::string>& /*operand_sequence*/,
                 std::string* /*new_value*/,
                 Logger* /*logger*/) const override {
    assert(false);
    return false;
  }

  bool FullMergeV2(const MergeOperationInput& merge_in,
                   MergeOperationOutput* merge_out) const override {
    // Put basically only looks at the current/latest value
    assert(!merge_in.operand_list.empty());
    merge_out->existing_operand = merge_in.operand_list.back();
    return true;
  }
};

} // end of anonymous namespace

namespace rocksdb {

std::shared_ptr<MergeOperator> MergeOperators::CreateDeprecatedPutOperator() {
  return std::make_shared<PutOperator>();
}

std::shared_ptr<MergeOperator> MergeOperators::CreatePutOperator() {
  return std::make_shared<PutOperatorV2>();
}
}
