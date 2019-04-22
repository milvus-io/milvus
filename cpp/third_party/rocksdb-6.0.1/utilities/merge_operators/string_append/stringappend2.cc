/**
 * @author Deon Nicholas (dnicholas@fb.com)
 * Copyright 2013 Facebook
 */

#include "stringappend2.h"

#include <memory>
#include <string>
#include <assert.h>

#include "rocksdb/slice.h"
#include "rocksdb/merge_operator.h"
#include "utilities/merge_operators.h"

namespace rocksdb {

// Constructor: also specify the delimiter character.
StringAppendTESTOperator::StringAppendTESTOperator(char delim_char)
    : delim_(delim_char) {
}

// Implementation for the merge operation (concatenates two strings)
bool StringAppendTESTOperator::FullMergeV2(
    const MergeOperationInput& merge_in,
    MergeOperationOutput* merge_out) const {
  // Clear the *new_value for writing.
  merge_out->new_value.clear();

  if (merge_in.existing_value == nullptr && merge_in.operand_list.size() == 1) {
    // Only one operand
    merge_out->existing_operand = merge_in.operand_list.back();
    return true;
  }

  // Compute the space needed for the final result.
  size_t numBytes = 0;
  for (auto it = merge_in.operand_list.begin();
       it != merge_in.operand_list.end(); ++it) {
    numBytes += it->size() + 1;   // Plus 1 for the delimiter
  }

  // Only print the delimiter after the first entry has been printed
  bool printDelim = false;

  // Prepend the *existing_value if one exists.
  if (merge_in.existing_value) {
    merge_out->new_value.reserve(numBytes + merge_in.existing_value->size());
    merge_out->new_value.append(merge_in.existing_value->data(),
                                merge_in.existing_value->size());
    printDelim = true;
  } else if (numBytes) {
    merge_out->new_value.reserve(
        numBytes - 1);  // Minus 1 since we have one less delimiter
  }

  // Concatenate the sequence of strings (and add a delimiter between each)
  for (auto it = merge_in.operand_list.begin();
       it != merge_in.operand_list.end(); ++it) {
    if (printDelim) {
      merge_out->new_value.append(1, delim_);
    }
    merge_out->new_value.append(it->data(), it->size());
    printDelim = true;
  }

  return true;
}

bool StringAppendTESTOperator::PartialMergeMulti(
    const Slice& /*key*/, const std::deque<Slice>& /*operand_list*/,
    std::string* /*new_value*/, Logger* /*logger*/) const {
  return false;
}

// A version of PartialMerge that actually performs "partial merging".
// Use this to simulate the exact behaviour of the StringAppendOperator.
bool StringAppendTESTOperator::_AssocPartialMergeMulti(
    const Slice& /*key*/, const std::deque<Slice>& operand_list,
    std::string* new_value, Logger* /*logger*/) const {
  // Clear the *new_value for writing
  assert(new_value);
  new_value->clear();
  assert(operand_list.size() >= 2);

  // Generic append
  // Determine and reserve correct size for *new_value.
  size_t size = 0;
  for (const auto& operand : operand_list) {
    size += operand.size();
  }
  size += operand_list.size() - 1;  // Delimiters
  new_value->reserve(size);

  // Apply concatenation
  new_value->assign(operand_list.front().data(), operand_list.front().size());

  for (std::deque<Slice>::const_iterator it = operand_list.begin() + 1;
       it != operand_list.end(); ++it) {
    new_value->append(1, delim_);
    new_value->append(it->data(), it->size());
  }

  return true;
}

const char* StringAppendTESTOperator::Name() const  {
  return "StringAppendTESTOperator";
}


std::shared_ptr<MergeOperator>
MergeOperators::CreateStringAppendTESTOperator() {
  return std::make_shared<StringAppendTESTOperator>(',');
}

} // namespace rocksdb
