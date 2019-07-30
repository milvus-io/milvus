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

#ifndef ARROW_IPC_TEST_COMMON_H
#define ARROW_IPC_TEST_COMMON_H

#include <cstdint>
#include <memory>

#include "arrow/array.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/type.h"

namespace arrow {
namespace ipc {
namespace test {

// A typedef used for test parameterization
typedef Status MakeRecordBatch(std::shared_ptr<RecordBatch>* out);

ARROW_EXPORT
void CompareArraysDetailed(int index, const Array& result, const Array& expected);

ARROW_EXPORT
void CompareBatchColumnsDetailed(const RecordBatch& result, const RecordBatch& expected);

ARROW_EXPORT
Status MakeRandomInt32Array(int64_t length, bool include_nulls, MemoryPool* pool,
                            std::shared_ptr<Array>* out, uint32_t seed = 0);

ARROW_EXPORT
Status MakeRandomListArray(const std::shared_ptr<Array>& child_array, int num_lists,
                           bool include_nulls, MemoryPool* pool,
                           std::shared_ptr<Array>* out);

ARROW_EXPORT
Status MakeRandomMapArray(const std::shared_ptr<Array>& child_array, int num_lists,
                          bool include_nulls, MemoryPool* pool,
                          std::shared_ptr<Array>* out);

ARROW_EXPORT
Status MakeRandomBooleanArray(const int length, bool include_nulls,
                              std::shared_ptr<Array>* out);

ARROW_EXPORT
Status MakeBooleanBatchSized(const int length, std::shared_ptr<RecordBatch>* out);

ARROW_EXPORT
Status MakeBooleanBatch(std::shared_ptr<RecordBatch>* out);

ARROW_EXPORT
Status MakeIntBatchSized(int length, std::shared_ptr<RecordBatch>* out,
                         uint32_t seed = 0);

ARROW_EXPORT
Status MakeIntRecordBatch(std::shared_ptr<RecordBatch>* out);

ARROW_EXPORT
Status MakeRandomStringArray(int64_t length, bool include_nulls, MemoryPool* pool,
                             std::shared_ptr<Array>* out);

ARROW_EXPORT
Status MakeStringTypesRecordBatch(std::shared_ptr<RecordBatch>* out,
                                  bool with_nulls = true);

ARROW_EXPORT
Status MakeStringTypesRecordBatchWithNulls(std::shared_ptr<RecordBatch>* out);

ARROW_EXPORT
Status MakeNullRecordBatch(std::shared_ptr<RecordBatch>* out);

ARROW_EXPORT
Status MakeListRecordBatch(std::shared_ptr<RecordBatch>* out);

ARROW_EXPORT
Status MakeFixedSizeListRecordBatch(std::shared_ptr<RecordBatch>* out);

ARROW_EXPORT
Status MakeZeroLengthRecordBatch(std::shared_ptr<RecordBatch>* out);

ARROW_EXPORT
Status MakeNonNullRecordBatch(std::shared_ptr<RecordBatch>* out);

ARROW_EXPORT
Status MakeDeeplyNestedList(std::shared_ptr<RecordBatch>* out);

ARROW_EXPORT
Status MakeStruct(std::shared_ptr<RecordBatch>* out);

ARROW_EXPORT
Status MakeUnion(std::shared_ptr<RecordBatch>* out);

ARROW_EXPORT
Status MakeDictionary(std::shared_ptr<RecordBatch>* out);

ARROW_EXPORT
Status MakeDictionaryFlat(std::shared_ptr<RecordBatch>* out);

ARROW_EXPORT
Status MakeDates(std::shared_ptr<RecordBatch>* out);

ARROW_EXPORT
Status MakeTimestamps(std::shared_ptr<RecordBatch>* out);

ARROW_EXPORT
Status MakeIntervals(std::shared_ptr<RecordBatch>* out);

ARROW_EXPORT
Status MakeTimes(std::shared_ptr<RecordBatch>* out);

ARROW_EXPORT
Status MakeFWBinary(std::shared_ptr<RecordBatch>* out);

ARROW_EXPORT
Status MakeDecimal(std::shared_ptr<RecordBatch>* out);

ARROW_EXPORT
Status MakeNull(std::shared_ptr<RecordBatch>* out);

}  // namespace test
}  // namespace ipc
}  // namespace arrow

#endif  // ARROW_IPC_TEST_COMMON_H
