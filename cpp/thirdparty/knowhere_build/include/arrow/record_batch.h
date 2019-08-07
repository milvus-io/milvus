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

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/type_fwd.h"
#include "arrow/util/iterator.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

/// \class RecordBatch
/// \brief Collection of equal-length arrays matching a particular Schema
///
/// A record batch is table-like data structure that is semantically a sequence
/// of fields, each a contiguous Arrow array
class ARROW_EXPORT RecordBatch {
 public:
  virtual ~RecordBatch() = default;

  /// \param[in] schema The record batch schema
  /// \param[in] num_rows length of fields in the record batch. Each array
  /// should have the same length as num_rows
  /// \param[in] columns the record batch fields as vector of arrays
  static std::shared_ptr<RecordBatch> Make(
      const std::shared_ptr<Schema>& schema, int64_t num_rows,
      const std::vector<std::shared_ptr<Array>>& columns);

  /// \brief Move-based constructor for a vector of Array instances
  static std::shared_ptr<RecordBatch> Make(const std::shared_ptr<Schema>& schema,
                                           int64_t num_rows,
                                           std::vector<std::shared_ptr<Array>>&& columns);

  /// \brief Construct record batch from vector of internal data structures
  /// \since 0.5.0
  ///
  /// This class is only provided with an rvalue-reference for the input data,
  /// and is intended for internal use, or advanced users.
  ///
  /// \param schema the record batch schema
  /// \param num_rows the number of semantic rows in the record batch. This
  /// should be equal to the length of each field
  /// \param columns the data for the batch's columns
  static std::shared_ptr<RecordBatch> Make(
      const std::shared_ptr<Schema>& schema, int64_t num_rows,
      std::vector<std::shared_ptr<ArrayData>>&& columns);

  /// \brief Construct record batch by copying vector of array data
  /// \since 0.5.0
  static std::shared_ptr<RecordBatch> Make(
      const std::shared_ptr<Schema>& schema, int64_t num_rows,
      const std::vector<std::shared_ptr<ArrayData>>& columns);

  /// \brief Determine if two record batches are exactly equal
  /// \return true if batches are equal
  bool Equals(const RecordBatch& other) const;

  /// \brief Determine if two record batches are approximately equal
  bool ApproxEquals(const RecordBatch& other) const;

  // \return the table's schema
  /// \return true if batches are equal
  std::shared_ptr<Schema> schema() const { return schema_; }

  /// \brief Retrieve an array from the record batch
  /// \param[in] i field index, does not boundscheck
  /// \return an Array object
  virtual std::shared_ptr<Array> column(int i) const = 0;

  /// \brief Retrieve an array from the record batch
  /// \param[in] name field name
  /// \return an Array or null if no field was found
  std::shared_ptr<Array> GetColumnByName(const std::string& name) const;

  /// \brief Retrieve an array's internaldata from the record batch
  /// \param[in] i field index, does not boundscheck
  /// \return an internal ArrayData object
  virtual std::shared_ptr<ArrayData> column_data(int i) const = 0;

  /// \brief Add column to the record batch, producing a new RecordBatch
  ///
  /// \param[in] i field index, which will be boundschecked
  /// \param[in] field field to be added
  /// \param[in] column column to be added
  /// \param[out] out record batch with column added
  virtual Status AddColumn(int i, const std::shared_ptr<Field>& field,
                           const std::shared_ptr<Array>& column,
                           std::shared_ptr<RecordBatch>* out) const = 0;

  /// \brief Add new nullable column to the record batch, producing a new
  /// RecordBatch.
  ///
  /// For non-nullable columns, use the Field-based version of this method.
  ///
  /// \param[in] i field index, which will be boundschecked
  /// \param[in] field_name name of field to be added
  /// \param[in] column column to be added
  /// \param[out] out record batch with column added
  virtual Status AddColumn(int i, const std::string& field_name,
                           const std::shared_ptr<Array>& column,
                           std::shared_ptr<RecordBatch>* out) const;

  /// \brief Remove column from the record batch, producing a new RecordBatch
  ///
  /// \param[in] i field index, does boundscheck
  /// \param[out] out record batch with column removed
  virtual Status RemoveColumn(int i, std::shared_ptr<RecordBatch>* out) const = 0;

  virtual std::shared_ptr<RecordBatch> ReplaceSchemaMetadata(
      const std::shared_ptr<const KeyValueMetadata>& metadata) const = 0;

  /// \brief Name in i-th column
  const std::string& column_name(int i) const;

  /// \return the number of columns in the table
  int num_columns() const;

  /// \return the number of rows (the corresponding length of each column)
  int64_t num_rows() const { return num_rows_; }

  /// \brief Slice each of the arrays in the record batch
  /// \param[in] offset the starting offset to slice, through end of batch
  /// \return new record batch
  virtual std::shared_ptr<RecordBatch> Slice(int64_t offset) const;

  /// \brief Slice each of the arrays in the record batch
  /// \param[in] offset the starting offset to slice
  /// \param[in] length the number of elements to slice from offset
  /// \return new record batch
  virtual std::shared_ptr<RecordBatch> Slice(int64_t offset, int64_t length) const = 0;

  /// \brief Check for schema or length inconsistencies
  /// \return Status
  virtual Status Validate() const;

 protected:
  RecordBatch(const std::shared_ptr<Schema>& schema, int64_t num_rows);

  std::shared_ptr<Schema> schema_;
  int64_t num_rows_;

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(RecordBatch);
};

/// \brief Abstract interface for reading stream of record batches
class ARROW_EXPORT RecordBatchReader {
 public:
  virtual ~RecordBatchReader();

  /// \return the shared schema of the record batches in the stream
  virtual std::shared_ptr<Schema> schema() const = 0;

  /// \brief Read the next record batch in the stream. Return null for batch
  /// when reaching end of stream
  ///
  /// \param[out] batch the next loaded batch, null at end of stream
  /// \return Status
  virtual Status ReadNext(std::shared_ptr<RecordBatch>* batch) = 0;

  /// \brief Consume entire stream as a vector of record batches
  Status ReadAll(std::vector<std::shared_ptr<RecordBatch>>* batches);

  /// \brief Read all batches and concatenate as arrow::Table
  Status ReadAll(std::shared_ptr<Table>* table);
};

}  // namespace arrow
