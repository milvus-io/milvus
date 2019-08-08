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

// Public API for the "Feather" file format, originally created at
// http://github.com/wesm/feather

#ifndef ARROW_IPC_FEATHER_H
#define ARROW_IPC_FEATHER_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/util/visibility.h"

namespace arrow {

class Array;
class Column;
class Status;
class Table;

namespace io {

class OutputStream;
class RandomAccessFile;

}  // namespace io

namespace ipc {
namespace feather {

static constexpr const int kFeatherVersion = 2;

// ----------------------------------------------------------------------
// Metadata accessor classes

/// \class TableReader
/// \brief An interface for reading columns from Feather files
class ARROW_EXPORT TableReader {
 public:
  TableReader();
  ~TableReader();

  /// \brief Open a Feather file from a RandomAccessFile interface
  ///
  /// \param[in] source a RandomAccessFile instance
  /// \param[out] out the table reader
  static Status Open(const std::shared_ptr<io::RandomAccessFile>& source,
                     std::unique_ptr<TableReader>* out);

  /// \brief Optional table description
  ///
  /// This does not return a const std::string& because a string has to be
  /// copied from the flatbuffer to be able to return a non-flatbuffer type
  std::string GetDescription() const;

  /// \brief Return true if the table has a description field populated
  bool HasDescription() const;

  /// \brief Return the version number of the Feather file
  int version() const;

  /// \brief Return the number of rows in the file
  int64_t num_rows() const;

  /// \brief Return the number of columns in the file
  int64_t num_columns() const;

  std::string GetColumnName(int i) const;

  /// \brief Read a column from the file as an arrow::Column.
  ///
  /// \param[in] i the column index to read
  /// \param[out] out the returned column
  /// \return Status
  ///
  /// This function is zero-copy if the file source supports zero-copy reads
  Status GetColumn(int i, std::shared_ptr<Column>* out);

  /// \brief Read all columns from the file as an arrow::Table.
  ///
  /// \param[out] out the returned table
  /// \return Status
  ///
  /// This function is zero-copy if the file source supports zero-copy reads
  Status Read(std::shared_ptr<Table>* out);

  /// \brief Read only the specified columns from the file as an arrow::Table.
  ///
  /// \param[in] indices the column indices to read
  /// \param[out] out the returned table
  /// \return Status
  ///
  /// This function is zero-copy if the file source supports zero-copy reads
  Status Read(const std::vector<int>& indices, std::shared_ptr<Table>* out);

  /// \brief Read only the specified columns from the file as an arrow::Table.
  ///
  /// \param[in] names the column names to read
  /// \param[out] out the returned table
  /// \return Status
  ///
  /// This function is zero-copy if the file source supports zero-copy reads
  Status Read(const std::vector<std::string>& names, std::shared_ptr<Table>* out);

 private:
  class ARROW_NO_EXPORT TableReaderImpl;
  std::unique_ptr<TableReaderImpl> impl_;
};

/// \class TableWriter
/// \brief Interface for writing Feather files
class ARROW_EXPORT TableWriter {
 public:
  ~TableWriter();

  /// \brief Create a new TableWriter that writes to an OutputStream
  /// \param[in] stream an output stream
  /// \param[out] out the returned table writer
  /// \return Status
  static Status Open(const std::shared_ptr<io::OutputStream>& stream,
                     std::unique_ptr<TableWriter>* out);

  /// \brief Set the description field in the file metadata
  void SetDescription(const std::string& desc);

  /// \brief Set the number of rows in the file
  void SetNumRows(int64_t num_rows);

  /// \brief Append a column to the file
  ///
  /// \param[in] name the column name
  /// \param[in] values the column values as a contiguous arrow::Array
  /// \return Status
  Status Append(const std::string& name, const Array& values);

  /// \brief Write a table to the file
  ///
  /// \param[in] table the table to be written
  /// \return Status
  Status Write(const Table& table);

  /// \brief Finalize the file by writing the file metadata and footer
  /// \return Status
  Status Finalize();

 private:
  TableWriter();
  class ARROW_NO_EXPORT TableWriterImpl;
  std::unique_ptr<TableWriterImpl> impl_;
};

}  // namespace feather
}  // namespace ipc
}  // namespace arrow

#endif  // ARROW_IPC_FEATHER_H
