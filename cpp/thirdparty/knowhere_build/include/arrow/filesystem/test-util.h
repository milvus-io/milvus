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

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "arrow/filesystem/filesystem.h"

namespace arrow {
namespace fs {

static constexpr double kTimeSlack = 2.0;  // In seconds

ARROW_EXPORT
void AssertFileStats(const FileStats& st, const std::string& path, FileType type);

ARROW_EXPORT
void AssertFileStats(const FileStats& st, const std::string& path, FileType type,
                     TimePoint mtime);

ARROW_EXPORT
void AssertFileStats(const FileStats& st, const std::string& path, FileType type,
                     TimePoint mtime, int64_t size);

ARROW_EXPORT
void AssertFileStats(const FileStats& st, const std::string& path, FileType type,
                     int64_t size);

ARROW_EXPORT
void CreateFile(FileSystem* fs, const std::string& path, const std::string& data);

// Sort of vector of FileStats by lexicographic path order
ARROW_EXPORT
void SortStats(std::vector<FileStats>* stats);

template <typename Duration>
void AssertDurationBetween(Duration d, double min_secs, double max_secs) {
  auto seconds = std::chrono::duration_cast<std::chrono::duration<double>>(d);
  ASSERT_GE(seconds.count(), min_secs);
  ASSERT_LE(seconds.count(), max_secs);
}

// Generic tests for FileSystem implementations.
// To use this class, subclass both from it and ::testing::Test,
// implement GetEmptyFileSystem(), and use GENERIC_FS_TEST_FUNCTIONS()
// to define the various tests.
class ARROW_EXPORT GenericFileSystemTest {
 public:
  virtual ~GenericFileSystemTest();

  void TestEmpty();
  void TestCreateDir();
  void TestDeleteDir();
  void TestDeleteFile();
  void TestDeleteFiles();
  void TestMoveFile();
  void TestMoveDir();
  void TestCopyFile();
  void TestGetTargetStatsSingle();
  void TestGetTargetStatsVector();
  void TestGetTargetStatsSelector();
  void TestOpenOutputStream();
  void TestOpenAppendStream();
  void TestOpenInputStream();
  void TestOpenInputFile();

 protected:
  virtual std::shared_ptr<FileSystem> GetEmptyFileSystem() = 0;

  void TestEmpty(FileSystem* fs);
  void TestCreateDir(FileSystem* fs);
  void TestDeleteDir(FileSystem* fs);
  void TestDeleteFile(FileSystem* fs);
  void TestDeleteFiles(FileSystem* fs);
  void TestMoveFile(FileSystem* fs);
  void TestMoveDir(FileSystem* fs);
  void TestCopyFile(FileSystem* fs);
  void TestGetTargetStatsSingle(FileSystem* fs);
  void TestGetTargetStatsVector(FileSystem* fs);
  void TestGetTargetStatsSelector(FileSystem* fs);
  void TestOpenOutputStream(FileSystem* fs);
  void TestOpenAppendStream(FileSystem* fs);
  void TestOpenInputStream(FileSystem* fs);
  void TestOpenInputFile(FileSystem* fs);
};

#define GENERIC_FS_TEST_FUNCTION(TEST_MACRO, TEST_CLASS, NAME) \
  TEST_MACRO(TEST_CLASS, NAME) { this->Test##NAME(); }

#define GENERIC_FS_TEST_FUNCTIONS_MACROS(TEST_MACRO, TEST_CLASS)           \
  GENERIC_FS_TEST_FUNCTION(TEST_MACRO, TEST_CLASS, Empty)                  \
  GENERIC_FS_TEST_FUNCTION(TEST_MACRO, TEST_CLASS, CreateDir)              \
  GENERIC_FS_TEST_FUNCTION(TEST_MACRO, TEST_CLASS, DeleteDir)              \
  GENERIC_FS_TEST_FUNCTION(TEST_MACRO, TEST_CLASS, DeleteFile)             \
  GENERIC_FS_TEST_FUNCTION(TEST_MACRO, TEST_CLASS, DeleteFiles)            \
  GENERIC_FS_TEST_FUNCTION(TEST_MACRO, TEST_CLASS, MoveFile)               \
  GENERIC_FS_TEST_FUNCTION(TEST_MACRO, TEST_CLASS, MoveDir)                \
  GENERIC_FS_TEST_FUNCTION(TEST_MACRO, TEST_CLASS, CopyFile)               \
  GENERIC_FS_TEST_FUNCTION(TEST_MACRO, TEST_CLASS, GetTargetStatsSingle)   \
  GENERIC_FS_TEST_FUNCTION(TEST_MACRO, TEST_CLASS, GetTargetStatsVector)   \
  GENERIC_FS_TEST_FUNCTION(TEST_MACRO, TEST_CLASS, GetTargetStatsSelector) \
  GENERIC_FS_TEST_FUNCTION(TEST_MACRO, TEST_CLASS, OpenOutputStream)       \
  GENERIC_FS_TEST_FUNCTION(TEST_MACRO, TEST_CLASS, OpenAppendStream)       \
  GENERIC_FS_TEST_FUNCTION(TEST_MACRO, TEST_CLASS, OpenInputStream)        \
  GENERIC_FS_TEST_FUNCTION(TEST_MACRO, TEST_CLASS, OpenInputFile)

#define GENERIC_FS_TEST_FUNCTIONS(TEST_CLASS) \
  GENERIC_FS_TEST_FUNCTIONS_MACROS(TEST_F, TEST_CLASS)

#define GENERIC_FS_TYPED_TEST_FUNCTIONS(TEST_CLASS) \
  GENERIC_FS_TEST_FUNCTIONS_MACROS(TYPED_TEST, TEST_CLASS)

}  // namespace fs
}  // namespace arrow
