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

#ifndef ARROW_IO_TEST_COMMON_H
#define ARROW_IO_TEST_COMMON_H

#include <memory>
#include <string>
#include <vector>

#include "arrow/status.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace io {

class MemoryMappedFile;

ARROW_EXPORT
void AssertFileContents(const std::string& path, const std::string& contents);

ARROW_EXPORT bool FileExists(const std::string& path);

ARROW_EXPORT bool FileIsClosed(int fd);

ARROW_EXPORT
Status ZeroMemoryMap(MemoryMappedFile* file);

class ARROW_EXPORT MemoryMapFixture {
 public:
  void TearDown();

  void CreateFile(const std::string& path, int64_t size);

  Status InitMemoryMap(int64_t size, const std::string& path,
                       std::shared_ptr<MemoryMappedFile>* mmap);

  void AppendFile(const std::string& path);

 private:
  std::vector<std::string> tmp_files_;
};

}  // namespace io
}  // namespace arrow

#endif  // ARROW_IO_TEST_COMMON_H
