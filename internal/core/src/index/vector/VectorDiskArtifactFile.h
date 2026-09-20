// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstddef>
#include <string>

namespace milvus::index {

enum class VectorDiskFileTransport {
    LegacySliced,
    RawUnsliced,
};

// A completed file in an immutable disk-vector artifact. Transport is recorded
// by the operation that produced the file and is never inferred from its name.
struct VectorDiskArtifactFile {
    std::string path;
    size_t size{0};
    VectorDiskFileTransport transport{VectorDiskFileTransport::LegacySliced};
};

}  // namespace milvus::index
