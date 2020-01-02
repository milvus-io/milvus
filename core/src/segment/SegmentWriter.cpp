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

#include "SegmentWriter.h"

namespace milvus {
namespace segment {

SegmentWriter::SegmentWriter(const std::string& location) : location_(location) {
}

Status
SegmentWriter::AddVectors(const std::string& field_name, const void* vectors, const segment::doc_id_t* doc_ids,
                          size_t count) {
    return Status::OK();
}

Status
SegmentWriter::Serialize() {
    return Status::OK();
}

Status
SegmentWriter::Flush() {
    return Status::OK();
}

}  // namespace segment
}  // namespace milvus
