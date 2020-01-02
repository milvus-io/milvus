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

#include "Vector.h"

#include "Vectors.h"

namespace milvus {
namespace segment {

Vector::Vector(void* data, size_t nbytes, int64_t* uids) : data_(data), nbytes_(nbytes), uids_(uids) {
}

Vector::Vector() {
}


void
Vector::SetData(void* data) {
    data_ = data;
}
void
Vector::SetNbytes(size_t nbytes) {
    nbytes_ = nbytes;
}
void
Vector::SetUids(int64_t* uids) {
    uids_ = uids;
}
void
Vector::SetCount(size_t count) {
    count_ = count;
}

void*
Vector::GetData() const {
    return data_;
}

size_t
Vector::GetNumBytes() const {
    return nbytes_;
}

int64_t*
Vector::GetUids() const {
    return uids_;
}

size_t
Vector::GetCount() const {
    return count_;
}

}  // namespace segment
}  // namespace milvus