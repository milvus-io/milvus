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

#include "scheduler/resource/DiskResource.h"

#include <string>
#include <utility>

namespace milvus {
namespace scheduler {

std::ostream&
operator<<(std::ostream& out, const DiskResource& resource) {
    out << resource.Dump();
    return out;
}

DiskResource::DiskResource(std::string name, uint64_t device_id, bool enable_executor)
    : Resource(std::move(name), ResourceType::DISK, device_id, enable_executor) {
}

void
DiskResource::LoadFile(TaskPtr task) {
}

void
DiskResource::Process(TaskPtr task) {
}

}  // namespace scheduler
}  // namespace milvus
