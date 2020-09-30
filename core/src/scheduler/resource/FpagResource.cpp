// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include "scheduler/resource/FpgaResource.h"

#include <utility>
#include "utils/Log.h"
namespace milvus {
namespace scheduler {

std::ostream&
operator<<(std::ostream& out, const FpgaResource& resource) {
    out << resource.Dump().dump();
    return out;
}

FpgaResource::FpgaResource(std::string name, uint64_t device_id, bool enable_executor)
    : Resource(std::move(name), ResourceType::FPGA, device_id, enable_executor) {
}

void
FpgaResource::LoadFile(TaskPtr task) {
    LOG_ENGINE_DEBUG_ << "jack:  LoadFile loadType cpu2fpga";
    task->Load(LoadType::CPU2FPGA, 0);
}

void
FpgaResource::Process(TaskPtr task) {
    LOG_ENGINE_DEBUG_ << "jack:  process loadType cpu2fpga";
    task->Execute();
}

}  // namespace scheduler
}  // namespace milvus
