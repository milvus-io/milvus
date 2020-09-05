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
#include <fiu-local.h>
#include <fiu-control.h>
#include <gtest/gtest.h>
#include <src/scheduler/task/BuildIndexTask.h>
#include <src/scheduler/task/SearchTask.h>
#include <src/scheduler/optimizer/FaissIVFPass.h>

#include "scheduler/optimizer/BuildIndexPass.h"
#include "scheduler/optimizer/FaissFlatPass.h"
#include "scheduler/optimizer/FaissIVFPass.h"

namespace milvus {
namespace scheduler {

#ifdef MILVUS_GPU_VERSION
TEST(Action_Test, TESTACTION) {
}

#endif

}  // namespace scheduler
}  // namespace milvus
