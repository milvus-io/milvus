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


#include "scheduler/ResourceFactory.h"
#include <gtest/gtest.h>

namespace {

namespace ms = milvus::scheduler;

} // namespace

TEST(ResourceFactoryTest, CREATE) {
    auto disk = ms::ResourceFactory::Create("ssd", "DISK", 0);
    auto cpu = ms::ResourceFactory::Create("cpu", "CPU", 0);
    auto gpu = ms::ResourceFactory::Create("gpu", "GPU", 0);

    ASSERT_TRUE(std::dynamic_pointer_cast<ms::DiskResource>(disk));
    ASSERT_TRUE(std::dynamic_pointer_cast<ms::CpuResource>(cpu));
    ASSERT_TRUE(std::dynamic_pointer_cast<ms::GpuResource>(gpu));
}
