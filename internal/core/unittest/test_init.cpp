// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

//
// Created by Mike Dog on 2021/3/15.
//
#include "test_utils/DataGen.h"
#include <gtest/gtest.h>
#include "segcore/segcore_init_c.h"

TEST(Init, Naive) {
    using namespace milvus;
    using namespace milvus::segcore;
    SegcoreInit(NULL);
}