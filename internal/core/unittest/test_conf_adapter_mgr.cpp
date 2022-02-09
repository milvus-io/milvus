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

#include <vector>
#include <thread>
#include <gtest/gtest.h>
#include <knowhere/index/vector_index/ConfAdapterMgr.h>

TEST(AdapterMgr, MultiThread) {
    auto run_case = [&]() {
        auto& ins = milvus::knowhere::AdapterMgr::GetInstance();
        auto adapter = ins.GetAdapter(milvus::knowhere::IndexEnum::INDEX_HNSW);
        ASSERT_TRUE(adapter != nullptr);
        ASSERT_ANY_THROW(ins.GetAdapter("not supported now!"));
    };

    size_t num = 4;
    std::vector<std::thread> threads;
    for (auto i = 0; i < num; i++) {
        threads.emplace_back(std::move(std::thread(run_case)));
    }

    for (auto i = 0; i < num; i++) {
        threads[i].join();
    }
}
