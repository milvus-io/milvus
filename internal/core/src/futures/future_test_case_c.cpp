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

#include "Future.h"
#include "Executor.h"

extern "C" CFuture*
future_create_test_case(int interval, int loop_cnt, int case_no) {
    auto future = milvus::futures::Future<int>::async(
        milvus::futures::getGlobalCPUExecutor(),
        0,
        [interval = interval, loop_cnt = loop_cnt, case_no = case_no](
            milvus::futures::CancellationToken token) {
            for (int i = 0; i < loop_cnt; i++) {
                if (case_no != 0) {
                    token.throwIfCancelled();
                }
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(interval));
            }
            switch (case_no) {
                case 1:
                    throw std::runtime_error("case 1");
                case 2:
                    throw folly::FutureNoExecutor();
                case 3:
                    throw milvus::SegcoreError(milvus::NotImplemented,
                                               "case 3");
            }
            return new int(case_no);
        });
    return static_cast<CFuture*>(static_cast<void*>(
        static_cast<milvus::futures::IFuture*>(future.release())));
}
