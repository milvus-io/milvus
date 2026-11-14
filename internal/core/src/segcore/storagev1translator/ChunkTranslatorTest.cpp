// Copyright (C) 2019-2025 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <chrono>
#include <future>
#include <thread>

#include "folly/ScopeGuard.h"
#include "folly/coro/BlockingWait.h"
#include "folly/coro/WithCancellation.h"
#include "folly/executors/CPUThreadPoolExecutor.h"
#include "gtest/gtest.h"
#include "segcore/storagev1translator/ChunkTranslator.h"
#include "storage/RemoteChunkManagerSingleton.h"
#include "storage/ThreadPools.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::segcore;
using namespace milvus::segcore::storagev1translator;

class ChunkTranslatorAwaitTest : public ::testing::TestWithParam<bool> {};

TEST_P(ChunkTranslatorAwaitTest, DownloadYieldsAndDrainsBeforeCancellation) {
    auto schema = std::make_shared<Schema>();
    const auto field = schema->AddDebugField("value", DataType::INT64);
    const auto dataset = DataGen(schema, 16);
    auto cm = storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    const auto load_info = PrepareInsertBinlog(
        kCollectionID, kPartitionID, kSegmentID, dataset, cm);
    const auto& info = load_info.field_infos.at(field.get());
    auto& pool = ThreadPools::GetExecutor(ThreadPoolPriority::LOW);
    const auto previous_threads = pool.numThreads();
    pool.setNumThreads(1);
    auto restore =
        folly::makeGuard([&] { pool.setNumThreads(previous_threads); });
    folly::CPUThreadPoolExecutor caller(1);
    for (const bool cancel : {false, true}) {
        ChunkTranslator translator(
            kSegmentID,
            (*schema)[field],
            FieldDataInfo(field.get(), 16, TestMmapPath),
            {{info.insert_files.front(), 16, info.memory_sizes.front()}},
            GetParam(),
            true,
            proto::common::LoadPriority::LOW,
            "disable",
            MmapChunkWritebackMode::Disabled);
        const std::vector<cachinglayer::cid_t> cids{0};
        std::promise<void> started;
        auto started_future = started.get_future();
        std::promise<void> release;
        auto released = release.get_future().share();
        pool.add([&] {
            started.set_value();
            released.wait();
        });
        folly::CancellationSource cancellation;
        std::future<
            std::vector<std::pair<cachinglayer::cid_t, std::unique_ptr<Chunk>>>>
            operation;
        auto cleanup = folly::makeGuard([&] { release.set_value(); });
        ASSERT_EQ(started_future.wait_for(std::chrono::seconds(5)),
                  std::future_status::ready);
        operation = std::async(std::launch::async, [&] {
            return folly::coro::blockingWait(folly::coro::co_withExecutor(
                &caller,
                folly::coro::co_withCancellation(
                    cancellation.getToken(),
                    translator.get_cells_async(nullptr, cids))));
        });
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (pool.getPendingTaskCount() == 0 &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        EXPECT_GT(pool.getPendingTaskCount(), 0);
        auto marker = std::make_shared<std::promise<void>>();
        auto marker_future = marker->get_future();
        caller.add([marker] { marker->set_value(); });
        EXPECT_EQ(marker_future.wait_for(std::chrono::seconds(2)),
                  std::future_status::ready);
        if (cancel) {
            cancellation.requestCancellation();
        }
        EXPECT_EQ(operation.wait_for(std::chrono::milliseconds(20)),
                  std::future_status::timeout);
        release.set_value();
        cleanup.dismiss();
        ASSERT_EQ(operation.wait_for(std::chrono::seconds(5)),
                  std::future_status::ready);
        if (cancel) {
            try {
                operation.get();
                FAIL() << "expected cancellation after issued download drains";
            } catch (const SegcoreError& error) {
                EXPECT_EQ(error.get_error_code(), ErrorCode::FollyCancel);
            }
        } else {
            auto cells = operation.get();
            ASSERT_EQ(cells.size(), 1);
            EXPECT_EQ(cells.front().first, 0);
            ASSERT_EQ(cells.front().second->RowNums(), 16);
            const auto* actual =
                reinterpret_cast<const int64_t*>(cells.front().second->RawData());
            const auto expected = dataset.get_col<int64_t>(field);
            for (size_t i = 0; i < expected.size(); ++i) {
                EXPECT_EQ(actual[i], expected[i]);
            }
        }
    }
}

INSTANTIATE_TEST_SUITE_P(ChunkTranslator,
                         ChunkTranslatorAwaitTest,
                         ::testing::Bool());
