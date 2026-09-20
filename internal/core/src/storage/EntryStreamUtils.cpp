// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "storage/EntryStreamUtils.h"
#include "folly/coro/WithCancellation.h"

namespace milvus::storage {
folly::coro::Task<void>
ReadInputStreamExactlyAsync(milvus::InputStream& input,
                            uint64_t offset,
                            uint8_t* destination,
                            size_t bytes,
                            folly::CancellationToken token) {
    ThrowIfCancelled(token, "InputStream::ReadExactly");
    if (bytes == 0) {
        co_return;
    }
    // Cancellation stops new slices, but must not release this destination or
    // its admission lease until the stream operation (including retries) drains.
    auto result =
        co_await folly::coro::co_awaitTry(folly::coro::co_withCancellation(
            folly::CancellationToken{},
            input.ReadAtAsync(destination, offset, bytes)));
    ThrowIfCancelled(token, "InputStream::ReadExactly");
    const auto n = std::move(result).value();
    if (!(n == bytes)) {
        ThrowInfo(ErrorCode::FileReadFailed,
                  "Short async stream read: expected {}, got {}",
                  bytes,
                  n);
    }
}
}  // namespace milvus::storage
