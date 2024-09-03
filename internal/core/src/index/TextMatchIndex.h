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

#pragma once

#include <string>
#include <boost/filesystem.hpp>

#include "index/InvertedIndexTantivy.h"

namespace milvus::index {

using stdclock = std::chrono::high_resolution_clock;
class TextMatchIndex : public InvertedIndexTantivy<std::string> {
 public:
    // for growing segment.
    explicit TextMatchIndex(int64_t commit_interval_in_ms);
    // for sealed segment.
    explicit TextMatchIndex(const std::string& path);
    // for building/loading index.
    explicit TextMatchIndex(const storage::FileManagerContext& ctx);

 public:
    BinarySet
    Upload(const Config& config) override;

    void
    Load(const Config& config);

 public:
    void
    AddText(const std::string& text, int64_t offset);

    void
    AddTexts(size_t n, const std::string* texts, int64_t offset_begin);

    void
    Finish();

    void
    Commit();

    void
    Reload();

 public:
    void
    CreateReader();

    TargetBitmap
    MatchQuery(const std::string& query);

 private:
    bool
    shouldTriggerCommit();

 private:
    std::atomic<stdclock::time_point> last_commit_time_;
    int64_t commit_interval_in_ms_;
};
}  // namespace milvus::index
