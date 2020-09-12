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

#pragma once

#include "db/meta/Meta.h"
#include "utils/Status.h"

#include <map>
#include <mutex>
#include <set>
#include <string>

namespace milvus {
namespace engine {
namespace meta {

class FilesHolder {
 public:
    FilesHolder();
    virtual ~FilesHolder();

    Status
    MarkFile(const meta::SegmentSchema& file);

    Status
    MarkFiles(const meta::SegmentsSchema& files);

    Status
    UnmarkFile(const meta::SegmentSchema& file);

    Status
    UnmarkFiles(const meta::SegmentsSchema& files);

    const milvus::engine::meta::SegmentsSchema&
    HoldFiles() const {
        return hold_files_;
    }

    milvus::engine::meta::SegmentsSchema&
    HoldFiles() {
        return hold_files_;
    }

    void
    ReleaseFiles();

    static bool
    CanBeDeleted(const meta::SegmentSchema& file);

    static void
    PrintInfo();

 private:
    class OngoingFileChecker {
     public:
        static OngoingFileChecker&
        GetInstance();

        Status
        MarkOngoingFile(const meta::SegmentSchema& file);

        Status
        MarkOngoingFiles(const meta::SegmentsSchema& files);

        Status
        UnmarkOngoingFile(const meta::SegmentSchema& file);

        Status
        UnmarkOngoingFiles(const meta::SegmentsSchema& files);

        bool
        CanBeDeleted(const meta::SegmentSchema& file);

        void
        PrintInfo();

     private:
        Status
        MarkOngoingFileNoLock(const meta::SegmentSchema& file);

        Status
        UnmarkOngoingFileNoLock(const meta::SegmentSchema& file);

     private:
        std::mutex mutex_;
        meta::Table2FileRef ongoing_files_;  // collection id mapping to (file id mapping to ongoing ref-count)
    };

 private:
    Status
    MarkFileInternal(const meta::SegmentSchema& file);

    Status
    UnmarkFileInternal(const meta::SegmentSchema& file);

 private:
    std::mutex mutex_;
    milvus::engine::meta::SegmentsSchema hold_files_;
    std::set<uint64_t> unique_ids_;
};

}  // namespace meta
}  // namespace engine
}  // namespace milvus
