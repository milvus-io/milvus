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

#include "db/meta/FilesHolder.h"
#include "db/snapshot/Snapshot.h"

#include <map>
#include <memory>
#include <set>
#include <string>

namespace milvus {
namespace engine {

class SnapshotVisitor {
 public:
    explicit SnapshotVisitor(snapshot::ScopedSnapshotT ss);
    explicit SnapshotVisitor(const std::string& collection_name);
    explicit SnapshotVisitor(snapshot::ID_TYPE collection_id);

    Status
    SegmentsToSearch(meta::FilesHolder& files_holder);

 protected:
    snapshot::ScopedSnapshotT ss_;
    Status status_;
};

class SegmentFileVisitor {
 public:
    using Ptr = std::shared_ptr<SegmentFileVisitor>;

    static Ptr
    Build(snapshot::ScopedSnapshotT ss, snapshot::ID_TYPE segment_file_id);

    SegmentFileVisitor() = default;

    const snapshot::SegmentFilePtr
    GetFile() const {
        return file_;
    }
    const snapshot::FieldPtr
    GetField() const {
        return field_;
    }
    const snapshot::FieldElementPtr
    GetFieldElement() const {
        return field_element_;
    }

    void
    SetFile(snapshot::SegmentFilePtr file) {
        file_ = file;
    }
    void
    SetField(snapshot::FieldPtr field) {
        field_ = field;
    }
    void
    SetFieldElement(snapshot::FieldElementPtr field_element) {
        field_element_ = field_element;
    }

 protected:
    snapshot::SegmentFilePtr file_;
    snapshot::FieldPtr field_;
    snapshot::FieldElementPtr field_element_;
};

class SegmentVisitor {
 public:
    using Ptr = std::shared_ptr<SegmentVisitor>;
    using FileT = typename SegmentFileVisitor::Ptr;
    using FilesMapT = std::map<snapshot::ID_TYPE, FileT>;

    static Ptr
    Build(snapshot::ScopedSnapshotT ss, snapshot::ID_TYPE segment_id);
    SegmentVisitor() = default;

    const FilesMapT&
    GetSegmentFiles() const {
        return files_map_;
    }
    const snapshot::SegmentPtr&
    GetSegment() const {
        return segment_;
    }

    void
    SetSegment(snapshot::SegmentPtr segment) {
        segment_ = segment;
    }
    void
    InsertSegmentFile(FileT segment_file) {
        files_map_[segment_file->GetFile()->GetID()] = segment_file;
    }

 protected:
    snapshot::SegmentPtr segment_;
    FilesMapT files_map_;
};

}  // namespace engine
}  // namespace milvus
