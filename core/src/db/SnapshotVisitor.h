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

class FieldElementVisitor {
 public:
    using Ptr = std::shared_ptr<FieldElementVisitor>;

    static Ptr
    Build(snapshot::ScopedSnapshotT ss, snapshot::ID_TYPE segment_id,
            snapshot::ID_TYPE field_element_id);

    FieldElementVisitor() = default;

    void
    SetFieldElement(snapshot::FieldElementPtr field_element) {
        field_element_ = field_element;
    }
    void
    SetFile(snapshot::SegmentFilePtr file) {
        file_ = file;
    }

    const snapshot::FieldElementPtr
    GetElement() const {
        return field_element_;
    }
    const snapshot::SegmentFilePtr
    GetFile() const {
        return file_;
    }

 protected:
    snapshot::FieldElementPtr field_element_;
    snapshot::SegmentFilePtr file_;
};

class SegmentFieldVisitor {
 public:
    using Ptr = std::shared_ptr<SegmentFieldVisitor>;
    using ElementT = typename FieldElementVisitor::Ptr;
    using ElementsMapT = std::map<snapshot::ID_TYPE, ElementT>;

    static Ptr
    Build(snapshot::ScopedSnapshotT ss, snapshot::ID_TYPE segment_id,
            snapshot::ID_TYPE field_id);

    SegmentFieldVisitor() = default;

    const ElementsMapT&
    GetElementVistors() const {
        return elements_map_;
    }
    const snapshot::FieldPtr&
    GetField() const {
        return field_;
    }

    void
    SetField(snapshot::FieldPtr field) {
        field_ = field;
    }

    void
    InsertElement(ElementT element) {
        elements_map_[element->GetElement()->GetID()] = element;
    }

 protected:
    ElementsMapT elements_map_;
    snapshot::FieldPtr field_;
};

class SegmentVisitor {
 public:
    using Ptr = std::shared_ptr<SegmentVisitor>;
    using FieldT = typename SegmentFieldVisitor::Ptr;
    using FieldsMapT = std::map<snapshot::ID_TYPE, FieldT>;

    static Ptr
    Build(snapshot::ScopedSnapshotT ss, snapshot::ID_TYPE segment_id);
    SegmentVisitor() = default;

    const FieldsMapT&
    GetFieldVisitors() const {
        return fields_map_;
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
    InsertField(FieldT field_visitor) {
        fields_map_[field_visitor->GetField()->GetID()] = field_visitor;
    }

    std::string
    ToString() const;

 protected:
    snapshot::SegmentPtr segment_;
    FieldsMapT fields_map_;
};

}  // namespace engine
}  // namespace milvus
