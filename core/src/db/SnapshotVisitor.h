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
    SegmentsToSearch(snapshot::IDS_TYPE& segment_ids);

    Status
    SegmentsToIndex(const std::string& field_name, snapshot::IDS_TYPE& segment_ids, bool force_build);

    Status
    SegmentsToMerge(snapshot::IDS_TYPE& segment_ids);

 protected:
    snapshot::ScopedSnapshotT ss_;
    Status status_;
};

class SegmentFieldElementVisitor {
 public:
    using Ptr = std::shared_ptr<SegmentFieldElementVisitor>;

    static Ptr
    Build(snapshot::ScopedSnapshotT ss, snapshot::ID_TYPE segment_id, snapshot::ID_TYPE field_element_id);
    static Ptr
    Build(snapshot::ScopedSnapshotT ss, const snapshot::FieldElementPtr& field_element,
          const snapshot::SegmentPtr& segment, const snapshot::SegmentFilePtr& segment_file);

    SegmentFieldElementVisitor() = default;

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
using SegmentFieldElementVisitorPtr = std::shared_ptr<SegmentFieldElementVisitor>;

class SegmentFieldVisitor {
 public:
    using Ptr = std::shared_ptr<SegmentFieldVisitor>;
    using ElementT = typename SegmentFieldElementVisitor::Ptr;
    using ElementsMapT = std::map<snapshot::ID_TYPE, ElementT>;

    static Ptr
    Build(snapshot::ScopedSnapshotT ss, snapshot::ID_TYPE segment_id, snapshot::ID_TYPE field_id);
    static Ptr
    Build(snapshot::ScopedSnapshotT ss, const snapshot::FieldPtr& field, const snapshot::SegmentPtr& segment,
          const snapshot::SegmentFile::VecT& segment_files);

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

    const ElementT
    GetElementVisitor(const FieldElementType elem_type) const {
        for (auto& kv : elements_map_) {
            auto& ev = kv.second;
            if (ev->GetElement()->GetFEtype() == elem_type) {
                return ev;
            }
        }
        return nullptr;
    }

 protected:
    ElementsMapT elements_map_;
    snapshot::FieldPtr field_;
};
using SegmentFieldVisitorPtr = SegmentFieldVisitor::Ptr;

class SegmentVisitor {
 public:
    using Ptr = std::shared_ptr<SegmentVisitor>;
    using FieldVisitorT = typename SegmentFieldVisitor::Ptr;
    using IdMapT = std::map<snapshot::ID_TYPE, FieldVisitorT>;
    using NameMapT = std::map<std::string, FieldVisitorT>;

    static Ptr
    Build(snapshot::ScopedSnapshotT ss, snapshot::ID_TYPE segment_id);
    static Ptr
    Build(snapshot::ScopedSnapshotT ss, const snapshot::SegmentPtr& segment,
          const snapshot::SegmentFile::VecT& segment_files);

    explicit SegmentVisitor(snapshot::ScopedSnapshotT ss);

    const IdMapT&
    GetFieldVisitors() const {
        return id_map_;
    }

    FieldVisitorT
    GetFieldVisitor(snapshot::ID_TYPE field_id) const {
        auto it = id_map_.find(field_id);
        if (it == id_map_.end()) {
            return nullptr;
        }
        return it->second;
    }

    FieldVisitorT
    GetFieldVisitor(const std::string& field_name) const {
        auto it = name_map_.find(field_name);
        if (it == name_map_.end()) {
            return nullptr;
        }
        return it->second;
    }
    const snapshot::ScopedSnapshotT&
    GetSnapshot() const {
        return snapshot_;
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
    InsertField(FieldVisitorT field_visitor) {
        id_map_[field_visitor->GetField()->GetID()] = field_visitor;
        name_map_[field_visitor->GetField()->GetName()] = field_visitor;
    }

    std::string
    ToString() const;

 protected:
    snapshot::ScopedSnapshotT snapshot_;
    snapshot::SegmentPtr segment_;
    IdMapT id_map_;
    NameMapT name_map_;
};
using SegmentVisitorPtr = SegmentVisitor::Ptr;

}  // namespace engine
}  // namespace milvus
