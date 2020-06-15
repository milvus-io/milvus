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

#include "db/snapshot/ResourceTypes.h"
#include "db/snapshot/Resources.h"
#include "db/snapshot/Utils.h"
#include "utils/Status.h"

#include <stdlib.h>
#include <time.h>
#include <any>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <thread>
#include <tuple>
#include <typeindex>
#include <typeinfo>
#include <unordered_map>
#include <utility>
#include <vector>

namespace milvus {
namespace engine {
namespace snapshot {

class Store {
 public:
    using MockIDST =
        std::tuple<ID_TYPE, ID_TYPE, ID_TYPE, ID_TYPE, ID_TYPE, ID_TYPE, ID_TYPE, ID_TYPE, ID_TYPE, ID_TYPE, ID_TYPE>;
    using MockResourcesT = std::tuple<CollectionCommit::MapT, Collection::MapT, SchemaCommit::MapT, FieldCommit::MapT,
                                      Field::MapT, FieldElement::MapT, PartitionCommit::MapT, Partition::MapT,
                                      SegmentCommit::MapT, Segment::MapT, SegmentFile::MapT>;

    static Store&
    GetInstance() {
        static Store store;
        return store;
    }

    template <typename... ResourceT>
    bool
    DoCommit(ResourceT&&... resources) {
        auto t = std::make_tuple(std::forward<ResourceT>(resources)...);
        auto& t_size = std::tuple_size<decltype(t)>::value;
        if (t_size == 0)
            return false;
        StartTransanction();
        std::apply([this](auto&&... resource) { ((std::cout << CommitResource(resource) << "\n"), ...); }, t);
        FinishTransaction();
        return true;
    }

    template <typename OpT>
    Status
    DoCommitOperation(OpT& op) {
        for (auto& step_v : op.GetSteps()) {
            auto id = ProcessOperationStep(step_v);
            op.SetStepResult(id);
        }
        return Status::OK();
    }

    template <typename OpT>
    void
    Apply(OpT& op) {
        op.ApplyToStore(*this);
    }

    void
    StartTransanction() {
    }
    void
    FinishTransaction() {
    }

    template <typename ResourceT>
    bool
    CommitResource(ResourceT&& resource) {
        std::cout << "Commit " << resource.Name << " " << resource.GetID() << std::endl;
        auto res = CreateResource<typename std::remove_reference<ResourceT>::type>(std::move(resource));
        if (!res)
            return false;
        return true;
    }

    template <typename ResourceT>
    Status
    GetResource(ID_TYPE id, typename ResourceT::Ptr& return_v) {
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        auto& resources = std::get<Index<typename ResourceT::MapT, MockResourcesT>::value>(resources_);
        auto it = resources.find(id);
        if (it == resources.end()) {
            /* std::cout << "Can't find " << ResourceT::Name << " " << id << " in ("; */
            /* for (auto& i : resources) { */
            /*     std::cout << i.first << ","; */
            /* } */
            /* std::cout << ")"; */
            return Status(SS_NOT_FOUND_ERROR, "DB resource not found");
        }
        auto& c = it->second;
        return_v = std::make_shared<ResourceT>(*c);
        /* std::cout << "<<< [Load] " << ResourceT::Name << " " << id
         * << " IsActive=" << return_v->IsActive() << std::endl; */
        return Status::OK();
    }

    Status
    GetCollection(const std::string& name, CollectionPtr& return_v) {
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        auto it = name_ids_.find(name);
        if (it == name_ids_.end()) {
            return Status(SS_NOT_FOUND_ERROR, "DB resource not found");
        }
        auto& id = it->second;
        lock.unlock();
        return GetResource<Collection>(id, return_v);
    }

    Status
    RemoveCollection(ID_TYPE id) {
        std::unique_lock<std::shared_timed_mutex> lock(mutex_);
        auto& resources = std::get<Collection::MapT>(resources_);
        auto it = resources.find(id);
        if (it == resources.end()) {
            return Status(SS_NOT_FOUND_ERROR, "DB resource not found");
        }

        auto name = it->second->GetName();
        resources.erase(it);
        name_ids_.erase(name);
        /* std::cout << ">>> [Remove] Collection " << id << std::endl; */
        return Status::OK();
    }

    template <typename ResourceT>
    Status
    RemoveResource(ID_TYPE id) {
        std::unique_lock<std::shared_timed_mutex> lock(mutex_);
        auto& resources = std::get<Index<typename ResourceT::MapT, MockResourcesT>::value>(resources_);
        auto it = resources.find(id);
        if (it == resources.end()) {
            return Status(SS_NOT_FOUND_ERROR, "DB resource not found");
        }

        resources.erase(it);
        /* std::cout << ">>> [Remove] " << ResourceT::Name << " " << id << std::endl; */
        return Status::OK();
    }

    IDS_TYPE
    AllActiveCollectionIds(bool reversed = true) const {
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        IDS_TYPE ids;
        auto& resources = std::get<Collection::MapT>(resources_);
        if (!reversed) {
            for (auto& kv : resources) {
                ids.push_back(kv.first);
            }
        } else {
            for (auto kv = resources.rbegin(); kv != resources.rend(); ++kv) {
                ids.push_back(kv->first);
            }
        }
        return ids;
    }

    IDS_TYPE
    AllActiveCollectionCommitIds(ID_TYPE collection_id, bool reversed = true) const {
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        IDS_TYPE ids;
        auto& resources = std::get<CollectionCommit::MapT>(resources_);
        if (!reversed) {
            for (auto& kv : resources) {
                if (kv.second->GetCollectionId() == collection_id) {
                    ids.push_back(kv.first);
                }
            }
        } else {
            for (auto kv = resources.rbegin(); kv != resources.rend(); ++kv) {
                if (kv->second->GetCollectionId() == collection_id) {
                    ids.push_back(kv->first);
                }
            }
        }
        return ids;
    }

    Status
    CreateCollection(Collection&& collection, CollectionPtr& return_v) {
        std::unique_lock<std::shared_timed_mutex> lock(mutex_);
        auto& resources = std::get<Collection::MapT>(resources_);
        if (!collection.HasAssigned() && (name_ids_.find(collection.GetName()) != name_ids_.end()) &&
            (resources[name_ids_[collection.GetName()]]->IsActive()) && !collection.IsDeactive()) {
            return Status(SS_DUPLICATED_ERROR, "Duplcated");
        }
        auto c = std::make_shared<Collection>(collection);
        auto& id = std::get<Index<Collection::MapT, MockResourcesT>::value>(ids_);
        c->SetID(++id);
        c->ResetCnt();
        resources[c->GetID()] = c;
        name_ids_[c->GetName()] = c->GetID();
        lock.unlock();
        GetResource<Collection>(c->GetID(), return_v);
        return Status::OK();
    }

    template <typename ResourceT>
    Status
    UpdateResource(ResourceT&& resource, typename ResourceT::Ptr& return_v) {
        std::unique_lock<std::shared_timed_mutex> lock(mutex_);
        auto& resources = std::get<typename ResourceT::MapT>(resources_);
        auto res = std::make_shared<ResourceT>(resource);
        auto& id = std::get<Index<typename ResourceT::MapT, MockResourcesT>::value>(ids_);
        res->ResetCnt();
        resources[res->GetID()] = res;
        lock.unlock();
        GetResource<ResourceT>(res->GetID(), return_v);
        return Status::OK();
    }

    template <typename ResourceT>
    Status
    CreateResource(ResourceT&& resource, typename ResourceT::Ptr& return_v) {
        if (resource.HasAssigned()) {
            return UpdateResource<ResourceT>(std::move(resource), return_v);
        }
        std::unique_lock<std::shared_timed_mutex> lock(mutex_);
        auto& resources = std::get<typename ResourceT::MapT>(resources_);
        auto res = std::make_shared<ResourceT>(resource);
        auto& id = std::get<Index<typename ResourceT::MapT, MockResourcesT>::value>(ids_);
        res->SetID(++id);
        res->ResetCnt();
        resources[res->GetID()] = res;
        lock.unlock();
        auto status = GetResource<ResourceT>(res->GetID(), return_v);
        /* std::cout << ">>> [Create] " << ResourceT::Name << " " << id << std::endl; */
        return Status::OK();
    }

    void
    DoReset() {
        ids_ = MockIDST();
        resources_ = MockResourcesT();
        name_ids_.clear();
    }

    void
    Mock() {
        DoReset();
        DoMock();
    }

 private:
    ID_TYPE
    ProcessOperationStep(const std::any& step_v) {
        if (const auto it = any_flush_vistors_.find(std::type_index(step_v.type())); it != any_flush_vistors_.cend()) {
            return it->second(step_v);
        } else {
            std::cerr << "Unregisted step type " << std::quoted(step_v.type().name());
            return 0;
        }
    }

    template <class T, class F>
    inline std::pair<const std::type_index, std::function<ID_TYPE(std::any const&)>>
    to_any_visitor(F const& f) {
        return {std::type_index(typeid(T)), [g = f](std::any const& a) -> ID_TYPE {
                    if constexpr (std::is_void_v<T>)
                        return g();
                    else
                        return g(std::any_cast<T const&>(a));
                }};
    }

    template <class T, class F>
    inline void
    register_any_visitor(F const& f) {
        std::cout << "Register visitor for type " << std::quoted(typeid(T).name()) << '\n';
        any_flush_vistors_.insert(to_any_visitor<T>(f));
    }

    Store() {
        register_any_visitor<Collection::Ptr>([this](auto c) {
            CollectionPtr n;
            CreateResource<Collection>(Collection(*c), n);
            return n->GetID();
        });
        register_any_visitor<CollectionCommit::Ptr>([this](auto c) {
            using T = CollectionCommit;
            using PtrT = typename T::Ptr;
            PtrT n;
            CreateResource<T>(T(*c), n);
            return n->GetID();
        });
        register_any_visitor<SchemaCommit::Ptr>([this](auto c) {
            using T = SchemaCommit;
            using PtrT = typename T::Ptr;
            PtrT n;
            CreateResource<T>(T(*c), n);
            return n->GetID();
        });
        register_any_visitor<FieldCommit::Ptr>([this](auto c) {
            using T = FieldCommit;
            using PtrT = typename T::Ptr;
            PtrT n;
            CreateResource<T>(T(*c), n);
            return n->GetID();
        });
        register_any_visitor<Field::Ptr>([this](auto c) {
            using T = Field;
            using PtrT = typename T::Ptr;
            PtrT n;
            CreateResource<T>(T(*c), n);
            return n->GetID();
        });
        register_any_visitor<FieldElement::Ptr>([this](auto c) {
            using T = FieldElement;
            using PtrT = typename T::Ptr;
            PtrT n;
            CreateResource<T>(T(*c), n);
            return n->GetID();
        });
        register_any_visitor<PartitionCommit::Ptr>([this](auto c) {
            using T = PartitionCommit;
            using PtrT = typename T::Ptr;
            PtrT n;
            CreateResource<T>(T(*c), n);
            return n->GetID();
        });
        register_any_visitor<Partition::Ptr>([this](auto c) {
            using T = Partition;
            using PtrT = typename T::Ptr;
            PtrT n;
            CreateResource<T>(T(*c), n);
            return n->GetID();
        });
        register_any_visitor<Segment::Ptr>([this](auto c) {
            using T = Segment;
            using PtrT = typename T::Ptr;
            PtrT n;
            CreateResource<T>(T(*c), n);
            return n->GetID();
        });
        register_any_visitor<SegmentCommit::Ptr>([this](auto c) {
            using T = SegmentCommit;
            using PtrT = typename T::Ptr;
            PtrT n;
            CreateResource<T>(T(*c), n);
            return n->GetID();
        });
        register_any_visitor<SegmentFile::Ptr>([this](auto c) {
            using T = SegmentFile;
            using PtrT = typename T::Ptr;
            PtrT n;
            CreateResource<T>(T(*c), n);
            return n->GetID();
        });
    }

    void
    DoMock() {
        Status status;
        unsigned int seed = 123;
        auto random = rand_r(&seed) % 2 + 4;
        std::vector<std::any> all_records;
        for (auto i = 1; i <= random; i++) {
            std::stringstream name;
            name << "c_" << std::get<Index<Collection::MapT, MockResourcesT>::value>(ids_) + 1;

            auto tc = Collection(name.str());
            tc.Activate();
            CollectionPtr c;
            CreateCollection(std::move(tc), c);
            all_records.push_back(c);

            MappingT schema_c_m;
            auto random_fields = rand_r(&seed) % 2 + 1;
            for (auto fi = 1; fi <= random_fields; ++fi) {
                std::stringstream fname;
                fname << "f_" << fi << "_" << std::get<Index<Field::MapT, MockResourcesT>::value>(ids_) + 1;
                FieldPtr field;
                CreateResource<Field>(Field(fname.str(), fi), field);
                all_records.push_back(field);
                MappingT f_c_m = {};

                auto random_elements = rand_r(&seed) % 2 + 2;
                for (auto fei = 1; fei <= random_elements; ++fei) {
                    std::stringstream fename;
                    fename << "fe_" << fei << "_";
                    fename << std::get<Index<FieldElement::MapT, MockResourcesT>::value>(ids_) + 1;

                    FieldElementPtr element;
                    CreateResource<FieldElement>(FieldElement(c->GetID(), field->GetID(), fename.str(), fei), element);
                    all_records.push_back(element);
                    f_c_m.insert(element->GetID());
                }
                FieldCommitPtr f_c;
                CreateResource<FieldCommit>(FieldCommit(c->GetID(), field->GetID(), f_c_m), f_c);
                all_records.push_back(f_c);
                schema_c_m.insert(f_c->GetID());
            }

            SchemaCommitPtr schema;
            CreateResource<SchemaCommit>(SchemaCommit(c->GetID(), schema_c_m), schema);
            all_records.push_back(schema);

            auto random_partitions = rand_r(&seed) % 2 + 1;
            MappingT c_c_m;
            for (auto pi = 1; pi <= random_partitions; ++pi) {
                std::stringstream pname;
                pname << "p_" << i << "_" << std::get<Index<Partition::MapT, MockResourcesT>::value>(ids_) + 1;
                PartitionPtr p;
                CreateResource<Partition>(Partition(pname.str(), c->GetID()), p);
                all_records.push_back(p);

                auto random_segments = rand_r(&seed) % 2 + 1;
                MappingT p_c_m;
                for (auto si = 1; si <= random_segments; ++si) {
                    SegmentPtr s;
                    CreateResource<Segment>(Segment(p->GetID(), si), s);
                    all_records.push_back(s);
                    auto& schema_m = schema->GetMappings();
                    MappingT s_c_m;
                    for (auto field_commit_id : schema_m) {
                        auto& field_commit = std::get<FieldCommit::MapT>(resources_)[field_commit_id];
                        auto& f_c_m = field_commit->GetMappings();
                        for (auto field_element_id : f_c_m) {
                            SegmentFilePtr sf;
                            CreateResource<SegmentFile>(SegmentFile(p->GetID(), s->GetID(), field_commit_id), sf);
                            all_records.push_back(sf);

                            s_c_m.insert(sf->GetID());
                        }
                    }
                    SegmentCommitPtr s_c;
                    CreateResource<SegmentCommit>(SegmentCommit(schema->GetID(), p->GetID(), s->GetID(), s_c_m), s_c);
                    all_records.push_back(s_c);
                    p_c_m.insert(s_c->GetID());
                }
                PartitionCommitPtr p_c;
                CreateResource<PartitionCommit>(PartitionCommit(c->GetID(), p->GetID(), p_c_m), p_c);
                all_records.push_back(p_c);
                c_c_m.insert(p_c->GetID());
            }
            CollectionCommitPtr c_c;
            CreateResource<CollectionCommit>(CollectionCommit(c->GetID(), schema->GetID(), c_c_m), c_c);
            all_records.push_back(c_c);
        }
        for (auto& record : all_records) {
            if (record.type() == typeid(std::shared_ptr<Collection>)) {
                const auto& r = std::any_cast<std::shared_ptr<Collection>>(record);
                r->Activate();
            } else if (record.type() == typeid(std::shared_ptr<CollectionCommit>)) {
                const auto& r = std::any_cast<std::shared_ptr<CollectionCommit>>(record);
                r->Activate();
            }
        }
    }

    MockResourcesT resources_;
    MockIDST ids_;
    std::map<std::string, ID_TYPE> name_ids_;
    std::unordered_map<std::type_index, std::function<ID_TYPE(std::any const&)>> any_flush_vistors_;
    mutable std::shared_timed_mutex mutex_;
};

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
