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

#include <stdlib.h>
#include <time.h>
#include <any>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <string>
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
    bool
    DoCommitOperation(OpT& op) {
        for (auto& step_v : op.GetSteps()) {
            auto id = ProcessOperationStep(step_v);
            op.SetStepResult(id);
        }
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
    std::shared_ptr<ResourceT>
    GetResource(ID_TYPE id) {
        auto& resources = std::get<Index<typename ResourceT::MapT, MockResourcesT>::value>(resources_);
        auto it = resources.find(id);
        if (it == resources.end()) {
            return nullptr;
        }
        auto& c = it->second;
        auto ret = std::make_shared<ResourceT>(*c);
        std::cout << "<<< [Load] " << ResourceT::Name << " " << id << " IsActive=" << ret->IsActive() << std::endl;
        return ret;
    }

    CollectionPtr
    GetCollection(const std::string& name) {
        auto it = name_collections_.find(name);
        if (it == name_collections_.end()) {
            return nullptr;
        }
        auto& c = it->second;
        auto ret = std::make_shared<Collection>(*c);
        std::cout << "<<< [Load] Collection " << name << std::endl;
        return ret;
    }

    bool
    RemoveCollection(ID_TYPE id) {
        auto& resources = std::get<Collection::MapT>(resources_);
        auto it = resources.find(id);
        if (it == resources.end()) {
            return false;
        }

        auto name = it->second->GetName();
        resources.erase(it);
        name_collections_.erase(name);
        std::cout << ">>> [Remove] Collection " << id << std::endl;
        return true;
    }

    template <typename ResourceT>
    bool
    RemoveResource(ID_TYPE id) {
        auto& resources = std::get<Index<typename ResourceT::MapT, MockResourcesT>::value>(resources_);
        auto it = resources.find(id);
        if (it == resources.end()) {
            return false;
        }

        resources.erase(it);
        std::cout << ">>> [Remove] " << ResourceT::Name << " " << id << std::endl;
        return true;
    }

    IDS_TYPE
    AllActiveCollectionIds(bool reversed = true) const {
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

    CollectionPtr
    CreateCollection(Collection&& collection) {
        auto& resources = std::get<Collection::MapT>(resources_);
        auto c = std::make_shared<Collection>(collection);
        auto& id = std::get<Index<Collection::MapT, MockResourcesT>::value>(ids_);
        c->SetID(++id);
        c->ResetCnt();
        resources[c->GetID()] = c;
        name_collections_[c->GetName()] = c;
        return GetResource<Collection>(c->GetID());
    }

    template <typename ResourceT>
    typename ResourceT::Ptr
    UpdateResource(ResourceT&& resource) {
        auto& resources = std::get<typename ResourceT::MapT>(resources_);
        auto res = std::make_shared<ResourceT>(resource);
        auto& id = std::get<Index<typename ResourceT::MapT, MockResourcesT>::value>(ids_);
        res->ResetCnt();
        resources[res->GetID()] = res;
        return GetResource<ResourceT>(res->GetID());
    }

    template <typename ResourceT>
    typename ResourceT::Ptr
    CreateResource(ResourceT&& resource) {
        if (resource.HasAssigned()) {
            return UpdateResource<ResourceT>(std::move(resource));
        }
        auto& resources = std::get<typename ResourceT::MapT>(resources_);
        auto res = std::make_shared<ResourceT>(resource);
        auto& id = std::get<Index<typename ResourceT::MapT, MockResourcesT>::value>(ids_);
        res->SetID(++id);
        res->ResetCnt();
        resources[res->GetID()] = res;
        return GetResource<ResourceT>(res->GetID());
    }

    /* CollectionPtr CreateCollection(const schema::CollectionSchemaPB& collection_schema) { */
    /*     auto collection = CreateCollection(Collection(collection_schema.name())); */
    /*     MappingT field_commit_ids = {}; */
    /*     for (auto i=0; i<collection_schema.fields_size(); ++i) { */
    /*         auto field_schema = collection_schema.fields(i); */
    /*         auto& field_name = field_schema.name(); */
    /*         auto& field_info = field_schema.info(); */
    /*         auto field_type = field_info.type(); */
    /*         auto field = CreateResource<Field>(Field(field_name, i)); */
    /*         MappingT element_ids = {}; */
    /*         auto raw_element = CreateResource<FieldElement>(FieldElement(collection->GetID(), */
    /*                     field->GetID(), "RAW", 1)); */
    /*         element_ids.insert(raw_element->GetID()); */
    /*         for(auto j=0; j<field_schema.elements_size(); ++j) { */
    /*             auto element_schema = field_schema.elements(j); */
    /*             auto& element_name = element_schema.name(); */
    /*             auto& element_info = element_schema.info(); */
    /*             auto element_type = element_info.type(); */
    /*             auto element = CreateResource<FieldElement>(FieldElement(collection->GetID(), field->GetID(), */
    /*                         element_name, element_type)); */
    /*             element_ids.insert(element->GetID()); */
    /*         } */
    /*         auto field_commit = CreateResource<FieldCommit>(FieldCommit(collection->GetID(),
     *         field->GetID(), element_ids)); */
    /*         field_commit_ids.insert(field_commit->GetID()); */
    /*     } */
    /*     auto schema_commit = CreateResource<SchemaCommit>(SchemaCommit(collection->GetID(), field_commit_ids)); */

    /*     MappingT empty_mappings = {}; */
    /*     auto partition = CreateResource<Partition>(Partition("_default", collection->GetID())); */
    /*     auto partition_commit = CreateResource<PartitionCommit>(PartitionCommit(collection->GetID(),
     *     partition->GetID(), */
    /*                 empty_mappings)); */
    /*     auto collection_commit = CreateResource<CollectionCommit>(CollectionCommit(collection->GetID(), */
    /*                 schema_commit->GetID(), {partition_commit->GetID()})); */
    /*     return collection; */
    /* } */

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
            auto n = CreateResource<Collection>(Collection(*c));
            return n->GetID();
        });
        register_any_visitor<CollectionCommit::Ptr>(
            [this](auto c) { return CreateResource<CollectionCommit>(CollectionCommit(*c))->GetID(); });
        /* register_any_visitor<SchemaCommit::Ptr>([this](auto c) { */
        /*     CreateResource<SchemaCommit>(SchemaCommit(*c)); */
        /* }); */
        /* register_any_visitor<FieldCommit::Ptr>([this](auto c) { */
        /*     CreateResource<FieldCommit>(FieldCommit(*c)); */
        /* }); */
        /* register_any_visitor<Field::Ptr>([this](auto c) { */
        /*     CreateResource<Field>(Field(*c)); */
        /* }); */
        /* register_any_visitor<FieldElement::Ptr>([this](auto c) { */
        /*     CreateResource<FieldElement>(FieldElement(*c)); */
        /* }); */
        register_any_visitor<PartitionCommit::Ptr>(
            [this](auto c) { return CreateResource<PartitionCommit>(PartitionCommit(*c))->GetID(); });
        /* register_any_visitor<Partition::Ptr>([this](auto c) { */
        /*     CreateResource<Partition>(Partition(*c)); */
        /* }); */
        register_any_visitor<Segment::Ptr>([this](auto c) { return CreateResource<Segment>(Segment(*c))->GetID(); });
        register_any_visitor<SegmentCommit::Ptr>(
            [this](auto c) { return CreateResource<SegmentCommit>(SegmentCommit(*c))->GetID(); });
        register_any_visitor<SegmentFile::Ptr>([this](auto c) {
            auto n = CreateResource<SegmentFile>(SegmentFile(*c));
            return n->GetID();
        });
    }

    void
    DoMock() {
        unsigned int seed = 123;
        auto random = rand_r(&seed) % 2 + 4;
        std::vector<std::any> all_records;
        for (auto i = 1; i <= random; i++) {
            std::stringstream name;
            name << "c_" << std::get<Index<Collection::MapT, MockResourcesT>::value>(ids_) + 1;

            auto c = CreateCollection(Collection(name.str()));
            all_records.push_back(c);

            MappingT schema_c_m;
            auto random_fields = rand_r(&seed) % 2 + 1;
            for (auto fi = 1; fi <= random_fields; ++fi) {
                std::stringstream fname;
                fname << "f_" << fi << "_" << std::get<Index<Field::MapT, MockResourcesT>::value>(ids_) + 1;
                auto field = CreateResource<Field>(Field(fname.str(), fi));
                all_records.push_back(field);
                MappingT f_c_m = {};

                auto random_elements = rand_r(&seed) % 2 + 2;
                for (auto fei = 1; fei <= random_elements; ++fei) {
                    std::stringstream fename;
                    fename << "fe_" << fei << "_";
                    fename << std::get<Index<FieldElement::MapT, MockResourcesT>::value>(ids_) + 1;

                    auto element =
                        CreateResource<FieldElement>(FieldElement(c->GetID(), field->GetID(), fename.str(), fei));
                    all_records.push_back(element);
                    f_c_m.insert(element->GetID());
                }
                auto f_c = CreateResource<FieldCommit>(FieldCommit(c->GetID(), field->GetID(), f_c_m));
                all_records.push_back(f_c);
                schema_c_m.insert(f_c->GetID());
            }

            auto schema = CreateResource<SchemaCommit>(SchemaCommit(c->GetID(), schema_c_m));
            all_records.push_back(schema);

            auto random_partitions = rand_r(&seed) % 2 + 1;
            MappingT c_c_m;
            for (auto pi = 1; pi <= random_partitions; ++pi) {
                std::stringstream pname;
                pname << "p_" << i << "_" << std::get<Index<Partition::MapT, MockResourcesT>::value>(ids_) + 1;
                auto p = CreateResource<Partition>(Partition(pname.str(), c->GetID()));
                all_records.push_back(p);

                auto random_segments = rand_r(&seed) % 2 + 1;
                MappingT p_c_m;
                for (auto si = 1; si <= random_segments; ++si) {
                    auto s = CreateResource<Segment>(Segment(p->GetID(), si));
                    all_records.push_back(s);
                    auto& schema_m = schema->GetMappings();
                    MappingT s_c_m;
                    for (auto field_commit_id : schema_m) {
                        auto& field_commit = std::get<FieldCommit::MapT>(resources_)[field_commit_id];
                        auto& f_c_m = field_commit->GetMappings();
                        for (auto field_element_id : f_c_m) {
                            auto sf = CreateResource<SegmentFile>(SegmentFile(p->GetID(), s->GetID(), field_commit_id));
                            all_records.push_back(sf);

                            s_c_m.insert(sf->GetID());
                        }
                    }
                    auto s_c =
                        CreateResource<SegmentCommit>(SegmentCommit(schema->GetID(), p->GetID(), s->GetID(), s_c_m));
                    all_records.push_back(s_c);
                    p_c_m.insert(s_c->GetID());
                }
                auto p_c = CreateResource<PartitionCommit>(PartitionCommit(c->GetID(), p->GetID(), p_c_m));
                all_records.push_back(p_c);
                c_c_m.insert(p_c->GetID());
            }
            auto c_c = CreateResource<CollectionCommit>(CollectionCommit(c->GetID(), schema->GetID(), c_c_m));
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

    void
    DoReset() {
        ids_ = MockIDST();
        resources_ = MockResourcesT();
        name_collections_.clear();
    }

    MockResourcesT resources_;
    MockIDST ids_;
    std::map<std::string, CollectionPtr> name_collections_;
    std::unordered_map<std::type_index, std::function<ID_TYPE(std::any const&)>> any_flush_vistors_;
};

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
