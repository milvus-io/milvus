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

#include "db/impl/DBImp.h"
#include "db/snapshot/ResourceContext.h"
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
#include <set>
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

    template <typename OpT>
    Status
    ApplyOperation(OpT& op) {
        auto session = DBImp::GetInstance().CreateSession();
        std::apply(
            [&](auto&... step_context_set) {
                std::size_t n{0};
                ((ApplyOpStep(op, n++, step_context_set, session)), ...);
            },
            op.GetStepHolders());

        ID_TYPE result_id;
        auto status = session->Commit(result_id);
        if (status.ok()) {
            op.SetStepResult(result_id);
        }

        return status;
    }

    template <typename T, typename OpT>
    void
    ApplyOpStep(OpT& op, size_t pos, std::set<std::shared_ptr<ResourceContext<T>>>& step_context_set, const SessionPtr& session) {
//        typename T::Ptr ret;
        for (auto& step_context : step_context_set) {
//            auto res = step_context->Resource();
//            CreateResource<T>(T(*res), ret);
//            res->SetID(ret->GetID());
            session->Apply<T>(step_context);
        }
        if (pos == op.GetPos()) {
            session->ResultPos();
//            op.SetStepResult(ret->GetID());
        }
    }

    template <typename OpT>
    void
    Apply(OpT& op) {
        op.ApplyToStore(*this);
    }

    template <typename ResourceT>
    Status
    GetResource(ID_TYPE id, typename ResourceT::Ptr& return_v) {
//        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        auto status = DBImp::GetInstance().Select<ResourceT>(id, return_v);

        if (!status.ok()) {
            return status;
        }

        if (return_v == nullptr) {
            std::string err = "Cannot select resource " + std::string(ResourceT::Name) + " from DB: No resource which id = " + std::to_string(id);
            return Status(SERVER_UNEXPECTED_ERROR, err);
        }

        return Status::OK();
    }

    Status
    GetCollection(const std::string& name, CollectionPtr& return_v) {

//        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        auto it = name_ids_.find(name);
        if (it == name_ids_.end()) {
            return Status(SS_NOT_FOUND_ERROR, "DB resource not found");
        }
        auto& id = it->second;
//        lock.unlock();
        return GetResource<Collection>(id, return_v);
    }

    Status
    RemoveCollection(ID_TYPE id) {
//        std::unique_lock<std::shared_timed_mutex> lock(mutex_);
        auto rc_ctx_p = ResourceContextBuilder<Collection>().SetTable(Collection::Name)
            .SetOp(oDelete).SetID(id).CreatePtr();

        int64_t result_id;
        return DBImp::GetInstance().Apply<Collection>(rc_ctx_p, result_id);
    }

    template <typename ResourceT>
    Status
    RemoveResource(ID_TYPE id) {
//        std::unique_lock<std::shared_timed_mutex> lock(mutex_);
        auto rc_ctx_p = ResourceContextBuilder<ResourceT>().SetTable(ResourceT::Name)
            .SetOp(oDelete).SetID(id).CreatePtr();

        int64_t result_id;
        return DBImp::GetInstance().Apply<ResourceT>(rc_ctx_p, result_id);
    }

    IDS_TYPE
    AllActiveCollectionIds(bool reversed = true) const {
//        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        IDS_TYPE ids;
        IDS_TYPE selected_ids;
        DBImp::GetInstance().SelectResourceIDs(Collection::Name, selected_ids, "", "");

        if (!reversed) {
            ids = selected_ids;
        } else {
            for (auto it = selected_ids.rbegin(); it != selected_ids.rend(); ++it) {
                ids.push_back(*it);
            }
        }

        return ids;
    }

    IDS_TYPE
    AllActiveCollectionCommitIds(ID_TYPE collection_id, bool reversed = true) const {
        IDS_TYPE ids, selected_ids;
        DBImp::GetInstance().SelectResourceIDs(CollectionCommit::Name, selected_ids, F_COLLECTON_ID, std::to_string(collection_id));

        if (!reversed) {
            ids = selected_ids;
        } else {
            for (auto it = selected_ids.rbegin(); it != selected_ids.rend(); ++it) {
                ids.push_back(*it);
            }
        }

        return ids;
    }

    Status
    CreateCollection(Collection&& collection, CollectionPtr& return_v) {
        auto status = CreateResource<Collection>(std::move(collection), return_v);
        if (!status.ok()) {
            return status;
        }

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
        /* std::cout << ">>> [Update] " << ResourceT::Name << " " << id; */
        /* std::cout << " " << std::boolalpha << res->IsActive() << std::endl; */
        return Status::OK();
    }

    template <typename ResourceT>
    Status
    CreateResource(ResourceT&& resource, typename ResourceT::Ptr& return_v) {
//        std::unique_lock<std::shared_timed_mutex> lock(mutex_);
        auto res_p = std::make_shared<ResourceT>(resource);
        auto res_ctx_p = ResourceContextBuilder<ResourceT>().SetOp(oAdd).SetResource(res_p).CreatePtr();

        int64_t result_id;
        auto status = DBImp::GetInstance().Apply<ResourceT>(res_ctx_p, result_id);
        if (!status.ok()) {
            return status;
        }

        return_v = std::make_shared<ResourceT>(resource);
        return_v->SetID(result_id);
        return_v->ResetCnt();

//        lock.unlock();
//        auto status = GetResource<ResourceT>(res->GetID(), return_v);
        /* std::cout << ">>> [Create] " << ResourceT::Name << " " << id; */
        /* std::cout << " " << std::boolalpha << res->IsActive() << std::endl; */
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
    Store() {
    }

    void
    DoMock() {
        Status status;
        ID_TYPE result_id;
        unsigned int seed = 123;
        auto random = rand_r(&seed) % 2 + 4;
        std::vector<std::any> all_records;
        std::unordered_map<ID_TYPE, FieldCommitPtr> field_commit_records;
        std::unordered_map<std::string, ID_TYPE> id_map = {
            {Collection::Name, 0}, {Field::Name, 0},
            {FieldElement::Name, 0}, {Partition::Name, 0}
        };

        for (auto i = 1; i <= random; i++) {
            std::stringstream name;
            name << "c_" << ++id_map[Collection::Name];

            auto tc = Collection(name.str());
            tc.Activate();
            CollectionPtr c;
            CreateCollection(std::move(tc), c);
            all_records.push_back(c);

            MappingT schema_c_m;
            auto random_fields = rand_r(&seed) % 2 + 1;
            for (auto fi = 1; fi <= random_fields; ++fi) {
                std::stringstream fname;
                fname << "f_" << fi << "_" << ++id_map[FieldElement::Name];

                Field temp_f(fname.str(), fi, FieldType::VECTOR);
                FieldPtr field;

                temp_f.Activate();
                CreateResource<Field>(std::move(temp_f), field);
                all_records.push_back(field);
                MappingT f_c_m = {};

                auto random_elements = rand_r(&seed) % 2 + 2;
                for (auto fei = 1; fei <= random_elements; ++fei) {
                    std::stringstream fename;
                    fename << "fe_" << field->GetID() << "_" << ++id_map[FieldElement::Name];

                    FieldElementPtr element;
                    FieldElement temp_fe(c->GetID(), field->GetID(), fename.str(), fei);
                    temp_fe.Activate();
                    CreateResource<FieldElement>(std::move(temp_fe), element);
                    all_records.push_back(element);
                    f_c_m.insert(element->GetID());
                }
                FieldCommitPtr f_c;
                CreateResource<FieldCommit>(FieldCommit(c->GetID(), field->GetID(), f_c_m, 0, 0, ACTIVE), f_c);
                all_records.push_back(f_c);
                field_commit_records.insert(std::pair<ID_TYPE, FieldCommitPtr>(f_c->GetID(), f_c));
                schema_c_m.insert(f_c->GetID());
            }

            SchemaCommitPtr schema;
            CreateResource<SchemaCommit>(SchemaCommit(c->GetID(), schema_c_m, 0, 0, ACTIVE), schema);
            all_records.push_back(schema);

            auto random_partitions = rand_r(&seed) % 2 + 1;
            MappingT c_c_m;
            for (auto pi = 1; pi <= random_partitions; ++pi) {
                std::stringstream pname;
                pname << "p_" << i << "_" << ++id_map[Partition::Name];
                PartitionPtr p;
                CreateResource<Partition>(Partition(pname.str(), c->GetID(), 0, 0, ACTIVE), p);
                all_records.push_back(p);

                auto random_segments = rand_r(&seed) % 2 + 1;
                MappingT p_c_m;
                for (auto si = 1; si <= random_segments; ++si) {
                    SegmentPtr s;
                    CreateResource<Segment>(Segment(c->GetID(), p->GetID(), si, 0, 0, ACTIVE), s);
                    all_records.push_back(s);
                    auto& schema_m = schema->GetMappings();
                    MappingT s_c_m;
                    for (auto field_commit_id : schema_m) {
                        auto& field_commit = field_commit_records.at(field_commit_id);
                        auto& f_c_m = field_commit->GetMappings();
                        for (auto& field_element_id : f_c_m) {
                            SegmentFilePtr sf;
                            CreateResource<SegmentFile>(
                                SegmentFile(c->GetID(), p->GetID(), s->GetID(), field_element_id, 0, 0, 0, 0, ACTIVE),
                                sf);
                            all_records.push_back(sf);

                            s_c_m.insert(sf->GetID());
                        }
                    }
                    SegmentCommitPtr s_c;
                    CreateResource<SegmentCommit>(
                        SegmentCommit(schema->GetID(), p->GetID(), s->GetID(), s_c_m, 0, 0, 0, 0, ACTIVE), s_c);
                    all_records.push_back(s_c);
                    p_c_m.insert(s_c->GetID());
                }
                PartitionCommitPtr p_c;
                CreateResource<PartitionCommit>(PartitionCommit(c->GetID(), p->GetID(), p_c_m, 0, 0, 0, 0, ACTIVE),
                                                p_c);
                all_records.push_back(p_c);
                c_c_m.insert(p_c->GetID());
            }
            CollectionCommitPtr c_c;
            CollectionCommit temp_cc(c->GetID(), schema->GetID(), c_c_m);
            temp_cc.Activate();
            CreateResource<CollectionCommit>(std::move(temp_cc), c_c);
            all_records.push_back(c_c);
        }
        for (auto& record : all_records) {
            if (record.type() == typeid(std::shared_ptr<Collection>)) {
                const auto& r = std::any_cast<std::shared_ptr<Collection>>(record);
                r->Activate();
                auto t_c_p = ResourceContextBuilder<Collection>().SetOp(oUpdate)
                    .SetResource(r)
                    .AddAttr(F_STATE)
                    .CreatePtr();

                DBImp::GetInstance().Apply<Collection>(t_c_p, result_id);
            } else if (record.type() == typeid(std::shared_ptr<CollectionCommit>)) {
                const auto& r = std::any_cast<std::shared_ptr<CollectionCommit>>(record);
                r->Activate();
                auto t_cc_p = ResourceContextBuilder<CollectionCommit>().SetOp(oUpdate)
                    .SetResource(r)
                    .AddAttr(F_STATE)
                    .CreatePtr();
                DBImp::GetInstance().Apply<CollectionCommit>(t_cc_p, result_id);
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
