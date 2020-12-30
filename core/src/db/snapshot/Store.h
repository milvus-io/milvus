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

#include "codecs/Codec.h"
#include "db/Utils.h"
#include "db/meta/MetaFactory.h"
#include "db/meta/condition/MetaRelation.h"
#include "db/meta/condition/MetaFilter.h"
#include "db/snapshot/ResourceContext.h"
#include "db/snapshot/ResourceHelper.h"
#include "db/snapshot/ResourceTypes.h"
#include "db/snapshot/Resources.h"
#include "db/snapshot/Utils.h"
#include "segment/Segment.h"
#include "utils/Exception.h"
#include "utils/Log.h"
#include "utils/Status.h"

#include <fiu/fiu-local.h>
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

namespace milvus::engine::snapshot {

class Store;
struct ApplyContext {
    using ApplyFunc = std::function<Status(int64_t&)>;
    using SuccessCBT = std::function<Status(ID_TYPE)>;
    using ErrorCBT = std::function<Status(Status)>;
    using TimeoutCBT = std::function<Status(std::shared_ptr<Store>)>;
    SuccessCBT on_succes_cb = {};
    ErrorCBT on_error_cb = {};
    TimeoutCBT on_timeout_cb = {};
};

class Store : public std::enable_shared_from_this<Store> {
 public:
    using Ptr = typename std::shared_ptr<Store>;

    explicit Store(meta::MetaAdapterPtr adapter, const std::string& root_path,
                   const std::set<std::string>& suffix_set = {})
        : adapter_(adapter), root_path_(root_path + engine::COLLECTIONS_FOLDER), suffix_set_(suffix_set) {
    }

    static Store::Ptr
    Build(const std::string& uri, const std::string& root_path, const std::set<std::string> suffix_set = {}) {
        DBMetaOptions options;
        options.backend_uri_ = uri;
        options.path_ = root_path;

        auto adapter = MetaFactory::Build(options);
        return std::make_shared<Store>(adapter, root_path, suffix_set);
    }

    const std::string&
    GetRootPath() const {
        return root_path_;
    }

    const std::set<std::string>&
    GetSuffixSet() const {
        return suffix_set_;
    }

    template <typename OpT>
    Status
    ApplyOperation(OpT& op, ApplyContext& context) {
        auto session = adapter_->CreateSession();
        std::apply(
            [&](auto&... step_context_set) {
                std::size_t n{0};
                ((ApplyOpStep(op, n++, step_context_set, session)), ...);
            },
            op.GetStepHolders());

        ID_TYPE result_id;
        auto status = session->Commit(result_id);
        fiu_do_on("Store.ApplyOperation.mock_timeout", { status = Status(SS_TIMEOUT, "Mock Timeout"); });

        if (status.ok() && context.on_succes_cb) {
            return context.on_succes_cb(result_id);
        } else if (status.code() == SS_TIMEOUT && context.on_timeout_cb) {
            return context.on_timeout_cb(shared_from_this());
            /* return context.on_timeout_cb( */
            /*         std::bind(static_cast<Status(meta::MetaSession::*)(int64_t&)>( */
            /*                 &meta::MetaSession::Commit), session, std::placeholders::_1)); */
        } else if (context.on_error_cb) {
            return context.on_error_cb(status);
        }

        return status;
    }

    template <typename T, typename OpT>
    void
    ApplyOpStep(OpT& op, size_t pos, std::set<std::shared_ptr<ResourceContext<T>>>& step_context_set,
                const meta::SessionPtr& session) {
        for (auto& step_context : step_context_set) {
            session->Apply<T>(step_context);
        }
        if (pos == op.GetPos()) {
            session->ResultPos();
        }
    }

    template <typename OpT>
    Status
    Apply(OpT& op) {
        return op.ApplyToStore(this->shared_from_this());
    }

    template <typename ResourceT>
    Status
    GetResource(ID_TYPE id, typename ResourceT::Ptr& return_v) {
        auto status = adapter_->Select<ResourceT>(id, return_v);

        if (!status.ok()) {
            return status;
        }

        if (return_v == nullptr) {
            std::string err = "Cannot select resource " + std::string(ResourceT::Name) +
                              " from DB: No resource which id = " + std::to_string(id);
            return Status(SS_NOT_FOUND_ERROR, err);
        }

        return Status::OK();
    }

    Status
    GetCollection(const std::string& name, CollectionPtr& return_v) {
        // TODO: Get active collection
        std::vector<CollectionPtr> resources;
        auto status = adapter_->SelectBy<Collection, std::string>(NameField::Name, {name}, resources);
        if (!status.ok()) {
            return status;
        }

        for (auto& res : resources) {
            if (res->IsActive()) {
                return_v = res;
                return Status::OK();
            }
        }

        return Status(SS_NOT_FOUND_ERROR, "DB resource not found");
    }

    template <typename ResourceT>
    Status
    GetInActiveResources(std::vector<typename ResourceT::Ptr>& return_vs) {
        std::vector<State> filter_states = {State::PENDING, State::DEACTIVE};
        return adapter_->SelectBy<ResourceT>(StateField::Name, filter_states, return_vs);
    }

    template <typename ResourceT>
    Status
    GetActiveResourcesByAttrs(std::vector<typename ResourceT::Ptr>& return_vs,
                              const std::vector<std::string>& target_attrs, int64_t upper_bound, int64_t low_bound) {
        std::vector<State> filter_states = {State::ACTIVE};
        auto relation = meta::ONE_(meta::Range_<ResourceT, StateField>(meta::Range::EQ, State::ACTIVE));
        if (upper_bound < std::numeric_limits<TS_TYPE>::max()) {
            relation = meta::AND_(relation, meta::Range_<ResourceT, UpdatedOnField>(meta::Range::LTE, upper_bound));
        }
        if (low_bound > std::numeric_limits<TS_TYPE>::min()) {
            relation = meta::AND_(relation, meta::Range_<ResourceT, UpdatedOnField>(meta::Range::GTE, low_bound));
        }
        return adapter_->Query<ResourceT>(relation, return_vs);
    }

    template <typename ResourceT>
    Status
    GetActiveResources(std::vector<typename ResourceT::Ptr>& return_vs) {
        std::vector<State> filter_states = {State::ACTIVE};
        return adapter_->SelectBy<ResourceT>(StateField::Name, filter_states, return_vs);
    }

    template <typename ResourceT>
    Status
    RemoveResource(ID_TYPE id) {
        auto rc_ctx_p =
            ResourceContextBuilder<ResourceT>(meta::oDelete).SetTable(ResourceT::Name).SetID(id).CreatePtr();

        int64_t result_id;
        return adapter_->Execute<ResourceT>(rc_ctx_p, result_id);
    }

    IDS_TYPE
    AllActiveCollectionIds(bool reversed = true) const {
        IDS_TYPE ids;
        IDS_TYPE selected_ids;
        std::vector<State> filter_states = {State::ACTIVE};
        adapter_->SelectResourceIDs<Collection>(selected_ids, StateField::Name, filter_states);

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
        adapter_->SelectResourceIDs<CollectionCommit, int64_t>(selected_ids, CollectionIdField::Name, {collection_id});

        if (!reversed) {
            ids = selected_ids;
        } else {
            for (auto it = selected_ids.rbegin(); it != selected_ids.rend(); ++it) {
                ids.push_back(*it);
            }
        }

        return ids;
    }

    template <typename ResourceT>
    Status
    CreateResource(ResourceT&& resource, typename ResourceT::Ptr& return_v) {
        auto res_p = std::make_shared<ResourceT>(resource);
        auto res_ctx_p = ResourceContextBuilder<ResourceT>(meta::oAdd).SetResource(res_p).CreatePtr();

        int64_t result_id;
        auto status = adapter_->Execute<ResourceT>(res_ctx_p, result_id);
        if (!status.ok()) {
            return status;
        }

        return_v = std::make_shared<ResourceT>(resource);
        return_v->SetID(result_id);
        return_v->ResetCnt();

        return Status::OK();
    }

    void
    DoReset() {
        auto status = adapter_->TruncateAll();
        if (!status.ok()) {
            LOG_ENGINE_ERROR_ << "TruncateAll failed: " << status.ToString();
        }
    }

    void
    Mock() {
        DoReset();
        DoMock();
    }

 private:
    void
    DoMock() {
        Status status;
        ID_TYPE result_id;
        unsigned int seed = 123;
        auto random = rand_r(&seed) % 2 + 4;
        std::vector<std::any> all_records;
        std::unordered_map<ID_TYPE, FieldCommitPtr> field_commit_records;
        std::unordered_map<std::string, ID_TYPE> id_map = {
            {Collection::Name, 0}, {Field::Name, 0}, {FieldElement::Name, 0}, {Partition::Name, 0}};

        for (auto i = 1; i <= random; i++) {
            std::stringstream name;
            name << "c_" << ++id_map[Collection::Name];

            auto tc = Collection(name.str());
            tc.Activate();
            CollectionPtr c;
            CreateResource<Collection>(std::move(tc), c);
            all_records.push_back(c);

            MappingT schema_c_m;
            auto random_fields = rand_r(&seed) % 2 + 1;
            for (auto fi = 1; fi <= random_fields; ++fi) {
                std::stringstream fname;
                fname << "f_" << fi << "_" << ++id_map[FieldElement::Name];

                Field temp_f(fname.str(), fi, DataType::VECTOR_FLOAT);
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
                    FieldElement temp_fe(c->GetID(), field->GetID(), fename.str(), (FieldElementType)fei);
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
                            FieldElementPtr fe_p;
                            GetResource<FieldElement>(field_element_id, fe_p);
                            CreateResource<SegmentFile>(
                                SegmentFile(c->GetID(), p->GetID(), s->GetID(), field_element_id, fe_p->GetFEtype(), 0,
                                            0, 0, 0, ACTIVE),
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
                PartitionCommit temp_pc(c->GetID(), p->GetID());
                temp_pc.UpdateFlushIds();
                temp_pc.GetMappings() = p_c_m;
                auto base_path = GetResPath<Partition>(GetRootPath(), p);
                temp_pc.FlushIds(base_path);
                temp_pc.Activate();
                CreateResource<PartitionCommit>(std::move(temp_pc), p_c);
                all_records.push_back(p_c);
                c_c_m.insert(p_c->GetID());
            }
            CollectionCommitPtr c_c;
            CollectionCommit temp_cc(c->GetID(), schema->GetID());
            temp_cc.UpdateFlushIds();
            temp_cc.GetMappings() = c_c_m;

            auto base_path = GetResPath<Collection>(GetRootPath(), c);
            temp_cc.FlushIds(base_path);
            temp_cc.Activate();
            CreateResource<CollectionCommit>(std::move(temp_cc), c_c);
            all_records.push_back(c_c);
        }
        for (auto& record : all_records) {
            if (record.type() == typeid(std::shared_ptr<Collection>)) {
                const auto& r = std::any_cast<std::shared_ptr<Collection>>(record);
                r->Activate();
                auto t_c_p =
                    ResourceContextBuilder<Collection>(meta::oUpdate).SetResource(r).AddAttr(meta::F_STATE).CreatePtr();

                adapter_->Execute<Collection>(t_c_p, result_id);
            } else if (record.type() == typeid(std::shared_ptr<CollectionCommit>)) {
                const auto& r = std::any_cast<std::shared_ptr<CollectionCommit>>(record);
                r->Activate();
                auto t_cc_p = ResourceContextBuilder<CollectionCommit>(meta::oUpdate)
                                  .SetResource(r)
                                  .AddAttr(meta::F_STATE)
                                  .CreatePtr();
                adapter_->Execute<CollectionCommit>(t_cc_p, result_id);
            }
        }
    }

    meta::MetaAdapterPtr adapter_;
    std::string root_path_;
    std::set<std::string> suffix_set_;
};

using StorePtr = Store::Ptr;
}  // namespace milvus::engine::snapshot
