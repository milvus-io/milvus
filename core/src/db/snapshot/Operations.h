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

#include "Snapshot.h"
#include "Store.h"
#include "Context.h"
#include <assert.h>
#include <vector>
#include <any>

namespace milvus {
namespace engine {
namespace snapshot {

using StepsT = std::vector<std::any>;

enum OpStatus {
    OP_PENDING = 0,
    OP_OK,
    OP_STALE_OK,
    OP_STALE_CANCEL,
    OP_STALE_RESCHEDULE,
    OP_FAIL_INVALID_PARAMS,
    OP_FAIL_DUPLICATED,
    OP_FAIL_FLUSH_META
};

class Operations {
public:
    /* static constexpr const char* Name = Derived::Name; */
    Operations(const OperationContext& context, ScopedSnapshotT prev_ss);
    Operations(const OperationContext& context, ID_TYPE collection_id, ID_TYPE commit_id = 0);

    const ScopedSnapshotT& GetPrevSnapshot() const {return prev_ss_;}

    virtual bool IsStale() const;

    template<typename StepT>
    void AddStep(const StepT& step);
    void SetStepResult(ID_TYPE id) { ids_.push_back(id); }

    StepsT& GetSteps() { return steps_; }

    virtual void OnExecute();
    virtual bool PreExecute();
    virtual bool DoExecute();
    virtual bool PostExecute();

    virtual ScopedSnapshotT GetSnapshot() const;

    virtual void operator()();

    virtual ~Operations() {}

protected:
    OperationContext context_;
    ScopedSnapshotT prev_ss_;
    StepsT steps_;
    std::vector<ID_TYPE> ids_;
    OpStatus status_ = OP_PENDING;
};

template<typename StepT>
void
Operations::AddStep(const StepT& step) {
    steps_.push_back(std::make_shared<StepT>(step));
}

template <typename ResourceT>
class CommitOperation : public Operations {
public:
    using BaseT = Operations;
    CommitOperation(const OperationContext& context, ScopedSnapshotT prev_ss)
        : BaseT(context, prev_ss) {};
    CommitOperation(const OperationContext& context, ID_TYPE collection_id, ID_TYPE commit_id = 0)
        : BaseT(context, collection_id, commit_id) {};

    virtual typename ResourceT::Ptr GetPrevResource() const {
        return nullptr;
    }

    typename ResourceT::Ptr GetResource() const  {
        if (status_ == OP_PENDING) return nullptr;
        if (ids_.size() == 0) return nullptr;
        resource_->SetID(ids_[0]);
        return resource_;
    }

    // PXU TODO
    /* virtual void OnFailed() */

protected:
    typename ResourceT::Ptr resource_;
};

} // snapshot
} // engine
} // milvus
