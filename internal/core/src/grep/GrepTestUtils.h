// Copyright (C) 2026 Zilliz. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

#pragma once

#include <folly/CancellationToken.h>
#include <memory>
#include <string_view>
#include <vector>

#include "common/EasyAssert.h"
#include "common/RegexQuery.h"
#include "common/Types.h"
#include "index/contracts/query/IIndexReaderBase.h"
#include "index/contracts/query/INgramReader.h"
#include "index/contracts/query/INullReader.h"

// Shared by the Grep runner tests and the grep filter expression tests.
namespace milvus::segcore {

struct NgramProbe {
    int queries = 0;
    std::vector<int64_t> input_rows;
    bool short_coverage = false;
    bool element_domain = false;
    bool fail = false;
    folly::CancellationSource* cancel = nullptr;
};

// Observe the production reader through the real segment inventory/cache path.
class ObservedNgramReader final : public index::IIndexReaderBase,
                                  public index::INgramReader,
                                  public index::INullReader {
 public:
    ObservedNgramReader(index::IIndexReaderBasePtr reader,
                        std::shared_ptr<NgramProbe> probe)
        : reader_(std::move(reader)), probe_(std::move(probe)) {
    }
    index::ReaderCaps
    Caps() const override {
        return reader_->Caps();
    }
    index::Domain
    CoordDomain() const override {
        return probe_->element_domain ? index::Domain::Element
                                      : reader_->CoordDomain();
    }
    int64_t
    Count() const override {
        return reader_->Count() - probe_->short_coverage;
    }
    DataType
    ValueType() const override {
        return reader_->ValueType();
    }
    bool
    CanHandleConditionTree(
        const RegexLiteralCondition& condition) const override {
        return Ngram().CanHandleConditionTree(condition);
    }
    void
    ConditionTreeCandidates(const RegexLiteralCondition& condition,
                            TargetBitmap& candidates) const override {
        ++probe_->queries;
        probe_->input_rows.clear();
        for (size_t i = 0; i < candidates.size(); ++i) {
            if (candidates[i])
                probe_->input_rows.push_back(i);
        }
        if (probe_->fail) {
            ThrowInfo(DataFormatBroken, "injected Grep posting failure");
        }
        Ngram().ConditionTreeCandidates(condition, candidates);
        if (probe_->cancel)
            probe_->cancel->requestCancellation();
    }
    bool
    CanHandle(std::string_view literal, index::PatternOp op) const override {
        return Ngram().CanHandle(literal, op);
    }
    bool
    CandidatesAreExact(std::string_view literal,
                       index::PatternOp op) const override {
        return Ngram().CandidatesAreExact(literal, op);
    }
    void
    Candidates(std::string_view literal,
               index::PatternOp op,
               TargetBitmap& candidates) const override {
        Ngram().Candidates(literal, op, candidates);
    }
    TargetBitmap
    IsNull() const override {
        return dynamic_cast<const index::INullReader&>(*reader_).IsNull();
    }
    TargetBitmap
    IsNotNull() const override {
        return dynamic_cast<const index::INullReader&>(*reader_).IsNotNull();
    }

 private:
    const index::INgramReader&
    Ngram() const {
        return dynamic_cast<const index::INgramReader&>(*reader_);
    }
    index::IIndexReaderBasePtr reader_;
    std::shared_ptr<NgramProbe> probe_;
};

}  // namespace milvus::segcore
