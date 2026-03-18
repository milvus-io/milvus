// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <fmt/core.h>
#include <memory>
#include <string>
#include <vector>

#include "common/FieldDataInterface.h"
#include "common/Vector.h"
#include "exec/expression/Expr.h"
#include "expr/ITypeExpr.h"
#include "segcore/SegmentInterface.h"

namespace milvus {
namespace exec {

class PhyMolFunctionFilterExpr : public SegmentExpr {
 public:
    PhyMolFunctionFilterExpr(
        const std::vector<std::shared_ptr<Expr>>& input,
        const std::shared_ptr<const milvus::expr::MolFunctionFilterExpr>& expr,
        const std::string& name,
        milvus::OpContext* op_ctx,
        const segcore::SegmentInternalInterface* segment,
        int64_t active_count,
        int64_t batch_size,
        int32_t consistency_level)
        : SegmentExpr(std::move(input),
                      name,
                      op_ctx,
                      segment,
                      expr->column_.field_id_,
                      expr->column_.nested_path_,
                      DataType::MOL,
                      active_count,
                      batch_size,
                      consistency_level),
          expr_(expr) {
    }

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

    std::optional<milvus::expr::ColumnInfo>
    GetColumnInfo() const override {
        return expr_->column_;
    }

    std::string
    ToString() const {
        return fmt::format("{}", expr_->ToString());
    }

    void
    MoveCursor() {
        if (segment_->type() == SegmentType::Sealed) {
            SegmentExpr::MoveCursor();
        }
    }

 private:
    VectorPtr
    EvalForDataSegment();

    // Fingerprint pre-filter: compute query fingerprint and build a
    // candidate bitmap from raw fingerprint chunks.
    void
    SearchFingerprintIndex();

    // Generate the query pattern fingerprint from SMILES.
    void
    EnsureQueryFingerprint();

    // Read raw BinaryVector chunk data and run knowhere brute-force
    // range search as a stateless pre-filter helper.
    void
    FallbackRawDataPreFilter();

    // Try to use MOL_PATTERN index on the mol field for pre-filtering.
    // Returns true if index was found and used (fp_candidates_ populated).
    bool
    TryMolPatternIndex();

 private:
    std::shared_ptr<const milvus::expr::MolFunctionFilterExpr> expr_;
    // Cached query pickle (converted from SMILES once per segment)
    std::string query_pickle_;
    bool query_pickle_cached_ = false;
    // Fingerprint pre-filter state
    std::vector<uint8_t> query_fingerprint_;
    bool query_fp_cached_ = false;
    TargetBitmap fp_candidates_;
    bool fp_candidates_cached_ = false;
};
}  //namespace exec
}  // namespace milvus
