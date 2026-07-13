// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "MatchExpr.h"

#include <algorithm>
#include <cstddef>
#include <string>
#include <vector>

#include "bitset/bitset.h"
#include "common/ArrayOffsets.h"
#include "common/EasyAssert.h"
#include "common/FieldMeta.h"
#include "common/Json.h"
#include "common/JsonCastType.h"
#include "common/Schema.h"
#include "common/Tracer.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/Utils.h"
#include "folly/FBVector.h"
#include "index/ScalarIndex.h"
#include "segcore/SegmentSealed.h"

namespace milvus {
namespace exec {

using MatchType = milvus::expr::MatchType;

// Core matching logic for a single row's elements
// Returns true if the row matches the condition
//
// Word-wise implementation: the row's element bits are consumed in 64-bit
// chunks (bitmap word width) instead of bit by bit. Invalid elements are
// masked out with the valid bitmap, which preserves the per-bit semantics:
// only valid elements are counted, and MatchAll requires every *valid*
// element to match (vacuously true when the row has no valid elements).
template <MatchType match_type, bool all_valid>
bool
MatchSingleRow(int64_t bitset_start,
               int64_t row_elem_count,
               const TargetBitmapView& match_bitset,
               const TargetBitmapView& valid_bitset,
               int64_t threshold) {
    using word_t = TargetBitmapView::policy_type::data_type;
    constexpr int64_t kWordBits = static_cast<int64_t>(8 * sizeof(word_t));

    int64_t hit_count = 0;

    for (int64_t done = 0; done < row_elem_count; done += kWordBits) {
        const size_t n =
            static_cast<size_t>(std::min(kWordBits, row_elem_count - done));
        const size_t pos = static_cast<size_t>(bitset_start + done);
        word_t m = match_bitset.read(pos, n);
        if constexpr (all_valid) {
            if constexpr (match_type == MatchType::MatchAll) {
                const word_t full = (n == static_cast<size_t>(kWordBits))
                                        ? ~word_t(0)
                                        : ((word_t(1) << n) - 1);
                if (m != full) {
                    return false;
                }
                continue;
            }
        } else {
            const word_t v = valid_bitset.read(pos, n);
            m &= v;
            if constexpr (match_type == MatchType::MatchAll) {
                // Some valid element unmatched?
                if (m != v) {
                    return false;
                }
                continue;
            }
        }
        if constexpr (match_type == MatchType::MatchAny) {
            if (m != 0) {
                return true;
            }
        } else if constexpr (match_type == MatchType::MatchLeast) {
            hit_count += __builtin_popcountll(m);
            if (hit_count >= threshold) {
                return true;
            }
        } else if constexpr (match_type == MatchType::MatchMost ||
                             match_type == MatchType::MatchExact) {
            hit_count += __builtin_popcountll(m);
            if (hit_count > threshold) {
                return false;
            }
        }
    }

    // Final match decision
    if constexpr (match_type == MatchType::MatchAny) {
        return false;
    } else if constexpr (match_type == MatchType::MatchAll) {
        // Every (valid) element matched; empty array returns true
        // (vacuous truth).
        return true;
    } else if constexpr (match_type == MatchType::MatchLeast) {
        return hit_count >= threshold;
    } else if constexpr (match_type == MatchType::MatchMost) {
        return hit_count <= threshold;
    } else if constexpr (match_type == MatchType::MatchExact) {
        return hit_count == threshold;
    }
    return false;
}

bool
MatchEmptyElements(MatchType match_type, int64_t threshold) {
    switch (match_type) {
        case MatchType::MatchAny:
            return false;
        case MatchType::MatchAll:
            return true;
        case MatchType::MatchLeast:
            return threshold <= 0;
        case MatchType::MatchMost:
            return threshold >= 0;
        case MatchType::MatchExact:
            return threshold == 0;
        default:
            ThrowInfo(OpTypeInvalid,
                      "Unsupported match type: {}",
                      static_cast<int>(match_type));
    }
    return false;
}

// Process contiguous rows [row_start, row_start + row_count)
template <MatchType match_type, bool all_valid>
void
ProcessContiguousRows(int64_t row_count,
                      int64_t row_start,
                      int64_t elem_start,
                      const IArrayOffsets* array_offsets,
                      const TargetBitmapView& match_bitset,
                      const TargetBitmapView& valid_bitset,
                      TargetBitmapView& result_bitset,
                      int64_t threshold) {
    if constexpr (match_type == MatchType::MatchAny && all_valid) {
        // Fast path: ANY-semantics reduction over contiguous rows. The match
        // bitmap covers exactly the element ranges of
        // [row_start, row_start + row_count) shifted by elem_start; jump
        // between set bits word-wise instead of walking rows element by
        // element.
        array_offsets->ElementBitsetToRowBitsetAny(
            match_bitset, elem_start, row_start, result_bitset);
        return;
    }
    for (int64_t i = 0; i < row_count; ++i) {
        auto [first_elem, last_elem] =
            array_offsets->ElementIDRangeOfRow(row_start + i);
        int64_t bitset_start = first_elem - elem_start;
        int64_t row_elem_count = last_elem - first_elem;

        bool matched = MatchSingleRow<match_type, all_valid>(bitset_start,
                                                             row_elem_count,
                                                             match_bitset,
                                                             valid_bitset,
                                                             threshold);
        if (matched) {
            result_bitset[i] = true;
        }
    }
}

// Process non-contiguous rows specified by offset_input
template <MatchType match_type, bool all_valid>
void
ProcessOffsetRows(const OffsetVector* row_offsets,
                  const IArrayOffsets* array_offsets,
                  const TargetBitmapView& match_bitset,
                  const TargetBitmapView& valid_bitset,
                  TargetBitmapView& result_bitset,
                  int64_t threshold) {
    int64_t elem_cursor = 0;
    for (size_t i = 0; i < row_offsets->size(); ++i) {
        auto [first_elem, last_elem] =
            array_offsets->ElementIDRangeOfRow((*row_offsets)[i]);
        int64_t row_elem_count = last_elem - first_elem;

        if (MatchSingleRow<match_type, all_valid>(elem_cursor,
                                                  row_elem_count,
                                                  match_bitset,
                                                  valid_bitset,
                                                  threshold)) {
            result_bitset[i] = true;
        }
        elem_cursor += row_elem_count;
    }
}

template <MatchType match_type, bool all_valid>
void
DispatchMatchProcessing(bool use_offset_input,
                        int64_t row_count,
                        int64_t row_start,
                        int64_t elem_start,
                        const OffsetVector* row_offsets,
                        const IArrayOffsets* array_offsets,
                        const TargetBitmapView& match_bitset,
                        const TargetBitmapView& valid_bitset,
                        TargetBitmapView& result_bitset,
                        int64_t threshold) {
    if (use_offset_input) {
        ProcessOffsetRows<match_type, all_valid>(row_offsets,
                                                 array_offsets,
                                                 match_bitset,
                                                 valid_bitset,
                                                 result_bitset,
                                                 threshold);
    } else {
        ProcessContiguousRows<match_type, all_valid>(row_count,
                                                     row_start,
                                                     elem_start,
                                                     array_offsets,
                                                     match_bitset,
                                                     valid_bitset,
                                                     result_bitset,
                                                     threshold);
    }
}

void
PhyMatchFilterExpr::Eval(EvalCtx& context, VectorPtr& result) {
    tracer::AutoSpan span("PhyMatchFilterExpr::Eval", tracer::GetRootSpan());

    auto input = context.get_offset_input();
    SetHasOffsetInput(input != nullptr);

    // Dispatch from schema. A real JSON field is resolvable by field_id, so we
    // intercept it before name-based array resolution. Struct logical ids from
    // the Go schema are NOT present in the C++ schema, so guard the type probe
    // with has_field. Plain scalar arrays and struct arrays are resolved by
    // name via ResolveArrayElementField, keeping the existing element
    // aggregation (ArrayOffsets) path unchanged.
    const auto& schema = segment_->get_schema();
    auto field_id = FieldId(expr_->get_field_id());
    if (schema.has_field(field_id) &&
        schema.GetFieldType(field_id) == DataType::JSON) {
        EvalJson(context, result);
        return;
    }

    auto field_meta = schema.ResolveArrayElementField(expr_->get_field_name());
    EvalWithOffsets(context,
                    result,
                    segment_->GetArrayOffsets(field_meta.get_id()),
                    field_meta.get_id());
}

void
PhyMatchFilterExpr::EvalJson(EvalCtx& context, VectorPtr& result) {
    // Prefer the element-level nested-index fast path when it is eligible; it
    // returns false (leaving `result` untouched) for anything it cannot serve
    // with bit-for-bit parity, in which case we run the brute-force per-row path,
    // which stays the source of truth.
    if (EvalJsonIndexed(context, result)) {
        return;
    }
    EvalJsonBrute(context, result);
}

bool
PhyMatchFilterExpr::EvalJsonIndexed(EvalCtx& context, VectorPtr& result) {
    // The nested JSON array index only exists on sealed segments; growing
    // segments have no such index and GetJsonArrayOffsets returns nullptr.
    if (segment_->type() != SegmentType::Sealed) {
        return false;
    }

    // The element predicate must be a single, simple, element-level ($ / $[sub])
    // comparison the index can serve. Anything else (arith like `$ % 2`, a
    // conjunct `$ > a && $ < b`, a binary range `a < $ < b`, a LIKE pattern, or a
    // variable-IN) is left to the brute path.
    const auto& child = expr_->inputs();
    if (child.size() != 1) {
        return false;
    }

    // Decode the child into (op_type, values) if it is a Unary comparison or a
    // literal Term; otherwise bail to brute.
    const milvus::expr::ColumnInfo* column = nullptr;
    milvus::proto::plan::OpType op_type = milvus::proto::plan::Invalid;
    std::vector<milvus::proto::plan::GenericValue> values;
    if (auto unary =
            std::dynamic_pointer_cast<const milvus::expr::UnaryRangeFilterExpr>(
                child[0])) {
        // Only plain comparisons map to a single index In/NotIn/Range call. A
        // populated extra_values_ means a compound/rewritten predicate the index
        // cannot serve as one call.
        if (!unary->extra_values_.empty()) {
            return false;
        }
        switch (unary->op_type_) {
            case milvus::proto::plan::Equal:
            case milvus::proto::plan::NotEqual:
            case milvus::proto::plan::GreaterThan:
            case milvus::proto::plan::GreaterEqual:
            case milvus::proto::plan::LessThan:
            case milvus::proto::plan::LessEqual:
                break;
            default:
                // PrefixMatch / PostfixMatch / InnerMatch / Match (LIKE) etc.
                return false;
        }
        column = &unary->column_;
        op_type = unary->op_type_;
        values.push_back(unary->val_);
    } else if (auto term = std::dynamic_pointer_cast<
                   const milvus::expr::TermFilterExpr>(child[0])) {
        // `$ in [..]`. A variable-IN (column IN column) has no literal set.
        if (term->is_in_field_ || term->vals_.empty()) {
            return false;
        }
        column = &term->column_;
        op_type = milvus::proto::plan::In;
        values = term->vals_;
    } else {
        return false;
    }

    // The predicate must reference this JSON field at element level. (The parser
    // guarantees this for a well-formed MATCH, but re-checking keeps the fast
    // path self-contained and safe against unexpected shapes.)
    if (!column->element_level_ ||
        column->data_type_ != DataType::JSON ||
        column->field_id_.get() != expr_->get_field_id()) {
        return false;
    }

    auto field_id = FieldId(expr_->get_field_id());
    auto query_pointer = milvus::Json::pointer(expr_->get_nested_path());

    // Tantivy-backed JSON indexes cannot serve numeric array-index path segments
    // (e.g. `a.0`): JSON Pointer does not distinguish an object key from an array
    // index, so a path with an integer segment is ambiguous. Fall back to brute,
    // mirroring SegmentExpr::IsJsonPathCompatible().
    for (const auto& seg : expr_->get_nested_path()) {
        if (!seg.empty() && milvus::IsInteger(seg)) {
            return false;
        }
    }

    // The JSON offsets table (element-range per row, with NULL/absent/non-array
    // rows marked non-valid) is what folds element hits back to rows. It is
    // nullptr on growing segments / when the field is absent.
    auto array_offsets =
        segment_->GetJsonArrayOffsets(field_id, query_pointer);
    if (array_offsets == nullptr) {
        return false;
    }

    // Choose the index element type from the literal value case, mirroring the
    // JSON element-level index dispatch in PhyUnaryRangeFilterExpr::Eval:
    //   bool   -> ScalarIndex<bool>   (cast ARRAY_BOOL)
    //   float  -> ScalarIndex<double> (cast ARRAY_DOUBLE)
    //   string -> ScalarIndex<string> (cast ARRAY_VARCHAR)
    //   int64  -> ScalarIndex<double> (cast ARRAY_DOUBLE), but only when the
    //             literal is injective in double; otherwise brute keeps int64
    //             precision, so fall back.
    // A Term set must be homogeneous; the first element's case drives the pick
    // and GetValueFromProto asserts the rest match.
    const auto val_case = values.front().val_case();
    switch (val_case) {
        case milvus::proto::plan::GenericValue::kBoolVal:
            return EvalJsonIndexedImpl<bool>(context,
                                             result,
                                             query_pointer,
                                             array_offsets,
                                             *column,
                                             op_type,
                                             values);
        case milvus::proto::plan::GenericValue::kFloatVal:
            return EvalJsonIndexedImpl<double>(context,
                                               result,
                                               query_pointer,
                                               array_offsets,
                                               *column,
                                               op_type,
                                               values);
        case milvus::proto::plan::GenericValue::kInt64Val:
            // int64 literals only match the ARRAY_DOUBLE index when they are
            // injective in double (|v| < 2^53). Outside that range the brute path
            // keeps int64-exact comparison, so fall back to preserve parity. This
            // mirrors SegmentExpr::IsInt64SafeForJsonDoubleIndex, inlined here
            // because PhyMatchFilterExpr does not derive from SegmentExpr.
            for (const auto& v : values) {
                constexpr int64_t kFirstNonInjectiveInteger = int64_t{1} << 53;
                if (v.val_case() !=
                        milvus::proto::plan::GenericValue::kInt64Val ||
                    v.int64_val() <= -kFirstNonInjectiveInteger ||
                    v.int64_val() >= kFirstNonInjectiveInteger) {
                    return false;
                }
            }
            return EvalJsonIndexedImpl<double>(context,
                                               result,
                                               query_pointer,
                                               array_offsets,
                                               *column,
                                               op_type,
                                               values);
        case milvus::proto::plan::GenericValue::kStringVal:
            return EvalJsonIndexedImpl<std::string>(context,
                                                    result,
                                                    query_pointer,
                                                    array_offsets,
                                                    *column,
                                                    op_type,
                                                    values);
        default:
            // Array literal / unset: not an element-scalar comparison.
            return false;
    }
}

template <typename T>
bool
PhyMatchFilterExpr::EvalJsonIndexedImpl(
    EvalCtx& context,
    VectorPtr& result,
    const std::string& index_pointer,
    std::shared_ptr<const milvus::IArrayOffsets> array_offsets,
    const milvus::expr::ColumnInfo& column,
    milvus::proto::plan::OpType op_type,
    const std::vector<milvus::proto::plan::GenericValue>& values) {
    using Index = index::ScalarIndex<T>;

    // Element (Milvus) type the ARRAY_* cast index must expose, used to select
    // the exact ARRAY-cast entry in PinJsonIndex (is_array=true).
    DataType element_data_type;
    if constexpr (std::is_same_v<T, bool>) {
        element_data_type = DataType::BOOL;
    } else if constexpr (std::is_same_v<T, double>) {
        element_data_type = DataType::DOUBLE;
    } else {
        element_data_type = DataType::VARCHAR;
    }

    auto field_id = FieldId(expr_->get_field_id());

    // Pin the element-level nested ARRAY-cast index for exactly (field, path).
    // is_array=true + element_data_type gates PinJsonIndex to the ARRAY_* cast
    // entry (IsDataTypeSupported), never the flat scalar cast. any_type=false so
    // a bare typed (non-nested) flat index is not mistaken for the nested one.
    auto pins = segment_->PinJsonIndex(op_ctx_,
                                       field_id,
                                       index_pointer,
                                       element_data_type,
                                       /*any_type=*/false,
                                       /*is_array=*/true);
    if (pins.empty()) {
        return false;
    }
    auto index_base = pins[0].get();
    if (index_base == nullptr || !index_base->IsNestedIndex()) {
        return false;
    }
    auto* index_ptr =
        const_cast<Index*>(dynamic_cast<const Index*>(index_base));
    if (index_ptr == nullptr) {
        return false;
    }

    // Alignment guard (the crux of correctness). The nested index materializes
    // one indexed element per *successfully typed* array element
    // (ConvertJsonToArrayFieldData drops null / type-mismatched / non-scalar
    // elements), whereas the JSON IArrayOffsets counts *every* raw array element
    // (array.size()). They share the same contiguous per-row element-id space
    // iff no element was ever dropped -- which, because the per-row indexed count
    // never exceeds the raw count, holds exactly when the two totals are equal.
    // If they differ, some row's element ranges disagree and folding the index
    // bitmap through the offsets would be wrong; fall back to brute, which
    // extracts all elements per row (type mismatches become non-matching /
    // invalid elements) and stays parity-correct.
    if (index_ptr->Size() != array_offsets->GetTotalElementCount()) {
        return false;
    }

    // Range is only meaningful for ordered types; a bool inequality would have
    // been rejected by the parser, but guard before instantiating Range for bool.
    if constexpr (std::is_same_v<T, bool>) {
        if (op_type != milvus::proto::plan::In &&
            op_type != milvus::proto::plan::Equal &&
            op_type != milvus::proto::plan::NotEqual) {
            return false;
        }
    }

    // Run the element predicate through the index -> element-level bitmap sized
    // to the total element count (== offsets total, by the guard above).
    // ScalarIndex::In/NotIn/Range return `const TargetBitmap`; copy-construct the
    // local from that prvalue (TargetBitmap has a copy ctor but a deleted copy
    // assignment), mirroring TermIndexFunc / UnaryIndexFunc.
    auto run_index = [&]() -> TargetBitmap {
        switch (op_type) {
            case milvus::proto::plan::In: {
                // Raw contiguous buffer (not std::vector<T>) so the bool
                // specialization -- std::vector<bool> has no data() -- still
                // yields a T* for ScalarIndex::In.
                auto typed = std::make_unique<T[]>(values.size());
                for (size_t i = 0; i < values.size(); ++i) {
                    typed[i] = GetValueWithCastNumber<T>(values[i]);
                }
                return index_ptr->In(values.size(), typed.get());
            }
            case milvus::proto::plan::Equal: {
                T v = GetValueWithCastNumber<T>(values.front());
                return index_ptr->In(1, &v);
            }
            case milvus::proto::plan::NotEqual: {
                T v = GetValueWithCastNumber<T>(values.front());
                return index_ptr->NotIn(1, &v);
            }
            default: {
                // GreaterThan / GreaterEqual / LessThan / LessEqual.
                if constexpr (std::is_same_v<T, bool>) {
                    // Unreachable (guarded above); keeps Range<bool> from being
                    // instantiated.
                    return TargetBitmap{};
                } else {
                    T v = GetValueWithCastNumber<T>(values.front());
                    return index_ptr->Range(v, op_type);
                }
            }
        }
    };
    TargetBitmap element_match = run_index();

    // In the aligned case every array element is a valid typed scalar, so there
    // are no invalid elements -- exactly the brute path's state when no element
    // is null/type-mismatched. Use the all-valid fast path; an empty (all-false)
    // valid view would be read for MatchAll otherwise.
    AssertInfo(
        element_match.size() ==
            static_cast<size_t>(array_offsets->GetTotalElementCount()),
        "nested index element bitmap size {} != offsets total element count {}",
        element_match.size(),
        array_offsets->GetTotalElementCount());

    auto input = context.get_offset_input();
    int64_t batch_rows =
        has_offset_input_ ? input->size()
                          : std::min(batch_size_, active_count_ - current_pos_);
    if (batch_rows <= 0) {
        result = nullptr;
        return true;
    }

    result = std::make_shared<ColumnVector>(TargetBitmap(batch_rows, false),
                                            TargetBitmap(batch_rows, true));
    auto col_vec = std::dynamic_pointer_cast<ColumnVector>(result);

    TargetBitmap empty_valid;
    TargetBitmapView empty_valid_view(empty_valid);

    // The row aggregators consume the match bitmap two different ways:
    //  - sequential batch: ProcessContiguousRows indexes the *whole-segment*
    //    element bitmap by global element id (bitset_start = first_elem -
    //    elem_start), so the raw index result is used as-is.
    //  - offset-input: ProcessOffsetRows walks a *compacted* bitmap, reading the
    //    selected rows' elements back-to-back (elem_cursor advances by each row's
    //    element count). Gather those element bits into that compacted layout,
    //    which is exactly RowOffsetsToElementOffsets(*input) order.
    if (has_offset_input_) {
        auto element_offsets = array_offsets->RowOffsetsToElementOffsets(*input);
        TargetBitmap compacted(element_offsets.size(), false);
        for (size_t i = 0; i < element_offsets.size(); ++i) {
            if (element_match[element_offsets[i]]) {
                compacted[i] = true;
            }
        }
        TargetBitmapView compacted_view(compacted);
        FoldElementBitsetToRows(col_vec.get(),
                                input,
                                batch_rows,
                                array_offsets.get(),
                                compacted_view,
                                empty_valid_view,
                                /*all_valid=*/true);
    } else {
        TargetBitmapView match_view(element_match);
        FoldElementBitsetToRows(col_vec.get(),
                                input,
                                batch_rows,
                                array_offsets.get(),
                                match_view,
                                empty_valid_view,
                                /*all_valid=*/true);
    }

    // Three-valued MATCH: exclude rows that do not resolve to a real JSON array
    // at the path. The offsets table already encoded this as row-valid bits at
    // build time (NULL / absent-path / non-array rows -> not valid, zero
    // elements). Clear both the match and valid bits for such rows so the result
    // is bit-identical to MaskJsonNonArrayRows on the brute path.
    const TargetBitmap* row_valid = array_offsets->GetRowValidBitmap();
    if (row_valid != nullptr) {
        TargetBitmapView bitset_view(col_vec->GetRawData(), col_vec->size());
        TargetBitmapView valid_view(col_vec->GetValidRawData(),
                                    col_vec->size());
        for (int64_t i = 0; i < batch_rows; ++i) {
            int64_t row_offset = has_offset_input_
                                     ? static_cast<int64_t>((*input)[i])
                                     : (current_pos_ + i);
            if (!(*row_valid)[row_offset]) {
                valid_view[i] = false;
                bitset_view[i] = false;
            }
        }
    }

    if (!has_offset_input_) {
        current_pos_ += batch_rows;
    }
    return true;
}

void
PhyMatchFilterExpr::FoldElementBitsetToRows(
    ColumnVector* col_vec,
    const OffsetVector* input,
    int64_t batch_rows,
    const milvus::IArrayOffsets* array_offsets,
    const TargetBitmapView& match_view,
    const TargetBitmapView& valid_view,
    bool all_valid) {
    TargetBitmapView bitset_view(col_vec->GetRawData(), col_vec->size());

    auto match_type = expr_->get_match_type();
    int64_t threshold = expr_->get_count();

    // elem_start is the global element id that bit 0 of `match_view` maps to.
    // This helper is fed either the whole-segment element bitmap (sequential
    // batch: bit i == global element i, so bit 0 == element 0) or the compacted
    // per-selected-row bitmap (offset-input: ProcessOffsetRows walks it with its
    // own elem_cursor and ignores elem_start). Both cases map bit 0 to global
    // element 0 / are elem_start-agnostic, so elem_start is always 0 here --
    // ProcessContiguousRows then reads bitset_start = first_elem - 0 = the global
    // element id directly, and ElementBitsetToRowBitsetAny reads
    // first_bit = starts[current_pos_] - 0.
    const int64_t elem_start = 0;

    auto dispatch = [&]<bool all_valid_v>() {
        switch (match_type) {
            case MatchType::MatchAny:
                DispatchMatchProcessing<MatchType::MatchAny, all_valid_v>(
                    has_offset_input_,
                    batch_rows,
                    current_pos_,
                    elem_start,
                    input,
                    array_offsets,
                    match_view,
                    valid_view,
                    bitset_view,
                    threshold);
                break;
            case MatchType::MatchAll:
                DispatchMatchProcessing<MatchType::MatchAll, all_valid_v>(
                    has_offset_input_,
                    batch_rows,
                    current_pos_,
                    elem_start,
                    input,
                    array_offsets,
                    match_view,
                    valid_view,
                    bitset_view,
                    threshold);
                break;
            case MatchType::MatchLeast:
                DispatchMatchProcessing<MatchType::MatchLeast, all_valid_v>(
                    has_offset_input_,
                    batch_rows,
                    current_pos_,
                    elem_start,
                    input,
                    array_offsets,
                    match_view,
                    valid_view,
                    bitset_view,
                    threshold);
                break;
            case MatchType::MatchMost:
                DispatchMatchProcessing<MatchType::MatchMost, all_valid_v>(
                    has_offset_input_,
                    batch_rows,
                    current_pos_,
                    elem_start,
                    input,
                    array_offsets,
                    match_view,
                    valid_view,
                    bitset_view,
                    threshold);
                break;
            case MatchType::MatchExact:
                DispatchMatchProcessing<MatchType::MatchExact, all_valid_v>(
                    has_offset_input_,
                    batch_rows,
                    current_pos_,
                    elem_start,
                    input,
                    array_offsets,
                    match_view,
                    valid_view,
                    bitset_view,
                    threshold);
                break;
            default:
                ThrowInfo(OpTypeInvalid,
                          "Unsupported match type: {}",
                          static_cast<int>(match_type));
        }
    };

    if (all_valid) {
        dispatch.template operator()<true>();
    } else {
        dispatch.template operator()<false>();
    }
}

void
PhyMatchFilterExpr::EvalJsonBrute(EvalCtx& context, VectorPtr& result) {
    auto input = context.get_offset_input();
    int64_t batch_rows =
        has_offset_input_ ? input->size()
                          : std::min(batch_size_, active_count_ - current_pos_);

    if (batch_rows <= 0) {
        result = nullptr;
        return;
    }

    result = std::make_shared<ColumnVector>(TargetBitmap(batch_rows, false),
                                            TargetBitmap(batch_rows, true));
    auto col_vec = std::dynamic_pointer_cast<ColumnVector>(result);
    TargetBitmapView result_view(col_vec->GetRawData(), col_vec->size());

    auto match_type = expr_->get_match_type();
    int64_t threshold = expr_->get_count();

    auto match_one_row = [&](int64_t row_idx,
                             const TargetBitmapView& match_view,
                             const TargetBitmapView& valid_view,
                             int64_t elem_count,
                             bool all_valid) {
        auto dispatch = [&]<bool all_valid_v>() {
            switch (match_type) {
                case MatchType::MatchAny:
                    return MatchSingleRow<MatchType::MatchAny, all_valid_v>(
                        0, elem_count, match_view, valid_view, threshold);
                case MatchType::MatchAll:
                    return MatchSingleRow<MatchType::MatchAll, all_valid_v>(
                        0, elem_count, match_view, valid_view, threshold);
                case MatchType::MatchLeast:
                    return MatchSingleRow<MatchType::MatchLeast, all_valid_v>(
                        0, elem_count, match_view, valid_view, threshold);
                case MatchType::MatchMost:
                    return MatchSingleRow<MatchType::MatchMost, all_valid_v>(
                        0, elem_count, match_view, valid_view, threshold);
                case MatchType::MatchExact:
                    return MatchSingleRow<MatchType::MatchExact, all_valid_v>(
                        0, elem_count, match_view, valid_view, threshold);
                default:
                    ThrowInfo(OpTypeInvalid,
                              "Unsupported match type: {}",
                              static_cast<int>(match_type));
            }
        };
        if (all_valid ? dispatch.template operator()<true>()
                      : dispatch.template operator()<false>()) {
            result_view[row_idx] = true;
        }
    };

    TargetBitmap empty_match;
    TargetBitmap empty_valid;
    TargetBitmapView empty_match_view(empty_match);
    TargetBitmapView empty_valid_view(empty_valid);

    for (int64_t i = 0; i < batch_rows; ++i) {
        int32_t row_id = has_offset_input_
                             ? (*input)[i]
                             : static_cast<int32_t>(current_pos_ + i);
        OffsetVector one_row;
        one_row.push_back(row_id);
        EvalCtx eval_ctx(context.get_exec_context(), &one_row);

        VectorPtr match_result;
        inputs_[0]->Eval(eval_ctx, match_result);
        if (match_result == nullptr) {
            match_one_row(i, empty_match_view, empty_valid_view, 0, true);
            continue;
        }

        auto match_result_col_vec =
            std::dynamic_pointer_cast<ColumnVector>(match_result);
        AssertInfo(match_result_col_vec != nullptr,
                   "Match result should be ColumnVector");
        AssertInfo(match_result_col_vec->IsBitmap(),
                   "Match result should be bitmap");

        TargetBitmapView match_view(match_result_col_vec->GetRawData(),
                                    match_result_col_vec->size());
        TargetBitmapView valid_view(match_result_col_vec->GetValidRawData(),
                                    match_result_col_vec->size());
        match_one_row(i,
                      match_view,
                      valid_view,
                      match_result_col_vec->size(),
                      valid_view.all());
    }

    // Three-valued MATCH for JSON: exclude any row that does not resolve to a
    // real JSON array at the path (field NULL, JSON null, missing key, or a
    // non-array value). A genuine empty array `[]` keeps vacuous semantics. This
    // is the JSON analogue of MaskNullRows (field-null masking) on the array/
    // struct path, so all three containers exclude "no real array" rows.
    MaskJsonNonArrayRows(col_vec.get(), input, batch_rows);

    if (!has_offset_input_) {
        current_pos_ += batch_rows;
    }
}

void
PhyMatchFilterExpr::EvalWithOffsets(
    EvalCtx& context,
    VectorPtr& result,
    std::shared_ptr<const IArrayOffsets> array_offsets,
    FieldId field_id) {
    auto input = context.get_offset_input();
    AssertInfo(array_offsets != nullptr, "Array offsets not available");

    int64_t batch_rows;
    int64_t elem_start;
    int64_t elem_count;
    FixedVector<int32_t> element_offsets_storage;
    EvalCtx eval_ctx(context.get_exec_context());

    if (has_offset_input_) {
        // offset_input mode: process all input row_ids at once
        batch_rows = input->size();
        element_offsets_storage =
            array_offsets->RowOffsetsToElementOffsets(*input);
        eval_ctx.set_offset_input(&element_offsets_storage);
        elem_start = 0;
        elem_count = element_offsets_storage.size();
    } else {
        // Sequential batch mode
        batch_rows = std::min(batch_size_, active_count_ - current_pos_);
        auto start_range = array_offsets->ElementIDRangeOfRow(current_pos_);
        auto end_range =
            array_offsets->ElementIDRangeOfRow(current_pos_ + batch_rows);
        elem_start = start_range.first;
        elem_count = end_range.first - elem_start;
    }

    if (batch_rows <= 0) {
        result = nullptr;
        return;
    }

    result = std::make_shared<ColumnVector>(TargetBitmap(batch_rows, false),
                                            TargetBitmap(batch_rows, true));

    auto col_vec = std::dynamic_pointer_cast<ColumnVector>(result);
    TargetBitmapView bitset_view(col_vec->GetRawData(), col_vec->size());

    auto match_type = expr_->get_match_type();
    int64_t threshold = expr_->get_count();

    VectorPtr match_result;
    if (elem_count > 0) {
        inputs_[0]->Eval(eval_ctx, match_result);
    } else if (!has_offset_input_) {
        // Keep element-level child expressions aligned across all-empty batches.
        inputs_[0]->MoveCursor();
    }
    if (match_result == nullptr) {
        AssertInfo(elem_count == 0,
                   "Match child returned empty result for non-empty element "
                   "batch, elem_count={}",
                   elem_count);
        if (MatchEmptyElements(match_type, threshold)) {
            bitset_view.set();
        }
        MaskNullRows(col_vec.get(), field_id, input, batch_rows);
        if (!has_offset_input_) {
            current_pos_ += batch_rows;
        }
        return;
    }
    auto match_result_col_vec =
        std::dynamic_pointer_cast<ColumnVector>(match_result);
    AssertInfo(match_result_col_vec != nullptr,
               "Match result should be ColumnVector");
    AssertInfo(match_result_col_vec->IsBitmap(),
               "Match result should be bitmap");
    // Backstop for the parser-side predicate-shape validation: the child MUST
    // be element-level (one bit per element). A row-level child (row-space
    // column ref or a constant-folded predicate) returns batch_rows bits; the
    // per-row aggregation below would then read up to elem_count bits from it,
    // i.e. past its end. Fail loudly instead of returning garbage.
    AssertInfo(match_result_col_vec->size() == elem_count,
               "MATCH predicate produced a non-element-level bitmap: got {} "
               "bits, expected {} (one per array element); the predicate must "
               "only reference $ / $[subField]",
               match_result_col_vec->size(),
               elem_count);
    TargetBitmapView match_result_bitset_view(
        match_result_col_vec->GetRawData(), match_result_col_vec->size());
    TargetBitmapView match_result_valid_view(
        match_result_col_vec->GetValidRawData(), match_result_col_vec->size());

    bool all_valid = match_result_valid_view.all();

    auto dispatch = [&]<bool all_valid_v>() {
        switch (match_type) {
            case MatchType::MatchAny:
                DispatchMatchProcessing<MatchType::MatchAny, all_valid_v>(
                    has_offset_input_,
                    batch_rows,
                    current_pos_,
                    elem_start,
                    input,
                    array_offsets.get(),
                    match_result_bitset_view,
                    match_result_valid_view,
                    bitset_view,
                    threshold);
                break;
            case MatchType::MatchAll:
                DispatchMatchProcessing<MatchType::MatchAll, all_valid_v>(
                    has_offset_input_,
                    batch_rows,
                    current_pos_,
                    elem_start,
                    input,
                    array_offsets.get(),
                    match_result_bitset_view,
                    match_result_valid_view,
                    bitset_view,
                    threshold);
                break;
            case MatchType::MatchLeast:
                DispatchMatchProcessing<MatchType::MatchLeast, all_valid_v>(
                    has_offset_input_,
                    batch_rows,
                    current_pos_,
                    elem_start,
                    input,
                    array_offsets.get(),
                    match_result_bitset_view,
                    match_result_valid_view,
                    bitset_view,
                    threshold);
                break;
            case MatchType::MatchMost:
                DispatchMatchProcessing<MatchType::MatchMost, all_valid_v>(
                    has_offset_input_,
                    batch_rows,
                    current_pos_,
                    elem_start,
                    input,
                    array_offsets.get(),
                    match_result_bitset_view,
                    match_result_valid_view,
                    bitset_view,
                    threshold);
                break;
            case MatchType::MatchExact:
                DispatchMatchProcessing<MatchType::MatchExact, all_valid_v>(
                    has_offset_input_,
                    batch_rows,
                    current_pos_,
                    elem_start,
                    input,
                    array_offsets.get(),
                    match_result_bitset_view,
                    match_result_valid_view,
                    bitset_view,
                    threshold);
                break;
            default:
                ThrowInfo(OpTypeInvalid,
                          "Unsupported match type: {}",
                          static_cast<int>(match_type));
        }
    };

    if (all_valid) {
        dispatch.template operator()<true>();
    } else {
        dispatch.template operator()<false>();
    }

    MaskNullRows(col_vec.get(), field_id, input, batch_rows);
    if (!has_offset_input_) {
        current_pos_ += batch_rows;
    }
}

void
PhyMatchFilterExpr::MaskNullRows(ColumnVector* col_vec,
                                 FieldId field_id,
                                 const OffsetVector* input,
                                 int64_t batch_rows) {
    // Build the physical row-offset list for this batch (offset-input vs
    // sequential scan), then ask the segment to clear the valid bits of NULL
    // field rows. ApplyFieldValidDataByOffsets only CLEARS invalid rows and is
    // a no-op for a non-nullable field, so this is safe for all field kinds and
    // for both sealed and growing segments.
    std::vector<int64_t> row_offsets(batch_rows);
    for (int64_t i = 0; i < batch_rows; ++i) {
        row_offsets[i] = has_offset_input_ ? static_cast<int64_t>((*input)[i])
                                           : (current_pos_ + i);
    }
    TargetBitmapView bitset_view(col_vec->GetRawData(), col_vec->size());
    TargetBitmapView valid_view(col_vec->GetValidRawData(), col_vec->size());
    segment_->ApplyFieldValidDataByOffsets(
        op_ctx_, field_id, row_offsets.data(), batch_rows, valid_view);
    for (int64_t i = 0; i < batch_rows; ++i) {
        if (!valid_view[i]) {
            bitset_view[i] = false;
        }
    }
}

void
PhyMatchFilterExpr::MaskJsonNonArrayRows(ColumnVector* col_vec,
                                         const OffsetVector* input,
                                         int64_t batch_rows) {
    auto field_id = FieldId(expr_->get_field_id());
    auto pointer = milvus::Json::pointer(expr_->get_nested_path());

    TargetBitmapView bitset_view(col_vec->GetRawData(), col_vec->size());
    TargetBitmapView valid_view(col_vec->GetValidRawData(), col_vec->size());

    // Three-valued MATCH for JSON: a row survives only if it produces a REAL
    // JSON array at the path. Field NULL, JSON null at path, missing key, and a
    // non-array scalar/object at the path all yield UNKNOWN -> excluded. A
    // genuine empty array `[]` is a real array and keeps vacuous semantics.
    auto check =
        [&](int64_t out_idx, const milvus::Json& json, bool row_valid) {
            bool is_real_array = false;
            if (row_valid) {
                auto doc = json.doc();
                if (!doc.error()) {
                    auto v = pointer.empty()
                                 ? doc.value().get_array()
                                 : doc.value().at_pointer(pointer).get_array();
                    is_real_array = !v.error();
                }
            }
            if (!is_real_array) {
                valid_view[out_idx] = false;
                bitset_view[out_idx] = false;
            }
        };

    std::vector<int64_t> row_offsets(batch_rows);
    for (int64_t i = 0; i < batch_rows; ++i) {
        row_offsets[i] = has_offset_input_ ? static_cast<int64_t>((*input)[i])
                                           : (current_pos_ + i);
    }

    // Iterate JSON rows by offset. PhyMatchFilterExpr does not derive from
    // SegmentExpr, so we replicate VisitJsonRowsByOffsets' access logic inline
    // against segment_.
    if (segment_->type() == SegmentType::Sealed) {
        if (segment_->is_chunked()) {
            for (int64_t i = 0; i < batch_rows; ++i) {
                auto [chunk_id, chunk_offset] =
                    segment_->get_chunk_by_offset(field_id, row_offsets[i]);
                FixedVector<int32_t> offs;
                offs.push_back(static_cast<int32_t>(chunk_offset));
                auto pw = segment_->get_views_by_offsets<Json>(
                    op_ctx_, field_id, chunk_id, offs);
                auto [data_vec, valid_data] = pw.get();
                bool row_valid = !valid_data.data() || valid_data[0];
                check(i, data_vec[0], row_valid);
            }
        } else {
            FixedVector<int32_t> all_offsets(batch_rows);
            for (int64_t i = 0; i < batch_rows; ++i) {
                all_offsets[i] = static_cast<int32_t>(row_offsets[i]);
            }
            auto pw = segment_->get_views_by_offsets<Json>(
                op_ctx_, field_id, 0, all_offsets);
            auto [data_vec, valid_data] = pw.get();
            for (int64_t i = 0; i < batch_rows; ++i) {
                bool row_valid = !valid_data.data() || valid_data[i];
                check(i, data_vec[i], row_valid);
            }
        }
    } else {  // growing
        for (int64_t i = 0; i < batch_rows; ++i) {
            auto row_offset = row_offsets[i];
            auto chunk_id = row_offset / size_per_chunk_;
            auto chunk_offset = row_offset % size_per_chunk_;
            auto pw = segment_->chunk_data<Json>(op_ctx_, field_id, chunk_id);
            auto chunk = pw.get();
            const Json* data = chunk.data() + chunk_offset;
            const bool* valid_data = chunk.valid_data();
            if (valid_data != nullptr) {
                valid_data += chunk_offset;
            }
            bool row_valid = !valid_data || valid_data[0];
            check(i, *data, row_valid);
        }
    }
}

}  // namespace exec
}  // namespace milvus
