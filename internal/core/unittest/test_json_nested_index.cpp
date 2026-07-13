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

// Differential correctness test for the element-level NESTED JSON-array-path
// index (the EvalJsonIndexed fast path in PhyMatchFilterExpr).
//
// Contract under test: for a MATCH_* quantifier over a JSON array PATH, when a
// pinnable nested ARRAY-cast index exists for (field, path) AND its element
// count equals the JSON offsets' total element count (the alignment guard),
// EvalJsonIndexed serves the query; otherwise it falls back to EvalJsonBrute,
// which is the source of truth. This test builds two structurally IDENTICAL
// sealed segments from the same JSON rows -- one with the nested index loaded
// (-> indexed path) and one without (-> brute path) -- and asserts the matched
// row set is BIT-IDENTICAL between the two for every quantifier / predicate.
//
// Edge rows exercised per element type: NULL json (nullable field), absent
// path, non-array value at path, empty array `[]`, plus normal arrays. All of
// these carry zero *successfully-typed* elements into the index and zero raw
// elements into the offsets, so the alignment guard stays satisfied and the
// indexed path is genuinely taken (not silently degraded to brute on both
// sides). Also covers the bare-root-array case (empty JSON pointer) and a
// large-magnitude element value (> 2^53) that the design flags as parity
// sensitive for the int64/double injectivity fallback.

#include <gtest/gtest.h>
#include <cstdint>
#include <memory>
#include <numeric>
#include <set>
#include <string>
#include <vector>

#include "common/ArrayOffsets.h"
#include "common/Consts.h"
#include "common/JsonCastType.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "index/Index.h"
#include "index/ScalarIndexSort.h"
#include "index/StringIndexSort.h"
#include "query/Plan.h"
#include "segcore/SegmentInterface.h"
#include "segcore/SegmentSealed.h"
#include "segcore/Types.h"
#include "test_utils/DataGen.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

namespace {

// Build an insert proto: pk (INT64) + a JSON field, one hand-crafted JSON
// document per row. When `valid` is non-empty it is applied as the per-row
// null bitmap (a false entry => NULL json row).
std::unique_ptr<InsertRecordProto>
BuildJsonInsert(const Schema& schema,
                FieldId pk_fid,
                FieldId json_fid,
                const std::vector<std::string>& json_rows,
                const std::vector<bool>& valid = {}) {
    auto insert_data = std::make_unique<InsertRecordProto>();
    const int64_t N = static_cast<int64_t>(json_rows.size());

    std::vector<int64_t> ids(N);
    std::iota(ids.begin(), ids.end(), 0);
    auto id_array = CreateDataArrayFrom(ids.data(), nullptr, N, schema[pk_fid]);
    insert_data->mutable_fields_data()->AddAllocated(id_array.release());

    const bool* valid_ptr = nullptr;
    FixedVector<bool> valid_vec;
    if (!valid.empty()) {
        valid_vec.resize(N);
        for (int64_t i = 0; i < N; ++i) {
            valid_vec[i] = valid[i];
        }
        valid_ptr = valid_vec.data();
    }
    // CreateDataArrayFrom dispatches DataType::JSON to a const std::string*
    // backing buffer (the raw JSON document text per row).
    auto json_array =
        CreateDataArrayFrom(json_rows.data(), valid_ptr, N, schema[json_fid]);
    insert_data->mutable_fields_data()->AddAllocated(json_array.release());

    insert_data->set_num_rows(N);
    return insert_data;
}

// Create a sealed segment with the insert proto's field data loaded.
std::shared_ptr<SegmentInterface>
MakeSealed(SchemaPtr schema, std::unique_ptr<InsertRecordProto> insert) {
    const int64_t N = insert->num_rows();
    GeneratedData generated;
    generated.schema_ = schema;
    generated.raw_ = insert.release();
    for (int64_t i = 0; i < N; ++i) {
        generated.row_ids_.push_back(i);
        generated.timestamps_.push_back(i);
    }
    auto seg = CreateSealedWithFieldDataLoaded(schema, generated);
    return std::shared_ptr<SegmentInterface>(std::move(seg));
}

// Parse `expr` against `schema`, run Retrieve, return matched segment offsets.
std::set<int64_t>
RetrieveRows(SegmentInterface* seg,
             const Schema& schema,
             SchemaPtr schema_ptr,
             const std::string& expr) {
    ScopedSchemaHandle schema_handle(schema);
    auto plan_str = schema_handle.Parse(expr);
    auto plan = milvus::query::CreateRetrievePlanByExpr(
        schema_ptr, plan_str.data(), plan_str.size());
    EXPECT_NE(plan, nullptr) << expr;
    auto result = seg->Retrieve(
        nullptr, plan.get(), 1L << 63, DEFAULT_MAX_OUTPUT_SIZE, false);
    EXPECT_NE(result, nullptr) << expr;
    std::set<int64_t> rows;
    for (const auto& offset : result->offset()) {
        rows.insert(offset);
    }
    return rows;
}

// Load a NESTED ARRAY-cast index onto a JSON path so PhyMatchFilterExpr's
// EvalJsonIndexed can pin it. `elements` is the flat, row-order concatenation
// of every array element at `pointer` across all rows (exactly what
// ConvertJsonToArrayFieldData<T> would materialize -- for these fixtures every
// array element is a valid scalar, so no element is dropped and the index's
// Size() equals the JSON offsets' total element count).
//
// The load key is (field_id, JSON_PATH=pointer, JSON_CAST_TYPE=cast). No
// GetCastType() call happens: ChunkedSegmentSealedImpl::LoadIndex reads the
// cast type straight from index_params[JSON_CAST_TYPE], so a plain nested
// ScalarIndexSort / StringIndexSort (not the JsonNestedIndexWrapper) is a
// faithful stand-in for the successfully-typed elements the wrapper produces.
template <typename Index, typename T>
void
LoadNestedJsonIndex(SegmentInterface* seg,
                    FieldId json_fid,
                    const std::string& pointer,
                    const std::string& cast_type_str,
                    const std::vector<T>& elements) {
    auto index = std::make_unique<Index>(storage::FileManagerContext(),
                                          /*is_nested_index=*/true);
    index->Build(elements.size(), elements.data(), nullptr);

    LoadIndexInfo info{};
    info.field_id = json_fid.get();
    info.field_type = DataType::JSON;
    info.index_params["index_type"] = index->Type();
    info.index_params[JSON_PATH] = pointer;
    info.index_params[JSON_CAST_TYPE] = cast_type_str;
    info.cache_index =
        CreateTestCacheIndex("nested_json_idx", std::move(index));
    auto sealed = dynamic_cast<SegmentSealed*>(seg);
    ASSERT_NE(sealed, nullptr);
    sealed->LoadIndex(info);
}

// std::vector<bool> has no data(); ScalarIndexSort<bool>::Build wants a bool*.
void
LoadNestedJsonBoolIndex(SegmentInterface* seg,
                        FieldId json_fid,
                        const std::string& pointer,
                        const std::vector<bool>& elements) {
    auto index = std::make_unique<index::ScalarIndexSort<bool>>(
        storage::FileManagerContext(), /*is_nested_index=*/true);
    // Materialize a contiguous bool buffer (std::vector<bool> is a bitset).
    auto buf = std::make_unique<bool[]>(elements.size());
    for (size_t i = 0; i < elements.size(); ++i) {
        buf[i] = elements[i];
    }
    index->Build(elements.size(), buf.get(), nullptr);

    LoadIndexInfo info{};
    info.field_id = json_fid.get();
    info.field_type = DataType::JSON;
    info.index_params["index_type"] = index->Type();
    info.index_params[JSON_PATH] = pointer;
    info.index_params[JSON_CAST_TYPE] = "ARRAY_BOOL";
    info.cache_index =
        CreateTestCacheIndex("nested_json_bool_idx", std::move(index));
    auto sealed = dynamic_cast<SegmentSealed*>(seg);
    ASSERT_NE(sealed, nullptr);
    sealed->LoadIndex(info);
}

SchemaPtr
MakeJsonSchema(FieldId& pk_fid, FieldId& json_fid, bool nullable) {
    auto schema = std::make_shared<Schema>();
    pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    json_fid = schema->AddDebugField("json", DataType::JSON, nullable);
    return schema;
}

// Assert idx_rows == brute_rows for every predicate/quantifier in `exprs`.
void
AssertIndexedEqualsBrute(SegmentInterface* indexed,
                         SegmentInterface* brute,
                         const Schema& schema,
                         SchemaPtr schema_ptr,
                         const std::vector<std::string>& exprs) {
    for (const auto& e : exprs) {
        auto idx_rows = RetrieveRows(indexed, schema, schema_ptr, e);
        auto brute_rows = RetrieveRows(brute, schema, schema_ptr, e);
        EXPECT_EQ(idx_rows, brute_rows)
            << "indexed vs brute-force divergence for: " << e;
    }
}

}  // namespace

// ============================================================================
// ARRAY_DOUBLE: ~500 rows of JSON at path /arr, mixing normal double arrays
// with all edge rows. Indexed (nested ScalarIndexSort<double>) vs brute over
// every MATCH_* quantifier and both an eq (`$ == v`) and a range (`$ > v`)
// element predicate (both of which the index serves).
// ============================================================================
TEST(JsonNestedIndexDifferential, ArrayDoubleMatchesBrute) {
    FieldId pk_fid, json_fid;
    auto schema = MakeJsonSchema(pk_fid, json_fid, /*nullable=*/true);

    const int64_t N = 500;
    std::vector<std::string> json_rows(N);
    std::vector<bool> valid(N, true);
    std::vector<double> elements;  // flat, row-order, successfully-typed

    for (int64_t i = 0; i < N; ++i) {
        // Sprinkle the five edge shapes across the first rows and then
        // periodically, so both contiguous-batch and offset paths see them.
        int shape = static_cast<int>(i % 25);
        if (shape == 0) {
            // NULL json row (nullable): zero raw elements, zero indexed.
            json_rows[i] = "{}";  // content irrelevant, valid bit is cleared
            valid[i] = false;
        } else if (shape == 1) {
            // absent path: no `/arr` key.
            json_rows[i] = R"({"other": [1.0, 2.0]})";
        } else if (shape == 2) {
            // non-array value at the path.
            json_rows[i] = R"({"arr": 42.5})";
        } else if (shape == 3) {
            // genuine empty array: valid row, zero elements.
            json_rows[i] = R"({"arr": []})";
        } else {
            // normal array of doubles; length 1..4, values keyed off i so
            // selectivity varies across all predicates.
            int len = 1 + (shape % 4);
            std::string body;
            for (int k = 0; k < len; ++k) {
                double v = static_cast<double>((i * 7 + k * 13) % 200) + 0.5;
                if (k) {
                    body += ",";
                }
                body += std::to_string(v);
                elements.push_back(v);
            }
            json_rows[i] = R"({"arr": [)" + body + "]}";
        }
    }

    auto insert_i = BuildJsonInsert(*schema, pk_fid, json_fid, json_rows, valid);
    auto indexed = MakeSealed(schema, std::move(insert_i));
    LoadNestedJsonIndex<index::ScalarIndexSort<double>, double>(
        indexed.get(), json_fid, "/arr", "ARRAY_DOUBLE", elements);

    // Prove the indexed path (EvalJsonIndexed) is actually taken, not silently
    // degraded to brute -- otherwise the differential below would be a trivial
    // brute-vs-brute tautology. Two conditions gate EvalJsonIndexed:
    //   (1) PinJsonIndex(field, "/arr", DOUBLE, any_type=false, is_array=true)
    //       returns the nested ARRAY_DOUBLE index, and
    //   (2) index->Size() == GetJsonArrayOffsets(...)->GetTotalElementCount()
    //       (the alignment guard).
    {
        auto* sealed_indexed =
            dynamic_cast<SegmentInternalInterface*>(indexed.get());
        ASSERT_NE(sealed_indexed, nullptr);
        auto pins = sealed_indexed->PinJsonIndex(nullptr,
                                                 json_fid,
                                                 "/arr",
                                                 DataType::DOUBLE,
                                                 /*any_type=*/false,
                                                 /*is_array=*/true);
        ASSERT_FALSE(pins.empty())
            << "nested ARRAY_DOUBLE JSON index was not pinnable -- the indexed "
               "fast path would fall back to brute";
        ASSERT_TRUE(pins[0].get()->IsNestedIndex());
        auto offsets = sealed_indexed->GetJsonArrayOffsets(json_fid, "/arr");
        ASSERT_NE(offsets, nullptr);
        // Alignment guard (index element count == offsets total element count).
        // `elements` is exactly what the index was built from, so this proves
        // the guard in EvalJsonIndexedImpl passes and the index is used.
        ASSERT_EQ(static_cast<int64_t>(elements.size()),
                  offsets->GetTotalElementCount())
            << "alignment guard would fail -> EvalJsonIndexed falls back to "
               "brute; the differential test would not exercise the index";
    }

    auto insert_b = BuildJsonInsert(*schema, pk_fid, json_fid, json_rows, valid);
    auto brute = MakeSealed(schema, std::move(insert_b));

    const std::vector<std::string> exprs = {
        // eq (-> index In) and range (-> index Range) element predicates,
        // across all five quantifiers.
        R"(MATCH_ANY(json["arr"], $ > 100.5))",
        R"(MATCH_ALL(json["arr"], $ > 100.5))",
        R"(MATCH_LEAST(json["arr"], $ > 100.5, threshold=2))",
        R"(MATCH_MOST(json["arr"], $ > 100.5, threshold=1))",
        R"(MATCH_EXACT(json["arr"], $ > 100.5, threshold=1))",
        R"(MATCH_ANY(json["arr"], $ == 50.5))",
        R"(MATCH_ALL(json["arr"], $ == 50.5))",
        R"(MATCH_LEAST(json["arr"], $ >= 50.5, threshold=1))",
        R"(MATCH_MOST(json["arr"], $ <= 50.5, threshold=2))",
        R"(MATCH_EXACT(json["arr"], $ != 50.5, threshold=0))",
        // Term ($ in [..]) -> index In.
        R"(MATCH_ANY(json["arr"], $ in [50.5, 100.5, 150.5]))",
    };
    AssertIndexedEqualsBrute(
        indexed.get(), brute.get(), *schema, schema, exprs);
}

// ============================================================================
// ARRAY_VARCHAR: nested StringIndexSort over a JSON string-array path. Same
// edge rows + quantifiers, string predicates (eq / range / term).
// ============================================================================
TEST(JsonNestedIndexDifferential, ArrayVarcharMatchesBrute) {
    FieldId pk_fid, json_fid;
    auto schema = MakeJsonSchema(pk_fid, json_fid, /*nullable=*/true);

    const int64_t N = 500;
    std::vector<std::string> json_rows(N);
    std::vector<bool> valid(N, true);
    std::vector<std::string> elements;

    const char* words[] = {"apple", "banana", "cherry", "date", "elder"};
    for (int64_t i = 0; i < N; ++i) {
        int shape = static_cast<int>(i % 25);
        if (shape == 0) {
            json_rows[i] = "{}";
            valid[i] = false;
        } else if (shape == 1) {
            json_rows[i] = R"({"other": ["x"]})";
        } else if (shape == 2) {
            json_rows[i] = R"({"arr": "not-an-array"})";
        } else if (shape == 3) {
            json_rows[i] = R"({"arr": []})";
        } else {
            int len = 1 + (shape % 4);
            std::string body;
            for (int k = 0; k < len; ++k) {
                std::string v = words[(i * 3 + k) % 5];
                if (k) {
                    body += ",";
                }
                body += "\"" + v + "\"";
                elements.push_back(v);
            }
            json_rows[i] = R"({"arr": [)" + body + "]}";
        }
    }

    auto insert_i = BuildJsonInsert(*schema, pk_fid, json_fid, json_rows, valid);
    auto indexed = MakeSealed(schema, std::move(insert_i));
    LoadNestedJsonIndex<index::StringIndexSort, std::string>(
        indexed.get(), json_fid, "/arr", "ARRAY_VARCHAR", elements);

    auto insert_b = BuildJsonInsert(*schema, pk_fid, json_fid, json_rows, valid);
    auto brute = MakeSealed(schema, std::move(insert_b));

    const std::vector<std::string> exprs = {
        R"(MATCH_ANY(json["arr"], $ == "apple"))",
        R"(MATCH_ALL(json["arr"], $ == "apple"))",
        R"(MATCH_LEAST(json["arr"], $ == "banana", threshold=1))",
        R"(MATCH_MOST(json["arr"], $ == "cherry", threshold=1))",
        R"(MATCH_EXACT(json["arr"], $ == "date", threshold=1))",
        R"(MATCH_ANY(json["arr"], $ > "cherry"))",
        R"(MATCH_ALL(json["arr"], $ >= "apple"))",
        R"(MATCH_ANY(json["arr"], $ in ["apple", "elder"]))",
        R"(MATCH_LEAST(json["arr"], $ < "date", threshold=2))",
    };
    AssertIndexedEqualsBrute(
        indexed.get(), brute.get(), *schema, schema, exprs);
}

// ============================================================================
// ARRAY_BOOL: nested ScalarIndexSort<bool>. Bool inequalities are rejected by
// the parser/fast-path guard, so only eq / neq / term predicates apply.
// ============================================================================
TEST(JsonNestedIndexDifferential, ArrayBoolMatchesBrute) {
    FieldId pk_fid, json_fid;
    auto schema = MakeJsonSchema(pk_fid, json_fid, /*nullable=*/true);

    const int64_t N = 500;
    std::vector<std::string> json_rows(N);
    std::vector<bool> valid(N, true);
    std::vector<bool> elements;

    for (int64_t i = 0; i < N; ++i) {
        int shape = static_cast<int>(i % 25);
        if (shape == 0) {
            json_rows[i] = "{}";
            valid[i] = false;
        } else if (shape == 1) {
            json_rows[i] = R"({"other": [true]})";
        } else if (shape == 2) {
            json_rows[i] = R"({"arr": true})";
        } else if (shape == 3) {
            json_rows[i] = R"({"arr": []})";
        } else {
            int len = 1 + (shape % 4);
            std::string body;
            for (int k = 0; k < len; ++k) {
                bool v = ((i + k) % 3) == 0;
                if (k) {
                    body += ",";
                }
                body += v ? "true" : "false";
                elements.push_back(v);
            }
            json_rows[i] = R"({"arr": [)" + body + "]}";
        }
    }

    auto insert_i = BuildJsonInsert(*schema, pk_fid, json_fid, json_rows, valid);
    auto indexed = MakeSealed(schema, std::move(insert_i));
    LoadNestedJsonBoolIndex(indexed.get(), json_fid, "/arr", elements);

    auto insert_b = BuildJsonInsert(*schema, pk_fid, json_fid, json_rows, valid);
    auto brute = MakeSealed(schema, std::move(insert_b));

    const std::vector<std::string> exprs = {
        R"(MATCH_ANY(json["arr"], $ == true))",
        R"(MATCH_ALL(json["arr"], $ == true))",
        R"(MATCH_ANY(json["arr"], $ == false))",
        R"(MATCH_LEAST(json["arr"], $ == true, threshold=1))",
        R"(MATCH_MOST(json["arr"], $ == true, threshold=1))",
        R"(MATCH_EXACT(json["arr"], $ == true, threshold=2))",
        R"(MATCH_ANY(json["arr"], $ != true))",
        R"(MATCH_ANY(json["arr"], $ in [true, false]))",
    };
    AssertIndexedEqualsBrute(
        indexed.get(), brute.get(), *schema, schema, exprs);
}

// ============================================================================
// Bare-root array: the JSON document itself is the array (empty JSON pointer).
// The stored index nested_path and GetJsonArrayOffsets pointer are both "".
// ============================================================================
TEST(JsonNestedIndexDifferential, BareRootArrayMatchesBrute) {
    FieldId pk_fid, json_fid;
    auto schema = MakeJsonSchema(pk_fid, json_fid, /*nullable=*/true);

    const int64_t N = 200;
    std::vector<std::string> json_rows(N);
    std::vector<bool> valid(N, true);
    std::vector<double> elements;

    for (int64_t i = 0; i < N; ++i) {
        int shape = static_cast<int>(i % 20);
        if (shape == 0) {
            json_rows[i] = "[]";  // valid empty root array
        } else if (shape == 1) {
            json_rows[i] = "{}";  // non-array root value
            valid[i] = true;
        } else if (shape == 2) {
            json_rows[i] = "null";  // JSON null root
            valid[i] = false;       // model as field-NULL
        } else {
            int len = 1 + (shape % 4);
            std::string body;
            for (int k = 0; k < len; ++k) {
                double v = static_cast<double>((i * 5 + k * 11) % 100) + 0.25;
                if (k) {
                    body += ",";
                }
                body += std::to_string(v);
                elements.push_back(v);
            }
            json_rows[i] = "[" + body + "]";
        }
    }

    auto insert_i = BuildJsonInsert(*schema, pk_fid, json_fid, json_rows, valid);
    auto indexed = MakeSealed(schema, std::move(insert_i));
    LoadNestedJsonIndex<index::ScalarIndexSort<double>, double>(
        indexed.get(), json_fid, /*pointer=*/"", "ARRAY_DOUBLE", elements);

    auto insert_b = BuildJsonInsert(*schema, pk_fid, json_fid, json_rows, valid);
    auto brute = MakeSealed(schema, std::move(insert_b));

    const std::vector<std::string> exprs = {
        "MATCH_ANY(json, $ > 50.25)",
        "MATCH_ALL(json, $ > 50.25)",
        "MATCH_ANY(json, $ == 25.25)",
        "MATCH_LEAST(json, $ > 50.25, threshold=2)",
        "MATCH_MOST(json, $ > 50.25, threshold=1)",
        "MATCH_EXACT(json, $ > 50.25, threshold=1)",
    };
    AssertIndexedEqualsBrute(
        indexed.get(), brute.get(), *schema, schema, exprs);
}

// ============================================================================
// Large-magnitude values (> 2^53): parity-sensitive because an int64 literal
// outside [-2^53, 2^53) is NOT injective in double, so EvalJsonIndexed must
// FALL BACK to brute to keep int64-exact comparison. The result must still
// equal brute. Includes an in-range (< 2^53) integer literal that DOES take
// the indexed double path, so both branches are exercised.
// ============================================================================
TEST(JsonNestedIndexDifferential, LargeMagnitudeValuesMatchBrute) {
    FieldId pk_fid, json_fid;
    auto schema = MakeJsonSchema(pk_fid, json_fid, /*nullable=*/false);

    // Values straddling 2^53 (=9007199254740992). Elements are written as JSON
    // integers; the ARRAY_DOUBLE cast reads them as doubles (lossy above 2^53),
    // exactly what both the nested index and the offsets see.
    const std::vector<std::string> json_rows = {
        R"({"arr": [1, 2, 3]})",
        R"({"arr": [9007199254740993, 100]})",   // 2^53 + 1
        R"({"arr": [9007199254740992]})",         // 2^53 exactly
        R"({"arr": [-9007199254740993]})",        // -(2^53 + 1)
        R"({"arr": [42, 9007199254740994]})",
        R"({"arr": []})",
    };
    const int64_t N = static_cast<int64_t>(json_rows.size());

    // Flat elements as doubles, matching the ARRAY_DOUBLE cast.
    std::vector<double> elements = {1.0,
                                    2.0,
                                    3.0,
                                    9007199254740993.0,
                                    100.0,
                                    9007199254740992.0,
                                    -9007199254740993.0,
                                    42.0,
                                    9007199254740994.0};

    auto insert_i = BuildJsonInsert(*schema, pk_fid, json_fid, json_rows);
    auto indexed = MakeSealed(schema, std::move(insert_i));
    LoadNestedJsonIndex<index::ScalarIndexSort<double>, double>(
        indexed.get(), json_fid, "/arr", "ARRAY_DOUBLE", elements);

    auto insert_b = BuildJsonInsert(*schema, pk_fid, json_fid, json_rows);
    auto brute = MakeSealed(schema, std::move(insert_b));

    const std::vector<std::string> exprs = {
        // In-range integer literal: |v| < 2^53 -> indexed double path taken.
        R"(MATCH_ANY(json["arr"], $ == 42))",
        R"(MATCH_ANY(json["arr"], $ > 50))",
        // Out-of-range integer literals: must fall back to brute (int64-exact).
        R"(MATCH_ANY(json["arr"], $ == 9007199254740993))",
        R"(MATCH_ANY(json["arr"], $ == 9007199254740992))",
        R"(MATCH_ANY(json["arr"], $ == -9007199254740993))",
        R"(MATCH_ANY(json["arr"], $ > 9007199254740992))",
        R"(MATCH_ANY(json["arr"], $ in [9007199254740993, 9007199254740994]))",
        // Float literal above 2^53 (kFloatVal -> indexed double path).
        R"(MATCH_ANY(json["arr"], $ > 9007199254740992.0))",
    };
    AssertIndexedEqualsBrute(
        indexed.get(), brute.get(), *schema, schema, exprs);
}
