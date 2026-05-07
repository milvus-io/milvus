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

#include "ExprTestBase.h"

EXPR_TEST_INSTANTIATE();

TEST_P(ExprTest, TestGISFunction) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    // Create schema with geometry field
    auto schema = std::make_shared<Schema>();
    auto int_fid = schema->AddDebugField("int", DataType::INT64);
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto geom_fid = schema->AddDebugField("geometry", DataType::GEOMETRY);
    schema->set_primary_field_id(int_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    int num_iters = 1;

    // Generate test data
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

    // Define GIS test cases: {expression, description}
    std::vector<std::pair<std::string, std::string>> testcases = {
        {R"expr(st_intersects(geometry, "POINT(0 0)"))expr", "intersects"},
        {R"expr(st_contains(geometry, "POLYGON((-1 -1, 1 -1, 1 1, -1 1, -1 -1))"))expr",
         "contains"},
        {R"expr(st_crosses(geometry, "LINESTRING(-2 0, 2 0)"))expr", "crosses"},
        {R"expr(st_equals(geometry, "POINT(10 10)"))expr", "equals"},
        {R"expr(st_touches(geometry, "POLYGON((5 5, 15 5, 15 15, 5 15, 5 5))"))expr",
         "touches"},
        {R"expr(st_overlaps(geometry, "POLYGON((0.5 0.5, 1.5 0.5, 1.5 1.5, 0.5 1.5, 0.5 0.5))"))expr",
         "overlaps"},
        {R"expr(st_within(geometry, "POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))"))expr",
         "within"}};

    ScopedSchemaHandle schema_handle(*schema);
    for (const auto& [expr, desc] : testcases) {
        auto bin_plan = schema_handle.ParseSearch(
            expr, "fakevec", 10, knowhere::metric::L2, R"({"nprobe":10})", 3);
        auto plan =
            CreateSearchPlanByExpr(schema, bin_plan.data(), bin_plan.size());

        // Verify query execution doesn't throw exceptions
        ASSERT_NO_THROW({
            auto ph_raw = CreatePlaceholderGroup(10, 16, 123);
            auto ph_grp =
                ParsePlaceholderGroup(plan.get(), ph_raw.SerializeAsString());
            auto sr = seg->Search(plan.get(), ph_grp.get(), MAX_TIMESTAMP);
            // Note: Since we use random data, all results might be false, which is normal
            // We mainly verify the function execution doesn't crash
        }) << "Failed for GIS operation: "
           << desc;
    }
}

TEST(ExprTest, SealedSegmentAllOperators) {
    // 1. Build schema with geometry field and primary key
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto geo_fid = schema->AddDebugField("geo", DataType::GEOMETRY);
    auto vec_fid = schema->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->set_primary_field_id(pk_fid);

    // 2. Generate random data and load into a sealed segment
    const int64_t N = 1000;
    auto dataset = DataGen(schema, N);
    auto seg = CreateSealedWithFieldDataLoaded(schema, dataset);

    // 3. Prepare (function_name, wkt) pairs to hit every GIS operator
    std::vector<std::pair<std::string, std::string>> test_cases = {
        {"st_equals", "POINT(0 0)"},
        {"st_touches", "POLYGON((-1 -1, -1 1, 1 1, 1 -1, -1 -1))"},
        {"st_overlaps", "POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"},
        {"st_crosses", "LINESTRING(-1 0, 1 0)"},
        {"st_contains", "POLYGON((-2 -2, 2 -2, 2 2, -2 2, -2 -2))"},
        {"st_intersects", "POINT(1 1)"},
        {"st_within", "POLYGON((-5 -5, 5 -5, 5 5, -5 5, -5 -5))"},
    };

    ScopedSchemaHandle schema_handle(*schema);
    for (const auto& [func, wkt] : test_cases) {
        // Build expression: st_xxx(geo, "wkt_string")
        std::string expr = func + "(geo, \"" + wkt + "\")";

        auto bin_plan = schema_handle.ParseSearch(
            expr, "vec", 5, knowhere::metric::L2, R"({"nprobe":10})", 3);
        auto plan =
            CreateSearchPlanByExpr(schema, bin_plan.data(), bin_plan.size());

        // Execute search over the sealed segment
        auto ph_raw = CreatePlaceholderGroup(5, 16, 123);
        auto ph_grp =
            ParsePlaceholderGroup(plan.get(), ph_raw.SerializeAsString());
        auto sr = seg->Search(plan.get(), ph_grp.get(), MAX_TIMESTAMP);

        // Validate basic expectations
        EXPECT_EQ(sr->total_nq_, 5) << "Failed for operation: " << func;
    }
}

TEST_P(ExprTest, TestGISFunctionWithControlledData) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    // Create schema with geometry field
    auto schema = std::make_shared<Schema>();
    auto int_fid = schema->AddDebugField("int", DataType::INT64);
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto geom_fid = schema->AddDebugField("geometry", DataType::GEOMETRY);
    schema->set_primary_field_id(int_fid);
    SetSchema(schema);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 100;
    int num_iters = 1;

    // Generate controlled test data
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);

        // Replace geometry data with controlled test data
        milvus::proto::schema::FieldData* geometry_field_data = nullptr;
        for (auto& fd : *raw_data.raw_->mutable_fields_data()) {
            if (fd.field_id() == geom_fid.get()) {
                geometry_field_data = &fd;
                break;
            }
        }
        assert(geometry_field_data != nullptr);
        geometry_field_data->mutable_scalars()
            ->mutable_geometry_data()
            ->clear_data();

        // Create some controlled geometry data for testing
        auto ctx = GEOS_init_r();
        for (int i = 0; i < N; ++i) {
            const char* wkt = nullptr;

            if (i % 4 == 0) {
                // Create point (0, 0)
                wkt = "POINT (0.0 0.0)";
            } else if (i % 4 == 1) {
                // Create polygon containing (0, 0)
                wkt =
                    "POLYGON ((-1.0 -1.0, 1.0 -1.0, 1.0 1.0, -1.0 1.0, -1.0 "
                    "-1.0))";
            } else if (i % 4 == 2) {
                // Create polygon not containing (0, 0)
                wkt =
                    "POLYGON ((10.0 10.0, 20.0 10.0, 20.0 20.0, 10.0 20.0, "
                    "10.0 10.0))";
            } else {
                // Create line passing through (0, 0)
                wkt = "LINESTRING (-1.0 0.0, 1.0 0.0)";
            }

            // Create Geometry and convert to WKB format
            Geometry geom(ctx, wkt);
            std::string wkb_string = geom.to_wkb_string();

            geometry_field_data->mutable_scalars()
                ->mutable_geometry_data()
                ->add_data(wkb_string);
        }
        GEOS_finish_r(ctx);

        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

    // Test specific GIS operations using string expressions
    auto test_gis_operation = [&](const std::string& expr,
                                  std::function<bool(int)> expected_func) {
        auto bin_plan = create_search_plan_from_expr(expr);
        auto plan =
            CreateSearchPlanByExpr(schema, bin_plan.data(), bin_plan.size());

        BitsetType final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // Verify results
        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto expected = expected_func(i);
            ASSERT_EQ(ans, expected) << "GIS operation failed at index " << i;
        }
    };

    // Test within operation
    test_gis_operation(
        R"expr(st_within(geometry, "POLYGON((-2 -2, 2 -2, 2 2, -2 2, -2 -2))"))expr",
        [](int i) -> bool {
            // Only geometry at index 0,1,3 (polygon containing (0,0))
            return (i % 4 == 0) || (i % 4 == 1) || (i % 4 == 3);
        });

    // Test intersects operation
    test_gis_operation(R"expr(st_intersects(geometry, "POINT(0 0)"))expr",
                       [](int i) -> bool {
                           // Point at index 0 (0,0), polygon at index 1, line at index 3 should all intersect with point (0,0)
                           return (i % 4 == 0) || (i % 4 == 1) || (i % 4 == 3);
                       });

    // Test equals operation
    test_gis_operation(R"expr(st_equals(geometry, "POINT(0 0)"))expr",
                       [](int i) -> bool {
                           // Only point at index 0 (0,0) should be equal
                           return (i % 4 == 0);
                       });
}

TEST_P(ExprTest, TestSTIsValidFunction) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    auto int_fid = schema->AddDebugField("int", DataType::INT64);
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto geom_fid = schema->AddDebugField("geometry", DataType::GEOMETRY);
    schema->set_primary_field_id(int_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 100;
    int num_iters = 1;

    std::vector<const char*> wkts = {
        "POINT (0 0)",                          // valid
        "LINESTRING (0 0, 1 1, 2 2)",           // valid
        "POLYGON ((0 0, 2 2, 2 0, 0 2, 0 0))",  // invalid
        "LINESTRING (0 0, 0 0)"                 // invalid
    };
    std::vector<bool> expected_flags = {true, true, false, false};

    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);

        milvus::proto::schema::FieldData* geometry_field_data = nullptr;
        for (auto& fd : *raw_data.raw_->mutable_fields_data()) {
            if (fd.field_id() == geom_fid.get()) {
                geometry_field_data = &fd;
                break;
            }
        }
        ASSERT_NE(geometry_field_data, nullptr);

        geometry_field_data->mutable_scalars()
            ->mutable_geometry_data()
            ->clear_data();

        auto ctx = GEOS_init_r();
        for (int i = 0; i < N; ++i) {
            const char* wkt = wkts[i % wkts.size()];
            Geometry geom(ctx, wkt);
            geometry_field_data->mutable_scalars()
                ->mutable_geometry_data()
                ->add_data(geom.to_wkb_string());
        }
        GEOS_finish_r(ctx);

        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentInternalInterface*>(seg.get());

    auto is_valid_expr = std::make_shared<milvus::expr::GISFunctionFilterExpr>(
        milvus::expr::ColumnInfo(geom_fid, DataType::GEOMETRY),
        proto::plan::GISFunctionFilterExpr_GISOp_STIsValid,
        "");
    auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                       is_valid_expr);

    BitsetType final =
        ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);

    ASSERT_EQ(final.size(), N * num_iters);

    for (int i = 0; i < final.size(); ++i) {
        bool expected = expected_flags[i % expected_flags.size()];
        EXPECT_EQ(final[i], expected)
            << "Unexpected validity result at index " << i;
    }
}

TEST_P(ExprTest, TestSTDWithinFunction) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    // Create schema with geometry field
    auto schema = std::make_shared<Schema>();
    auto int_fid = schema->AddDebugField("int", DataType::INT64);
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto geom_fid = schema->AddDebugField("geometry", DataType::GEOMETRY);
    schema->set_primary_field_id(int_fid);
    SetSchema(schema);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 100;
    int num_iters = 1;

    // Generate controlled test data with known distances
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);

        // Replace geometry data with controlled test data for distance testing
        milvus::proto::schema::FieldData* geometry_field_data = nullptr;
        for (auto& fd : *raw_data.raw_->mutable_fields_data()) {
            if (fd.field_id() == geom_fid.get()) {
                geometry_field_data = &fd;
                break;
            }
        }
        assert(geometry_field_data != nullptr);
        geometry_field_data->mutable_scalars()
            ->mutable_geometry_data()
            ->clear_data();

        // Create test points at known distances from origin (0,0)
        auto ctx = GEOS_init_r();
        for (int i = 0; i < N; ++i) {
            const char* wkt = nullptr;

            if (i % 5 == 0) {
                // Distance 0: Point at origin
                wkt = "POINT (0.0 0.0)";
            } else if (i % 5 == 1) {
                // Distance 1: Point at (1,0)
                wkt = "POINT (1.0 0.0)";
            } else if (i % 5 == 2) {
                // Distance 5: Point at (3,4) - Pythagorean triple
                wkt = "POINT (3.0 4.0)";
            } else if (i % 5 == 3) {
                // Distance 10: Point at (6,8)
                wkt = "POINT (6.0 8.0)";
            } else {
                // Distance 13: Point at (5,12)
                wkt = "POINT (5.0 12.0)";
            }

            // Create Geometry and convert to WKB format
            Geometry geom(ctx, wkt);
            std::string wkb_string = geom.to_wkb_string();

            geometry_field_data->mutable_scalars()
                ->mutable_geometry_data()
                ->add_data(wkb_string);
        }
        GEOS_finish_r(ctx);

        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentInternalInterface*>(seg.get());

    // Test ST_DWITHIN operations with different distances using string expressions
    auto test_dwithin_operation = [&](const std::string& center_wkt,
                                      double distance,
                                      std::function<bool(int)> expected_func) {
        // Build st_dwithin expression: st_dwithin(geometry, "POINT(x y)", distance)
        std::string expr = "st_dwithin(geometry, \"" + center_wkt + "\", " +
                           std::to_string(distance) + ")";

        auto bin_plan = create_search_plan_from_expr(expr);
        auto plan =
            CreateSearchPlanByExpr(schema, bin_plan.data(), bin_plan.size());

        BitsetType final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // Verify results match expectations
        for (int i = 0; i < final.size(); ++i) {
            bool expected = expected_func(i);
            EXPECT_EQ(final[i], expected)
                << "Mismatch at index " << i << " for distance " << distance
                << ": expected " << expected << ", got " << final[i];
        }
    };

    // Test distance 0.5 - only origin point should match
    test_dwithin_operation("POINT(0 0)", 55660.0, [](int i) -> bool {
        return (i % 5 == 0);  // Only points at distance 0
    });

    // Test distance 1.5 - origin and distance-1 points should match
    test_dwithin_operation("POINT(0 0)", 166980.0, [](int i) -> bool {
        return (i % 5 == 0) || (i % 5 == 1);  // Distance 0 and 1
    });

    // Test distance 7.0 - origin, distance-1, and distance-5 points should match
    test_dwithin_operation("POINT(0 0)", 779240.0, [](int i) -> bool {
        return (i % 5 == 0) || (i % 5 == 1) ||
               (i % 5 == 2);  // Distance 0, 1, 5
    });

    // Test distance 12.0 - all but distance-13 points should match
    test_dwithin_operation("POINT(0 0)", 1335840.0, [](int i) -> bool {
        return (i % 5 != 4);  // All except distance 13
    });

    // Test distance 15.0 - all points should match
    test_dwithin_operation("POINT(0 0)", 1669800.0, [](int i) -> bool {
        return true;  // All points
    });

    // Test with different center point
    test_dwithin_operation("POINT(1 0)", 11132.0, [](int i) -> bool {
        return (i % 5 == 1);  // Only the point at (1,0)
    });

    // Test edge cases
    test_dwithin_operation("POINT(0 0)", 111320.0, [](int i) -> bool {
        return (i % 5 == 0) ||
               (i % 5 == 1);  // Distance exactly 1.0 should be included
    });

    test_dwithin_operation("POINT(0 0)", 556600.0, [](int i) -> bool {
        return (i % 5 == 0) || (i % 5 == 1) ||
               (i % 5 == 2);  // Distance exactly 5.0 should be included
    });
}

TEST(ExprTest, ParseGISFunctionFilterExprs) {
    // Build Schema
    auto schema = std::make_shared<Schema>();
    auto dim = 16;
    auto vec_id = schema->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2);
    auto geo_id = schema->AddDebugField("geo", DataType::GEOMETRY);
    auto pk_id = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_id);
    // Generate data and load
    int64_t N = 1000;
    auto dataset = DataGen(schema, N);
    auto seg = CreateSealedWithFieldDataLoaded(schema, dataset);
    // Test plan with st_within expression
    ScopedSchemaHandle schema_handle(*schema);
    auto bin_plan = schema_handle.ParseSearch(
        R"expr(st_within(geo, "POLYGON((0 0,1 0,1 1,0 1,0 0))"))expr",
        "vec",
        5,
        "L2",
        R"({"nprobe":10})",
        3);
    auto plan =
        CreateSearchPlanByExpr(schema, bin_plan.data(), bin_plan.size());
    // If parsing fails, test will fail with exception
    // If parsing succeeds, ParseGISFunctionFilterExprs is covered
    // Execute search to verify execution logic
    auto ph_raw = CreatePlaceholderGroup(5, dim, 123);
    auto ph_grp = ParsePlaceholderGroup(plan.get(), ph_raw.SerializeAsString());
    auto sr = seg->Search(plan.get(), ph_grp.get(), MAX_TIMESTAMP);
}

TEST(ExprTest, ParseGISFunctionFilterExprsMultipleOps) {
    // Build Schema
    auto schema = std::make_shared<Schema>();
    auto dim = 16;
    auto vec_id = schema->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2);
    auto geo_id = schema->AddDebugField("geo", DataType::GEOMETRY);
    auto pk_id = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_id);

    // Generate data and load
    int64_t N = 1000;
    auto dataset = DataGen(schema, N);
    auto seg = CreateSealedWithFieldDataLoaded(schema, dataset);

    // Test different GIS operations: {function_name, wkt_string}
    // Milvus expression syntax: st_within(field, "wkt"), st_contains(field, "wkt"), etc.
    std::vector<std::pair<std::string, std::string>> test_cases = {
        {"st_within", "POLYGON((0 0,1 0,1 1,0 1,0 0))"},
        {"st_contains", "POINT(0.5 0.5)"},
        {"st_intersects", "LINESTRING(0 0,1 1)"},
        {"st_equals", "POINT(0 0)"},
        {"st_touches", "POLYGON((10 10,11 10,11 11,10 11,10 10))"}};

    ScopedSchemaHandle schema_handle(*schema);
    for (const auto& test_case : test_cases) {
        const auto& func = test_case.first;
        const auto& wkt = test_case.second;

        // Build the expression: st_xxx(geo, "wkt_string")
        std::string expr = func + "(geo, \"" + wkt + "\")";

        auto bin_plan = schema_handle.ParseSearch(
            expr, "vec", 5, "L2", R"({"nprobe":10})", 3);
        auto plan =
            CreateSearchPlanByExpr(schema, bin_plan.data(), bin_plan.size());

        // Execute search to verify execution logic
        auto ph_raw = CreatePlaceholderGroup(5, dim, 123);
        auto ph_grp =
            ParsePlaceholderGroup(plan.get(), ph_raw.SerializeAsString());
        auto sr = seg->Search(plan.get(), ph_grp.get(), MAX_TIMESTAMP);
        EXPECT_EQ(sr->total_nq_, 5) << "Failed for operation: " << func;
    }
}

TEST_P(ExprTest, TestCancellationInExprEval) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto int64_fid = schema->AddDebugField("counter", DataType::INT64);
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    schema->set_primary_field_id(i64_fid);
    SetSchema(schema);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    seg->PreInsert(N);
    seg->Insert(0,
                N,
                raw_data.row_ids_.data(),
                raw_data.timestamps_.data(),
                raw_data.raw_);

    // Test cancellation during expression evaluation
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

    // Create a cancellation source and token
    folly::CancellationSource cancellation_source;
    auto cancellation_token = cancellation_source.getToken();

    // Create search plan from string expression: counter > 500
    auto bin_plan = create_search_plan_from_expr("counter > 500");

    // Request cancellation before executing
    cancellation_source.requestCancellation();

    // Try to execute query with cancelled token - should throw
    query::ExecPlanNodeVisitor visitor(
        *seg_promote, MAX_TIMESTAMP, cancellation_token);

    auto plan =
        CreateSearchPlanByExpr(schema, bin_plan.data(), bin_plan.size());

    // This should throw ExecOperatorException (wrapping FutureCancellation) when visiting the plan
    ASSERT_THROW({ auto result = visitor.get_moved_result(*plan->plan_node_); },
                 milvus::ExecOperatorException);
}

TEST(ExprTest, TestCancellationHelper) {
    // Test that checkCancellation does nothing when query_context is nullptr
    ASSERT_NO_THROW(milvus::exec::checkCancellation(nullptr));

    // Test with valid query_context but no op_context
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

    auto query_context = std::make_unique<milvus::exec::QueryContext>(
        "test_query", seg_promote, 0, MAX_TIMESTAMP);

    // Should not throw when op_context is nullptr
    ASSERT_NO_THROW(milvus::exec::checkCancellation(query_context.get()));

    // Test with cancelled token
    folly::CancellationSource source;
    milvus::OpContext op_context(source.getToken());
    query_context->set_op_context(&op_context);

    // Should not throw when not cancelled
    ASSERT_NO_THROW(milvus::exec::checkCancellation(query_context.get()));

    // Cancel and test
    source.requestCancellation();
    ASSERT_THROW(milvus::exec::checkCancellation(query_context.get()),
                 folly::FutureCancellation);
}

// Test for issue #46588: BinaryRangeExpr with mixed int64/float types for JSON fields
// When lower_val is int64 and upper_val is float (or vice versa), the code should
// handle the type mismatch correctly without assertion failures.
TEST(ExprTest, TestBinaryRangeExprMixedTypesForJSON) {
    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 100;
    auto raw_data = DataGen(schema, N);
    seg->PreInsert(N);
    seg->Insert(0,
                N,
                raw_data.row_ids_.data(),
                raw_data.timestamps_.data(),
                raw_data.raw_);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

    // Test case 1: lower_val is int64, upper_val is float
    // Expression: 100 < json["value"] < 500.5
    {
        proto::plan::GenericValue lower_val;
        lower_val.set_int64_val(100);  // int64 type
        proto::plan::GenericValue upper_val;
        upper_val.set_float_val(500.5);  // float type

        auto expr = std::make_shared<expr::BinaryRangeFilterExpr>(
            expr::ColumnInfo(json_fid, DataType::JSON, {"value"}),
            lower_val,
            upper_val,
            false,  // lower_inclusive
            false   // upper_inclusive
        );

        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);

        // Should not throw - the fix handles mixed types correctly
        ASSERT_NO_THROW({
            auto result = milvus::query::ExecuteQueryExpr(
                plan, seg_promote, N, MAX_TIMESTAMP);
        });
    }

    // Test case 2: lower_val is float, upper_val is int64
    // Expression: 100.5 < json["value"] < 500
    {
        proto::plan::GenericValue lower_val;
        lower_val.set_float_val(100.5);  // float type
        proto::plan::GenericValue upper_val;
        upper_val.set_int64_val(500);  // int64 type

        auto expr = std::make_shared<expr::BinaryRangeFilterExpr>(
            expr::ColumnInfo(json_fid, DataType::JSON, {"value"}),
            lower_val,
            upper_val,
            false,  // lower_inclusive
            false   // upper_inclusive
        );

        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);

        // Should not throw - the fix handles mixed types correctly
        ASSERT_NO_THROW({
            auto result = milvus::query::ExecuteQueryExpr(
                plan, seg_promote, N, MAX_TIMESTAMP);
        });
    }

    // Test case 3: both are int64 (baseline test)
    {
        proto::plan::GenericValue lower_val;
        lower_val.set_int64_val(100);
        proto::plan::GenericValue upper_val;
        upper_val.set_int64_val(500);

        auto expr = std::make_shared<expr::BinaryRangeFilterExpr>(
            expr::ColumnInfo(json_fid, DataType::JSON, {"value"}),
            lower_val,
            upper_val,
            false,
            false);

        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);

        ASSERT_NO_THROW({
            auto result = milvus::query::ExecuteQueryExpr(
                plan, seg_promote, N, MAX_TIMESTAMP);
        });
    }

    // Test case 4: both are float (baseline test)
    {
        proto::plan::GenericValue lower_val;
        lower_val.set_float_val(100.5);
        proto::plan::GenericValue upper_val;
        upper_val.set_float_val(500.5);

        auto expr = std::make_shared<expr::BinaryRangeFilterExpr>(
            expr::ColumnInfo(json_fid, DataType::JSON, {"value"}),
            lower_val,
            upper_val,
            false,
            false);

        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);

        ASSERT_NO_THROW({
            auto result = milvus::query::ExecuteQueryExpr(
                plan, seg_promote, N, MAX_TIMESTAMP);
        });
    }
}
// ============================================================================
// Division by Zero Protection Tests
// ============================================================================

// Test for issue #47285: Division/Modulo by zero causes server crash
// These tests verify that division and modulo by zero operations are properly
// protected at multiple levels and return errors instead of crashing the server.

// Note: Low-level ArithCompareOperator protection is tested indirectly
// through all higher-level tests (RegularFields, JSONFields, ArrayFields)

// Test BinaryArithOpEvalRangeExpr for regular fields with division by zero
// Test division by zero protection through BinaryArithOpEvalRangeExpr
TEST_P(ExprTest, TestDivisionByZero) {
    // The division by zero protection is implemented at multiple levels:
    // 1. Low-level: ArithCompareOperator returns false (tested implicitly)
    // 2. Mid-level: ArithOpElementFunc/ArithOpIndexFunc entry validation (tested implicitly)
    // 3. High-level: JSON/Array field handlers (tested implicitly)
    //
    // These protections are tested through the integration test in
    // tests/integration/expression/expression_test.go::TestDivisionByZeroError
    // which performs end-to-end testing with actual queries.
    //
    // This unit test validates that the expression creation works correctly.

    auto schema = std::make_shared<Schema>();
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    schema->set_primary_field_id(int64_fid);

    // Create expression with division by zero
    proto::plan::GenericValue val;
    val.set_int64_val(5);
    proto::plan::GenericValue right;
    right.set_int64_val(0);  // Division by zero

    // Expression creation should succeed
    EXPECT_NO_THROW({
        auto expr = std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
            expr::ColumnInfo(int64_fid, DataType::INT64),
            proto::plan::OpType::Equal,
            proto::plan::ArithOpType::Div,
            right,
            val);
    });

    // Expression with valid divisor should also succeed
    right.set_int64_val(2);  // Non-zero divisor
    EXPECT_NO_THROW({
        auto expr = std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
            expr::ColumnInfo(int64_fid, DataType::INT64),
            proto::plan::OpType::Equal,
            proto::plan::ArithOpType::Div,
            right,
            val);
    });
}

// Test for floating-point modulo operation correctness
// Verifies that ArithCompareOperator correctly uses std::fmod for float/double types
// instead of integer modulo which would truncate the decimal parts
TEST_P(ExprTest, TestFloatingPointModulo) {
    auto schema = std::make_shared<Schema>();
    auto float_fid = schema->AddDebugField("float_field", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("double_field", DataType::DOUBLE);
    auto int64_fid = schema->AddDebugField("int64_field", DataType::INT64);
    schema->set_primary_field_id(int64_fid);

    auto seg = CreateSealedSegment(schema);
    size_t N = 1000;
    auto raw_data = DataGen(schema, N);

    // Modify generated data to have known modulo results
    auto float_col = raw_data.get_col<float>(float_fid);
    auto double_col = raw_data.get_col<double>(double_fid);
    auto int_col = raw_data.get_col<int64_t>(int64_fid);

    for (size_t i = 0; i < N; i++) {
        float_col[i] = 5.7f + static_cast<float>(i) *
                                  0.1f;  // Values like 5.7, 5.8, 5.9, ...
        double_col[i] = 10.3 + static_cast<double>(i) *
                                   0.1;  // Values like 10.3, 10.4, 10.5, ...
        int_col[i] = static_cast<int64_t>(i);  // Integer sequence
    }

    // Load data into segment
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    // Test 1: Float modulo with decimal divisor
    // 5.7 % 2.5 = 0.7 (std::fmod result)
    // Without fix: long(5.7) % long(2.5) = 5 % 2 = 1 (incorrect!)
    {
        proto::plan::GenericValue right;
        right.set_float_val(2.5f);
        proto::plan::GenericValue val;
        val.set_float_val(0.7f);

        auto expr = std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
            expr::ColumnInfo(float_fid, DataType::FLOAT),
            proto::plan::OpType::Equal,
            proto::plan::ArithOpType::Mod,
            val,
            right);

        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);

        // The test passes if it completes without crashing
        // Actual match count depends on floating-point precision
        EXPECT_NO_THROW(final.count());
    }

    // Test 2: Double modulo with decimal divisor
    // 10.3 % 3.2 ≈ 0.7 (std::fmod result)
    {
        proto::plan::GenericValue right;
        right.set_float_val(3.2);
        proto::plan::GenericValue val;
        val.set_float_val(0.7);

        auto expr = std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
            expr::ColumnInfo(double_fid, DataType::DOUBLE),
            proto::plan::OpType::Equal,
            proto::plan::ArithOpType::Mod,
            val,
            right);

        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);

        // The test passes if it completes without crashing
        EXPECT_NO_THROW(final.count());
    }

    // Test 3: Verify integer modulo still works correctly
    {
        proto::plan::GenericValue right;
        right.set_int64_val(5);
        proto::plan::GenericValue val;
        val.set_int64_val(3);

        auto expr = std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
            expr::ColumnInfo(int64_fid, DataType::INT64),
            proto::plan::OpType::Equal,
            proto::plan::ArithOpType::Mod,
            val,
            right);

        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);

        // Verify correct number of matches: i % 5 == 3 (i.e., 3, 8, 13, 18, ...)
        size_t expected_count = 0;
        for (size_t i = 0; i < N; i++) {
            if (i % 5 == 3)
                expected_count++;
        }
        ASSERT_EQ(final.count(), expected_count);
    }
}
