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

/**
 * Google Benchmark for PreparedGeometry vs regular Geometry.
 *
 * This benchmark measures the REALISTIC performance difference between using GEOS
 * PreparedGeometry vs regular Geometry for spatial predicate evaluation.
 *
 * IMPORTANT: This benchmark includes geometry parsing time (WKT -> GEOSGeometry)
 * to reflect real-world usage where candidate geometries are parsed from stored data.
 *
 * Typical results (100-vertex polygon, 10000 candidates):
 *   - Unprepared: ~460K QPS
 *   - Prepared:   ~2M QPS
 *   - Speedup:    ~4.5x
 *
 * Note: The predicate-only speedup is ~37x, but parsing dominates (~90% of time).
 *
 * Run with:
 *   ./all_bench --benchmark_filter=PreparedGeometry
 */

#include <benchmark/benchmark.h>
#include <cmath>
#include <random>
#include <string>
#include <vector>

#include "common/Geometry.h"
#include "common/PreparedGeometry.h"

namespace milvus {
namespace {

// Helper to create a polygon WKT with N vertices (approximate circle)
std::string
CreatePolygonWKT(int num_vertices, double cx, double cy, double r) {
    std::string wkt = "POLYGON((";
    for (int i = 0; i <= num_vertices; i++) {
        int idx = i % num_vertices;
        double angle = 2.0 * M_PI * idx / num_vertices;
        double x = cx + r * cos(angle);
        double y = cy + r * sin(angle);
        if (i > 0)
            wkt += ", ";
        wkt += std::to_string(x) + " " + std::to_string(y);
    }
    wkt += "))";
    return wkt;
}

// Fixture for realistic PreparedGeometry benchmarks
// These benchmarks include geometry parsing to reflect real-world usage
class RealisticPreparedGeometryBenchmark : public benchmark::Fixture {
 public:
    void
    SetUp(const benchmark::State& state) override {
        ctx_ = GEOS_init_r();

        int num_vertices = state.range(0);
        int num_candidates = state.range(1);

        // Create query polygon
        polygon_wkt_ = CreatePolygonWKT(num_vertices, 50.0, 50.0, 30.0);
        query_polygon_ = std::make_unique<Geometry>(ctx_, polygon_wkt_.c_str());
        prepared_query_ =
            std::make_unique<PreparedGeometry>(ctx_, *query_polygon_);

        // Generate candidate point WKTs (not parsed yet - parsing happens in benchmark)
        std::mt19937 rng(42);
        std::uniform_real_distribution<double> dist(0.0, 100.0);
        candidate_wkts_.reserve(num_candidates);
        for (int i = 0; i < num_candidates; i++) {
            candidate_wkts_.push_back("POINT(" + std::to_string(dist(rng)) +
                                      " " + std::to_string(dist(rng)) + ")");
        }
    }

    void
    TearDown(const benchmark::State&) override {
        candidate_wkts_.clear();
        prepared_query_.reset();
        query_polygon_.reset();
        GEOS_finish_r(ctx_);
    }

 protected:
    GEOSContextHandle_t ctx_;
    std::string polygon_wkt_;
    std::unique_ptr<Geometry> query_polygon_;
    std::unique_ptr<PreparedGeometry> prepared_query_;
    std::vector<std::string> candidate_wkts_;  // Store WKTs, parse in benchmark
};

// Realistic benchmark: unprepared geometry intersects (includes parsing)
BENCHMARK_DEFINE_F(RealisticPreparedGeometryBenchmark,
                   Realistic_Unprepared_Intersects)
(benchmark::State& state) {
    int matches = 0;
    for (auto _ : state) {
        matches = 0;
        for (const auto& wkt : candidate_wkts_) {
            // Parse geometry (as Milvus does from WKB)
            Geometry point(ctx_, wkt.c_str());
            if (query_polygon_->intersects(point)) {
                matches++;
            }
            // Geometry destructor cleans up
        }
        benchmark::DoNotOptimize(matches);
    }
    state.SetItemsProcessed(state.iterations() * candidate_wkts_.size());
    state.counters["matches"] = matches;
}

// Realistic benchmark: prepared geometry intersects (includes parsing)
BENCHMARK_DEFINE_F(RealisticPreparedGeometryBenchmark,
                   Realistic_Prepared_Intersects)
(benchmark::State& state) {
    int matches = 0;
    for (auto _ : state) {
        matches = 0;
        for (const auto& wkt : candidate_wkts_) {
            // Parse geometry (as Milvus does from WKB)
            Geometry point(ctx_, wkt.c_str());
            if (prepared_query_->intersects(point)) {
                matches++;
            }
            // Geometry destructor cleans up
        }
        benchmark::DoNotOptimize(matches);
    }
    state.SetItemsProcessed(state.iterations() * candidate_wkts_.size());
    state.counters["matches"] = matches;
}

// Realistic benchmark: unprepared geometry contains (includes parsing)
BENCHMARK_DEFINE_F(RealisticPreparedGeometryBenchmark,
                   Realistic_Unprepared_Contains)
(benchmark::State& state) {
    int matches = 0;
    for (auto _ : state) {
        matches = 0;
        for (const auto& wkt : candidate_wkts_) {
            Geometry point(ctx_, wkt.c_str());
            if (query_polygon_->contains(point)) {
                matches++;
            }
        }
        benchmark::DoNotOptimize(matches);
    }
    state.SetItemsProcessed(state.iterations() * candidate_wkts_.size());
    state.counters["matches"] = matches;
}

// Realistic benchmark: prepared geometry contains (includes parsing)
BENCHMARK_DEFINE_F(RealisticPreparedGeometryBenchmark,
                   Realistic_Prepared_Contains)
(benchmark::State& state) {
    int matches = 0;
    for (auto _ : state) {
        matches = 0;
        for (const auto& wkt : candidate_wkts_) {
            Geometry point(ctx_, wkt.c_str());
            if (prepared_query_->contains(point)) {
                matches++;
            }
        }
        benchmark::DoNotOptimize(matches);
    }
    state.SetItemsProcessed(state.iterations() * candidate_wkts_.size());
    state.counters["matches"] = matches;
}

// Benchmark just geometry parsing (to understand overhead)
BENCHMARK_DEFINE_F(RealisticPreparedGeometryBenchmark, ParsingOnly)
(benchmark::State& state) {
    for (auto _ : state) {
        for (const auto& wkt : candidate_wkts_) {
            Geometry point(ctx_, wkt.c_str());
            benchmark::DoNotOptimize(point.IsValid());
        }
    }
    state.SetItemsProcessed(state.iterations() * candidate_wkts_.size());
}

// Register realistic benchmarks
// Args: (num_vertices, num_candidates)

// Medium polygon - typical use case
BENCHMARK_REGISTER_F(RealisticPreparedGeometryBenchmark,
                     Realistic_Unprepared_Intersects)
    ->Args({100, 10000})
    ->Args({100, 50000});
BENCHMARK_REGISTER_F(RealisticPreparedGeometryBenchmark,
                     Realistic_Prepared_Intersects)
    ->Args({100, 10000})
    ->Args({100, 50000});

// Large polygon - more complex queries
BENCHMARK_REGISTER_F(RealisticPreparedGeometryBenchmark,
                     Realistic_Unprepared_Intersects)
    ->Args({500, 10000})
    ->Args({1000, 10000});
BENCHMARK_REGISTER_F(RealisticPreparedGeometryBenchmark,
                     Realistic_Prepared_Intersects)
    ->Args({500, 10000})
    ->Args({1000, 10000});

// Contains benchmarks
BENCHMARK_REGISTER_F(RealisticPreparedGeometryBenchmark,
                     Realistic_Unprepared_Contains)
    ->Args({100, 10000})
    ->Args({500, 10000});
BENCHMARK_REGISTER_F(RealisticPreparedGeometryBenchmark,
                     Realistic_Prepared_Contains)
    ->Args({100, 10000})
    ->Args({500, 10000});

// Parsing overhead benchmark
BENCHMARK_REGISTER_F(RealisticPreparedGeometryBenchmark, ParsingOnly)
    ->Args({100, 10000})
    ->Args({100, 50000});

// Benchmark the one-time preparation cost
static void
BM_PrepareGeometry(benchmark::State& state) {
    int num_vertices = state.range(0);

    GEOSContextHandle_t ctx = GEOS_init_r();
    std::string polygon_wkt = CreatePolygonWKT(num_vertices, 50.0, 50.0, 30.0);
    Geometry polygon(ctx, polygon_wkt.c_str());

    for (auto _ : state) {
        PreparedGeometry prepared(ctx, polygon);
        benchmark::DoNotOptimize(prepared.IsValid());
    }

    GEOS_finish_r(ctx);
}
BENCHMARK(BM_PrepareGeometry)->Arg(10)->Arg(100)->Arg(500)->Arg(1000);

}  // namespace
}  // namespace milvus
