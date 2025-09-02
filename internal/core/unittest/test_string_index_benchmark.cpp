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

#include <gtest/gtest.h>
#include <chrono>
#include <random>
#include <vector>
#include <string>
#include <map>
#include <iomanip>
#include <sstream>
#include <memory>
#include <set>
#include <algorithm>

#include "index/StringIndexMarisa.h"
#include "index/StringIndexBTree.h"

namespace milvus::index {

class StringIndexBenchmark : public ::testing::Test {
 protected:
    // Generate test data with specified cardinality
    std::vector<std::string>
    GenerateData(size_t num_rows,
                 size_t cardinality,
                 const std::string& prefix = "key_") {
        std::vector<std::string> data;
        data.reserve(num_rows);

        // Generate unique values
        std::vector<std::string> unique_values;
        unique_values.reserve(cardinality);
        for (size_t i = 0; i < cardinality; ++i) {
            unique_values.push_back(prefix + std::to_string(i));
        }

        // Distribute values across rows (uniform distribution)
        std::random_device rd;
        std::mt19937 gen(42);  // Fixed seed for reproducibility
        std::uniform_int_distribution<> dis(0, cardinality - 1);

        for (size_t i = 0; i < num_rows; ++i) {
            data.push_back(unique_values[dis(gen)]);
        }

        return data;
    }

    // Generate random string with specified length range
    std::string
    GenerateRandomString(size_t min_len, size_t max_len, std::mt19937& gen) {
        const std::string charset =
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        std::uniform_int_distribution<> len_dis(min_len, max_len);
        std::uniform_int_distribution<> char_dis(0, charset.size() - 1);

        size_t length = len_dis(gen);
        std::string result;
        result.reserve(length);

        for (size_t i = 0; i < length; ++i) {
            result += charset[char_dis(gen)];
        }

        return result;
    }

    // Generate random string data with specified cardinality
    std::vector<std::string>
    GenerateRandomStringData(size_t num_rows,
                             size_t cardinality,
                             size_t min_len = 5,
                             size_t max_len = 10) {
        std::vector<std::string> data;
        data.reserve(num_rows);

        // Generate unique random values
        std::set<std::string> unique_set;
        std::mt19937 gen(42);  // Fixed seed for reproducibility

        while (unique_set.size() < cardinality) {
            unique_set.insert(GenerateRandomString(min_len, max_len, gen));
        }

        std::vector<std::string> unique_values(unique_set.begin(),
                                               unique_set.end());
        // Don't sort here - keep the natural order for building index

        // Distribute values across rows (uniform distribution)
        std::uniform_int_distribution<> dis(0, cardinality - 1);

        for (size_t i = 0; i < num_rows; ++i) {
            data.push_back(unique_values[dis(gen)]);
        }

        return data;
    }

    // Time a function execution
    template <typename Func>
    double
    TimeExecution(Func func, int iterations = 100) {
        // Warm up
        for (int i = 0; i < 5; ++i) {
            func();
        }

        auto start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < iterations; ++i) {
            func();
        }
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> diff = end - start;
        return diff.count() / iterations;
    }

    // Time random range queries to simulate real-world access patterns
    double
    TimeRandomRangeQueries(StringIndexPtr& index,
                           const std::vector<std::string>& unique_sorted_values,
                           int range_percentage,
                           int iterations = 100) {
        size_t actual_unique_count = unique_sorted_values.size();
        size_t range_size = (actual_unique_count * range_percentage) / 100;

        // Generate different random ranges for each iteration
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<size_t> dist(
            0, actual_unique_count - range_size);

        std::vector<std::pair<std::string, std::string>> ranges;
        for (int i = 0; i < iterations; ++i) {
            size_t start_idx = dist(gen);
            size_t end_idx = start_idx + range_size - 1;
            ranges.push_back({unique_sorted_values[start_idx],
                              unique_sorted_values[end_idx]});
        }

        // No warmup - simulate cold cache scenario
        auto start = std::chrono::high_resolution_clock::now();
        for (const auto& [range_start, range_end] : ranges) {
            auto bitmap = index->Range(range_start, true, range_end, true);
            std::cout << "bitmap count: " << bitmap.count() << std::endl;
        }
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> diff = end - start;
        return diff.count() / iterations;
    }

    struct BenchmarkResult {
        std::string index_type;
        size_t cardinality;
        size_t num_rows;
        double build_time_ms;
        double in_query_time_ms;
        double prefix_match_time_ms;
        size_t index_size_bytes;
        size_t serialized_size_bytes;
        // Range scan percentages
        std::map<int, double> range_scan_times;  // percentage -> time_ms
        // Load mode info
        std::string load_mode = "";
        double load_time_ms = 0.0;
    };

    void
    PrintResultsWithRangeScan(const std::vector<BenchmarkResult>& results) {
        // Organize results by cardinality and mode
        std::map<size_t, std::map<std::string, std::vector<BenchmarkResult>>>
            organized;

        for (const auto& r : results) {
            std::string mode = r.load_mode.empty() ? "DEFAULT" : r.load_mode;
            organized[r.cardinality][mode].push_back(r);
        }

        // Print header
        std::cout << "\n=== String Index Benchmark: RAM vs MMAP Performance "
                     "Comparison ===\n\n";
        std::cout << std::left;
        std::cout << std::setw(15) << "Index" << std::setw(12) << "Cardinality"
                  << std::setw(10) << "Rows" << std::setw(12) << "Build(ms)"
                  << std::setw(12) << "Load(ms)" << std::setw(10) << "IN(ms)"
                  << std::setw(12) << "Prefix(ms)" << std::setw(12)
                  << "R20%(ms)" << std::setw(12) << "R40%(ms)" << std::setw(12)
                  << "R60%(ms)" << std::setw(12) << "R80%(ms)" << std::setw(12)
                  << "Size(MB)" << "\n";
        std::cout << std::string(145, '-') << "\n";

        // Process each cardinality
        for (const auto& [card, mode_results] : organized) {
            // Print RAM results first
            if (mode_results.count("RAM") > 0) {
                auto ram_results = mode_results.at("RAM");
                std::sort(
                    ram_results.begin(),
                    ram_results.end(),
                    [](const BenchmarkResult& a, const BenchmarkResult& b) {
                        return a.index_type >
                               b.index_type;  // MARISA-RAM > BTree-RAM
                    });

                for (const auto& r : ram_results) {
                    std::cout
                        << std::setw(15) << r.index_type << std::setw(12)
                        << r.cardinality << std::setw(10) << r.num_rows
                        << std::setw(12) << std::fixed << std::setprecision(2)
                        << r.build_time_ms << std::setw(12) << std::fixed
                        << std::setprecision(2) << r.load_time_ms
                        << std::setw(10) << std::fixed << std::setprecision(3)
                        << r.in_query_time_ms << std::setw(12) << std::fixed
                        << std::setprecision(3) << r.prefix_match_time_ms;

                    for (int pct : {20, 40, 60, 80}) {
                        auto it = r.range_scan_times.find(pct);
                        if (it != r.range_scan_times.end()) {
                            std::cout << std::setw(12) << std::fixed
                                      << std::setprecision(3) << it->second;
                        } else {
                            std::cout << std::setw(12) << "N/A";
                        }
                    }

                    std::cout << std::setw(12) << std::fixed
                              << std::setprecision(2)
                              << (r.index_size_bytes / 1024.0 / 1024.0) << "\n";
                }
            }

            std::cout << "\n";  // Empty line between RAM and MMAP

            // Print MMAP results
            if (mode_results.count("MMAP") > 0) {
                auto mmap_results = mode_results.at("MMAP");
                std::sort(
                    mmap_results.begin(),
                    mmap_results.end(),
                    [](const BenchmarkResult& a, const BenchmarkResult& b) {
                        return a.index_type >
                               b.index_type;  // MARISA-MMAP > BTree-MMAP
                    });

                for (const auto& r : mmap_results) {
                    std::cout
                        << std::setw(15) << r.index_type << std::setw(12)
                        << r.cardinality << std::setw(10) << r.num_rows
                        << std::setw(12) << std::fixed << std::setprecision(2)
                        << r.build_time_ms << std::setw(12) << std::fixed
                        << std::setprecision(2) << r.load_time_ms
                        << std::setw(10) << std::fixed << std::setprecision(3)
                        << r.in_query_time_ms << std::setw(12) << std::fixed
                        << std::setprecision(3) << r.prefix_match_time_ms;

                    for (int pct : {20, 40, 60, 80}) {
                        auto it = r.range_scan_times.find(pct);
                        if (it != r.range_scan_times.end()) {
                            std::cout << std::setw(12) << std::fixed
                                      << std::setprecision(3) << it->second;
                        } else {
                            std::cout << std::setw(12) << "N/A";
                        }
                    }

                    std::cout << std::setw(12) << std::fixed
                              << std::setprecision(2)
                              << (r.index_size_bytes / 1024.0 / 1024.0) << "\n";
                }
            }

            std::cout
                << "\n\n";  // Two empty lines between different cardinality groups
        }
    }

    // Benchmark index with random strings and range scan percentages
    BenchmarkResult
    BenchmarkRandomStringIndex(
        const std::string& index_type,
        StringIndexPtr index,
        const std::vector<std::string>& data,
        const std::vector<std::string>& unique_sorted_values,
        size_t cardinality,
        int query_iterations = 10,
        bool skip_build = false) {
        BenchmarkResult result;
        result.index_type = index_type;
        result.cardinality = cardinality;
        result.num_rows = data.size();

        // Build index if not skipped (when testing load modes, index is pre-built)
        if (!skip_build) {
            auto start = std::chrono::high_resolution_clock::now();
            index->Build(data.size(), data.data());
            auto end = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::milli> build_time = end - start;
            result.build_time_ms = build_time.count();
        } else {
            result.build_time_ms = 0;
        }

        // Get index size
        result.index_size_bytes = index->Size();

        // Serialize to get actual size
        try {
            Config config;
            auto binary_set = index->Serialize(config);
            result.serialized_size_bytes = 0;
            for (auto& [name, binary] : binary_set.binary_map_) {
                result.serialized_size_bytes += binary->size;
            }
        } catch (...) {
            result.serialized_size_bytes = 0;
        }

        // Prepare IN query values (random selection)
        std::vector<std::string> in_values;
        size_t query_count = std::min(size_t(10), cardinality);
        for (size_t i = 0; i < query_count; ++i) {
            in_values.push_back(
                unique_sorted_values[i * cardinality / query_count]);
        }

        // IN query benchmark
        result.in_query_time_ms = TimeExecution(
            [&]() {
                auto bitmap = index->In(in_values.size(), in_values.data());
            },
            query_iterations);

        // Prefix match benchmark (use first few chars of a random value)
        std::string prefix = unique_sorted_values[cardinality / 2];
        if (prefix.length() > 3) {
            prefix = prefix.substr(0, 3);
        }
        result.prefix_match_time_ms =
            TimeExecution([&]() { auto bitmap = index->PrefixMatch(prefix); },
                          query_iterations);

        // Range query benchmarks for different percentages
        std::vector<int> percentages = {20, 40, 60, 80};
        for (int pct : percentages) {
            // Use the new random range query method to simulate real-world patterns
            result.range_scan_times[pct] = TimeRandomRangeQueries(
                index, unique_sorted_values, pct, query_iterations);
        }

        return result;
    }

    std::vector<BenchmarkResult>
    BenchmarkWithLoadModes(bool ram_mode,
                           const std::string& index_type,
                           const std::vector<std::string>& data,
                           const std::vector<std::string>& unique_sorted_values,
                           size_t cardinality,
                           int query_iterations = 100) {
        std::vector<BenchmarkResult> results;

        // First build and serialize the index
        StringIndexPtr build_index;
        if (index_type == "MARISA") {
            build_index = CreateStringIndexMarisa();
        } else {
            build_index = CreateStringIndexBTree();
        }

        auto build_start = std::chrono::high_resolution_clock::now();
        build_index->Build(data.size(), data.data());
        auto build_end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> build_time =
            build_end - build_start;

        Config config;
        auto binary_set = build_index->Serialize(config);

        // Test RAM loading
        if (ram_mode) {
            StringIndexPtr ram_index;
            if (index_type == "MARISA") {
                ram_index = CreateStringIndexMarisa();
            } else {
                ram_index = CreateStringIndexBTree();
            }

            Config load_config;
            auto start = std::chrono::high_resolution_clock::now();
            ram_index->Load(binary_set, load_config);
            auto end = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::milli> load_time = end - start;

            auto result =
                BenchmarkRandomStringIndex(index_type + "-RAM",
                                           std::move(ram_index),
                                           data,
                                           unique_sorted_values,
                                           cardinality,
                                           query_iterations,
                                           true);  // skip_build = true
            result.load_mode = "RAM";
            result.load_time_ms = load_time.count();
            result.build_time_ms = build_time.count();
            results.push_back(result);
        } else {
            StringIndexPtr mmap_index;
            if (index_type == "MARISA") {
                mmap_index = CreateStringIndexMarisa();
            } else {
                mmap_index = CreateStringIndexBTree();
            }

            Config load_config;
            load_config[MMAP_FILE_PATH] = "/tmp/test_mmap";

            auto start = std::chrono::high_resolution_clock::now();
            mmap_index->Load(binary_set, load_config);
            auto end = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::milli> load_time = end - start;

            auto result =
                BenchmarkRandomStringIndex(index_type + "-MMAP",
                                           std::move(mmap_index),
                                           data,
                                           unique_sorted_values,
                                           cardinality,
                                           query_iterations,
                                           true);  // skip_build = true
            result.load_mode = "MMAP";
            result.load_time_ms = load_time.count();
            result.build_time_ms = build_time.count();
            results.push_back(result);
        }

        return results;
    }
};

TEST_F(StringIndexBenchmark, RandomStringWithRangeScan) {
    const size_t NUM_ROWS = 1000000;  // 1M rows
    const std::vector<size_t> CARDINALITIES = {
        100, 1000, 10000, 100000, 1000000};
    const int QUERY_ITERATIONS = 20;

    std::cout
        << "\n=== Random String Benchmark with Range Scan Percentages ===\n";
    std::cout << "String length: 8-12 characters, Testing range scans: 20%, "
                 "40%, 60%, 80%\n";
    std::cout << "Testing both MMAP and RAM loading modes\n";
    std::cout << "Test order: MMAP first, then RAM\n";
    std::vector<BenchmarkResult> results;

    // Store all test data to avoid regeneration
    struct TestData {
        size_t cardinality;
        std::vector<std::string> data;
        std::vector<std::string> unique_sorted;
    };
    std::vector<TestData> all_test_data;

    // Generate all test data first
    for (size_t cardinality : CARDINALITIES) {
        std::cout << "\nGenerating test data for cardinality: " << cardinality
                  << std::endl;
        auto data = GenerateRandomStringData(NUM_ROWS, cardinality, 8, 12);
        std::set<std::string> unique_set(data.begin(), data.end());
        std::vector<std::string> unique_sorted(unique_set.begin(),
                                               unique_set.end());
        all_test_data.push_back(
            {cardinality, std::move(data), std::move(unique_sorted)});
    }

    // Then, run all RAM tests
    std::cout << "\n=== Running RAM Tests ===\n";
    for (const auto& test_data : all_test_data) {
        std::cout << "\nRAM test for cardinality: " << test_data.cardinality
                  << std::endl;

        // Test MARISA RAM
        auto marisa_results = BenchmarkWithLoadModes(true,
                                                     "MARISA",
                                                     test_data.data,
                                                     test_data.unique_sorted,
                                                     test_data.cardinality,
                                                     QUERY_ITERATIONS);
        // Only add RAM result
        for (const auto& r : marisa_results) {
            if (r.load_mode == "RAM") {
                results.push_back(r);
            }
        }

        // Test BTree RAM
        auto btree_results = BenchmarkWithLoadModes(true,
                                                    "BTree",
                                                    test_data.data,
                                                    test_data.unique_sorted,
                                                    test_data.cardinality,
                                                    QUERY_ITERATIONS);
        // Only add RAM result
        for (const auto& r : btree_results) {
            if (r.load_mode == "RAM") {
                results.push_back(r);
            }
        }
    }

    PrintResultsWithRangeScan(results);
}
}  // namespace milvus::index