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

#include "index/StringIndexMarisa.h"
#include "index/StringIndexBTree.h"
#include "index/IndexFactory.h"

namespace milvus::index {

class StringIndexPerfTest : public ::testing::Test {
 protected:
    // Generate test data with specified cardinality
    std::vector<std::string> 
    GenerateData(size_t num_rows, size_t cardinality, const std::string& prefix = "key_") {
        std::vector<std::string> data;
        data.reserve(num_rows);
        
        // Generate unique values
        std::vector<std::string> unique_values;
        unique_values.reserve(cardinality);
        for (size_t i = 0; i < cardinality; ++i) {
            unique_values.push_back(prefix + std::to_string(i));
        }
        
        // Distribute values across rows
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, cardinality - 1);
        
        for (size_t i = 0; i < num_rows; ++i) {
            data.push_back(unique_values[dis(gen)]);
        }
        
        return data;
    }
    
    // Generate data with zipf distribution (more realistic)
    std::vector<std::string>
    GenerateZipfData(size_t num_rows, size_t cardinality, double skew = 1.0) {
        std::vector<std::string> data;
        data.reserve(num_rows);
        
        // Generate unique values
        std::vector<std::string> unique_values;
        unique_values.reserve(cardinality);
        for (size_t i = 0; i < cardinality; ++i) {
            unique_values.push_back("item_" + std::to_string(i));
        }
        
        // Generate Zipf distribution
        std::vector<double> probabilities;
        double sum = 0.0;
        for (size_t i = 1; i <= cardinality; ++i) {
            double prob = 1.0 / std::pow(i, skew);
            probabilities.push_back(prob);
            sum += prob;
        }
        
        // Normalize
        for (auto& p : probabilities) {
            p /= sum;
        }
        
        // Generate data
        std::random_device rd;
        std::mt19937 gen(rd());
        std::discrete_distribution<> dis(probabilities.begin(), probabilities.end());
        
        for (size_t i = 0; i < num_rows; ++i) {
            data.push_back(unique_values[dis(gen)]);
        }
        
        return data;
    }
    
    // Time a function execution
    template<typename Func>
    double TimeExecution(Func func, int iterations = 100) {
        auto start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < iterations; ++i) {
            func();
        }
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> diff = end - start;
        return diff.count() / iterations;
    }
    
    struct PerfResult {
        std::string index_type;
        size_t cardinality;
        double build_time_ms;
        double in_query_time_ms;
        double prefix_match_time_ms;
        double range_query_time_ms;
        size_t memory_bytes;
    };
    
    void PrintResults(const std::vector<PerfResult>& results) {
        std::cout << "\n=== Performance Test Results (1M rows) ===\n\n";
        std::cout << std::setw(12) << "Index" 
                  << std::setw(12) << "Cardinality"
                  << std::setw(12) << "Build(ms)"
                  << std::setw(12) << "IN(ms)"
                  << std::setw(12) << "Prefix(ms)"
                  << std::setw(12) << "Range(ms)"
                  << std::setw(12) << "Memory(MB)" << "\n";
        std::cout << std::string(84, '-') << "\n";
        
        for (const auto& r : results) {
            std::cout << std::setw(12) << r.index_type
                      << std::setw(12) << r.cardinality
                      << std::setw(12) << std::fixed << std::setprecision(2) << r.build_time_ms
                      << std::setw(12) << std::fixed << std::setprecision(2) << r.in_query_time_ms
                      << std::setw(12) << std::fixed << std::setprecision(2) << r.prefix_match_time_ms
                      << std::setw(12) << std::fixed << std::setprecision(2) << r.range_query_time_ms
                      << std::setw(12) << std::fixed << std::setprecision(2) 
                      << (r.memory_bytes / 1024.0 / 1024.0) << "\n";
        }
    }
};

TEST_F(StringIndexPerfTest, ComparePerformance) {
    const size_t NUM_ROWS = 1000000;  // 1M rows
    const std::vector<size_t> CARDINALITIES = {10, 100, 1000, 10000, 100000, 500000};
    const int QUERY_ITERATIONS = 100;
    
    std::vector<PerfResult> results;
    
    for (size_t cardinality : CARDINALITIES) {
        std::cout << "\nTesting with cardinality: " << cardinality << "\n";
        
        // Generate test data
        auto data = GenerateData(NUM_ROWS, cardinality);
        
        // Prepare query values
        std::vector<std::string> in_values;
        for (size_t i = 0; i < std::min(size_t(10), cardinality); ++i) {
            in_values.push_back("key_" + std::to_string(i));
        }
        
        std::string prefix = "key_1";
        std::string range_start = "key_100";
        std::string range_end = "key_200";
        
        // Test MARISA
        {
            PerfResult result;
            result.index_type = "MARISA";
            result.cardinality = cardinality;
            
            auto index = CreateStringIndexMarisa();
            
            // Build time
            auto start = std::chrono::high_resolution_clock::now();
            index->Build(data.size(), data.data());
            auto end = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::milli> diff = end - start;
            result.build_time_ms = diff.count();
            
            // Memory usage
            result.memory_bytes = index->Size() * 8;  // Approximate
            
            // IN query
            result.in_query_time_ms = TimeExecution([&]() {
                auto bitmap = index->In(in_values.size(), in_values.data());
            }, QUERY_ITERATIONS);
            
            // Prefix match
            result.prefix_match_time_ms = TimeExecution([&]() {
                auto bitmap = index->PrefixMatch(prefix);
            }, QUERY_ITERATIONS);
            
            // Range query
            result.range_query_time_ms = TimeExecution([&]() {
                auto bitmap = index->Range(range_start, true, range_end, true);
            }, QUERY_ITERATIONS);
            
            results.push_back(result);
        }
        
        // Test BTree
        {
            PerfResult result;
            result.index_type = "BTree";
            result.cardinality = cardinality;
            
            auto index = CreateStringIndexBTree();
            
            // Build time
            auto start = std::chrono::high_resolution_clock::now();
            index->Build(data.size(), data.data());
            auto end = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::milli> diff = end - start;
            result.build_time_ms = diff.count();
            
            // Memory usage
            result.memory_bytes = index->Size() * 50;  // Approximate (more overhead)
            
            // IN query
            result.in_query_time_ms = TimeExecution([&]() {
                auto bitmap = index->In(in_values.size(), in_values.data());
            }, QUERY_ITERATIONS);
            
            // Prefix match
            result.prefix_match_time_ms = TimeExecution([&]() {
                auto bitmap = index->PrefixMatch(prefix);
            }, QUERY_ITERATIONS);
            
            // Range query
            result.range_query_time_ms = TimeExecution([&]() {
                auto bitmap = index->Range(range_start, true, range_end, true);
            }, QUERY_ITERATIONS);
            
            results.push_back(result);
        }
    }
    
    PrintResults(results);
}

TEST_F(StringIndexPerfTest, ZipfDistributionPerformance) {
    const size_t NUM_ROWS = 1000000;
    const std::vector<size_t> CARDINALITIES = {100, 1000, 10000, 100000};
    const double ZIPF_SKEW = 1.5;  // Higher skew = more concentration on popular items
    const int QUERY_ITERATIONS = 100;
    
    std::cout << "\n=== Zipf Distribution Test (skew=" << ZIPF_SKEW << ") ===\n";
    std::vector<PerfResult> results;
    
    for (size_t cardinality : CARDINALITIES) {
        std::cout << "\nTesting Zipf distribution with cardinality: " << cardinality << "\n";
        
        // Generate Zipf-distributed data
        auto data = GenerateZipfData(NUM_ROWS, cardinality, ZIPF_SKEW);
        
        // Query for popular items (more realistic)
        std::vector<std::string> popular_items;
        for (size_t i = 0; i < std::min(size_t(5), cardinality); ++i) {
            popular_items.push_back("item_" + std::to_string(i));
        }
        
        // Test MARISA
        {
            PerfResult result;
            result.index_type = "MARISA-Zipf";
            result.cardinality = cardinality;
            
            auto index = CreateStringIndexMarisa();
            
            auto start = std::chrono::high_resolution_clock::now();
            index->Build(data.size(), data.data());
            auto end = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::milli> diff = end - start;
            result.build_time_ms = diff.count();
            
            result.memory_bytes = index->Size() * 8;
            
            result.in_query_time_ms = TimeExecution([&]() {
                auto bitmap = index->In(popular_items.size(), popular_items.data());
            }, QUERY_ITERATIONS);
            
            result.prefix_match_time_ms = TimeExecution([&]() {
                auto bitmap = index->PrefixMatch("item_1");
            }, QUERY_ITERATIONS);
            
            result.range_query_time_ms = TimeExecution([&]() {
                auto bitmap = index->Range("item_10", true, "item_20", true);
            }, QUERY_ITERATIONS);
            
            results.push_back(result);
        }
        
        // Test BTree
        {
            PerfResult result;
            result.index_type = "BTree-Zipf";
            result.cardinality = cardinality;
            
            auto index = CreateStringIndexBTree();
            
            auto start = std::chrono::high_resolution_clock::now();
            index->Build(data.size(), data.data());
            auto end = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::milli> diff = end - start;
            result.build_time_ms = diff.count();
            
            result.memory_bytes = index->Size() * 50;
            
            result.in_query_time_ms = TimeExecution([&]() {
                auto bitmap = index->In(popular_items.size(), popular_items.data());
            }, QUERY_ITERATIONS);
            
            result.prefix_match_time_ms = TimeExecution([&]() {
                auto bitmap = index->PrefixMatch("item_1");
            }, QUERY_ITERATIONS);
            
            result.range_query_time_ms = TimeExecution([&]() {
                auto bitmap = index->Range("item_10", true, "item_20", true);
            }, QUERY_ITERATIONS);
            
            results.push_back(result);
        }
    }
    
    PrintResults(results);
}

TEST_F(StringIndexPerfTest, ExtremeCasesPerformance) {
    const size_t NUM_ROWS = 1000000;
    const int QUERY_ITERATIONS = 100;
    
    std::cout << "\n=== Extreme Cases Performance Test ===\n";
    std::vector<PerfResult> results;
    
    // Case 1: Very low cardinality (10 unique values)
    {
        std::cout << "\nCase 1: Very low cardinality (10 unique values)\n";
        auto data = GenerateData(NUM_ROWS, 10, "status_");
        
        // MARISA
        {
            PerfResult result;
            result.index_type = "MARISA-Low";
            result.cardinality = 10;
            
            auto index = CreateStringIndexMarisa();
            
            auto start = std::chrono::high_resolution_clock::now();
            index->Build(data.size(), data.data());
            auto end = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::milli> diff = end - start;
            result.build_time_ms = diff.count();
            
            result.memory_bytes = index->Size() * 8;
            
            // Query for one value (will match ~100K rows)
            std::vector<std::string> query = {"status_0"};
            result.in_query_time_ms = TimeExecution([&]() {
                auto bitmap = index->In(query.size(), query.data());
            }, QUERY_ITERATIONS);
            
            results.push_back(result);
        }
        
        // BTree
        {
            PerfResult result;
            result.index_type = "BTree-Low";
            result.cardinality = 10;
            
            auto index = CreateStringIndexBTree();
            
            auto start = std::chrono::high_resolution_clock::now();
            index->Build(data.size(), data.data());
            auto end = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::milli> diff = end - start;
            result.build_time_ms = diff.count();
            
            result.memory_bytes = index->Size() * 50;
            
            std::vector<std::string> query = {"status_0"};
            result.in_query_time_ms = TimeExecution([&]() {
                auto bitmap = index->In(query.size(), query.data());
            }, QUERY_ITERATIONS);
            
            results.push_back(result);
        }
    }
    
    // Case 2: Very high cardinality (almost unique)
    {
        std::cout << "\nCase 2: Very high cardinality (900K unique values)\n";
        auto data = GenerateData(NUM_ROWS, 900000, "uuid_");
        
        // MARISA
        {
            PerfResult result;
            result.index_type = "MARISA-High";
            result.cardinality = 900000;
            
            auto index = CreateStringIndexMarisa();
            
            auto start = std::chrono::high_resolution_clock::now();
            index->Build(data.size(), data.data());
            auto end = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::milli> diff = end - start;
            result.build_time_ms = diff.count();
            
            result.memory_bytes = index->Size() * 8;
            
            // Query for 100 values
            std::vector<std::string> query;
            for (int i = 0; i < 100; ++i) {
                query.push_back("uuid_" + std::to_string(i * 1000));
            }
            result.in_query_time_ms = TimeExecution([&]() {
                auto bitmap = index->In(query.size(), query.data());
            }, QUERY_ITERATIONS);
            
            results.push_back(result);
        }
        
        // BTree
        {
            PerfResult result;
            result.index_type = "BTree-High";
            result.cardinality = 900000;
            
            auto index = CreateStringIndexBTree();
            
            auto start = std::chrono::high_resolution_clock::now();
            index->Build(data.size(), data.data());
            auto end = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::milli> diff = end - start;
            result.build_time_ms = diff.count();
            
            result.memory_bytes = index->Size() * 50;
            
            std::vector<std::string> query;
            for (int i = 0; i < 100; ++i) {
                query.push_back("uuid_" + std::to_string(i * 1000));
            }
            result.in_query_time_ms = TimeExecution([&]() {
                auto bitmap = index->In(query.size(), query.data());
            }, QUERY_ITERATIONS);
            
            results.push_back(result);
        }
    }
    
    PrintResults(results);
}

}  // namespace milvus::index