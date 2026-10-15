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

#include <arrow/io/memory.h>
#include <boost/regex.hpp>
#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/RegexQuery.h"
#include "index/Families.h"
#include "index/Meta.h"
#include "index/contracts/Registry.h"
#include "index/contracts/build/ScalarBuildInput.h"
#include "index/contracts/query/IPatternMatchReader.h"
#include "storage/IndexEntryDirectStreamWriter.h"
#include "storage/IndexEntryReader.h"
#include "storage/RemoteInputStream.h"
#include "storage/RemoteOutputStream.h"

namespace milvus::index::test {
namespace {

IIndexReaderBasePtr
BuildTantivyStringReader(const std::vector<std::string>& data) {
    const Config params{{FIELD_ID, 101},
                        {"field_type", static_cast<int>(DataType::VARCHAR)},
                        {"value_type", static_cast<int>(DataType::VARCHAR)},
                        {"nested", false},
                        {SCALAR_INDEX_ENGINE_VERSION, 3}};
    auto output = arrow::io::BufferOutputStream::Create().ValueOrDie();
    {
        std::vector<std::string_view> values;
        values.reserve(data.size());
        for (const auto& value : data) {
            values.emplace_back(value);
        }
        auto builder =
            BuilderRegistry<ScalarBuildInput<std::string_view>>::Instance()
                .Create(families::kInverted, params);
        AssertInfo(builder != nullptr, "inverted benchmark builder is missing");
        const ScalarBuildBatch<std::string_view> batch{values, {}};
        auto artifact = std::move(*builder).Build({std::span(&batch, 1)});
        storage::IndexEntryDirectStreamWriter writer(
            std::make_shared<storage::RemoteOutputStream>(output), 4096);
        artifact->Serialize(writer);
        writer.Finish();
    }
    // Open independently after the builder, artifact and borrowed views die.
    auto input = std::make_shared<storage::RemoteInputStream>(
        std::make_shared<arrow::io::BufferReader>(
            output->Finish().ValueOrDie()));
    auto source = storage::IndexEntryReader::Open(input, input->Size());
    storage::LoadOptions options;
    options.params = params;
    const auto loader = LoaderRegistry::Instance().Lookup(families::kInverted);
    AssertInfo(static_cast<bool>(loader),
               "inverted benchmark loader is missing");
    return loader.Load(
        {OpenedIndexSource{PackedIndexSource{
             std::shared_ptr<storage::IndexEntryReader>(std::move(source))}},
         std::move(options)});
}

std::vector<std::string>
GenerateBenchStrings(size_t count, size_t average_length, unsigned seed = 42) {
    std::mt19937 rng(seed);
    std::uniform_int_distribution<int> char_dist('a', 'z');
    std::uniform_int_distribution<size_t> length_dist(average_length / 2,
                                                      average_length * 3 / 2);
    std::vector<std::string> result;
    result.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        std::string value;
        const auto length = length_dist(rng);
        value.reserve(length);
        for (size_t j = 0; j < length; ++j) {
            value += static_cast<char>(char_dist(rng));
        }
        result.push_back(std::move(value));
    }
    return result;
}

struct BenchResult {
    double microseconds;
    int64_t count;
};

template <typename Fn>
BenchResult
TimeBench(Fn&& fn, int warmup = 3, int iterations = 10) {
    volatile int64_t observed = 0;
    for (int i = 0; i < warmup; ++i) {
        observed = fn();
    }
    int64_t total = 0;
    const auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < iterations; ++i) {
        total += fn();
    }
    const double microseconds = std::chrono::duration<double, std::micro>(
                                    std::chrono::steady_clock::now() - start)
                                    .count() /
                                iterations;
    static_cast<void>(observed);
    return {microseconds, total / iterations};
}

template <typename Matcher>
int64_t
CountMatches(const std::vector<std::string>& data, Matcher& matcher) {
    int64_t count = 0;
    for (const auto& value : data) {
        count += matcher(value) ? 1 : 0;
    }
    return count;
}

void
PrintRow(const std::string& name, const BenchResult& result) {
    std::cout << "  " << std::left << std::setw(25) << name << std::right
              << std::setw(12) << std::fixed << std::setprecision(0)
              << result.microseconds << " us" << std::setw(12) << result.count
              << '\n';
}

TEST(IndexBenchmark, TantivyVsBruteForce) {
    constexpr size_t count = 100000;
    const auto data = GenerateBenchStrings(count, 50);
    const auto reader = BuildTantivyStringReader(data);
    ASSERT_NE(reader, nullptr);
    ASSERT_EQ(reader->Count(), data.size());
    const auto* tantivy =
        dynamic_cast<const IPatternMatchReader*>(reader.get());
    ASSERT_NE(tantivy, nullptr);
    const std::vector<std::pair<std::string, std::string>> patterns{
        {"prefix: abc%", "abc%"},
        {"suffix: %xyz", "%xyz"},
        {"inner: %hello%", "%hello%"},
        {"complex: %ab%cd%ef%", "%ab%cd%ef%"},
        {"exact: abcdefghij", "abcdefghij"},
        {"single-char: a_c_e_g", "a_c_e_g"},
        {"multi-seg: %a%b%c%d%", "%a%b%c%d%"},
        {"mixed: test%_abc%", "test%_abc%"}};
    std::cout << "\n====== Tantivy Index vs Brute-Force (" << count
              << " strings, avg 50 bytes) ======\n";
    PatternMatchTranslator translator;
    for (const auto& [name, pattern] : patterns) {
        SCOPED_TRACE(pattern);
        const auto regex_pattern = translator(pattern);
        RegexMatcher re2(regex_pattern);
        // The benchmark generates ASCII; Boost and RE2 see identical bytes.
        const boost::regex boost_regex(regex_pattern);
        auto boost_matcher = [&](const std::string& value) {
            return boost::regex_match(value, boost_regex);
        };
        LikePatternMatcher like(pattern);
        const auto re2_result =
            TimeBench([&] { return CountMatches(data, re2); });
        const auto boost_result =
            TimeBench([&] { return CountMatches(data, boost_matcher); });
        const auto like_result =
            TimeBench([&] { return CountMatches(data, like); });
        const auto tantivy_result = TimeBench([&] {
            return static_cast<int64_t>(
                tantivy->PatternMatch(pattern, PatternOp::Match).count());
        });
        EXPECT_EQ(boost_result.count, re2_result.count);
        EXPECT_EQ(like_result.count, re2_result.count);
        EXPECT_EQ(tantivy_result.count, re2_result.count);
        std::cout << "\n  " << name << '\n';
        PrintRow("RE2 (brute-force)", re2_result);
        PrintRow("Boost (brute-force)", boost_result);
        PrintRow("LikePatternMatcher", like_result);
        PrintRow("Tantivy (index)", tantivy_result);
    }
}

TEST(IndexBenchmark, TantivyDataScaling) {
    PatternMatchTranslator translator;
    const std::string pattern = "%abc%def%";
    const auto regex_pattern = translator(pattern);
    std::cout << "\n====== Data Size Scaling (pattern: %abc%def%) ======\n"
              << std::left << "  " << std::setw(10) << "N" << std::right
              << std::setw(15) << "RE2(us)" << std::setw(18) << "LikePat(us)"
              << std::setw(18) << "Tantivy(us)" << '\n';
    for (const size_t count : {1000, 10000, 100000}) {
        SCOPED_TRACE(count);
        const auto data = GenerateBenchStrings(count, 50);
        const auto reader = BuildTantivyStringReader(data);
        ASSERT_NE(reader, nullptr);
        ASSERT_EQ(reader->Count(), data.size());
        const auto* tantivy =
            dynamic_cast<const IPatternMatchReader*>(reader.get());
        ASSERT_NE(tantivy, nullptr);
        RegexMatcher re2(regex_pattern);
        LikePatternMatcher like(pattern);
        const auto re2_result =
            TimeBench([&] { return CountMatches(data, re2); });
        const auto like_result =
            TimeBench([&] { return CountMatches(data, like); });
        const auto tantivy_result = TimeBench([&] {
            return static_cast<int64_t>(
                tantivy->PatternMatch(pattern, PatternOp::Match).count());
        });
        EXPECT_EQ(like_result.count, re2_result.count);
        EXPECT_EQ(tantivy_result.count, re2_result.count);
        std::cout << "  " << std::left << std::setw(10) << count << std::right
                  << std::fixed << std::setprecision(0) << std::setw(12)
                  << re2_result.microseconds << " us" << std::setw(15)
                  << like_result.microseconds << " us" << std::setw(15)
                  << tantivy_result.microseconds << " us\n";
    }
}

}  // namespace
}  // namespace milvus::index::test
