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

// Benchmark: ContainsAll brute-force std::set copy vs bitmask approach
//
// Mirrors the production ContainsAllMatcher<T> class from JsonContainsExpr.cpp
// using the same set_if_found() API. The benchmark uses std::unordered_map as
// a stand-in for ankerl::unordered_dense::map (same O(1) lookup semantics).
//
// The matcher is rebuilt per sub-batch to match production behavior where
// ContainsAllMatcher is constructed inside execute_sub_batch (once per chunk).
//
// Note: the string path uses std::unordered_map<std::string, uint32_t> which
// requires std::string(val) conversion on lookup. Production uses
// ankerl::unordered_dense::map<std::string_view, uint32_t> with zero-copy
// lookup, so the string benchmark numbers are pessimistic for the bitmask
// path (real production gains will be higher).
//
// Self-contained — no Milvus headers needed.
//
// Build:
//   g++ -O2 -std=c++17 -o bench_array_contains_all bench_array_contains_all.cpp
// Run:
//   ./bench_array_contains_all

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <random>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

// ============================================================================
// ContainsAllMatcher — mirrors production class in JsonContainsExpr.cpp
// Uses std::unordered_map as stand-in for ankerl::unordered_dense::map.
// For string_view: stores std::string keys (pessimistic vs production which
// uses ankerl::unordered_dense::map<string_view, uint32_t> with zero-copy).
// ============================================================================

template <typename T>
class ContainsAllMatcher {
 public:
    explicit ContainsAllMatcher(const std::set<T>& targets) {
        target_count_ = targets.size();
        use_small_ = (target_count_ <= 64);
        uint32_t idx = 0;
        for (const auto& t : targets) {
            value_to_bit_[t] = idx++;
        }
        if (use_small_) {
            full_mask_ = (target_count_ == 64)
                             ? ~uint64_t(0)
                             : (uint64_t(1) << target_count_) - 1;
        } else {
            num_words_ = (target_count_ + 63) / 64;
        }
    }

    bool
    set_if_found(const T& val, uint64_t& found) const {
        auto it = value_to_bit_.find(val);
        if (it != value_to_bit_.end()) {
            found |= (uint64_t(1) << it->second);
            return found == full_mask_;
        }
        return false;
    }

    bool
    set_if_found(const T& val,
                 std::vector<uint64_t>& found,
                 size_t& remaining) const {
        auto it = value_to_bit_.find(val);
        if (it != value_to_bit_.end()) {
            uint32_t idx = it->second;
            uint64_t bit = uint64_t(1) << (idx % 64);
            uint64_t& word = found[idx / 64];
            if (!(word & bit)) {
                word |= bit;
                return --remaining == 0;
            }
        }
        return false;
    }

    bool
    use_small() const {
        return use_small_;
    }
    size_t
    target_count() const {
        return target_count_;
    }
    uint64_t
    full_mask() const {
        return full_mask_;
    }
    size_t
    num_words() const {
        return num_words_;
    }

 private:
    std::unordered_map<T, uint32_t> value_to_bit_;
    size_t target_count_{0};
    bool use_small_{true};
    uint64_t full_mask_{0};
    size_t num_words_{0};
};

// string_view specialization: stores std::string keys since std::unordered_map
// does not support heterogeneous lookup. This adds a std::string conversion
// per lookup that is NOT present in production (which uses ankerl with native
// string_view keys). String benchmark numbers are therefore pessimistic.
template <>
class ContainsAllMatcher<std::string_view> {
 public:
    explicit ContainsAllMatcher(const std::set<std::string_view>& targets) {
        target_count_ = targets.size();
        use_small_ = (target_count_ <= 64);
        uint32_t idx = 0;
        for (const auto& t : targets) {
            value_to_bit_[std::string(t)] = idx++;
        }
        if (use_small_) {
            full_mask_ = (target_count_ == 64)
                             ? ~uint64_t(0)
                             : (uint64_t(1) << target_count_) - 1;
        } else {
            num_words_ = (target_count_ + 63) / 64;
        }
    }

    // Note: std::string(val) conversion here is benchmark-only overhead.
    // Production uses ankerl::unordered_dense::map<string_view, uint32_t>.
    bool
    set_if_found(const std::string_view& val, uint64_t& found) const {
        auto it = value_to_bit_.find(std::string(val));
        if (it != value_to_bit_.end()) {
            found |= (uint64_t(1) << it->second);
            return found == full_mask_;
        }
        return false;
    }

    bool
    set_if_found(const std::string_view& val,
                 std::vector<uint64_t>& found,
                 size_t& remaining) const {
        auto it = value_to_bit_.find(std::string(val));
        if (it != value_to_bit_.end()) {
            uint32_t idx = it->second;
            uint64_t bit = uint64_t(1) << (idx % 64);
            uint64_t& word = found[idx / 64];
            if (!(word & bit)) {
                word |= bit;
                return --remaining == 0;
            }
        }
        return false;
    }

    bool
    use_small() const {
        return use_small_;
    }
    size_t
    target_count() const {
        return target_count_;
    }
    uint64_t
    full_mask() const {
        return full_mask_;
    }
    size_t
    num_words() const {
        return num_words_;
    }

 private:
    std::unordered_map<std::string, uint32_t> value_to_bit_;
    size_t target_count_{0};
    bool use_small_{true};
    uint64_t full_mask_{0};
    size_t num_words_{0};
};

// ============================================================================
// BranchState — exact maintainer sketch: if (use_small_) inside mark()
// This is the simplest possible wrapper with no indirection tricks.
// ============================================================================

template <typename T>
class ContainsAllBranchState {
 public:
    explicit ContainsAllBranchState(const ContainsAllMatcher<T>& matcher)
        : matcher_(matcher), use_small_(matcher.use_small()) {
        if (!use_small_) {
            found_large_.resize(matcher.num_words());
        }
    }

    void
    reset() {
        if (use_small_) {
            found_small_ = 0;
        } else {
            std::fill(found_large_.begin(), found_large_.end(), 0);
            remaining_ = matcher_.target_count();
        }
    }

    bool
    mark(const T& val) {
        if (use_small_) {
            return matcher_.set_if_found(val, found_small_);
        } else {
            return matcher_.set_if_found(val, found_large_, remaining_);
        }
    }

    bool
    all_found() const {
        if (use_small_) {
            return found_small_ == matcher_.full_mask();
        } else {
            return remaining_ == 0;
        }
    }

 private:
    const ContainsAllMatcher<T>& matcher_;
    bool use_small_;
    uint64_t found_small_{0};
    std::vector<uint64_t> found_large_;
    size_t remaining_{0};
};

// ============================================================================
// RowState — lightweight per-row state with reset()/mark()/all_found() API
// Keeps small/large branch out of inner loop via function pointer dispatch.
// ============================================================================

template <typename T>
class ContainsAllRowState {
 public:
    explicit ContainsAllRowState(const ContainsAllMatcher<T>& matcher)
        : matcher_(matcher) {
        if (matcher.use_small()) {
            mark_fn_ = &mark_small;
            all_found_fn_ = &all_found_small;
        } else {
            found_large_.resize(matcher.num_words());
            mark_fn_ = &mark_large;
            all_found_fn_ = &all_found_large;
        }
    }

    void
    reset() {
        if (found_large_.empty()) {
            found_small_ = 0;
        } else {
            std::fill(found_large_.begin(), found_large_.end(), 0);
            remaining_ = matcher_.target_count();
        }
    }

    bool
    mark(const T& val) {
        return mark_fn_(*this, val);
    }

    bool
    all_found() const {
        return all_found_fn_(*this);
    }

 private:
    static bool
    mark_small(ContainsAllRowState& s, const T& val) {
        return s.matcher_.set_if_found(val, s.found_small_);
    }
    static bool
    mark_large(ContainsAllRowState& s, const T& val) {
        return s.matcher_.set_if_found(val, s.found_large_, s.remaining_);
    }
    static bool
    all_found_small(const ContainsAllRowState& s) {
        return s.found_small_ == s.matcher_.full_mask();
    }
    static bool
    all_found_large(const ContainsAllRowState& s) {
        return s.remaining_ == 0;
    }

    const ContainsAllMatcher<T>& matcher_;
    uint64_t found_small_{0};
    std::vector<uint64_t> found_large_;
    size_t remaining_{0};
    bool (*mark_fn_)(ContainsAllRowState&, const T&);
    bool (*all_found_fn_)(const ContainsAllRowState&);
};

// ============================================================================
// Minimal ArrayView simulation (mirrors milvus::ArrayView layout)
// ============================================================================

struct Int64Array {
    const int64_t* data;
    int length;

    int64_t
    get(int j) const {
        return data[j];
    }
};

struct StringArray {
    const char* data;
    const uint32_t* offsets;
    int length;
    size_t total_size;

    std::string_view
    get(int j) const {
        size_t start = offsets[j];
        size_t end = (j == length - 1) ? total_size
                                       : static_cast<size_t>(offsets[j + 1]);
        return std::string_view(data + start, end - start);
    }
};

// ============================================================================
// JSON path simulation: single-pass iterator with optional type errors
// (mirrors simdjson forward-only iteration in ExecJsonContainsAll)
// ============================================================================

struct JsonArrayElement {
    enum Tag { INT64, DOUBLE, STRING, ERROR } tag;
    int64_t i64_val;
    double f64_val;
};

struct JsonArray {
    std::vector<JsonArrayElement> elements;
    int length;
};

// ============================================================================
// Original: copies std::set per row, erases found elements
// Matches the original ExecArrayContainsAll / ExecJsonContainsAll pattern
// ============================================================================

// --- Array path ---
template <typename ArrayT, typename T>
void
ContainsAll_SetCopy(const std::vector<ArrayT>& arrays,
                    const std::set<T>& targets,
                    std::vector<bool>& results) {
    results.resize(arrays.size());
    for (size_t i = 0; i < arrays.size(); ++i) {
        std::set<T> tmp(targets);
        for (int j = 0; j < arrays[i].length; ++j) {
            tmp.erase(arrays[i].get(j));
            if (tmp.empty()) {
                break;
            }
        }
        results[i] = tmp.empty();
    }
}

// --- JSON path (int64 with double fallback) ---
void
ContainsAll_SetCopy_Json(const std::vector<JsonArray>& arrays,
                         const std::set<int64_t>& targets,
                         std::vector<bool>& results) {
    results.resize(arrays.size());
    for (size_t i = 0; i < arrays.size(); ++i) {
        std::set<int64_t> tmp(targets);
        for (const auto& elem : arrays[i].elements) {
            if (elem.tag == JsonArrayElement::INT64) {
                tmp.erase(elem.i64_val);
            } else if (elem.tag == JsonArrayElement::DOUBLE) {
                if (elem.f64_val == std::floor(elem.f64_val)) {
                    tmp.erase(static_cast<int64_t>(elem.f64_val));
                }
            }
            if (tmp.empty()) {
                break;
            }
        }
        results[i] = tmp.empty();
    }
}

// ============================================================================
// Optimized: uses ContainsAllMatcher with set_if_found() API
// Matcher is rebuilt per call to match production behavior where it is
// constructed inside execute_sub_batch (once per chunk).
// ============================================================================

// --- Array path ---
template <typename ArrayT, typename T>
void
ContainsAll_Bitmask(const std::vector<ArrayT>& arrays,
                    const std::set<T>& targets,
                    std::vector<bool>& results) {
    results.resize(arrays.size());
    // Matcher built once per sub-batch, matching production placement inside
    // execute_sub_batch lambda in ExecArrayContainsAll.
    ContainsAllMatcher<T> matcher(targets);
    // Hoist vector allocation outside per-row loop (reset with std::fill).
    std::vector<uint64_t> found_large(
        matcher.use_small() ? 0 : matcher.num_words());
    for (size_t i = 0; i < arrays.size(); ++i) {
        if (static_cast<size_t>(arrays[i].length) < matcher.target_count()) {
            results[i] = false;
            continue;
        }
        if (matcher.use_small()) {
            uint64_t found = 0;
            bool matched = false;
            for (int j = 0; j < arrays[i].length; ++j) {
                if (matcher.set_if_found(arrays[i].get(j), found)) {
                    matched = true;
                    break;
                }
            }
            results[i] = matched || (found == matcher.full_mask());
        } else {
            std::fill(found_large.begin(), found_large.end(), 0);
            size_t remaining = matcher.target_count();
            bool matched = false;
            for (int j = 0; j < arrays[i].length; ++j) {
                if (matcher.set_if_found(
                        arrays[i].get(j), found_large, remaining)) {
                    matched = true;
                    break;
                }
            }
            results[i] = matched || (remaining == 0);
        }
    }
}

// --- JSON path (int64 with double fallback) ---
void
ContainsAll_Bitmask_Json(const std::vector<JsonArray>& arrays,
                         const std::set<int64_t>& targets,
                         std::vector<bool>& results) {
    results.resize(arrays.size());
    // Matcher built once per sub-batch, matching production placement.
    ContainsAllMatcher<int64_t> matcher(targets);
    // Hoist vector allocation outside per-row loop.
    std::vector<uint64_t> found_large(
        matcher.use_small() ? 0 : matcher.num_words());
    for (size_t i = 0; i < arrays.size(); ++i) {
        if (matcher.use_small()) {
            uint64_t found = 0;
            bool matched = false;
            for (const auto& elem : arrays[i].elements) {
                if (elem.tag == JsonArrayElement::INT64) {
                    if (matcher.set_if_found(elem.i64_val, found)) {
                        matched = true;
                        break;
                    }
                } else if (elem.tag == JsonArrayElement::DOUBLE) {
                    if (elem.f64_val == std::floor(elem.f64_val)) {
                        if (matcher.set_if_found(
                                static_cast<int64_t>(elem.f64_val), found)) {
                            matched = true;
                            break;
                        }
                    }
                }
            }
            results[i] = matched || (found == matcher.full_mask());
        } else {
            std::fill(found_large.begin(), found_large.end(), 0);
            size_t remaining = matcher.target_count();
            bool matched = false;
            for (const auto& elem : arrays[i].elements) {
                if (elem.tag == JsonArrayElement::INT64) {
                    if (matcher.set_if_found(
                            elem.i64_val, found_large, remaining)) {
                        matched = true;
                        break;
                    }
                } else if (elem.tag == JsonArrayElement::DOUBLE) {
                    if (elem.f64_val == std::floor(elem.f64_val)) {
                        if (matcher.set_if_found(
                                static_cast<int64_t>(elem.f64_val),
                                found_large,
                                remaining)) {
                            matched = true;
                            break;
                        }
                    }
                }
            }
            results[i] = matched || (remaining == 0);
        }
    }
}

// ============================================================================
// BranchState API: exact maintainer sketch — if(use_small_) inside mark()
// ============================================================================

// --- Array path ---
template <typename ArrayT, typename T>
void
ContainsAll_BranchState(const std::vector<ArrayT>& arrays,
                        const std::set<T>& targets,
                        std::vector<bool>& results) {
    results.resize(arrays.size());
    ContainsAllMatcher<T> matcher(targets);
    ContainsAllBranchState<T> state(matcher);
    for (size_t i = 0; i < arrays.size(); ++i) {
        if (static_cast<size_t>(arrays[i].length) < matcher.target_count()) {
            results[i] = false;
            continue;
        }
        state.reset();
        bool matched = false;
        for (int j = 0; j < arrays[i].length; ++j) {
            if (state.mark(arrays[i].get(j))) {
                matched = true;
                break;
            }
        }
        results[i] = matched || state.all_found();
    }
}

// --- JSON path (int64 with double fallback) ---
void
ContainsAll_BranchState_Json(const std::vector<JsonArray>& arrays,
                             const std::set<int64_t>& targets,
                             std::vector<bool>& results) {
    results.resize(arrays.size());
    ContainsAllMatcher<int64_t> matcher(targets);
    ContainsAllBranchState<int64_t> state(matcher);
    for (size_t i = 0; i < arrays.size(); ++i) {
        state.reset();
        bool matched = false;
        for (const auto& elem : arrays[i].elements) {
            if (elem.tag == JsonArrayElement::INT64) {
                if (state.mark(elem.i64_val)) {
                    matched = true;
                    break;
                }
            } else if (elem.tag == JsonArrayElement::DOUBLE) {
                if (elem.f64_val == std::floor(elem.f64_val)) {
                    if (state.mark(static_cast<int64_t>(elem.f64_val))) {
                        matched = true;
                        break;
                    }
                }
            }
        }
        results[i] = matched || state.all_found();
    }
}

// ============================================================================
// FnPtrState API: function-pointer dispatch, no per-element branching
// ============================================================================

// --- Array path ---
template <typename ArrayT, typename T>
void
ContainsAll_FnPtrState(const std::vector<ArrayT>& arrays,
                       const std::set<T>& targets,
                       std::vector<bool>& results) {
    results.resize(arrays.size());
    ContainsAllMatcher<T> matcher(targets);
    ContainsAllRowState<T> state(matcher);
    for (size_t i = 0; i < arrays.size(); ++i) {
        if (static_cast<size_t>(arrays[i].length) < matcher.target_count()) {
            results[i] = false;
            continue;
        }
        state.reset();
        bool matched = false;
        for (int j = 0; j < arrays[i].length; ++j) {
            if (state.mark(arrays[i].get(j))) {
                matched = true;
                break;
            }
        }
        results[i] = matched || state.all_found();
    }
}

// --- JSON path (int64 with double fallback) ---
void
ContainsAll_FnPtrState_Json(const std::vector<JsonArray>& arrays,
                            const std::set<int64_t>& targets,
                            std::vector<bool>& results) {
    results.resize(arrays.size());
    ContainsAllMatcher<int64_t> matcher(targets);
    ContainsAllRowState<int64_t> state(matcher);
    for (size_t i = 0; i < arrays.size(); ++i) {
        state.reset();
        bool matched = false;
        for (const auto& elem : arrays[i].elements) {
            if (elem.tag == JsonArrayElement::INT64) {
                if (state.mark(elem.i64_val)) {
                    matched = true;
                    break;
                }
            } else if (elem.tag == JsonArrayElement::DOUBLE) {
                if (elem.f64_val == std::floor(elem.f64_val)) {
                    if (state.mark(static_cast<int64_t>(elem.f64_val))) {
                        matched = true;
                        break;
                    }
                }
            }
        }
        results[i] = matched || state.all_found();
    }
}

// ============================================================================
// Data generation
// ============================================================================

struct Int64Storage {
    std::vector<std::vector<int64_t>> data_store;
    std::vector<Int64Array> views;
};

Int64Storage
GenerateInt64Arrays(size_t num_arrays,
                    int min_len,
                    int max_len,
                    int64_t value_range,
                    std::mt19937& rng) {
    Int64Storage storage;
    storage.data_store.reserve(num_arrays);
    storage.views.reserve(num_arrays);

    std::uniform_int_distribution<int> len_dist(min_len, max_len);
    std::uniform_int_distribution<int64_t> val_dist(0, value_range - 1);

    for (size_t i = 0; i < num_arrays; ++i) {
        int len = len_dist(rng);
        storage.data_store.emplace_back(len);
        auto& arr = storage.data_store.back();
        for (int j = 0; j < len; ++j) {
            arr[j] = val_dist(rng);
        }
        storage.views.push_back({arr.data(), len});
    }
    return storage;
}

Int64Storage
GenerateInt64ArraysZipf(size_t num_arrays,
                        int min_len,
                        int max_len,
                        int64_t value_range,
                        double zipf_s,
                        std::mt19937& rng) {
    std::vector<double> cdf(value_range);
    double sum = 0;
    for (int64_t i = 1; i <= value_range; ++i) {
        sum += 1.0 / std::pow(static_cast<double>(i), zipf_s);
        cdf[i - 1] = sum;
    }
    for (auto& c : cdf) {
        c /= sum;
    }

    std::uniform_real_distribution<double> u01(0.0, 1.0);
    std::uniform_int_distribution<int> len_dist(min_len, max_len);

    Int64Storage storage;
    storage.data_store.reserve(num_arrays);
    storage.views.reserve(num_arrays);

    for (size_t i = 0; i < num_arrays; ++i) {
        int len = len_dist(rng);
        storage.data_store.emplace_back(len);
        auto& arr = storage.data_store.back();
        for (int j = 0; j < len; ++j) {
            double r = u01(rng);
            auto it = std::lower_bound(cdf.begin(), cdf.end(), r);
            arr[j] = static_cast<int64_t>(it - cdf.begin());
        }
        storage.views.push_back({arr.data(), len});
    }
    return storage;
}

struct StringStorage {
    std::vector<std::vector<uint32_t>> offsets_store;
    std::vector<std::vector<char>> data_store;
    std::vector<StringArray> views;
};

StringStorage
GenerateStringArrays(size_t num_arrays,
                     int min_len,
                     int max_len,
                     int64_t value_range,
                     std::mt19937& rng) {
    StringStorage storage;
    storage.offsets_store.reserve(num_arrays);
    storage.data_store.reserve(num_arrays);
    storage.views.reserve(num_arrays);

    std::uniform_int_distribution<int> len_dist(min_len, max_len);
    std::uniform_int_distribution<int64_t> val_dist(0, value_range - 1);

    for (size_t i = 0; i < num_arrays; ++i) {
        int len = len_dist(rng);
        storage.offsets_store.emplace_back(len);
        storage.data_store.emplace_back();
        auto& offsets = storage.offsets_store.back();
        auto& data = storage.data_store.back();

        for (int j = 0; j < len; ++j) {
            std::string val = "str_" + std::to_string(val_dist(rng));
            offsets[j] = static_cast<uint32_t>(data.size());
            data.insert(data.end(), val.begin(), val.end());
        }

        storage.views.push_back(
            {data.data(), offsets.data(), len, data.size()});
    }
    return storage;
}

struct JsonStorage {
    std::vector<JsonArray> arrays;
};

JsonStorage
GenerateJsonArrays(size_t num_arrays,
                   int min_len,
                   int max_len,
                   int64_t value_range,
                   double error_rate,
                   double double_rate,
                   std::mt19937& rng) {
    JsonStorage storage;
    storage.arrays.reserve(num_arrays);

    std::uniform_int_distribution<int> len_dist(min_len, max_len);
    std::uniform_int_distribution<int64_t> val_dist(0, value_range - 1);
    std::uniform_real_distribution<double> prob(0.0, 1.0);

    for (size_t i = 0; i < num_arrays; ++i) {
        int len = len_dist(rng);
        JsonArray ja;
        ja.length = len;
        ja.elements.resize(len);
        for (int j = 0; j < len; ++j) {
            double p = prob(rng);
            int64_t v = val_dist(rng);
            if (p < error_rate) {
                ja.elements[j] = {JsonArrayElement::ERROR, 0, 0.0};
            } else if (p < error_rate + double_rate) {
                ja.elements[j] = {
                    JsonArrayElement::DOUBLE, 0, static_cast<double>(v)};
            } else {
                ja.elements[j] = {JsonArrayElement::INT64, v, 0.0};
            }
        }
        storage.arrays.push_back(std::move(ja));
    }
    return storage;
}

// ============================================================================
// Timing + per-row correctness verification
// ============================================================================

template <typename Func>
double
TimeMicroseconds(Func&& func, int iterations = 5) {
    func();  // warmup
    double total_us = 0;
    for (int i = 0; i < iterations; ++i) {
        auto start = std::chrono::high_resolution_clock::now();
        func();
        auto end = std::chrono::high_resolution_clock::now();
        total_us +=
            std::chrono::duration<double, std::micro>(end - start).count();
    }
    return total_us / iterations;
}

bool
VerifyRowLevel(const std::vector<bool>& a,
               const std::vector<bool>& b,
               const std::string& label) {
    if (a.size() != b.size()) {
        std::cerr << "  *** " << label << ": SIZE MISMATCH " << a.size()
                  << " vs " << b.size() << " ***" << std::endl;
        return false;
    }
    size_t mismatches = 0;
    size_t first_mismatch = 0;
    for (size_t i = 0; i < a.size(); ++i) {
        if (a[i] != b[i]) {
            if (mismatches == 0) {
                first_mismatch = i;
            }
            mismatches++;
        }
    }
    if (mismatches > 0) {
        std::cerr << "  *** " << label << ": " << mismatches
                  << " ROW-LEVEL MISMATCHES (first at row " << first_mismatch
                  << ") ***" << std::endl;
        return false;
    }
    return true;
}

int
CountTrue(const std::vector<bool>& v) {
    int c = 0;
    for (bool b : v) {
        if (b)
            c++;
    }
    return c;
}

void
PrintResult(const std::string& label,
            const std::string& params,
            double us_before,
            double us_after,
            int matches_before,
            int matches_after,
            bool correct) {
    double ms_before = us_before / 1000.0;
    double ms_after = us_after / 1000.0;
    std::cout << std::fixed << std::setprecision(1);
    std::cout << "  " << std::setw(50) << std::left << label << std::setw(10)
              << std::right << ms_before << std::setw(10) << ms_after
              << std::setprecision(2) << std::setw(8) << us_before / us_after
              << "x" << (correct ? "" : " [FAIL]") << std::endl;
}

// ============================================================================
// Runners
// ============================================================================

void
RunInt64Benchmark(const std::string& label,
                  size_t num_arrays,
                  int min_len,
                  int max_len,
                  int num_targets,
                  int64_t value_range,
                  bool use_zipf = false) {
    std::mt19937 rng(42);
    auto storage =
        use_zipf ? GenerateInt64ArraysZipf(
                       num_arrays, min_len, max_len, value_range, 1.2, rng)
                 : GenerateInt64Arrays(
                       num_arrays, min_len, max_len, value_range, rng);

    std::set<int64_t> targets;
    std::uniform_int_distribution<int64_t> val_dist(0, value_range - 1);
    while (static_cast<int>(targets.size()) < num_targets) {
        targets.insert(val_dist(rng));
    }

    std::vector<bool> res_before, res_after;
    double us_before = TimeMicroseconds(
        [&]() { ContainsAll_SetCopy(storage.views, targets, res_before); });
    double us_after = TimeMicroseconds(
        [&]() { ContainsAll_BranchState(storage.views, targets, res_after); });

    bool correct = VerifyRowLevel(res_before, res_after, label);
    std::string params = "";
    PrintResult(label,
                params,
                us_before,
                us_after,
                CountTrue(res_before),
                CountTrue(res_after),
                correct);
}

void
RunStringBenchmark(const std::string& label,
                   size_t num_arrays,
                   int min_len,
                   int max_len,
                   int num_targets,
                   int64_t value_range) {
    std::mt19937 rng(42);
    auto storage =
        GenerateStringArrays(num_arrays, min_len, max_len, value_range, rng);

    std::set<std::string_view> targets_sv;
    std::vector<std::string> target_strings;
    {
        std::set<std::string> targets_str;
        std::uniform_int_distribution<int64_t> val_dist(0, value_range - 1);
        while (static_cast<int>(targets_str.size()) < num_targets) {
            targets_str.insert("str_" + std::to_string(val_dist(rng)));
        }
        target_strings.assign(targets_str.begin(), targets_str.end());
    }
    for (const auto& s : target_strings) {
        targets_sv.insert(std::string_view(s));
    }

    std::vector<bool> res_before, res_after;
    double us_before = TimeMicroseconds(
        [&]() { ContainsAll_SetCopy(storage.views, targets_sv, res_before); });
    double us_after = TimeMicroseconds([&]() {
        ContainsAll_BranchState(storage.views, targets_sv, res_after);
    });

    bool correct = VerifyRowLevel(res_before, res_after, label);
    std::string params = "";
    PrintResult(label,
                params,
                us_before,
                us_after,
                CountTrue(res_before),
                CountTrue(res_after),
                correct);
}

void
RunJsonBenchmark(const std::string& label,
                 size_t num_arrays,
                 int min_len,
                 int max_len,
                 int num_targets,
                 int64_t value_range,
                 double error_rate,
                 double double_rate) {
    std::mt19937 rng(42);
    auto storage = GenerateJsonArrays(num_arrays,
                                      min_len,
                                      max_len,
                                      value_range,
                                      error_rate,
                                      double_rate,
                                      rng);

    std::set<int64_t> targets;
    std::uniform_int_distribution<int64_t> val_dist(0, value_range - 1);
    while (static_cast<int>(targets.size()) < num_targets) {
        targets.insert(val_dist(rng));
    }

    std::vector<bool> res_before, res_after;
    double us_before = TimeMicroseconds([&]() {
        ContainsAll_SetCopy_Json(storage.arrays, targets, res_before);
    });
    double us_after = TimeMicroseconds([&]() {
        ContainsAll_BranchState_Json(storage.arrays, targets, res_after);
    });

    bool correct = VerifyRowLevel(res_before, res_after, label);
    std::string params = "";
    PrintResult(label,
                params,
                us_before,
                us_after,
                CountTrue(res_before),
                CountTrue(res_after),
                correct);
}

int
main() {
    std::cout << "ContainsAll Brute-Force Benchmark: Before vs After"
              << std::endl;
    std::cout << "Before: std::set copy-per-row + erase (original master code)"
              << std::endl;
    std::cout << "After:  ContainsAllMatcher bitmask + "
                 "reset()/mark()/all_found() state API"
              << std::endl;
    std::cout << std::string(80, '=') << std::endl;
    std::cout << std::setw(50) << std::left << "  Test" << std::setw(10)
              << std::right << "Before" << std::setw(10) << "After"
              << std::setw(9) << "Speedup" << std::endl;
    std::cout << std::string(80, '-') << std::endl;

    RunInt64Benchmark(
        "int64: 1M rows, len 5-20, 3 targets", 1000000, 5, 20, 3, 1000);

    RunInt64Benchmark(
        "int64: 1M rows, len 10-30, 10 targets", 1000000, 10, 30, 10, 10000);

    RunInt64Benchmark(
        "int64: 500K rows, len 20-50, 30 targets", 500000, 20, 50, 30, 50000);

    RunInt64Benchmark("int64: 1M rows, len 1-5, 5 targets (early exit)",
                      1000000,
                      1,
                      5,
                      5,
                      100);

    RunInt64Benchmark("int64: 1M rows, len 10-20, 3 targets, small range",
                      1000000,
                      10,
                      20,
                      3,
                      10);

    // Large target set (> 64) — exercises dynamic bitset fallback
    RunInt64Benchmark("int64: 200K rows, len 50-100, 80 targets (large bitset)",
                      200000,
                      50,
                      100,
                      80,
                      100000);

    // Zipf-skewed data (realistic value distribution)
    RunInt64Benchmark("int64: 1M rows, len 10-30, 5 targets, Zipf s=1.2",
                      1000000,
                      10,
                      30,
                      5,
                      5000,
                      /*use_zipf=*/true);

    // ---- ARRAY path (string) ----

    RunStringBenchmark(
        "string: 500K rows, len 5-20, 3 targets", 500000, 5, 20, 3, 1000);

    RunStringBenchmark(
        "string: 500K rows, len 10-30, 10 targets", 500000, 10, 30, 10, 10000);

    // ---- JSON path (int64 with double/error elements) ----

    RunJsonBenchmark("json: 1M rows, len 5-20, 3 targets, no errors",
                     1000000,
                     5,
                     20,
                     3,
                     1000,
                     0.0,
                     0.0);

    RunJsonBenchmark("json: 1M rows, len 10-30, 10 targets, 10% double",
                     1000000,
                     10,
                     30,
                     10,
                     10000,
                     0.0,
                     0.1);

    RunJsonBenchmark(
        "json: 1M rows, len 10-30, 5 targets, 5% error + 10% double",
        1000000,
        10,
        30,
        5,
        5000,
        0.05,
        0.1);

    RunJsonBenchmark("json: 500K rows, len 20-50, 20 targets, 15% error",
                     500000,
                     20,
                     50,
                     20,
                     50000,
                     0.15,
                     0.05);

    return 0;
}
