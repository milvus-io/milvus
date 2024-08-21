#include <cstdint>
#include <cassert>
#include <boost/filesystem.hpp>
#include <iostream>
#include <random>
#include <set>
#include <map>

#include "tantivy-binding.h"
#include "tantivy-wrapper.h"

using namespace milvus::tantivy;

template <typename T>
void
run() {
    std::cout << "run " << typeid(T).name() << std::endl;

    auto path = "/tmp/inverted-index/test-binding/";
    boost::filesystem::remove_all(path);
    boost::filesystem::create_directories(path);

    if (tantivy_index_exist(path)) {
        auto w = TantivyIndexWrapper(path);
        auto cnt = w.count();
        std::cout << "index already exist, open it, count: " << cnt
                  << std::endl;
        return;
    }

    auto w = TantivyIndexWrapper("test_field_name", guess_data_type<T>(), path);

    T arr[] = {1, 2, 3, 4, 5, 6};
    auto l = sizeof(arr) / sizeof(T);

    w.add_data(arr, l, 0);

    w.finish();

    assert(w.count() == l);

    {
        auto hits = w.term_query<T>(2);
        hits.debug();
    }

    {
        auto hits = w.lower_bound_range_query<T>(1, false);
        hits.debug();
    }

    {
        auto hits = w.upper_bound_range_query<T>(4, false);
        hits.debug();
    }

    {
        auto hits = w.range_query<T>(2, 4, false, false);
        hits.debug();
    }
}

template <>
void
run<bool>() {
    std::cout << "run bool" << std::endl;

    auto path = "/tmp/inverted-index/test-binding/";
    boost::filesystem::remove_all(path);
    boost::filesystem::create_directories(path);

    if (tantivy_index_exist(path)) {
        auto w = TantivyIndexWrapper(path);
        auto cnt = w.count();
        std::cout << "index already exist, open it, count: " << cnt
                  << std::endl;
        return;
    }

    auto w =
        TantivyIndexWrapper("test_field_name", TantivyDataType::Bool, path);

    bool arr[] = {true, false, false, true, false, true};
    auto l = sizeof(arr) / sizeof(bool);

    w.add_data(arr, l, 0);

    w.finish();

    assert(w.count() == l);

    {
        auto hits = w.term_query<bool>(true);
        hits.debug();
    }
}

template <>
void
run<std::string>() {
    std::cout << "run string" << std::endl;

    auto path = "/tmp/inverted-index/test-binding/";
    boost::filesystem::remove_all(path);
    boost::filesystem::create_directories(path);

    if (tantivy_index_exist(path)) {
        auto w = TantivyIndexWrapper(path);
        auto cnt = w.count();
        std::cout << "index already exist, open it, count: " << cnt
                  << std::endl;
        return;
    }

    auto w =
        TantivyIndexWrapper("test_field_name", TantivyDataType::Keyword, path);

    std::vector<std::string> arr = {"a", "b", "aaa", "abbb"};
    auto l = arr.size();

    w.add_data<std::string>(arr.data(), l, 0);

    w.finish();

    assert(w.count() == l);

    {
        auto hits = w.term_query<std::string>("a");
        hits.debug();
    }

    {
        auto hits = w.lower_bound_range_query<std::string>("aa", true);
        hits.debug();
    }

    {
        auto hits = w.upper_bound_range_query<std::string>("ab", true);
        hits.debug();
    }

    {
        auto hits = w.range_query<std::string>("aa", "ab", true, true);
        hits.debug();
    }

    {
        auto hits = w.prefix_query("a");
        hits.debug();
    }

    {
        auto hits = w.regex_query("a(.|\n)*");
        hits.debug();
    }
}

void
test_32717() {
    using T = int16_t;

    auto path = "/tmp/inverted-index/test-binding/";
    boost::filesystem::remove_all(path);
    boost::filesystem::create_directories(path);

    if (tantivy_index_exist(path)) {
        auto w = TantivyIndexWrapper(path);
        auto cnt = w.count();
        std::cout << "index already exist, open it, count: " << cnt
                  << std::endl;
        return;
    }

    auto w = TantivyIndexWrapper("test_field_name", guess_data_type<T>(), path);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dis(1, 1000);
    std::vector<int16_t> arr;
    std::map<int16_t, std::set<int>> inverted;
    size_t l = 1000000;
    for (size_t i = 0; i < l; i++) {
        auto n = static_cast<int16_t>(dis(gen));
        arr.push_back(n);
        if (inverted.find(n) == inverted.end()) {
            inverted[n] = std::set<int>();
        }
        inverted[n].insert(i);
    }

    w.add_data(arr.data(), l, 0);
    w.finish();
    assert(w.count() == l);

    for (int16_t term = 1; term < 1000; term += 10) {
        auto hits = w.term_query(term);
        for (size_t i = 0; i < hits.array_.len; i++) {
            assert(arr[hits.array_.array[i]] == term);
        }
    }
}

std::set<uint32_t>
to_set(const RustArrayWrapper& w) {
    std::set<uint32_t> s(w.array_.array, w.array_.array + w.array_.len);
    return s;
}

template <typename T>
std::map<T, std::set<uint32_t>>
build_inverted_index(const std::vector<std::vector<T>>& vec_of_array) {
    std::map<T, std::set<uint32_t>> inverted_index;
    for (uint32_t i = 0; i < vec_of_array.size(); i++) {
        for (const auto& term : vec_of_array[i]) {
            inverted_index[term].insert(i);
        }
    }
    return inverted_index;
}

void
test_array_int() {
    using T = int64_t;

    auto path = "/tmp/inverted-index/test-binding/";
    boost::filesystem::remove_all(path);
    boost::filesystem::create_directories(path);
    auto w = TantivyIndexWrapper("test_field_name", guess_data_type<T>(), path);

    std::vector<std::vector<T>> vec_of_array{
        {10, 40, 50},
        {20, 50},
        {10, 50, 60},
    };

    int64_t offset = 0;
    for (const auto& arr : vec_of_array) {
        w.add_multi_data(arr.data(), arr.size(), offset++);
    }
    w.finish();

    assert(w.count() == vec_of_array.size());

    auto inverted_index = build_inverted_index(vec_of_array);
    for (const auto& [term, posting_list] : inverted_index) {
        auto hits = to_set(w.term_query(term));
        assert(posting_list == hits);
    }
}

void
test_array_string() {
    using T = std::string;

    auto path = "/tmp/inverted-index/test-binding/";
    boost::filesystem::remove_all(path);
    boost::filesystem::create_directories(path);
    auto w =
        TantivyIndexWrapper("test_field_name", TantivyDataType::Keyword, path);

    std::vector<std::vector<T>> vec_of_array{
        {"10", "40", "50"},
        {"20", "50"},
        {"10", "50", "60"},
    };

    int64_t offset = 0;
    for (const auto& arr : vec_of_array) {
        w.add_multi_data(arr.data(), arr.size(), offset++);
    }
    w.finish();

    assert(w.count() == vec_of_array.size());

    auto inverted_index = build_inverted_index(vec_of_array);
    for (const auto& [term, posting_list] : inverted_index) {
        auto hits = to_set(w.term_query(term));
        assert(posting_list == hits);
    }
}

int
main(int argc, char* argv[]) {
    test_32717();

    run<int8_t>();
    run<int16_t>();
    run<int32_t>();
    run<int64_t>();

    run<float>();
    run<double>();

    run<bool>();

    run<std::string>();

    test_array_int();
    test_array_string();

    return 0;
}
