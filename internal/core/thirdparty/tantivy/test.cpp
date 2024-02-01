#include <cstdint>
#include <cassert>
#include <boost/filesystem.hpp>
#include <iostream>

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

    w.add_data(arr, l);

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

    w.add_data(arr, l);

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

    w.add_data<std::string>(arr.data(), l);

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

int
main(int argc, char* argv[]) {
    run<int8_t>();
    run<int16_t>();
    run<int32_t>();
    run<int64_t>();

    run<float>();
    run<double>();

    run<bool>();

    run<std::string>();

    return 0;
}
