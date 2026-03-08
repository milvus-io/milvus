#include <cstdint>
#include <cassert>
#include <boost/filesystem.hpp>
#include <iostream>
#include <random>

#include "tantivy-binding.h"
#include "tantivy-wrapper.h"
#include "time_recorder.h"

using namespace milvus::tantivy;

void
build_index(size_t n = 1000000) {
    auto path = "/tmp/inverted-index/test-binding/";
    boost::filesystem::remove_all(path);
    boost::filesystem::create_directories(path);

    auto w =
        TantivyIndexWrapper("test_field_name", TantivyDataType::Keyword, path);

    std::vector<std::string> arr;
    arr.reserve(n);

    std::default_random_engine er(42);
    int64_t sample = 10000;
    for (size_t i = 0; i < n; i++) {
        auto x = er() % sample;
        arr.push_back(std::to_string(x));
    }

    w.add_data<std::string>(arr.data(), arr.size(), 0);

    w.finish();
    assert(w.count() == n);
}

void
search(size_t repeat = 10) {
    TimeRecorder tr("bench-tantivy-search");

    auto path = "/tmp/inverted-index/test-binding/";
    assert(tantivy_index_exist(path));
    tr.RecordSection("check if index exist");

    auto w = TantivyIndexWrapper(path);
    auto cnt = w.count();
    tr.RecordSection("count num_entities");
    std::cout << "index already exist, open it, count: " << cnt << std::endl;

    for (size_t i = 0; i < repeat; i++) {
        w.lower_bound_range_query<std::string>(std::to_string(45), false);
        tr.RecordSection("query");
    }

    tr.ElapseFromBegin("done");
}

int
main(int argc, char* argv[]) {
    build_index(1000000);
    search(10);

    return 0;
}
