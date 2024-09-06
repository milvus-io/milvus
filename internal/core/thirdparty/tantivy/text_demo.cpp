#include <string>
#include <vector>
#include <boost/filesystem/operations.hpp>

#include "tantivy-binding.h"
#include "tantivy-wrapper.h"

using namespace milvus::tantivy;

std::set<uint32_t>
to_set(const RustArrayWrapper& w) {
    std::set<uint32_t> s(w.array_.array, w.array_.array + w.array_.len);
    return s;
}

int
main(int argc, char* argv[]) {
    auto text_index = TantivyIndexWrapper("text_demo", true, "");
    auto write_single_text = [&text_index](const std::string& s,
                                           int64_t offset) {
        text_index.add_data(&s, 1, offset);
    };

    {
        write_single_text("football, basketball, pingpang", 0);
        write_single_text("swimming, football", 1);
        write_single_text("Avatar", 2);
        write_single_text("Action, Adventure, Fantasy, Science Fiction", 3);
        write_single_text("Ingenious Film Partners, Twentiesth Century Fox", 4);
        write_single_text("Sam Worthington as Jack Sully", 5);
        text_index.commit();
    }

    text_index.create_reader();
    {
        auto result = to_set(text_index.match_query("football"));
        assert(result.size() == 2);
        assert(result.find(0) != result.end());
        assert(result.find(1) != result.end());
    }

    {
        auto result = to_set(text_index.match_query("basketball"));
        assert(result.size() == 1);
        assert(result.find(0) != result.end());
    }

    {
        auto result = to_set(text_index.match_query("swimming"));
        assert(result.size() == 1);
        assert(result.find(1) != result.end());
    }

    {
        auto result = to_set(text_index.match_query("basketball, swimming"));
        assert(result.size() == 2);
        assert(result.find(0) != result.end());
        assert(result.find(1) != result.end());
    }

    {
        auto result = to_set(text_index.match_query("avatar"));
        assert(result.size() == 1);
        assert(result.find(2) != result.end());
    }

    return 0;
}
