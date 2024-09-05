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
    std::string tokenizer_name = "jieba";
    std::map<std::string, std::string> tokenizer_params;
    tokenizer_params["tokenizer"] = tokenizer_name;

    auto text_index = TantivyIndexWrapper(
        "text_demo", true, "", tokenizer_name.c_str(), tokenizer_params);
    auto write_single_text = [&text_index](const std::string& s,
                                           int64_t offset) {
        text_index.add_data(&s, 1, offset);
    };

    {
        write_single_text(
            "张华考上了北京大学；李萍进了中等技术学校；我在百货公司当售货员：我"
            "们都有光明的前途",
            0);
        write_single_text("测试中文分词器的效果", 1);
        write_single_text("黄金时代", 2);
        write_single_text("青铜时代", 3);
        text_index.commit();
    }

    text_index.create_reader();
    text_index.register_tokenizer(tokenizer_name.c_str(), tokenizer_params);

    {
        auto result = to_set(text_index.match_query("北京"));
        assert(result.size() == 1);
        assert(result.find(0) != result.end());
    }

    {
        auto result = to_set(text_index.match_query("效果"));
        assert(result.size() == 1);
        assert(result.find(1) != result.end());
    }

    {
        auto result = to_set(text_index.match_query("时代"));
        assert(result.size() == 2);
        assert(result.find(2) != result.end());
        assert(result.find(3) != result.end());
    }

    return 0;
}
