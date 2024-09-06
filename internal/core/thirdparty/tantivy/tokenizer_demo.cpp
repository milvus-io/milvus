#include <iostream>
#include "token-stream.h"
#include "tokenizer.h"

using Map = std::map<std::string, std::string>;

using namespace milvus::tantivy;

void
test_tokenizer(const Map& m, std::string&& text) {
    Tokenizer tokenizer(m);

    auto token_stream = tokenizer.CreateTokenStream(std::move(text));
    while (token_stream->advance()) {
        auto token = token_stream->get_token();
        std::cout << token << std::endl;
    }
}

int
main(int argc, char* argv[]) {
    // default tokenizer
    {
        Map m;
        test_tokenizer(m, "football, basketball, pingpang");
    }

    // jieba tokenizer
    {
        Map m;
        std::string tokenizer_name = "jieba";
        m["tokenizer"] = tokenizer_name;
        test_tokenizer(m,
                       "张华考上了北京大学；李萍进了中等技术学校；我在百货公司"
                       "当售货员：我们都有光明的前途");
    }

    return 0;
}
