#include <string>
#include <vector>

#include "tantivy-binding.h"

int
main(int argc, char* argv[]) {
    std::vector<std::string> data{"data1", "data2", "data3"};
    std::vector<const char*> datas{};
    for (auto& s : data) {
        datas.push_back(s.c_str());
    }

    print_vector_of_strings(datas.data(), datas.size());

    return 0;
}
