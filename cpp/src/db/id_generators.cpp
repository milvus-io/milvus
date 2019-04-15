#include <chrono>
#include <assert.h>
#include <iostream>

#include "id_generators.h"


namespace zilliz {
namespace vecwise {
namespace engine {

IDGenerator::~IDGenerator() {}

IDNumber SimpleIDGenerator::getNextIDNumber() {
    auto now = std::chrono::system_clock::now();
    auto micros = std::chrono::duration_cast<std::chrono::microseconds>(
            now.time_since_epoch()).count();
    return micros * MAX_IDS_PER_MICRO;
}

void SimpleIDGenerator::getNextIDNumbers(size_t n, IDNumbers& ids) {
    assert(n < MAX_IDS_PER_MICRO);
    auto now = std::chrono::system_clock::now();
    auto micros = std::chrono::duration_cast<std::chrono::microseconds>(
            now.time_since_epoch()).count();
    micros *= MAX_IDS_PER_MICRO;

    ids.clear();
    for (int pos=0; pos<n; ++pos) {
        ids.push_back(micros+pos);
    }
}


} // namespace engine
} // namespace vecwise
} // namespace zilliz
