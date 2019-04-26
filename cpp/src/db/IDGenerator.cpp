#include <chrono>
#include <assert.h>
#include <iostream>

#include "IDGenerator.h"


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

void SimpleIDGenerator::nextIDNumbers(size_t n, IDNumbers& ids) {
    if (n > MAX_IDS_PER_MICRO) {
        nextIDNumbers(n-MAX_IDS_PER_MICRO, ids);
        nextIDNumbers(MAX_IDS_PER_MICRO, ids);
        return;
    }
    if (n <= 0) {
        return;
    }

    auto now = std::chrono::system_clock::now();
    auto micros = std::chrono::duration_cast<std::chrono::microseconds>(
            now.time_since_epoch()).count();
    micros *= MAX_IDS_PER_MICRO;

    for (int pos=0; pos<n; ++pos) {
        ids.push_back(micros+pos);
    }

}

void SimpleIDGenerator::getNextIDNumbers(size_t n, IDNumbers& ids) {
    ids.clear();
    nextIDNumbers(n, ids);
}


} // namespace engine
} // namespace vecwise
} // namespace zilliz
