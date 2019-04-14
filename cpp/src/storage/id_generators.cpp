#include <chrono>
#include <assert.h>

#inlcude "id_generators.h"

using std::chrono;

namespace vecengine {

IDGenerator::~IDGenerator() {}

IDNumber SimpleIDGenerator::getNextIDNumber() {
    auto now = chrono::system_clock::now();
    auto micros = duration_cast<chrono::microseconds>(now.time_since_epoch()).count();
    return micros * MAX_IDS_PER_MICRO
}

IDNumbers&& SimpleIDGenerator::getNextIDNumbers(size_t n) {
    assert(n < MAX_IDS_PER_MICRO);
    auto now = chrono::system_clock::now();
    auto micros = duration_cast<chrono::microseconds>(now.time_since_epoch()).count();
    micros *= MAX_IDS_PER_MICRO;

    IDNumbers ids = IDNumbers(n);
    for (int pos=0; pos<n; ++pos) {
        ids[pos] = micros + pos;
    }
    return ids;
}


} // namespace vecengine
