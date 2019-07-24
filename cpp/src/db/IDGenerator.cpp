////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include "IDGenerator.h"

#include <chrono>
#include <assert.h>
#include <iostream>

namespace zilliz {
namespace milvus {
namespace engine {

constexpr size_t SimpleIDGenerator::MAX_IDS_PER_MICRO;

IDNumber SimpleIDGenerator::GetNextIDNumber() {
    auto now = std::chrono::system_clock::now();
    auto micros = std::chrono::duration_cast<std::chrono::microseconds>(
            now.time_since_epoch()).count();
    return micros * MAX_IDS_PER_MICRO;
}

void SimpleIDGenerator::NextIDNumbers(size_t n, IDNumbers& ids) {
    if (n > MAX_IDS_PER_MICRO) {
        NextIDNumbers(n-MAX_IDS_PER_MICRO, ids);
        NextIDNumbers(MAX_IDS_PER_MICRO, ids);
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

void SimpleIDGenerator::GetNextIDNumbers(size_t n, IDNumbers& ids) {
    ids.clear();
    NextIDNumbers(n, ids);
}


} // namespace engine
} // namespace milvus
} // namespace zilliz
