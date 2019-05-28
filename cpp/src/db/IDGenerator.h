////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#pragma once

#include "Types.h"

#include <cstddef>
#include <vector>

namespace zilliz {
namespace vecwise {
namespace engine {

class IDGenerator {
public:
    virtual IDNumber GetNextIDNumber() = 0;
    virtual void GetNextIDNumbers(size_t n, IDNumbers& ids) = 0;

    virtual ~IDGenerator();

}; // IDGenerator


class SimpleIDGenerator : public IDGenerator {
public:
    virtual IDNumber GetNextIDNumber() override;
    virtual void GetNextIDNumbers(size_t n, IDNumbers& ids) override;

private:
    void NextIDNumbers(size_t n, IDNumbers& ids);
    const size_t MAX_IDS_PER_MICRO = 1000;

}; // SimpleIDGenerator


} // namespace engine
} // namespace vecwise
} // namespace zilliz
