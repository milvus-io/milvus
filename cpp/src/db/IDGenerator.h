////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#pragma once

#include <vector>
#include "Types.h"

namespace zilliz {
namespace vecwise {
namespace engine {

class IDGenerator {
public:
    virtual IDNumber getNextIDNumber() = 0;
    virtual void getNextIDNumbers(size_t n, IDNumbers& ids) = 0;

    virtual ~IDGenerator();

}; // IDGenerator


class SimpleIDGenerator : public IDGenerator {
public:
    virtual IDNumber getNextIDNumber() override;
    virtual void getNextIDNumbers(size_t n, IDNumbers& ids) override;

private:
    void nextIDNumbers(size_t n, IDNumbers& ids);
    const size_t MAX_IDS_PER_MICRO = 1000;

}; // SimpleIDGenerator


} // namespace engine
} // namespace vecwise
} // namespace zilliz
