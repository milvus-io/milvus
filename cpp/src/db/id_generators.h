#pragma once

#include <vector>
#include "types.h"

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
    const size_t MAX_IDS_PER_MICRO = 1000;

}; // SimpleIDGenerator


} // namespace engine
} // namespace vecwise
} // namespace zilliz
