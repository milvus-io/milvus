#ifndef UTILS_ID_GENERATORS_H_
#define UTILS_ID_GENERATORS_H_

#include <vector>
#include "types.h"

namespace zilliz {
namespace vecwise {
namespace engine {

class IDGenerator {
public:
    virtual IDNumber getNextIDNumber() = 0;
    virtual IDNumbers&& getNextIDNumbers(size_t n_) = 0;

    virtual ~IDGenerator();

}; // IDGenerator


class SimpleIDGenerator : public IDGenerator {
public:
    virtual IDNumber getNextIDNumber() override;
    virtual IDNumbers&& getNextIDNumbers(size_t n_) override;

private:
    const MAX_IDS_PER_MICRO = 1000;

}; // SimpleIDGenerator


} // namespace engine
} // namespace vecwise
} // namespace zilliz

#endif // UTILS_ID_GENERATORS_H_
