
#pragma once

#include <memory>
#include <knowhere/common/array.h>


namespace zilliz {
namespace knowhere {

ArrayPtr
CopyArray(const ArrayPtr &origin);

SchemaPtr
CopySchema(const SchemaPtr &origin);

} // namespace knowhere
} // namespace zilliz
