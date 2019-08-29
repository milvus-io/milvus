#pragma once

#include <memory>
#include "arrow/type.h"


namespace zilliz {
namespace knowhere {


using DataType = arrow::DataType;
using Field = arrow::Field;
using FieldPtr = std::shared_ptr<arrow::Field>;
using Schema = arrow::Schema;
using SchemaPtr = std::shared_ptr<Schema>;
using SchemaConstPtr = std::shared_ptr<const Schema>;



} // namespace knowhere
} // namespace zilliz
