
#include "knowhere/adapter/arrow.h"

namespace zilliz {
namespace knowhere {

ArrayPtr
CopyArray(const ArrayPtr &origin) {
    ArrayPtr copy = nullptr;
    auto copy_data = origin->data()->Copy();
    switch (origin->type_id()) {
#define DEFINE_TYPE(type, clazz)                                  \
        case arrow::Type::type: {                                 \
            copy = std::make_shared<arrow::clazz>(copy_data);     \
        }
        DEFINE_TYPE(BOOL, BooleanArray)
        DEFINE_TYPE(BINARY, BinaryArray)
        DEFINE_TYPE(FIXED_SIZE_BINARY, FixedSizeBinaryArray)
        DEFINE_TYPE(DECIMAL, Decimal128Array)
        DEFINE_TYPE(FLOAT, NumericArray<arrow::FloatType>)
        DEFINE_TYPE(INT64, NumericArray<arrow::Int64Type>)
        default:break;
    }
    return copy;
}

SchemaPtr
CopySchema(const SchemaPtr &origin) {
    std::vector<std::shared_ptr<Field>> fields;
    for (auto &field : origin->fields()) {
        auto copy = std::make_shared<Field>(field->name(), field->type(),field->nullable(), nullptr);
        fields.emplace_back(copy);
    }
    return std::make_shared<Schema>(std::move(fields));
}


} // namespace knowhere
} // namespace zilliz
