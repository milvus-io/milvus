#include "common/JsonCastType.h"

namespace milvus {

const std::unordered_map<std::string, const JsonCastType>
    JsonCastType::json_cast_type_map_ = {
        {"BOOL",
         JsonCastType(JsonCastType::DataType::BOOL,
                      JsonCastType::DataType::BOOL)},
        {"DOUBLE",
         JsonCastType(JsonCastType::DataType::DOUBLE,
                      JsonCastType::DataType::DOUBLE)},
        {"VARCHAR",
         JsonCastType(JsonCastType::DataType::VARCHAR,
                      JsonCastType::DataType::VARCHAR)},
        {"ARRAY_BOOL",
         JsonCastType(JsonCastType::DataType::ARRAY,
                      JsonCastType::DataType::BOOL)},
        {"ARRAY_DOUBLE",
         JsonCastType(JsonCastType::DataType::ARRAY,
                      JsonCastType::DataType::DOUBLE)},
        {"ARRAY_VARCHAR",
         JsonCastType(JsonCastType::DataType::ARRAY,
                      JsonCastType::DataType::VARCHAR)}};

const JsonCastType JsonCastType::UNKNOWN = JsonCastType(
    JsonCastType::DataType::UNKNOWN, JsonCastType::DataType::UNKNOWN);

JsonCastType
JsonCastType::FromString(const std::string& str) {
    auto it = json_cast_type_map_.find(str);
    if (it == json_cast_type_map_.end()) {
        PanicInfo(Unsupported, "Invalid json cast type: " + str);
    }
    return it->second;
}

JsonCastType::JsonCastType(DataType data_type, DataType element_type)
    : data_type_(data_type), element_type_(element_type) {
}

JsonCastType::DataType
JsonCastType::data_type() const {
    return data_type_;
}

JsonCastType::DataType
JsonCastType::element_type() const {
    return element_type_;
}

TantivyDataType
JsonCastType::ToTantivyType() const {
    switch (element_type()) {
        case JsonCastType::DataType::BOOL:
            return TantivyDataType::Bool;
        case JsonCastType::DataType::DOUBLE:
            return TantivyDataType::F64;
        case JsonCastType::DataType::VARCHAR:
            return TantivyDataType::Keyword;
        default:
            PanicInfo(DataTypeInvalid, "Invalid data type:{}", element_type());
    }
}

MilvusDataType
JsonCastType::ToMilvusDataType() const {
    switch (element_type()) {
        case JsonCastType::DataType::BOOL:
            return MilvusDataType::BOOL;
        case JsonCastType::DataType::DOUBLE:
            return MilvusDataType::DOUBLE;
        case JsonCastType::DataType::VARCHAR:
            return MilvusDataType::VARCHAR;
        default:
            PanicInfo(DataTypeInvalid, "Invalid data type:{}", element_type());
    }
}
}  // namespace milvus
