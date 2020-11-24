#include "InsertRecord.h"

namespace milvus::segcore {

InsertRecord::InsertRecord(const Schema& schema) : uids_(1), timestamps_(1) {
    for (auto& field : schema) {
        if (field.is_vector()) {
            Assert(field.get_data_type() == DataType::VECTOR_FLOAT);
            entity_vec_.emplace_back(std::make_shared<ConcurrentVector<float>>(field.get_dim()));
            continue;
        }
        switch (field.get_data_type()) {
            case DataType::INT8: {
                entity_vec_.emplace_back(std::make_shared<ConcurrentVector<int8_t, true>>());
                break;
            }
            case DataType::INT16: {
                entity_vec_.emplace_back(std::make_shared<ConcurrentVector<int16_t, true>>());
                break;
            }
            case DataType::INT32: {
                entity_vec_.emplace_back(std::make_shared<ConcurrentVector<int32_t, true>>());
                break;
            }

            case DataType::INT64: {
                entity_vec_.emplace_back(std::make_shared<ConcurrentVector<int64_t, true>>());
                break;
            }

            case DataType::FLOAT: {
                entity_vec_.emplace_back(std::make_shared<ConcurrentVector<float, true>>());
                break;
            }

            case DataType::DOUBLE: {
                entity_vec_.emplace_back(std::make_shared<ConcurrentVector<double, true>>());
                break;
            }
            default: {
                PanicInfo("unsupported");
            }
        }
    }
}
}  // namespace milvus::segcore
