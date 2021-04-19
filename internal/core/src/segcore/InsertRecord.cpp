#include "InsertRecord.h"

namespace milvus::segcore {

InsertRecord::InsertRecord(const Schema& schema) : uids_(1), timestamps_(1) {
    for (auto& field : schema) {
        if (field.is_vector()) {
            Assert(field.get_data_type() == DataType::VECTOR_FLOAT);
            entity_vec_.emplace_back(std::make_shared<ConcurrentVector<float>>(field.get_dim()));
        } else {
            Assert(field.get_data_type() == DataType::INT32);
            entity_vec_.emplace_back(std::make_shared<ConcurrentVector<int32_t, true>>());
        }
    }
}
}  // namespace milvus::segcore
