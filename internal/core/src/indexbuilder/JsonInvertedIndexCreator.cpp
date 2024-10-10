#include "indexbuilder/JsonInvertedIndexCreator.h"
#include <string>
#include <string_view>
#include <type_traits>
#include "common/EasyAssert.h"
#include "common/FieldDataInterface.h"
#include "common/Json.h"
#include "common/Types.h"
#include "log/Log.h"
#include "simdjson/error.h"

namespace milvus::indexbuilder {

template <typename T>
void
JsonInvertedIndexCreator<T>::build_index_for_json(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas) {
    using GetType =
        std::conditional_t<std::is_same_v<std::string, T>, std::string_view, T>;
    int64_t offset = 0;
    LOG_INFO("Start to build json inverted index for field: {}", nested_path_);
    for (const auto& data : field_datas) {
        auto n = data->get_num_rows();
        for (int64_t i = 0; i < n; i++) {
            auto json_column = static_cast<const Json*>(data->RawValue(i));
            if (this->schema_.nullable() && !data->is_valid(i)) {
                this->null_offset.push_back(i);
                continue;
            }
            value_result<GetType> res = json_column->at<GetType>(nested_path_);
            auto err = res.error();
            if (err != simdjson::SUCCESS) {
                AssertInfo(err == simdjson::INCORRECT_TYPE ||
                               err == simdjson::NO_SUCH_FIELD,
                           "Failed to parse json, err: {}",
                           err);
                offset++;
                continue;
            }
            if constexpr (std::is_same_v<GetType, std::string_view>) {
                auto value = std::string(res.value());
                this->wrapper_->template add_data(&value, 1, offset++);
            } else {
                auto value = res.value();
                this->wrapper_->template add_data(&value, 1, offset++);
            }
        }
    }
}

template class JsonInvertedIndexCreator<bool>;
template class JsonInvertedIndexCreator<int64_t>;
template class JsonInvertedIndexCreator<double>;
template class JsonInvertedIndexCreator<std::string>;

}  // namespace milvus::indexbuilder