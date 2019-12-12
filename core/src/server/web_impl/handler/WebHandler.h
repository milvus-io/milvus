//
// Created by yhz on 2019/12/4.
//

# pragma once

#include <string>

#include "oatpp/core/data/mapping/type/Object.hpp"
#include "oatpp/core/macro/codegen.hpp"

#include "../dto/ResultDto.hpp"

namespace milvus {
namespace server {
namespace web {

class WebHandler {
 public:
    HasTableDto::ObjectWrapper hasTable(const std::string& tableName) const;
};

} // namespace web
} // namespace server
} // namespace milvus

