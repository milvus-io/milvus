#pragma once

#include "oatpp/core/data/mapping/type/Object.hpp"
#include "oatpp/core/macro/codegen.hpp"

namespace milvus {
namespace server {
namespace web {

#include OATPP_CODEGEN_BEGIN(DTO)

class StatusDto: public oatpp::data::mapping::type::Object {

    DTO_INIT(StatusDto, Object)

    DTO_FIELD(String, reason, "reason");
    DTO_FIELD(Int32, errorCode, "error-code");
};

class HasTableDto : public oatpp::data::mapping::type::Object {

    DTO_INIT(HasTableDto, Object)

    DTO_FIELD(Boolean, reply, "reply");
    DTO_FIELD(StatusDto::ObjectWrapper, status);
};

#include OATPP_CODEGEN_END(DTO)

} // namespace web
} // namespace server
} // namespace milvus