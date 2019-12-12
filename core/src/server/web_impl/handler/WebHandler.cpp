//
// Created by yhz on 2019/12/4.
//

#include "WebHandler.h"

namespace milvus {
namespace server {
namespace web {

HasTableDto::ObjectWrapper
WebHandler::hasTable(const std::string& tableName) const {
    auto status = StatusDto::createShared();
    status->errorCode = 0;
    status->reason = "Successfully";

    auto hasTable = HasTableDto::createShared();
    hasTable->reply = false;
    hasTable->status = hasTable->status->createShared();
    hasTable->status = status;

    return hasTable;
}

} // namespace web
} // namespace server
} // namespace milvus
