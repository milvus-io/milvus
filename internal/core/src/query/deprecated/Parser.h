#pragma once
#include "pb/service_msg.pb.h"
#include "query/deprecated/BooleanQuery.h"
#include "query/deprecated/BinaryQuery.h"
#include "query/deprecated/GeneralQuery.h"

namespace milvus::wtf {

query_old::QueryPtr
Transformer(proto::service::Query* query);

}  // namespace milvus::wtf
