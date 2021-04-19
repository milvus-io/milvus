#pragma once
//#include "pb/message.pb.h"
#include "pb/service_msg.pb.h"
#include "query/BooleanQuery.h"
#include "query/BinaryQuery.h"
#include "query/GeneralQuery.h"

namespace milvus::wtf {

query_old::QueryPtr
tester(proto::service::Query* query);



}  // namespace milvus::wtf
