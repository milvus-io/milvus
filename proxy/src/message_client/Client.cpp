
#include "Client.h"

namespace milvus {
namespace message_client {

MsgClient::MsgClient(const std::string &serviceUrl) : pulsar::Client(serviceUrl) {}

MsgClient::MsgClient(const std::string &serviceUrl, const pulsar::ClientConfiguration& clientConfiguration)
              : pulsar::Client(serviceUrl, clientConfiguration) {}

}
}