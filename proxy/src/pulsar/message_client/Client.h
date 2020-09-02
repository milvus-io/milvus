#pragma once

#include "pulsar/Client.h"
#include "pulsar/ClientConfiguration.h"

namespace message_client {

using Result = pulsar::Result;
using Message = pulsar::Message;

class MsgClient : public pulsar::Client{
public:
  MsgClient(const std::string& serviceUrl);
  MsgClient(const std::string& serviceUrl, const pulsar::ClientConfiguration& clientConfiguration);

  void set_client_id(int64_t id) { client_id_ = id; }

  int64_t get_client_id() { return client_id_; }

private:
  int64_t client_id_;
};

}
