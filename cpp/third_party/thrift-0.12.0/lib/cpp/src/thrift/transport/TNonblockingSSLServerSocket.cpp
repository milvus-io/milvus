/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <thrift/transport/TNonblockingSSLServerSocket.h>
#include <thrift/transport/TSSLSocket.h>

namespace apache {
namespace thrift {
namespace transport {

/**
 * Nonblocking SSL server socket implementation.
 */
TNonblockingSSLServerSocket::TNonblockingSSLServerSocket(int port, stdcxx::shared_ptr<TSSLSocketFactory> factory)
  : TNonblockingServerSocket(port), factory_(factory) {
  factory_->server(true);
}

TNonblockingSSLServerSocket::TNonblockingSSLServerSocket(const std::string& address,
                                   int port,
                                   stdcxx::shared_ptr<TSSLSocketFactory> factory)
  : TNonblockingServerSocket(address, port), factory_(factory) {
  factory_->server(true);
}

TNonblockingSSLServerSocket::TNonblockingSSLServerSocket(int port,
                                   int sendTimeout,
                                   int recvTimeout,
                                   stdcxx::shared_ptr<TSSLSocketFactory> factory)
  : TNonblockingServerSocket(port, sendTimeout, recvTimeout), factory_(factory) {
  factory_->server(true);
}

stdcxx::shared_ptr<TSocket> TNonblockingSSLServerSocket::createSocket(THRIFT_SOCKET client) {
  stdcxx::shared_ptr<TSSLSocket> tSSLSocket;
  tSSLSocket = factory_->createSocket(client);
  tSSLSocket->setLibeventSafe();
  return tSSLSocket;
}
}
}
}
