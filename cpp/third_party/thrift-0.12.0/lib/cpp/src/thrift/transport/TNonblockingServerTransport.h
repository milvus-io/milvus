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

#ifndef _THRIFT_TRANSPORT_TNONBLOCKINGSERVERTRANSPORT_H_
#define _THRIFT_TRANSPORT_TNONBLOCKINGSERVERTRANSPORT_H_ 1

#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportException.h>
#include <thrift/stdcxx.h>

namespace apache {
namespace thrift {
namespace transport {

/**
 * Server transport framework. A server needs to have some facility for
 * creating base transports to read/write from.  The server is expected
 * to keep track of TTransport children that it creates for purposes of
 * controlling their lifetime.
 */
class TNonblockingServerTransport {
public:
  virtual ~TNonblockingServerTransport() {}

  /**
   * Starts the server transport listening for new connections. Prior to this
   * call most transports will not return anything when accept is called.
   *
   * @throws TTransportException if we were unable to listen
   */
  virtual void listen() {}

  /**
   * Gets a new dynamically allocated transport object and passes it to the
   * caller. Note that it is the explicit duty of the caller to free the
   * allocated object. The returned TTransport object must always be in the
   * opened state. NULL should never be returned, instead an Exception should
   * always be thrown.
   *
   * @return A new TTransport object
   * @throws TTransportException if there is an error
   */
  stdcxx::shared_ptr<TSocket> accept() {
    stdcxx::shared_ptr<TSocket> result = acceptImpl();
    if (!result) {
      throw TTransportException("accept() may not return NULL");
    }
    return result;
  }

  /**
  * Utility method
  * 
  * @return server socket file descriptor 
  * @throw TTransportException If an error occurs
  */

  virtual THRIFT_SOCKET getSocketFD() = 0;

  virtual int getPort() = 0;

  virtual int getListenPort() = 0;

  /**
   * Closes this transport such that future calls to accept will do nothing.
   */
  virtual void close() = 0;

protected:
  TNonblockingServerTransport() {}

  /**
   * Subclasses should implement this function for accept.
   *
   * @return A newly allocated TTransport object
   * @throw TTransportException If an error occurs
   */
  virtual stdcxx::shared_ptr<TSocket> acceptImpl() = 0;

};
}
}
} // apache::thrift::transport

#endif // #ifndef _THRIFT_TRANSPORT_TNONBLOCKINGSERVERTRANSPORT_H_
