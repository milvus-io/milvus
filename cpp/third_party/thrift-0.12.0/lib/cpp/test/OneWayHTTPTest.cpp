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

#include <boost/test/auto_unit_test.hpp>
#include <boost/thread.hpp>
#include <iostream>
#include <climits>
#include <vector>
#include <thrift/concurrency/Monitor.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TJSONProtocol.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/THttpServer.h>
#include <thrift/transport/THttpClient.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TSocket.h>
#include <thrift/stdcxx.h>
#include <thrift/transport/TBufferTransports.h>
#include "gen-cpp/OneWayService.h"

BOOST_AUTO_TEST_SUITE(OneWayHTTPTest)

using namespace apache::thrift;
using apache::thrift::protocol::TProtocol;
using apache::thrift::protocol::TBinaryProtocol;
using apache::thrift::protocol::TBinaryProtocolFactory;
using apache::thrift::protocol::TJSONProtocol;
using apache::thrift::protocol::TJSONProtocolFactory;
using apache::thrift::server::TThreadedServer;
using apache::thrift::server::TServerEventHandler;
using apache::thrift::transport::TTransport;
using apache::thrift::transport::THttpServer;
using apache::thrift::transport::THttpServerTransportFactory;
using apache::thrift::transport::THttpClient;
using apache::thrift::transport::TBufferedTransport;
using apache::thrift::transport::TBufferedTransportFactory;
using apache::thrift::transport::TMemoryBuffer;
using apache::thrift::transport::TServerSocket;
using apache::thrift::transport::TSocket;
using apache::thrift::transport::TTransportException;
using apache::thrift::stdcxx::shared_ptr;
using std::cout;
using std::cerr;
using std::endl;
using std::string;
namespace utf = boost::unit_test;

// Define this env var to enable some logging (in case you need to debug)
#undef ENABLE_STDERR_LOGGING

class OneWayServiceHandler : public onewaytest::OneWayServiceIf {
public:
  OneWayServiceHandler() {}

  void roundTripRPC() override {
#ifdef ENABLE_STDERR_LOGGING
    cerr << "roundTripRPC()" << endl;
#endif
  }
  void oneWayRPC() {
#ifdef ENABLE_STDERR_LOGGING
    cerr << "oneWayRPC()" << std::endl ;
#endif
 }
};

class OneWayServiceCloneFactory : virtual public onewaytest::OneWayServiceIfFactory {
 public:
  virtual ~OneWayServiceCloneFactory() {}
  virtual onewaytest::OneWayServiceIf* getHandler(const ::apache::thrift::TConnectionInfo& connInfo)
  {
    (void)connInfo ;
    return new OneWayServiceHandler;
  }
  virtual void releaseHandler( onewaytest::OneWayServiceIf* handler) {
    delete handler;
  }
};

class RPC0ThreadClass {
public:
  RPC0ThreadClass(TThreadedServer& server) : server_(server) { } // Constructor
~RPC0ThreadClass() { } // Destructor

void Run() {
  server_.serve() ;
}
 TThreadedServer& server_ ;
} ;

using apache::thrift::concurrency::Monitor;
using apache::thrift::concurrency::Mutex;
using apache::thrift::concurrency::Synchronized;

// copied from IntegrationTest
class TServerReadyEventHandler : public TServerEventHandler, public Monitor {
public:
  TServerReadyEventHandler() : isListening_(false), accepted_(0) {}
  virtual ~TServerReadyEventHandler() {}
  virtual void preServe() {
    Synchronized sync(*this);
    isListening_ = true;
    notify();
  }
  virtual void* createContext(shared_ptr<TProtocol> input,
                              shared_ptr<TProtocol> output) {
    Synchronized sync(*this);
    ++accepted_;
    notify();

    (void)input;
    (void)output;
    return NULL;
  }
  bool isListening() const { return isListening_; }
  uint64_t acceptedCount() const { return accepted_; }

private:
  bool isListening_;
  uint64_t accepted_;
};

class TBlockableBufferedTransport : public TBufferedTransport {
 public:
  TBlockableBufferedTransport(stdcxx::shared_ptr<TTransport> transport)
    : TBufferedTransport(transport, 10240),
    blocked_(false) {
  }

  uint32_t write_buffer_length() {
    uint32_t have_bytes = static_cast<uint32_t>(wBase_ - wBuf_.get());
    return have_bytes ;
  }

  void block() {
    blocked_ = true ;
#ifdef ENABLE_STDERR_LOGGING
    cerr << "block flushing\n" ;
#endif
 }
  void unblock() {
    blocked_ = false ;
#ifdef ENABLE_STDERR_LOGGING
    cerr << "unblock flushing, buffer is\n<<" << std::string((char *)wBuf_.get(), write_buffer_length()) << ">>\n" ;
#endif
 }

  void flush() override {
    if (blocked_) {
#ifdef ENABLE_STDERR_LOGGING
      cerr << "flush was blocked\n" ;
#endif
      return ;
    }
    TBufferedTransport::flush() ;
  }

  bool blocked_ ;
} ;

BOOST_AUTO_TEST_CASE( JSON_BufferedHTTP )
{
  stdcxx::shared_ptr<TServerSocket> ss = stdcxx::make_shared<TServerSocket>(0) ;
  TThreadedServer server(
    stdcxx::make_shared<onewaytest::OneWayServiceProcessorFactory>(stdcxx::make_shared<OneWayServiceCloneFactory>()),
    ss, //port
    stdcxx::make_shared<THttpServerTransportFactory>(),
    stdcxx::make_shared<TJSONProtocolFactory>());

  stdcxx::shared_ptr<TServerReadyEventHandler> pEventHandler(new TServerReadyEventHandler) ;
  server.setServerEventHandler(pEventHandler);

#ifdef ENABLE_STDERR_LOGGING
  cerr << "Starting the server...\n";
#endif
  RPC0ThreadClass t(server) ;
  boost::thread thread(&RPC0ThreadClass::Run, &t);

  {
    Synchronized sync(*(pEventHandler.get()));
    while (!pEventHandler->isListening()) {
      pEventHandler->wait();
    }
  }

  int port = ss->getPort() ;
#ifdef ENABLE_STDERR_LOGGING
  cerr << "port " << port << endl ;
#endif

  {
    stdcxx::shared_ptr<TSocket> socket(new TSocket("localhost", port));
    socket->setRecvTimeout(10000) ; // 1000msec should be enough
    stdcxx::shared_ptr<TBlockableBufferedTransport> blockable_transport(new TBlockableBufferedTransport(socket));
    stdcxx::shared_ptr<TTransport> transport(new THttpClient(blockable_transport, "localhost", "/service"));
    stdcxx::shared_ptr<TProtocol> protocol(new TJSONProtocol(transport));
    onewaytest::OneWayServiceClient client(protocol);


    transport->open();
    client.roundTripRPC();
    blockable_transport->block() ;
    uint32_t size0 = blockable_transport->write_buffer_length() ;
    client.send_oneWayRPC() ;
    uint32_t size1 = blockable_transport->write_buffer_length() ;
    client.send_oneWayRPC() ;
    uint32_t size2 = blockable_transport->write_buffer_length() ;
    BOOST_CHECK((size1 - size0) == (size2 - size1)) ;
    blockable_transport->unblock() ;
    client.send_roundTripRPC() ;
    blockable_transport->flush() ;
    try {
      client.recv_roundTripRPC() ;
    } catch (TTransportException e) {
      BOOST_ERROR( "we should not get a transport exception -- this means we failed: " + std::string(e.what()) ) ;
    }
    transport->close();
  }
  server.stop();
  thread.join() ;
#ifdef ENABLE_STDERR_LOGGING
  cerr << "finished.\n";
#endif
}

BOOST_AUTO_TEST_SUITE_END()
