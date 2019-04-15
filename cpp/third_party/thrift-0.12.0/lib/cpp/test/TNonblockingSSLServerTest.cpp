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

#define BOOST_TEST_MODULE TNonblockingSSLServerTest
#include <boost/test/unit_test.hpp>
#include <boost/smart_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/filesystem.hpp>
#include <boost/format.hpp>

#include "thrift/server/TNonblockingServer.h"
#include "thrift/transport/TSSLSocket.h"
#include "thrift/transport/TNonblockingSSLServerSocket.h"

#include "gen-cpp/ParentService.h"

#include <event.h>

using namespace apache::thrift;
using apache::thrift::concurrency::Guard;
using apache::thrift::concurrency::Monitor;
using apache::thrift::concurrency::Mutex;
using apache::thrift::server::TServerEventHandler;
using apache::thrift::transport::TSSLSocketFactory;
using apache::thrift::transport::TSSLSocket;

struct Handler : public test::ParentServiceIf {
  void addString(const std::string& s) { strings_.push_back(s); }
  void getStrings(std::vector<std::string>& _return) { _return = strings_; }
  std::vector<std::string> strings_;

  // dummy overrides not used in this test
  int32_t incrementGeneration() { return 0; }
  int32_t getGeneration() { return 0; }
  void getDataWait(std::string&, const int32_t) {}
  void onewayWait() {}
  void exceptionWait(const std::string&) {}
  void unexpectedExceptionWait(const std::string&) {}
};

boost::filesystem::path keyDir;
boost::filesystem::path certFile(const std::string& filename)
{
  return keyDir / filename;
}

struct GlobalFixtureSSL
{
    GlobalFixtureSSL()
    {
      using namespace boost::unit_test::framework;
      for (int i = 0; i < master_test_suite().argc; ++i)
      {
        BOOST_TEST_MESSAGE(boost::format("argv[%1%] = \"%2%\"") % i % master_test_suite().argv[i]);
      }

#ifdef __linux__
      // OpenSSL calls send() without MSG_NOSIGPIPE so writing to a socket that has
      // disconnected can cause a SIGPIPE signal...
      signal(SIGPIPE, SIG_IGN);
#endif

      TSSLSocketFactory::setManualOpenSSLInitialization(true);
      apache::thrift::transport::initializeOpenSSL();

      keyDir = boost::filesystem::current_path().parent_path().parent_path().parent_path() / "test" / "keys";
      if (!boost::filesystem::exists(certFile("server.crt")))
      {
        keyDir = boost::filesystem::path(master_test_suite().argv[master_test_suite().argc - 1]);
        if (!boost::filesystem::exists(certFile("server.crt")))
        {
          throw std::invalid_argument("The last argument to this test must be the directory containing the test certificate(s).");
        }
      }
    }

    virtual ~GlobalFixtureSSL()
    {
      apache::thrift::transport::cleanupOpenSSL();
#ifdef __linux__
      signal(SIGPIPE, SIG_DFL);
#endif
    }
};

#if (BOOST_VERSION >= 105900)
BOOST_GLOBAL_FIXTURE(GlobalFixtureSSL);
#else
BOOST_GLOBAL_FIXTURE(GlobalFixtureSSL)
#endif

stdcxx::shared_ptr<TSSLSocketFactory> createServerSocketFactory() {
  stdcxx::shared_ptr<TSSLSocketFactory> pServerSocketFactory;

  pServerSocketFactory.reset(new TSSLSocketFactory());
  pServerSocketFactory->ciphers("ALL:!ADH:!LOW:!EXP:!MD5:@STRENGTH");
  pServerSocketFactory->loadCertificate(certFile("server.crt").string().c_str());
  pServerSocketFactory->loadPrivateKey(certFile("server.key").string().c_str());
  pServerSocketFactory->server(true);
  return pServerSocketFactory;
}

stdcxx::shared_ptr<TSSLSocketFactory> createClientSocketFactory() {
  stdcxx::shared_ptr<TSSLSocketFactory> pClientSocketFactory;

  pClientSocketFactory.reset(new TSSLSocketFactory());
  pClientSocketFactory->authenticate(true);
  pClientSocketFactory->loadCertificate(certFile("client.crt").string().c_str());
  pClientSocketFactory->loadPrivateKey(certFile("client.key").string().c_str());
  pClientSocketFactory->loadTrustedCertificates(certFile("CA.pem").string().c_str());
  return pClientSocketFactory;
}

class Fixture {
private:
  struct ListenEventHandler : public TServerEventHandler {
    public:
      ListenEventHandler(Mutex* mutex) : listenMonitor_(mutex), ready_(false) {}

      void preServe() /* override */ {
        Guard g(listenMonitor_.mutex());
        ready_ = true;
        listenMonitor_.notify();
      }

      Monitor listenMonitor_;
      bool ready_;
  };

  struct Runner : public apache::thrift::concurrency::Runnable {
    int port;
    stdcxx::shared_ptr<event_base> userEventBase;
    stdcxx::shared_ptr<TProcessor> processor;
    stdcxx::shared_ptr<server::TNonblockingServer> server;
    stdcxx::shared_ptr<ListenEventHandler> listenHandler;
    stdcxx::shared_ptr<TSSLSocketFactory> pServerSocketFactory;
    stdcxx::shared_ptr<transport::TNonblockingSSLServerSocket> socket;
    Mutex mutex_;

    Runner() {
      listenHandler.reset(new ListenEventHandler(&mutex_));
    }

    virtual void run() {
      // When binding to explicit port, allow retrying to workaround bind failures on ports in use
      int retryCount = port ? 10 : 0;
      pServerSocketFactory = createServerSocketFactory();  
      startServer(retryCount);
    }

    void readyBarrier() {
      // block until server is listening and ready to accept connections
      Guard g(mutex_);
      while (!listenHandler->ready_) {
        listenHandler->listenMonitor_.wait();
      }
    }
  private:
    void startServer(int retry_count) {
      try {
        socket.reset(new transport::TNonblockingSSLServerSocket(port, pServerSocketFactory));
        server.reset(new server::TNonblockingServer(processor, socket));
	      server->setServerEventHandler(listenHandler);
        server->setNumIOThreads(1);
        if (userEventBase) {
          server->registerEvents(userEventBase.get());
        }
        server->serve();
      } catch (const transport::TTransportException&) {
        if (retry_count > 0) {
          ++port;
          startServer(retry_count - 1);
        } else {
          throw;
        }
      }
    }
  };

  struct EventDeleter {
    void operator()(event_base* p) { event_base_free(p); }
  };

protected:
  Fixture() : processor(new test::ParentServiceProcessor(stdcxx::make_shared<Handler>())) {}

  ~Fixture() {
    if (server) {
      server->stop();
    }
    if (thread) {
      thread->join();
    }
  }

  void setEventBase(event_base* user_event_base) {
    userEventBase_.reset(user_event_base, EventDeleter());
  }

  int startServer(int port) {
    stdcxx::shared_ptr<Runner> runner(new Runner);
    runner->port = port;
    runner->processor = processor;
    runner->userEventBase = userEventBase_;

    apache::thrift::stdcxx::scoped_ptr<apache::thrift::concurrency::ThreadFactory> threadFactory(
        new apache::thrift::concurrency::PlatformThreadFactory(
#if !USE_BOOST_THREAD && !USE_STD_THREAD
            concurrency::PlatformThreadFactory::OTHER, concurrency::PlatformThreadFactory::NORMAL,
            1,
#endif
            false));
    thread = threadFactory->newThread(runner);
    thread->start();
    runner->readyBarrier();

    server = runner->server;
    return runner->port;
  }

  bool canCommunicate(int serverPort) {
    stdcxx::shared_ptr<TSSLSocketFactory> pClientSocketFactory = createClientSocketFactory();
    stdcxx::shared_ptr<TSSLSocket> socket = pClientSocketFactory->createSocket("localhost", serverPort);
    socket->open();
    test::ParentServiceClient client(stdcxx::make_shared<protocol::TBinaryProtocol>(
        stdcxx::make_shared<transport::TFramedTransport>(socket)));
    client.addString("foo");
    std::vector<std::string> strings;
    client.getStrings(strings);
    return strings.size() == 1 && !(strings[0].compare("foo"));
  }

private:
  stdcxx::shared_ptr<event_base> userEventBase_;
  stdcxx::shared_ptr<test::ParentServiceProcessor> processor;
protected:
  stdcxx::shared_ptr<server::TNonblockingServer> server;
private:
  stdcxx::shared_ptr<apache::thrift::concurrency::Thread> thread;

};

BOOST_AUTO_TEST_SUITE(TNonblockingSSLServerTest)

BOOST_FIXTURE_TEST_CASE(get_specified_port, Fixture) {
  int specified_port = startServer(12345);
  BOOST_REQUIRE_GE(specified_port, 12345);
  BOOST_REQUIRE_EQUAL(server->getListenPort(), specified_port);
  BOOST_CHECK(canCommunicate(specified_port));

  server->stop();
}

BOOST_FIXTURE_TEST_CASE(get_assigned_port, Fixture) {
  int specified_port = startServer(0);
  BOOST_REQUIRE_EQUAL(specified_port, 0);
  int assigned_port = server->getListenPort();
  BOOST_REQUIRE_NE(assigned_port, 0);
  BOOST_CHECK(canCommunicate(assigned_port));

  server->stop();
}

BOOST_FIXTURE_TEST_CASE(provide_event_base, Fixture) {
  event_base* eb = event_base_new();
  setEventBase(eb);
  startServer(0);

  // assert that the server works
  BOOST_CHECK(canCommunicate(server->getListenPort()));
#if LIBEVENT_VERSION_NUMBER > 0x02010400
  // also assert that the event_base is actually used when it's easy
  BOOST_CHECK_GT(event_base_get_num_events(eb, EVENT_BASE_COUNT_ADDED), 0);
#endif
}

BOOST_AUTO_TEST_SUITE_END()
