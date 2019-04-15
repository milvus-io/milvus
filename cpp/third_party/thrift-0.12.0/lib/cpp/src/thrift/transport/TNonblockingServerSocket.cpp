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

#include <thrift/thrift-config.h>

#include <cstring>
#include <stdexcept>
#include <sys/types.h>
#ifdef HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif
#ifdef HAVE_SYS_UN_H
#include <sys/un.h>
#endif
#ifdef HAVE_SYS_POLL_H
#include <sys/poll.h>
#endif
#ifdef HAVE_NETINET_IN_H
#include <netinet/in.h>
#include <netinet/tcp.h>
#endif
#ifdef HAVE_NETDB_H
#include <netdb.h>
#endif
#include <fcntl.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include <thrift/transport/TSocket.h>
#include <thrift/transport/TNonblockingServerSocket.h>
#include <thrift/transport/PlatformSocket.h>

#ifndef AF_LOCAL
#define AF_LOCAL AF_UNIX
#endif

#ifndef SOCKOPT_CAST_T
#ifndef _WIN32
#define SOCKOPT_CAST_T void
#else
#define SOCKOPT_CAST_T char
#endif // _WIN32
#endif

template <class T>
inline const SOCKOPT_CAST_T* const_cast_sockopt(const T* v) {
  return reinterpret_cast<const SOCKOPT_CAST_T*>(v);
}

template <class T>
inline SOCKOPT_CAST_T* cast_sockopt(T* v) {
  return reinterpret_cast<SOCKOPT_CAST_T*>(v);
}

namespace apache {
namespace thrift {
namespace transport {

using std::string;
using stdcxx::shared_ptr;

TNonblockingServerSocket::TNonblockingServerSocket(int port)
  : port_(port),
    listenPort_(port),
    serverSocket_(THRIFT_INVALID_SOCKET),
    acceptBacklog_(DEFAULT_BACKLOG),
    sendTimeout_(0),
    recvTimeout_(0),
    retryLimit_(0),
    retryDelay_(0),
    tcpSendBuffer_(0),
    tcpRecvBuffer_(0),
    keepAlive_(false),
    listening_(false) {
}

TNonblockingServerSocket::TNonblockingServerSocket(int port, int sendTimeout, int recvTimeout)
  : port_(port),
    listenPort_(port),
    serverSocket_(THRIFT_INVALID_SOCKET),
    acceptBacklog_(DEFAULT_BACKLOG),
    sendTimeout_(sendTimeout),
    recvTimeout_(recvTimeout),
    retryLimit_(0),
    retryDelay_(0),
    tcpSendBuffer_(0),
    tcpRecvBuffer_(0),
    keepAlive_(false),
    listening_(false) {
}

TNonblockingServerSocket::TNonblockingServerSocket(const string& address, int port)
  : port_(port),
    listenPort_(port),
    address_(address),
    serverSocket_(THRIFT_INVALID_SOCKET),
    acceptBacklog_(DEFAULT_BACKLOG),
    sendTimeout_(0),
    recvTimeout_(0),
    retryLimit_(0),
    retryDelay_(0),
    tcpSendBuffer_(0),
    tcpRecvBuffer_(0),
    keepAlive_(false),
    listening_(false) {
}

TNonblockingServerSocket::TNonblockingServerSocket(const string& path)
  : port_(0),
    listenPort_(0),
    path_(path),
    serverSocket_(THRIFT_INVALID_SOCKET),
    acceptBacklog_(DEFAULT_BACKLOG),
    sendTimeout_(0),
    recvTimeout_(0),
    retryLimit_(0),
    retryDelay_(0),
    tcpSendBuffer_(0),
    tcpRecvBuffer_(0),
    keepAlive_(false),
    listening_(false) {
}

TNonblockingServerSocket::~TNonblockingServerSocket() {
  close();
}

void TNonblockingServerSocket::setSendTimeout(int sendTimeout) {
  sendTimeout_ = sendTimeout;
}

void TNonblockingServerSocket::setRecvTimeout(int recvTimeout) {
  recvTimeout_ = recvTimeout;
}

void TNonblockingServerSocket::setAcceptBacklog(int accBacklog) {
  acceptBacklog_ = accBacklog;
}

void TNonblockingServerSocket::setRetryLimit(int retryLimit) {
  retryLimit_ = retryLimit;
}

void TNonblockingServerSocket::setRetryDelay(int retryDelay) {
  retryDelay_ = retryDelay;
}

void TNonblockingServerSocket::setTcpSendBuffer(int tcpSendBuffer) {
  tcpSendBuffer_ = tcpSendBuffer;
}

void TNonblockingServerSocket::setTcpRecvBuffer(int tcpRecvBuffer) {
  tcpRecvBuffer_ = tcpRecvBuffer;
}

void TNonblockingServerSocket::listen() {
  listening_ = true;
#ifdef _WIN32
  TWinsockSingleton::create();
#endif // _WIN32
  
  // Validate port number
  if (port_ < 0 || port_ > 0xFFFF) {
    throw TTransportException(TTransportException::BAD_ARGS, "Specified port is invalid");
  }

  const struct addrinfo *res;
  int error;
  char port[sizeof("65535")];
  THRIFT_SNPRINTF(port, sizeof(port), "%d", port_);

  struct addrinfo hints;
  std::memset(&hints, 0, sizeof(hints));
  hints.ai_family = PF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_PASSIVE | AI_ADDRCONFIG;

  // If address is not specified use wildcard address (NULL)
  TGetAddrInfoWrapper info(address_.empty() ? NULL : &address_[0], port, &hints);

  error = info.init();
  if (error) {
    GlobalOutput.printf("getaddrinfo %d: %s", error, THRIFT_GAI_STRERROR(error));
    close();
    throw TTransportException(TTransportException::NOT_OPEN,
                              "Could not resolve host for server socket.");
  }

  // Pick the ipv6 address first since ipv4 addresses can be mapped
  // into ipv6 space.
  for (res = info.res(); res; res = res->ai_next) {
    if (res->ai_family == AF_INET6 || res->ai_next == NULL)
      break;
  }

  if (!path_.empty()) {
    serverSocket_ = socket(PF_UNIX, SOCK_STREAM, IPPROTO_IP);
  } else if (res != NULL) {
    serverSocket_ = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
  }

  if (serverSocket_ == THRIFT_INVALID_SOCKET) {
    int errno_copy = THRIFT_GET_SOCKET_ERROR;
    GlobalOutput.perror("TNonblockingServerSocket::listen() socket() ", errno_copy);
    close();
    throw TTransportException(TTransportException::NOT_OPEN,
                              "Could not create server socket.",
                              errno_copy);
  }

  // Set THRIFT_NO_SOCKET_CACHING to prevent 2MSL delay on accept
  int one = 1;
  if (-1 == setsockopt(serverSocket_,
                       SOL_SOCKET,
                       THRIFT_NO_SOCKET_CACHING,
                       cast_sockopt(&one),
                       sizeof(one))) {
// ignore errors coming out of this setsockopt on Windows.  This is because
// SO_EXCLUSIVEADDRUSE requires admin privileges on WinXP, but we don't
// want to force servers to be an admin.
#ifndef _WIN32
    int errno_copy = THRIFT_GET_SOCKET_ERROR;
    GlobalOutput.perror("TNonblockingServerSocket::listen() setsockopt() THRIFT_NO_SOCKET_CACHING ",
                        errno_copy);
    close();
    throw TTransportException(TTransportException::NOT_OPEN,
                              "Could not set THRIFT_NO_SOCKET_CACHING",
                              errno_copy);
#endif
  }

  // Set TCP buffer sizes
  if (tcpSendBuffer_ > 0) {
    if (-1 == setsockopt(serverSocket_,
                         SOL_SOCKET,
                         SO_SNDBUF,
                         cast_sockopt(&tcpSendBuffer_),
                         sizeof(tcpSendBuffer_))) {
      int errno_copy = THRIFT_GET_SOCKET_ERROR;
      GlobalOutput.perror("TNonblockingServerSocket::listen() setsockopt() SO_SNDBUF ", errno_copy);
      close();
      throw TTransportException(TTransportException::NOT_OPEN,
                                "Could not set SO_SNDBUF",
                                errno_copy);
    }
  }

  if (tcpRecvBuffer_ > 0) {
    if (-1 == setsockopt(serverSocket_,
                         SOL_SOCKET,
                         SO_RCVBUF,
                         cast_sockopt(&tcpRecvBuffer_),
                         sizeof(tcpRecvBuffer_))) {
      int errno_copy = THRIFT_GET_SOCKET_ERROR;
      GlobalOutput.perror("TNonblockingServerSocket::listen() setsockopt() SO_RCVBUF ", errno_copy);
      close();
      throw TTransportException(TTransportException::NOT_OPEN,
                                "Could not set SO_RCVBUF",
                                errno_copy);
    }
  }

#ifdef IPV6_V6ONLY
  if (res->ai_family == AF_INET6 && path_.empty()) {
    int zero = 0;
    if (-1 == setsockopt(serverSocket_,
                         IPPROTO_IPV6,
                         IPV6_V6ONLY,
                         cast_sockopt(&zero),
                         sizeof(zero))) {
      GlobalOutput.perror("TNonblockingServerSocket::listen() IPV6_V6ONLY ", THRIFT_GET_SOCKET_ERROR);
    }
  }
#endif // #ifdef IPV6_V6ONLY

  // Turn linger off, don't want to block on calls to close
  struct linger ling = {0, 0};
  if (-1 == setsockopt(serverSocket_, SOL_SOCKET, SO_LINGER, cast_sockopt(&ling), sizeof(ling))) {
    int errno_copy = THRIFT_GET_SOCKET_ERROR;
    GlobalOutput.perror("TNonblockingServerSocket::listen() setsockopt() SO_LINGER ", errno_copy);
    close();
    throw TTransportException(TTransportException::NOT_OPEN, "Could not set SO_LINGER", errno_copy);
  }

  // Keepalive to ensure full result flushing
  if (-1 == setsockopt(serverSocket_, SOL_SOCKET, SO_KEEPALIVE, const_cast_sockopt(&one), sizeof(one))) {
    int errno_copy = THRIFT_GET_SOCKET_ERROR;
    GlobalOutput.perror("TNonblockingServerSocket::listen() setsockopt() SO_KEEPALIVE ", errno_copy);
    close();
    throw TTransportException(TTransportException::NOT_OPEN,
      "Could not set TCP_NODELAY",
      errno_copy);
  }

  // Set TCP nodelay if available, MAC OS X Hack
  // See http://lists.danga.com/pipermail/memcached/2005-March/001240.html
#ifndef TCP_NOPUSH
  // Unix Sockets do not need that
  if (path_.empty()) {
    // TCP Nodelay, speed over bandwidth
    if (-1
        == setsockopt(serverSocket_, IPPROTO_TCP, TCP_NODELAY, cast_sockopt(&one), sizeof(one))) {
      int errno_copy = THRIFT_GET_SOCKET_ERROR;
      GlobalOutput.perror("TNonblockingServerSocket::listen() setsockopt() TCP_NODELAY ", errno_copy);
      close();
      throw TTransportException(TTransportException::NOT_OPEN,
                                "Could not set TCP_NODELAY",
                                errno_copy);
    }
  }
#endif

  // Set NONBLOCK on the accept socket
  int flags = THRIFT_FCNTL(serverSocket_, THRIFT_F_GETFL, 0);
  if (flags == -1) {
    int errno_copy = THRIFT_GET_SOCKET_ERROR;
    GlobalOutput.perror("TNonblockingServerSocket::listen() THRIFT_FCNTL() THRIFT_F_GETFL ", errno_copy);
    close();
    throw TTransportException(TTransportException::NOT_OPEN,
                              "THRIFT_FCNTL() THRIFT_F_GETFL failed",
                              errno_copy);
  }

  if (-1 == THRIFT_FCNTL(serverSocket_, THRIFT_F_SETFL, flags | THRIFT_O_NONBLOCK)) {
    int errno_copy = THRIFT_GET_SOCKET_ERROR;
    GlobalOutput.perror("TNonblockingServerSocket::listen() THRIFT_FCNTL() THRIFT_O_NONBLOCK ", errno_copy);
    close();
    throw TTransportException(TTransportException::NOT_OPEN,
                              "THRIFT_FCNTL() THRIFT_F_SETFL THRIFT_O_NONBLOCK failed",
                              errno_copy);
  }

#ifdef TCP_LOW_MIN_RTO
  if (TSocket::getUseLowMinRto()) {
    if (-1 == setsockopt(s, IPPROTO_TCP, TCP_LOW_MIN_RTO, const_cast_sockopt(&one), sizeof(one))) {
      int errno_copy = THRIFT_GET_SOCKET_ERROR;
      GlobalOutput.perror("TNonblockingServerSocket::listen() setsockopt() TCP_LOW_MIN_RTO ", errno_copy);
      close();
      throw TTransportException(TTransportException::NOT_OPEN,
        "Could not set TCP_NODELAY",
        errno_copy);
    }
  }
#endif

  // prepare the port information
  // we may want to try to bind more than once, since THRIFT_NO_SOCKET_CACHING doesn't
  // always seem to work. The client can configure the retry variables.
  int retries = 0;
  int errno_copy = 0;

  if (!path_.empty()) {

#ifndef _WIN32

    // Unix Domain Socket
    size_t len = path_.size() + 1;
    if (len > sizeof(((sockaddr_un*)NULL)->sun_path)) {
      errno_copy = THRIFT_GET_SOCKET_ERROR;
      GlobalOutput.perror("TSocket::listen() Unix Domain socket path too long", errno_copy);
      throw TTransportException(TTransportException::NOT_OPEN,
                                "Unix Domain socket path too long",
                                errno_copy);
    }

    struct sockaddr_un address;
    address.sun_family = AF_UNIX;
    memcpy(address.sun_path, path_.c_str(), len);

    socklen_t structlen = static_cast<socklen_t>(sizeof(address));

    if (!address.sun_path[0]) { // abstract namespace socket
#ifdef __linux__
      // sun_path is not null-terminated in this case and structlen determines its length
      structlen -= sizeof(address.sun_path) - len;
#else
      GlobalOutput.perror("TSocket::open() Abstract Namespace Domain sockets only supported on linux: ", -99);
      throw TTransportException(TTransportException::NOT_OPEN,
                                " Abstract Namespace Domain socket path not supported");
#endif
    }

    do {
      if (0 == ::bind(serverSocket_, (struct sockaddr*)&address, structlen)) {
        break;
      }
      errno_copy = THRIFT_GET_SOCKET_ERROR;
      // use short circuit evaluation here to only sleep if we need to
    } while ((retries++ < retryLimit_) && (THRIFT_SLEEP_SEC(retryDelay_) == 0));
#else
    GlobalOutput.perror("TSocket::open() Unix Domain socket path not supported on windows", -99);
    throw TTransportException(TTransportException::NOT_OPEN,
                              " Unix Domain socket path not supported");
#endif
  } else {
    do {
      if (0 == ::bind(serverSocket_, res->ai_addr, static_cast<int>(res->ai_addrlen))) {
        break;
      }
      errno_copy = THRIFT_GET_SOCKET_ERROR;
      // use short circuit evaluation here to only sleep if we need to
    } while ((retries++ < retryLimit_) && (THRIFT_SLEEP_SEC(retryDelay_) == 0));

    // retrieve bind info
    if (port_ == 0 && retries <= retryLimit_) {
      struct sockaddr_storage sa;
      socklen_t len = sizeof(sa);
      std::memset(&sa, 0, len);
      if (::getsockname(serverSocket_, reinterpret_cast<struct sockaddr*>(&sa), &len) < 0) {
        errno_copy = THRIFT_GET_SOCKET_ERROR;
        GlobalOutput.perror("TNonblockingServerSocket::getPort() getsockname() ", errno_copy);
      } else {
        if (sa.ss_family == AF_INET6) {
          const struct sockaddr_in6* sin = reinterpret_cast<const struct sockaddr_in6*>(&sa);
          listenPort_ = ntohs(sin->sin6_port);
        } else {
          const struct sockaddr_in* sin = reinterpret_cast<const struct sockaddr_in*>(&sa);
          listenPort_ = ntohs(sin->sin_port);
        }
      }
    }
  }

  // throw an error if we failed to bind properly
  if (retries > retryLimit_) {
    char errbuf[1024];
    if (!path_.empty()) {
      THRIFT_SNPRINTF(errbuf, sizeof(errbuf), "TNonblockingServerSocket::listen() PATH %s", path_.c_str());
    } else {
      THRIFT_SNPRINTF(errbuf, sizeof(errbuf), "TNonblockingServerSocket::listen() BIND %d", port_);
    }
    GlobalOutput(errbuf);
    close();
    throw TTransportException(TTransportException::NOT_OPEN,
                              "Could not bind",
                              errno_copy);
  }

  if (listenCallback_)
    listenCallback_(serverSocket_);

  // Call listen
  if (-1 == ::listen(serverSocket_, acceptBacklog_)) {
    errno_copy = THRIFT_GET_SOCKET_ERROR;
    GlobalOutput.perror("TNonblockingServerSocket::listen() listen() ", errno_copy);
    close();
    throw TTransportException(TTransportException::NOT_OPEN, "Could not listen", errno_copy);
  }

  // The socket is now listening!
}

int TNonblockingServerSocket::getPort() {
  return port_;
}

int TNonblockingServerSocket::getListenPort() {
  return listenPort_;
}

shared_ptr<TSocket> TNonblockingServerSocket::acceptImpl() {
  if (serverSocket_ == THRIFT_INVALID_SOCKET) {
    throw TTransportException(TTransportException::NOT_OPEN, "TNonblockingServerSocket not listening");
  }
  
  struct sockaddr_storage clientAddress;
  int size = sizeof(clientAddress);
  THRIFT_SOCKET clientSocket
      = ::accept(serverSocket_, (struct sockaddr*)&clientAddress, (socklen_t*)&size);

  if (clientSocket == THRIFT_INVALID_SOCKET) {
    int errno_copy = THRIFT_GET_SOCKET_ERROR;
    GlobalOutput.perror("TNonblockingServerSocket::acceptImpl() ::accept() ", errno_copy);
    throw TTransportException(TTransportException::UNKNOWN, "accept()", errno_copy);
  }

  // Explicitly set this socket to NONBLOCK mode
  int flags = THRIFT_FCNTL(clientSocket, THRIFT_F_GETFL, 0);
  if (flags == -1) {
    int errno_copy = THRIFT_GET_SOCKET_ERROR;
    ::THRIFT_CLOSESOCKET(clientSocket);
    GlobalOutput.perror("TNonblockingServerSocket::acceptImpl() THRIFT_FCNTL() THRIFT_F_GETFL ", errno_copy);
    throw TTransportException(TTransportException::UNKNOWN,
                              "THRIFT_FCNTL(THRIFT_F_GETFL)",
                              errno_copy);
  }

  if (-1 == THRIFT_FCNTL(clientSocket, THRIFT_F_SETFL, flags | THRIFT_O_NONBLOCK)) {
    int errno_copy = THRIFT_GET_SOCKET_ERROR;
    ::THRIFT_CLOSESOCKET(clientSocket);
    GlobalOutput
        .perror("TNonblockingServerSocket::acceptImpl() THRIFT_FCNTL() THRIFT_F_SETFL ~THRIFT_O_NONBLOCK ",
                errno_copy);
    throw TTransportException(TTransportException::UNKNOWN,
                              "THRIFT_FCNTL(THRIFT_F_SETFL)",
                              errno_copy);
  }

  shared_ptr<TSocket> client = createSocket(clientSocket);
  if (sendTimeout_ > 0) {
    client->setSendTimeout(sendTimeout_);
  }
  if (recvTimeout_ > 0) {
    client->setRecvTimeout(recvTimeout_);
  }
  if (keepAlive_) {
    client->setKeepAlive(keepAlive_);
  }
  client->setCachedAddress((sockaddr*)&clientAddress, size);

  if (acceptCallback_)
    acceptCallback_(clientSocket);

  return client;
}

shared_ptr<TSocket> TNonblockingServerSocket::createSocket(THRIFT_SOCKET clientSocket) {
  return shared_ptr<TSocket>(new TSocket(clientSocket));
}

void TNonblockingServerSocket::close() {
  if (serverSocket_ != THRIFT_INVALID_SOCKET) {
    shutdown(serverSocket_, THRIFT_SHUT_RDWR);
    ::THRIFT_CLOSESOCKET(serverSocket_);
  }
  serverSocket_ = THRIFT_INVALID_SOCKET;
  listening_ = false;
}
}
}
} // apache::thrift::transport
