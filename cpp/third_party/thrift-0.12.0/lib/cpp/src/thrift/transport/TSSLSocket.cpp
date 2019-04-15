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

#include <errno.h>
#include <string>
#include <cstring>
#ifdef HAVE_ARPA_INET_H
#include <arpa/inet.h>
#endif
#include <sys/types.h>
#ifdef HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif
#ifdef HAVE_SYS_POLL_H
#include <sys/poll.h>
#endif
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif

#define OPENSSL_VERSION_NO_THREAD_ID_BEFORE    0x10000000L
#define OPENSSL_ENGINE_CLEANUP_REQUIRED_BEFORE 0x10100000L

#include <boost/shared_array.hpp>
#include <openssl/opensslv.h>
#if (OPENSSL_VERSION_NUMBER < OPENSSL_ENGINE_CLEANUP_REQUIRED_BEFORE)
#include <openssl/engine.h>
#endif
#include <openssl/err.h>
#include <openssl/rand.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>
#include <thrift/concurrency/Mutex.h>
#include <thrift/transport/TSSLSocket.h>
#include <thrift/transport/PlatformSocket.h>
#include <thrift/TToString.h>

using namespace apache::thrift::concurrency;
using std::string;

struct CRYPTO_dynlock_value {
  Mutex mutex;
};

namespace apache {
namespace thrift {
namespace transport {

// OpenSSL initialization/cleanup

static bool openSSLInitialized = false;
static boost::shared_array<Mutex> mutexes;

static void callbackLocking(int mode, int n, const char*, int) {
  if (mode & CRYPTO_LOCK) {
    // assertion of (px != 0) here typically means that a TSSLSocket's lifetime
    // exceeded the lifetime of the TSSLSocketFactory that created it, and the
    // TSSLSocketFactory already ran cleanupOpenSSL(), which deleted "mutexes".
    mutexes[n].lock();
  } else {
    mutexes[n].unlock();
  }
}

#if (OPENSSL_VERSION_NUMBER < OPENSSL_VERSION_NO_THREAD_ID_BEFORE)
static unsigned long callbackThreadID() {
#ifdef _WIN32
  return (unsigned long)GetCurrentThreadId();
#else
  return (unsigned long)pthread_self();
#endif
}
#endif

static CRYPTO_dynlock_value* dyn_create(const char*, int) {
  return new CRYPTO_dynlock_value;
}

static void dyn_lock(int mode, struct CRYPTO_dynlock_value* lock, const char*, int) {
  if (lock != NULL) {
    if (mode & CRYPTO_LOCK) {
      lock->mutex.lock();
    } else {
      lock->mutex.unlock();
    }
  }
}

static void dyn_destroy(struct CRYPTO_dynlock_value* lock, const char*, int) {
  delete lock;
}

void initializeOpenSSL() {
  if (openSSLInitialized) {
    return;
  }
  openSSLInitialized = true;
  SSL_library_init();
  SSL_load_error_strings();
  ERR_load_crypto_strings();

  // static locking
  // newer versions of OpenSSL changed CRYPTO_num_locks - see THRIFT-3878
#ifdef CRYPTO_num_locks
  mutexes = boost::shared_array<Mutex>(new Mutex[CRYPTO_num_locks()]);
#else
  mutexes = boost::shared_array<Mutex>(new Mutex[ ::CRYPTO_num_locks()]);
#endif

#if (OPENSSL_VERSION_NUMBER < OPENSSL_VERSION_NO_THREAD_ID_BEFORE)
  CRYPTO_set_id_callback(callbackThreadID);
#endif

  CRYPTO_set_locking_callback(callbackLocking);

  // dynamic locking
  CRYPTO_set_dynlock_create_callback(dyn_create);
  CRYPTO_set_dynlock_lock_callback(dyn_lock);
  CRYPTO_set_dynlock_destroy_callback(dyn_destroy);
}

void cleanupOpenSSL() {
  if (!openSSLInitialized) {
    return;
  }
  openSSLInitialized = false;

  // https://wiki.openssl.org/index.php/Library_Initialization#Cleanup
  // we purposefully do NOT call FIPS_mode_set(0) and leave it up to the enclosing application to manage FIPS entirely
#if (OPENSSL_VERSION_NUMBER < OPENSSL_ENGINE_CLEANUP_REQUIRED_BEFORE)
  ENGINE_cleanup();             // https://www.openssl.org/docs/man1.1.0/crypto/ENGINE_cleanup.html - cleanup call is needed before 1.1.0
#endif
  CONF_modules_unload(1);
  EVP_cleanup();
  CRYPTO_cleanup_all_ex_data();
  ERR_remove_state(0);
  ERR_free_strings();

  mutexes.reset();
}

static void buildErrors(string& message, int errno_copy = 0, int sslerrno = 0);
static bool matchName(const char* host, const char* pattern, int size);
static char uppercase(char c);

// SSLContext implementation
SSLContext::SSLContext(const SSLProtocol& protocol) {
  if (protocol == SSLTLS) {
    ctx_ = SSL_CTX_new(SSLv23_method());
#ifndef OPENSSL_NO_SSL3
  } else if (protocol == SSLv3) {
    ctx_ = SSL_CTX_new(SSLv3_method());
#endif
  } else if (protocol == TLSv1_0) {
    ctx_ = SSL_CTX_new(TLSv1_method());
  } else if (protocol == TLSv1_1) {
    ctx_ = SSL_CTX_new(TLSv1_1_method());
  } else if (protocol == TLSv1_2) {
    ctx_ = SSL_CTX_new(TLSv1_2_method());
  } else {
    /// UNKNOWN PROTOCOL!
    throw TSSLException("SSL_CTX_new: Unknown protocol");
  }

  if (ctx_ == NULL) {
    string errors;
    buildErrors(errors);
    throw TSSLException("SSL_CTX_new: " + errors);
  }
  SSL_CTX_set_mode(ctx_, SSL_MODE_AUTO_RETRY);

  // Disable horribly insecure SSLv2 and SSLv3 protocols but allow a handshake
  // with older clients so they get a graceful denial.
  if (protocol == SSLTLS) {
      SSL_CTX_set_options(ctx_, SSL_OP_NO_SSLv2);
      SSL_CTX_set_options(ctx_, SSL_OP_NO_SSLv3);   // THRIFT-3164
  }
}

SSLContext::~SSLContext() {
  if (ctx_ != NULL) {
    SSL_CTX_free(ctx_);
    ctx_ = NULL;
  }
}

SSL* SSLContext::createSSL() {
  SSL* ssl = SSL_new(ctx_);
  if (ssl == NULL) {
    string errors;
    buildErrors(errors);
    throw TSSLException("SSL_new: " + errors);
  }
  return ssl;
}

// TSSLSocket implementation
TSSLSocket::TSSLSocket(stdcxx::shared_ptr<SSLContext> ctx)
  : TSocket(), server_(false), ssl_(NULL), ctx_(ctx) {
  init();
}

TSSLSocket::TSSLSocket(stdcxx::shared_ptr<SSLContext> ctx, stdcxx::shared_ptr<THRIFT_SOCKET> interruptListener)
        : TSocket(), server_(false), ssl_(NULL), ctx_(ctx) {
  init();
  interruptListener_ = interruptListener;
}

TSSLSocket::TSSLSocket(stdcxx::shared_ptr<SSLContext> ctx, THRIFT_SOCKET socket)
  : TSocket(socket), server_(false), ssl_(NULL), ctx_(ctx) {
  init();
}

TSSLSocket::TSSLSocket(stdcxx::shared_ptr<SSLContext> ctx, THRIFT_SOCKET socket, stdcxx::shared_ptr<THRIFT_SOCKET> interruptListener)
        : TSocket(socket, interruptListener), server_(false), ssl_(NULL), ctx_(ctx) {
  init();
}

TSSLSocket::TSSLSocket(stdcxx::shared_ptr<SSLContext> ctx, string host, int port)
  : TSocket(host, port), server_(false), ssl_(NULL), ctx_(ctx) {
  init();
}

TSSLSocket::TSSLSocket(stdcxx::shared_ptr<SSLContext> ctx, string host, int port, stdcxx::shared_ptr<THRIFT_SOCKET> interruptListener)
        : TSocket(host, port), server_(false), ssl_(NULL), ctx_(ctx) {
  init();
  interruptListener_ = interruptListener;
}

TSSLSocket::~TSSLSocket() {
  close();
}

bool TSSLSocket::hasPendingDataToRead() {
  if (!isOpen()) {
    return false;
  }
  initializeHandshake();
  if (!checkHandshake())
    throw TSSLException("TSSLSocket::hasPendingDataToRead: Handshake is not completed");
  // data may be available in SSL buffers (note: SSL_pending does not have a failure mode)
  return SSL_pending(ssl_) > 0 || TSocket::hasPendingDataToRead();
}

void TSSLSocket::init() {
  handshakeCompleted_ = false;
  readRetryCount_ = 0;
  eventSafe_ = false;
}

bool TSSLSocket::isOpen() {
  if (ssl_ == NULL || !TSocket::isOpen()) {
    return false;
  }
  int shutdown = SSL_get_shutdown(ssl_);
  // "!!" is squelching C4800 "forcing bool -> true or false" performance warning
  bool shutdownReceived = !!(shutdown & SSL_RECEIVED_SHUTDOWN);
  bool shutdownSent = !!(shutdown & SSL_SENT_SHUTDOWN);
  if (shutdownReceived && shutdownSent) {
    return false;
  }
  return true;
}

/*
 * Note: This method is not libevent safe.
*/
bool TSSLSocket::peek() {
  if (!isOpen()) {
    return false;
  }
  initializeHandshake();
  if (!checkHandshake())
    throw TSSLException("SSL_peek: Handshake is not completed");
  int rc;
  uint8_t byte;
  do {
    rc = SSL_peek(ssl_, &byte, 1);
    if (rc < 0) {

      int errno_copy = THRIFT_GET_SOCKET_ERROR;
      int error = SSL_get_error(ssl_, rc);
      switch (error) {
        case SSL_ERROR_SYSCALL:
          if ((errno_copy != THRIFT_EINTR)
              && (errno_copy != THRIFT_EAGAIN)) {
            break;
          }
        // fallthrough
        case SSL_ERROR_WANT_READ:
        case SSL_ERROR_WANT_WRITE:
          // in the case of SSL_ERROR_SYSCALL we want to wait for an read event again
          waitForEvent(error != SSL_ERROR_WANT_WRITE);
          continue;
        default:;// do nothing
      }
      string errors;
      buildErrors(errors, errno_copy, error);
      throw TSSLException("SSL_peek: " + errors);
    } else if (rc == 0) {
      ERR_clear_error();
      break;
    }
  } while (true);
  return (rc > 0);
}

void TSSLSocket::open() {
  if (isOpen() || server()) {
    throw TTransportException(TTransportException::BAD_ARGS);
  }
  TSocket::open();
}

/*
 * Note: This method is not libevent safe.
*/
void TSSLSocket::close() {
  if (ssl_ != NULL) {
    try {
      int rc;
      int errno_copy = 0;
      int error = 0;

      do {
        rc = SSL_shutdown(ssl_);
        if (rc <= 0) {
          errno_copy = THRIFT_GET_SOCKET_ERROR;
          error = SSL_get_error(ssl_, rc);
          switch (error) {
            case SSL_ERROR_SYSCALL:
              if ((errno_copy != THRIFT_EINTR)
                  && (errno_copy != THRIFT_EAGAIN)) {
                break;
              }
            // fallthrough
            case SSL_ERROR_WANT_READ:
            case SSL_ERROR_WANT_WRITE:
              // in the case of SSL_ERROR_SYSCALL we want to wait for an write/read event again
              waitForEvent(error == SSL_ERROR_WANT_READ);
              rc = 2;
            default:;// do nothing
          }
        }
      } while (rc == 2);

      if (rc < 0) {
        string errors;
        buildErrors(errors, errno_copy, error);
        GlobalOutput(("SSL_shutdown: " + errors).c_str());
      }
    } catch (TTransportException& te) {
      // Don't emit an exception because this method is called by the
      // destructor. There's also not much that a user can do to recover, so
      // just clean up as much as possible without throwing, similar to the rc
      // < 0 case above.
      GlobalOutput.printf("SSL_shutdown: %s", te.what());
    }
    SSL_free(ssl_);
    ssl_ = NULL;
    handshakeCompleted_ = false;
    ERR_remove_state(0);
  }
  TSocket::close();
}

/*
 * Returns number of bytes read in SSL Socket.
 * If eventSafe is set, and it may returns 0 bytes then read method
 * needs to be called again until it is successfull or it throws
 * exception incase of failure.
*/
uint32_t TSSLSocket::read(uint8_t* buf, uint32_t len) {
  initializeHandshake();
  if (!checkHandshake())
    throw TTransportException(TTransportException::UNKNOWN, "retry again");
  int32_t bytes = 0;
  while (readRetryCount_ < maxRecvRetries_) {
    bytes = SSL_read(ssl_, buf, len);
    int32_t errno_copy = THRIFT_GET_SOCKET_ERROR;
    int32_t error = SSL_get_error(ssl_, bytes);
    readRetryCount_++;
    if (error == SSL_ERROR_NONE) {
      readRetryCount_ = 0;
      break;
    }
    unsigned int waitEventReturn;
    bool breakout = false;
    switch (error) {
      case SSL_ERROR_ZERO_RETURN:
        throw TTransportException(TTransportException::END_OF_FILE, "client disconnected");

      case SSL_ERROR_SYSCALL:
        if (errno_copy == 0 && ERR_peek_error() == 0) {
          breakout = true;
          break;
        }
        if ((errno_copy != THRIFT_EINTR)
            && (errno_copy != THRIFT_EAGAIN)) {
              break;
        }
        if (readRetryCount_ >= maxRecvRetries_) {
          // THRIFT_EINTR needs to be handled manually and we can tolerate
          // a certain number
          break;
        }
      // fallthrough

      case SSL_ERROR_WANT_READ:
      case SSL_ERROR_WANT_WRITE:
        if (isLibeventSafe()) {
          if (readRetryCount_ < maxRecvRetries_) {
            // THRIFT_EINTR needs to be handled manually and we can tolerate
            // a certain number
            throw TTransportException(TTransportException::UNKNOWN, "retry again");
          }
          throw TTransportException(TTransportException::INTERNAL_ERROR, "too much recv retries");
        }
        // in the case of SSL_ERROR_SYSCALL we want to wait for an read event again
        else if ((waitEventReturn = waitForEvent(error != SSL_ERROR_WANT_WRITE)) == TSSL_EINTR ) {
          // repeat operation
          if (readRetryCount_ < maxRecvRetries_) {
            // THRIFT_EINTR needs to be handled manually and we can tolerate
            // a certain number
            continue;
          }
          throw TTransportException(TTransportException::INTERNAL_ERROR, "too much recv retries");
        }
        else if (waitEventReturn == TSSL_DATA) {
            // in case of SSL and huge thrift packets, there may be a number of
            // socket operations, before any data becomes available by SSL_read().
            // Therefore the number of retries should not be increased and
            // the operation should be repeated.
            readRetryCount_--;
            continue;
        }
        throw TTransportException(TTransportException::INTERNAL_ERROR, "unkown waitForEvent return value");
      default:;// do nothing
    }
    if (breakout) {
      break;
    }
    string errors;
    buildErrors(errors, errno_copy, error);
    throw TSSLException("SSL_read: " + errors);
  }
  return bytes;
}

void TSSLSocket::write(const uint8_t* buf, uint32_t len) {
  initializeHandshake();
  if (!checkHandshake())
    return;
  // loop in case SSL_MODE_ENABLE_PARTIAL_WRITE is set in SSL_CTX.
  uint32_t written = 0;
  while (written < len) {
    ERR_clear_error();
    int32_t bytes = SSL_write(ssl_, &buf[written], len - written);
    if (bytes <= 0) {
      int errno_copy = THRIFT_GET_SOCKET_ERROR;
      int error = SSL_get_error(ssl_, bytes);
      switch (error) {
        case SSL_ERROR_SYSCALL:
          if ((errno_copy != THRIFT_EINTR)
              && (errno_copy != THRIFT_EAGAIN)) {
            break;
          }
        // fallthrough        
        case SSL_ERROR_WANT_READ:
        case SSL_ERROR_WANT_WRITE:
          if (isLibeventSafe()) {
            return;
          }
          else {
            // in the case of SSL_ERROR_SYSCALL we want to wait for an write event again
            waitForEvent(error == SSL_ERROR_WANT_READ);
            continue;
          }
        default:;// do nothing
      }
      string errors;
      buildErrors(errors, errno_copy, error);
      throw TSSLException("SSL_write: " + errors);
    }
    written += bytes;
  }
}

/*
 * Returns number of bytes written in SSL Socket.
 * If eventSafe is set, and it may returns 0 bytes then write method
 * needs to be called again until it is successfull or it throws
 * exception incase of failure.
*/
uint32_t TSSLSocket::write_partial(const uint8_t* buf, uint32_t len) {
  initializeHandshake();
  if (!checkHandshake())
    return 0;
  // loop in case SSL_MODE_ENABLE_PARTIAL_WRITE is set in SSL_CTX.
  uint32_t written = 0;
  while (written < len) {
    ERR_clear_error();
    int32_t bytes = SSL_write(ssl_, &buf[written], len - written);
    if (bytes <= 0) {
      int errno_copy = THRIFT_GET_SOCKET_ERROR;
      int error = SSL_get_error(ssl_, bytes);
      switch (error) {
        case SSL_ERROR_SYSCALL:
          if ((errno_copy != THRIFT_EINTR)
              && (errno_copy != THRIFT_EAGAIN)) {
            break;
          }
        // fallthrough
        case SSL_ERROR_WANT_READ:
        case SSL_ERROR_WANT_WRITE:
          if (isLibeventSafe()) {
            return 0;
          }
          else {
            // in the case of SSL_ERROR_SYSCALL we want to wait for an write event again
            waitForEvent(error == SSL_ERROR_WANT_READ);
            continue;
          }
        default:;// do nothing
      }
      string errors;
      buildErrors(errors, errno_copy, error);
      throw TSSLException("SSL_write: " + errors);
    }
    written += bytes;
  }
  return written;
}

void TSSLSocket::flush() {
  // Don't throw exception if not open. Thrift servers close socket twice.
  if (ssl_ == NULL) {
    return;
  }
  initializeHandshake();
  if (!checkHandshake())
    throw TSSLException("BIO_flush: Handshake is not completed");
  BIO* bio = SSL_get_wbio(ssl_);
  if (bio == NULL) {
    throw TSSLException("SSL_get_wbio returns NULL");
  }
  if (BIO_flush(bio) != 1) {
    int errno_copy = THRIFT_GET_SOCKET_ERROR;
    string errors;
    buildErrors(errors, errno_copy);
    throw TSSLException("BIO_flush: " + errors);
  }
}

void TSSLSocket::initializeHandshakeParams() {
  // set underlying socket to non-blocking
  int flags;
  if ((flags = THRIFT_FCNTL(socket_, THRIFT_F_GETFL, 0)) < 0
      || THRIFT_FCNTL(socket_, THRIFT_F_SETFL, flags | THRIFT_O_NONBLOCK) < 0) {
    GlobalOutput.perror("thriftServerEventHandler: set THRIFT_O_NONBLOCK (THRIFT_FCNTL) ",
                        THRIFT_GET_SOCKET_ERROR);
    ::THRIFT_CLOSESOCKET(socket_);
    return;
  }
  ssl_ = ctx_->createSSL();

  SSL_set_fd(ssl_, static_cast<int>(socket_));
}

bool TSSLSocket::checkHandshake() {
  return handshakeCompleted_;
}

void TSSLSocket::initializeHandshake() {
  if (!TSocket::isOpen()) {
    throw TTransportException(TTransportException::NOT_OPEN);
  }
  if (checkHandshake()) {
    return;
  }

  if (ssl_ == NULL) {
    initializeHandshakeParams();
  }

  int rc;
  int errno_copy = 0;
  int error = 0;
  if (server()) {
    do {
      rc = SSL_accept(ssl_);
      if (rc <= 0) {
        errno_copy = THRIFT_GET_SOCKET_ERROR;
        error = SSL_get_error(ssl_, rc);
        switch (error) {
          case SSL_ERROR_SYSCALL:
            if ((errno_copy != THRIFT_EINTR)
                && (errno_copy != THRIFT_EAGAIN)) {
              break;
            }
          // fallthrough
          case SSL_ERROR_WANT_READ:
          case SSL_ERROR_WANT_WRITE:
            if (isLibeventSafe()) {
              return;
            }
            else {
              // repeat operation
              // in the case of SSL_ERROR_SYSCALL we want to wait for an write/read event again
              waitForEvent(error == SSL_ERROR_WANT_READ);
              rc = 2;
            }
          default:;// do nothing
        }
      }
    } while (rc == 2);
  } else {
    // OpenSSL < 0.9.8f does not have SSL_set_tlsext_host_name()
    #if defined(SSL_set_tlsext_host_name)
      // set the SNI hostname
      SSL_set_tlsext_host_name(ssl_, getHost().c_str());
    #endif
    do {
      rc = SSL_connect(ssl_);
      if (rc <= 0) {
        errno_copy = THRIFT_GET_SOCKET_ERROR;
        error = SSL_get_error(ssl_, rc);
        switch (error) {
          case SSL_ERROR_SYSCALL:
            if ((errno_copy != THRIFT_EINTR)
                && (errno_copy != THRIFT_EAGAIN)) {
              break;
            }
          // fallthrough
          case SSL_ERROR_WANT_READ:
          case SSL_ERROR_WANT_WRITE:
            if (isLibeventSafe()) {
              return;
            }
            else {
              // repeat operation
              // in the case of SSL_ERROR_SYSCALL we want to wait for an write/read event again
              waitForEvent(error == SSL_ERROR_WANT_READ);
              rc = 2;
            }
          default:;// do nothing
        }
      }
    } while (rc == 2);
  }
  if (rc <= 0) {
    string fname(server() ? "SSL_accept" : "SSL_connect");
    string errors;
    buildErrors(errors, errno_copy, error);
    throw TSSLException(fname + ": " + errors);
  }
  authorize();
  handshakeCompleted_ = true;
}

void TSSLSocket::authorize() {
  int rc = SSL_get_verify_result(ssl_);
  if (rc != X509_V_OK) { // verify authentication result
    throw TSSLException(string("SSL_get_verify_result(), ") + X509_verify_cert_error_string(rc));
  }

  X509* cert = SSL_get_peer_certificate(ssl_);
  if (cert == NULL) {
    // certificate is not present
    if (SSL_get_verify_mode(ssl_) & SSL_VERIFY_FAIL_IF_NO_PEER_CERT) {
      throw TSSLException("authorize: required certificate not present");
    }
    // certificate was optional: didn't intend to authorize remote
    if (server() && access_ != NULL) {
      throw TSSLException("authorize: certificate required for authorization");
    }
    return;
  }
  // certificate is present
  if (access_ == NULL) {
    X509_free(cert);
    return;
  }
  // both certificate and access manager are present

  string host;
  sockaddr_storage sa;
  socklen_t saLength = sizeof(sa);

  if (getpeername(socket_, (sockaddr*)&sa, &saLength) != 0) {
    sa.ss_family = AF_UNSPEC;
  }

  AccessManager::Decision decision = access_->verify(sa);

  if (decision != AccessManager::SKIP) {
    X509_free(cert);
    if (decision != AccessManager::ALLOW) {
      throw TSSLException("authorize: access denied based on remote IP");
    }
    return;
  }

  // extract subjectAlternativeName
  STACK_OF(GENERAL_NAME)* alternatives
      = (STACK_OF(GENERAL_NAME)*)X509_get_ext_d2i(cert, NID_subject_alt_name, NULL, NULL);
  if (alternatives != NULL) {
    const int count = sk_GENERAL_NAME_num(alternatives);
    for (int i = 0; decision == AccessManager::SKIP && i < count; i++) {
      const GENERAL_NAME* name = sk_GENERAL_NAME_value(alternatives, i);
      if (name == NULL) {
        continue;
      }
      char* data = (char*)ASN1_STRING_data(name->d.ia5);
      int length = ASN1_STRING_length(name->d.ia5);
      switch (name->type) {
      case GEN_DNS:
        if (host.empty()) {
          host = (server() ? getPeerHost() : getHost());
        }
        decision = access_->verify(host, data, length);
        break;
      case GEN_IPADD:
        decision = access_->verify(sa, data, length);
        break;
      }
    }
    sk_GENERAL_NAME_pop_free(alternatives, GENERAL_NAME_free);
  }

  if (decision != AccessManager::SKIP) {
    X509_free(cert);
    if (decision != AccessManager::ALLOW) {
      throw TSSLException("authorize: access denied");
    }
    return;
  }

  // extract commonName
  X509_NAME* name = X509_get_subject_name(cert);
  if (name != NULL) {
    X509_NAME_ENTRY* entry;
    unsigned char* utf8;
    int last = -1;
    while (decision == AccessManager::SKIP) {
      last = X509_NAME_get_index_by_NID(name, NID_commonName, last);
      if (last == -1)
        break;
      entry = X509_NAME_get_entry(name, last);
      if (entry == NULL)
        continue;
      ASN1_STRING* common = X509_NAME_ENTRY_get_data(entry);
      int size = ASN1_STRING_to_UTF8(&utf8, common);
      if (host.empty()) {
        host = (server() ? getPeerHost() : getHost());
      }
      decision = access_->verify(host, (char*)utf8, size);
      OPENSSL_free(utf8);
    }
  }
  X509_free(cert);
  if (decision != AccessManager::ALLOW) {
    throw TSSLException("authorize: cannot authorize peer");
  }
}

/*
 * Note: This method is not libevent safe.
*/
unsigned int TSSLSocket::waitForEvent(bool wantRead) {
  int fdSocket;
  BIO* bio;

  if (wantRead) {
    bio = SSL_get_rbio(ssl_);
  } else {
    bio = SSL_get_wbio(ssl_);
  }

  if (bio == NULL) {
    throw TSSLException("SSL_get_?bio returned NULL");
  }

  if (BIO_get_fd(bio, &fdSocket) <= 0) {
    throw TSSLException("BIO_get_fd failed");
  }

  struct THRIFT_POLLFD fds[2];
  memset(fds, 0, sizeof(fds));
  fds[0].fd = fdSocket;
  // use POLLIN also on write operations too, this is needed for operations
  // which requires read and write on the socket.
  fds[0].events = wantRead ? THRIFT_POLLIN : THRIFT_POLLIN | THRIFT_POLLOUT;

  if (interruptListener_) {
    fds[1].fd = *(interruptListener_.get());
    fds[1].events = THRIFT_POLLIN;
  }

  int timeout = -1;
  if (wantRead && recvTimeout_) {
    timeout = recvTimeout_;
  }
  if (!wantRead && sendTimeout_) {
    timeout = sendTimeout_;
  }

  int ret = THRIFT_POLL(fds, interruptListener_ ? 2 : 1, timeout);

  if (ret < 0) {
    // error cases
    if (THRIFT_GET_SOCKET_ERROR == THRIFT_EINTR) {
      return TSSL_EINTR; // repeat operation
    }
    int errno_copy = THRIFT_GET_SOCKET_ERROR;
    GlobalOutput.perror("TSSLSocket::read THRIFT_POLL() ", errno_copy);
    throw TTransportException(TTransportException::UNKNOWN, "Unknown", errno_copy);
  } else if (ret > 0){
    if (fds[1].revents & THRIFT_POLLIN) {
      throw TTransportException(TTransportException::INTERRUPTED, "Interrupted");
    }
    return TSSL_DATA;
  } else {
    throw TTransportException(TTransportException::TIMED_OUT, "THRIFT_POLL (timed out)");
  }
}

// TSSLSocketFactory implementation
uint64_t TSSLSocketFactory::count_ = 0;
Mutex TSSLSocketFactory::mutex_;
bool TSSLSocketFactory::manualOpenSSLInitialization_ = false;

TSSLSocketFactory::TSSLSocketFactory(SSLProtocol protocol) : server_(false) {
  Guard guard(mutex_);
  if (count_ == 0) {
    if (!manualOpenSSLInitialization_) {
      initializeOpenSSL();
    }
    randomize();
  }
  count_++;
  ctx_ = stdcxx::shared_ptr<SSLContext>(new SSLContext(protocol));
}

TSSLSocketFactory::~TSSLSocketFactory() {
  Guard guard(mutex_);
  ctx_.reset();
  count_--;
  if (count_ == 0 && !manualOpenSSLInitialization_) {
    cleanupOpenSSL();
  }
}

stdcxx::shared_ptr<TSSLSocket> TSSLSocketFactory::createSocket() {
  stdcxx::shared_ptr<TSSLSocket> ssl(new TSSLSocket(ctx_));
  setup(ssl);
  return ssl;
}

stdcxx::shared_ptr<TSSLSocket> TSSLSocketFactory::createSocket(stdcxx::shared_ptr<THRIFT_SOCKET> interruptListener) {
  stdcxx::shared_ptr<TSSLSocket> ssl(new TSSLSocket(ctx_, interruptListener));
  setup(ssl);
  return ssl;
}

stdcxx::shared_ptr<TSSLSocket> TSSLSocketFactory::createSocket(THRIFT_SOCKET socket) {
  stdcxx::shared_ptr<TSSLSocket> ssl(new TSSLSocket(ctx_, socket));
  setup(ssl);
  return ssl;
}

stdcxx::shared_ptr<TSSLSocket> TSSLSocketFactory::createSocket(THRIFT_SOCKET socket, stdcxx::shared_ptr<THRIFT_SOCKET> interruptListener) {
  stdcxx::shared_ptr<TSSLSocket> ssl(new TSSLSocket(ctx_, socket, interruptListener));
  setup(ssl);
  return ssl;
}

stdcxx::shared_ptr<TSSLSocket> TSSLSocketFactory::createSocket(const string& host, int port) {
  stdcxx::shared_ptr<TSSLSocket> ssl(new TSSLSocket(ctx_, host, port));
  setup(ssl);
  return ssl;
}

stdcxx::shared_ptr<TSSLSocket> TSSLSocketFactory::createSocket(const string& host, int port, stdcxx::shared_ptr<THRIFT_SOCKET> interruptListener) {
  stdcxx::shared_ptr<TSSLSocket> ssl(new TSSLSocket(ctx_, host, port, interruptListener));
  setup(ssl);
  return ssl;
}


void TSSLSocketFactory::setup(stdcxx::shared_ptr<TSSLSocket> ssl) {
  ssl->server(server());
  if (access_ == NULL && !server()) {
    access_ = stdcxx::shared_ptr<AccessManager>(new DefaultClientAccessManager);
  }
  if (access_ != NULL) {
    ssl->access(access_);
  }
}

void TSSLSocketFactory::ciphers(const string& enable) {
  int rc = SSL_CTX_set_cipher_list(ctx_->get(), enable.c_str());
  if (ERR_peek_error() != 0) {
    string errors;
    buildErrors(errors);
    throw TSSLException("SSL_CTX_set_cipher_list: " + errors);
  }
  if (rc == 0) {
    throw TSSLException("None of specified ciphers are supported");
  }
}

void TSSLSocketFactory::authenticate(bool required) {
  int mode;
  if (required) {
    mode = SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT | SSL_VERIFY_CLIENT_ONCE;
  } else {
    mode = SSL_VERIFY_NONE;
  }
  SSL_CTX_set_verify(ctx_->get(), mode, NULL);
}

void TSSLSocketFactory::loadCertificate(const char* path, const char* format) {
  if (path == NULL || format == NULL) {
    throw TTransportException(TTransportException::BAD_ARGS,
                              "loadCertificateChain: either <path> or <format> is NULL");
  }
  if (strcmp(format, "PEM") == 0) {
    if (SSL_CTX_use_certificate_chain_file(ctx_->get(), path) == 0) {
      int errno_copy = THRIFT_GET_SOCKET_ERROR;
      string errors;
      buildErrors(errors, errno_copy);
      throw TSSLException("SSL_CTX_use_certificate_chain_file: " + errors);
    }
  } else {
    throw TSSLException("Unsupported certificate format: " + string(format));
  }
}

void TSSLSocketFactory::loadPrivateKey(const char* path, const char* format) {
  if (path == NULL || format == NULL) {
    throw TTransportException(TTransportException::BAD_ARGS,
                              "loadPrivateKey: either <path> or <format> is NULL");
  }
  if (strcmp(format, "PEM") == 0) {
    if (SSL_CTX_use_PrivateKey_file(ctx_->get(), path, SSL_FILETYPE_PEM) == 0) {
      int errno_copy = THRIFT_GET_SOCKET_ERROR;
      string errors;
      buildErrors(errors, errno_copy);
      throw TSSLException("SSL_CTX_use_PrivateKey_file: " + errors);
    }
  }
}

void TSSLSocketFactory::loadTrustedCertificates(const char* path, const char* capath) {
  if (path == NULL) {
    throw TTransportException(TTransportException::BAD_ARGS,
                              "loadTrustedCertificates: <path> is NULL");
  }
  if (SSL_CTX_load_verify_locations(ctx_->get(), path, capath) == 0) {
    int errno_copy = THRIFT_GET_SOCKET_ERROR;
    string errors;
    buildErrors(errors, errno_copy);
    throw TSSLException("SSL_CTX_load_verify_locations: " + errors);
  }
}

void TSSLSocketFactory::randomize() {
  RAND_poll();
}

void TSSLSocketFactory::overrideDefaultPasswordCallback() {
  SSL_CTX_set_default_passwd_cb(ctx_->get(), passwordCallback);
  SSL_CTX_set_default_passwd_cb_userdata(ctx_->get(), this);
}

int TSSLSocketFactory::passwordCallback(char* password, int size, int, void* data) {
  TSSLSocketFactory* factory = (TSSLSocketFactory*)data;
  string userPassword;
  factory->getPassword(userPassword, size);
  int length = static_cast<int>(userPassword.size());
  if (length > size) {
    length = size;
  }
  strncpy(password, userPassword.c_str(), length);
  userPassword.assign(userPassword.size(), '*');
  return length;
}

// extract error messages from error queue
void buildErrors(string& errors, int errno_copy, int sslerrno) {
  unsigned long errorCode;
  char message[256];

  errors.reserve(512);
  while ((errorCode = ERR_get_error()) != 0) {
    if (!errors.empty()) {
      errors += "; ";
    }
    const char* reason = ERR_reason_error_string(errorCode);
    if (reason == NULL) {
      THRIFT_SNPRINTF(message, sizeof(message) - 1, "SSL error # %lu", errorCode);
      reason = message;
    }
    errors += reason;
  }
  if (errors.empty()) {
    if (errno_copy != 0) {
      errors += TOutput::strerror_s(errno_copy);
    }
  }
  if (errors.empty()) {
    errors = "error code: " + to_string(errno_copy);
  }
  if (sslerrno) {
    errors += " (SSL_error_code = " + to_string(sslerrno) + ")";
    if (sslerrno == SSL_ERROR_SYSCALL) {
      char buf[4096];
      int err;
      while ((err = ERR_get_error()) != 0) {
        errors += " ";
        errors += ERR_error_string(err, buf);
      }
    }
  }
}

/**
 * Default implementation of AccessManager
 */
Decision DefaultClientAccessManager::verify(const sockaddr_storage& sa) throw() {
  (void)sa;
  return SKIP;
}

Decision DefaultClientAccessManager::verify(const string& host,
                                            const char* name,
                                            int size) throw() {
  if (host.empty() || name == NULL || size <= 0) {
    return SKIP;
  }
  return (matchName(host.c_str(), name, size) ? ALLOW : SKIP);
}

Decision DefaultClientAccessManager::verify(const sockaddr_storage& sa,
                                            const char* data,
                                            int size) throw() {
  bool match = false;
  if (sa.ss_family == AF_INET && size == sizeof(in_addr)) {
    match = (memcmp(&((sockaddr_in*)&sa)->sin_addr, data, size) == 0);
  } else if (sa.ss_family == AF_INET6 && size == sizeof(in6_addr)) {
    match = (memcmp(&((sockaddr_in6*)&sa)->sin6_addr, data, size) == 0);
  }
  return (match ? ALLOW : SKIP);
}

/**
 * Match a name with a pattern. The pattern may include wildcard. A single
 * wildcard "*" can match up to one component in the domain name.
 *
 * @param  host    Host name, typically the name of the remote host
 * @param  pattern Name retrieved from certificate
 * @param  size    Size of "pattern"
 * @return True, if "host" matches "pattern". False otherwise.
 */
bool matchName(const char* host, const char* pattern, int size) {
  bool match = false;
  int i = 0, j = 0;
  while (i < size && host[j] != '\0') {
    if (uppercase(pattern[i]) == uppercase(host[j])) {
      i++;
      j++;
      continue;
    }
    if (pattern[i] == '*') {
      while (host[j] != '.' && host[j] != '\0') {
        j++;
      }
      i++;
      continue;
    }
    break;
  }
  if (i == size && host[j] == '\0') {
    match = true;
  }
  return match;
}

// This is to work around the Turkish locale issue, i.e.,
// toupper('i') != toupper('I') if locale is "tr_TR"
char uppercase(char c) {
  if ('a' <= c && c <= 'z') {
    return c + ('A' - 'a');
  }
  return c;
}
}
}
}
