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

#include <errno.h>
#include <netdb.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <openssl/ssl.h>
#include <pthread.h>

#include <glib-object.h>
#include <glib.h>

#include <thrift/c_glib/thrift.h>
#include <thrift/c_glib/transport/thrift_transport.h>
#include <thrift/c_glib/transport/thrift_socket.h>
#include <thrift/c_glib/transport/thrift_ssl_socket.h>


#if defined(WIN32)
#define MUTEX_TYPE            HANDLE
#define MUTEX_SETUP(x)        (x) = CreateMutex(NULL, FALSE, NULL)
#define MUTEX_CLEANUP(x)      CloseHandle(x)
#define MUTEX_LOCK(x)         WaitForSingleObject((x), INFINITE)
#define MUTEX_UNLOCK(x)       ReleaseMutex(x)
#else
#define MUTEX_TYPE            pthread_mutex_t
#define MUTEX_SETUP(x)        pthread_mutex_init(&(x), NULL)
#define MUTEX_CLEANUP(x)      pthread_mutex_destroy(&(x))
#define MUTEX_LOCK(x)         pthread_mutex_lock(&(x))
#define MUTEX_UNLOCK(x)       pthread_mutex_unlock(&(x))
#endif

#define OPENSSL_VERSION_NO_THREAD_ID 0x10000000L


/* object properties */
enum _ThriftSSLSocketProperties
{
  PROP_THRIFT_SSL_SOCKET_CONTEXT = 3,
  PROP_THRIFT_SSL_SELF_SIGNED
};

/* To hold a global state management of openssl for all instances */
static gboolean thrift_ssl_socket_openssl_initialized=FALSE;
/* This array will store all of the mutexes available to OpenSSL. */
static MUTEX_TYPE *thrift_ssl_socket_global_mutex_buf=NULL;


/**
 * OpenSSL uniq id function.
 *
 * @return    thread id
 */
static unsigned long thrift_ssl_socket_static_id_function(void)
{
#if defined(WIN32)
  return GetCurrentThreadId();
#else
  return ((unsigned long) pthread_self());
#endif
}

static void thrift_ssl_socket_static_locking_callback(int mode, int n, const char* unk, int id) {
  if (mode & CRYPTO_LOCK)
    MUTEX_LOCK(thrift_ssl_socket_global_mutex_buf[n]);
  else
    MUTEX_UNLOCK(thrift_ssl_socket_global_mutex_buf[n]);
}

static int thrift_ssl_socket_static_thread_setup(void)
{
  int i;

  thrift_ssl_socket_global_mutex_buf = malloc(CRYPTO_num_locks() * sizeof(MUTEX_TYPE));
  if (!thrift_ssl_socket_global_mutex_buf)
    return 0;
  for (i = 0;  i < CRYPTO_num_locks(  );  i++)
    MUTEX_SETUP(thrift_ssl_socket_global_mutex_buf[i]);
  CRYPTO_set_id_callback(thrift_ssl_socket_static_id_function);
  CRYPTO_set_locking_callback(thrift_ssl_socket_static_locking_callback);
  return 1;
}

static int thrift_ssl_socket_static_thread_cleanup(void)
{
  int i;
  if (!thrift_ssl_socket_global_mutex_buf)
    return 0;
  CRYPTO_set_id_callback(NULL);
  CRYPTO_set_locking_callback(NULL);
  for (i = 0;  i < CRYPTO_num_locks(  );  i++)
    MUTEX_CLEANUP(thrift_ssl_socket_global_mutex_buf[i]);
  free(thrift_ssl_socket_global_mutex_buf);
  thrift_ssl_socket_global_mutex_buf = NULL;
  return 1;
}

/*
static void* thrift_ssl_socket_dyn_lock_create_callback(const char* unk, int id) {
  g_print("We should create a lock\n");
  return NULL;
}

static void thrift_ssl_socket_dyn_lock_callback(int mode, void* lock, const char* unk, int id) {
  if (lock != NULL) {
    if (mode & CRYPTO_LOCK) {
      g_printf("We should lock thread %d\n");
    } else {
      g_printf("We should unlock thread %d\n");
    }
  }
}

static void thrift_ssl_socket_dyn_lock_destroy_callback(void* lock, const char* unk, int id) {
  g_printf("We must destroy the lock\n");
}
 */


G_DEFINE_TYPE(ThriftSSLSocket, thrift_ssl_socket, THRIFT_TYPE_SOCKET)



/**
 * When there's a thread context attached, we pass the SSL socket context so it
 * can check if the error is outside SSL, on I/O for example
 * @param socket
 * @param error_msg
 * @param thrift_error_no
 * @param ssl_error
 * @param error
 */
static
void thrift_ssl_socket_get_ssl_error(ThriftSSLSocket *socket, const guchar *error_msg, guint thrift_error_no, int ssl_error, GError **error)
{
  unsigned long error_code;
  char buffer[1024];
  int buffer_size=1024;
  gboolean first_error = TRUE;
  int ssl_error_type = SSL_get_error(socket->ssl, ssl_error);
  if(ssl_error_type>0){
      switch(ssl_error_type){
	case SSL_ERROR_SSL:
	  buffer_size-=snprintf(buffer, buffer_size, "SSL %s: ", error_msg);
	  while ((error_code = ERR_get_error()) != 0 && buffer_size>1) {
	      const char* reason = ERR_reason_error_string(error_code);
	      if(reason!=NULL){
		  if(!first_error) {
		      buffer_size-=snprintf(buffer+(1024-buffer_size), buffer_size, "\n\t");
		      first_error=FALSE;
		  }
		  buffer_size-=snprintf(buffer+(1024-buffer_size), buffer_size, "%lX(%s) -> %s", error_code, reason, SSL_state_string(socket->ssl));
	      }
	  }
	  break;
	case SSL_ERROR_SYSCALL:
	  buffer_size-=snprintf(buffer, buffer_size, "%s: ", error_msg);
	  buffer_size-=snprintf(buffer+(1024-buffer_size), buffer_size, "%lX -> %s", errno, strerror(errno));
	  break;
	case SSL_ERROR_WANT_READ:
	  buffer_size-=snprintf(buffer, buffer_size, "%s: ", error_msg);
	  buffer_size-=snprintf(buffer+(1024-buffer_size), buffer_size, "%lX -> %s", ssl_error_type, "Error while reading from underlaying layer");
	  break;
	case SSL_ERROR_WANT_WRITE:
	  buffer_size-=snprintf(buffer, buffer_size, "%s: ", error_msg);
	  buffer_size-=snprintf(buffer+(1024-buffer_size), buffer_size, "%lX -> %s", ssl_error_type, "Error while writting to underlaying layer");
	  break;

      }
      g_set_error (error, THRIFT_TRANSPORT_ERROR,
		   thrift_error_no, "%s", buffer);
  }
}

/**
 * For global SSL errors
 * @param error_msg
 * @param thrift_error_no
 * @param error
 */
static
void thrift_ssl_socket_get_error(const guchar *error_msg, guint thrift_error_no, GError **error)
{
  unsigned long error_code;
  while ((error_code = ERR_get_error()) != 0) {
      const char* reason = ERR_reason_error_string(error_code);
      if (reason == NULL) {
	  g_set_error (error, THRIFT_TRANSPORT_ERROR,
		       thrift_error_no,
		       "SSL error %lX: %s", error_code, error_msg);
      }else{
	  g_set_error (error, THRIFT_TRANSPORT_ERROR,
		       thrift_error_no,
		       "SSL error %lX %s: %s", error_code,reason, error_msg);
      }
  }
}



/* implements thrift_transport_is_open */
gboolean
thrift_ssl_socket_is_open (ThriftTransport *transport)
{
  return thrift_socket_is_open(transport);
}

/* overrides thrift_transport_peek */
gboolean
thrift_ssl_socket_peek (ThriftTransport *transport, GError **error)
{
  gboolean retval = FALSE;
  ThriftSSLSocket *ssl_socket = THRIFT_SSL_SOCKET (transport);
  if (thrift_ssl_socket_is_open (transport))
    {
      int rc;
      gchar byte;
      rc = SSL_peek(ssl_socket->ssl, &byte, 1);
      if (rc < 0) {
	  thrift_ssl_socket_get_ssl_error(ssl_socket, "Check socket data",
					  THRIFT_SSL_SOCKET_ERROR_SSL, rc, error);
      }
      if (rc == 0) {
	  ERR_clear_error();
      }
      retval = (rc > 0);
    }
  return retval;
}

/* implements thrift_transport_open */
gboolean
thrift_ssl_socket_open (ThriftTransport *transport, GError **error)
{
  ERR_clear_error();

  if (!thrift_socket_open(transport, error)) {
      return FALSE;
  }

  if (!THRIFT_SSL_SOCKET_GET_CLASS(transport)->handle_handshake(transport, error)) {
      thrift_ssl_socket_close(transport, NULL);
      return FALSE;
  }

  return TRUE;
}

/* implements thrift_transport_close */
gboolean
thrift_ssl_socket_close (ThriftTransport *transport, GError **error)
{
  gboolean retval = FALSE;
  ThriftSSLSocket *ssl_socket = THRIFT_SSL_SOCKET(transport);
  if(ssl_socket!=NULL && ssl_socket->ssl) {
      int rc = SSL_shutdown(ssl_socket->ssl);
/*      if (rc < 0) {
	  int errno_copy = THRIFT_SSL_SOCKET_ERROR_SSL;
      }*/
      SSL_free(ssl_socket->ssl);
      ssl_socket->ssl = NULL;
      ERR_remove_state(0);
  }
  return thrift_socket_close(transport, error);
}

/* implements thrift_transport_read */
gint32
thrift_ssl_socket_read (ThriftTransport *transport, gpointer buf,
			guint32 len, GError **error)
{
  guint maxRecvRetries_ = 10;
  ThriftSSLSocket *ssl_socket = THRIFT_SSL_SOCKET (transport);
  guint bytes = 0;
  guint retries = 0;
  ThriftSocket *socket = THRIFT_SOCKET (transport);
  g_return_val_if_fail (socket->sd != THRIFT_INVALID_SOCKET && ssl_socket->ssl!=NULL, FALSE);

  for (retries=0; retries < maxRecvRetries_; retries++) {
      bytes = SSL_read(ssl_socket->ssl, buf, len);
      if (bytes >= 0)
	break;
      int errno_copy = THRIFT_GET_SOCKET_ERROR;
      if (SSL_get_error(ssl_socket->ssl, bytes) == SSL_ERROR_SYSCALL) {
	  if (ERR_get_error() == 0 && errno_copy == THRIFT_EINTR) {
	      continue;
	  }
      }else{
	  thrift_ssl_socket_get_ssl_error(ssl_socket, "Receive error",
					  THRIFT_SSL_SOCKET_ERROR_SSL, bytes, error);

      }
      return -1;
  }
  return bytes;
}

/* implements thrift_transport_read_end
 * called when write is complete.  nothing to do on our end. */
gboolean
thrift_ssl_socket_read_end (ThriftTransport *transport, GError **error)
{
  /* satisfy -Wall */
  THRIFT_UNUSED_VAR (transport);
  THRIFT_UNUSED_VAR (error);
  return TRUE;
}

/* implements thrift_transport_write */
gboolean
thrift_ssl_socket_write (ThriftTransport *transport, const gpointer buf,
			 const guint32 len, GError **error)
{
  ThriftSSLSocket *ssl_socket = THRIFT_SSL_SOCKET (transport);
  gint ret = 0;
  guint sent = 0;
  ThriftSocket *socket = THRIFT_SOCKET (transport);
  g_return_val_if_fail (socket->sd != THRIFT_INVALID_SOCKET && ssl_socket->ssl!=NULL, FALSE);

  while (sent < len)
    {
      ret = SSL_write (ssl_socket->ssl, (guint8 *)buf + sent, len - sent);
      if (ret < 0)
	{
	  thrift_ssl_socket_get_ssl_error(ssl_socket, "Send error",
					  THRIFT_SSL_SOCKET_ERROR_SSL, ret, error);
	  return FALSE;
	}
      sent += ret;
    }

  return sent==len;
}

/* implements thrift_transport_write_end
 * called when write is complete.  nothing to do on our end. */
gboolean
thrift_ssl_socket_write_end (ThriftTransport *transport, GError **error)
{
  /* satisfy -Wall */
  THRIFT_UNUSED_VAR (transport);
  THRIFT_UNUSED_VAR (error);
  return TRUE;
}

/* implements thrift_transport_flush
 * flush pending data.  since we are not buffered, this is a no-op */
gboolean
thrift_ssl_socket_flush (ThriftTransport *transport, GError **error)
{
  ThriftSSLSocket *ssl_socket = THRIFT_SSL_SOCKET (transport);
  gint ret = 0;
  guint sent = 0;

  ThriftSocket *socket = THRIFT_SOCKET (transport);
  g_return_val_if_fail (socket->sd != THRIFT_INVALID_SOCKET && ssl_socket->ssl!=NULL, FALSE);

  BIO* bio = SSL_get_wbio(ssl_socket->ssl);
  if (bio == NULL) {
      g_set_error (error, THRIFT_TRANSPORT_ERROR,
		   THRIFT_TRANSPORT_ERROR_SEND,
		   "failed to flush, wbio returned null");
      return FALSE;
  }
  if (BIO_flush(bio) != 1) {
      g_set_error (error, THRIFT_TRANSPORT_ERROR,
		   THRIFT_TRANSPORT_ERROR_SEND,
		   "failed to flush it returned error");
      return FALSE;
  }
  return TRUE;
}


gboolean
thrift_ssl_socket_handle_handshake(ThriftTransport * transport, GError **error)
{
  ThriftSSLSocket *ssl_socket = THRIFT_SSL_SOCKET (transport);
  ThriftSocket *socket = THRIFT_SOCKET (transport);
  g_return_val_if_fail (thrift_transport_is_open (transport), FALSE);

  if(THRIFT_SSL_SOCKET_GET_CLASS(ssl_socket)->create_ssl_context(transport, error)){
      /*Context created*/
      SSL_set_fd(ssl_socket->ssl, socket->sd);
      int rc;
      if(ssl_socket->server){
	  rc = SSL_accept(ssl_socket->ssl);
      }else{
	  rc = SSL_connect(ssl_socket->ssl);
      }
      if (rc <= 0) {
	  thrift_ssl_socket_get_ssl_error(ssl_socket, "Error while connect/bind", THRIFT_SSL_SOCKET_ERROR_CONNECT_BIND, rc, error);
	  return FALSE;
      }
  }else
    return FALSE;

  return thrift_ssl_socket_authorize(transport, error);
}

gboolean
thrift_ssl_socket_create_ssl_context(ThriftTransport * transport, GError **error)
{
  ThriftSSLSocket *socket = THRIFT_SSL_SOCKET (transport);

  if(socket->ctx!=NULL){
      if(socket->ssl!=NULL) {
	  return TRUE;
      }

      socket->ssl = SSL_new(socket->ctx);
      if (socket->ssl == NULL) {
	  g_set_error (error, THRIFT_TRANSPORT_ERROR,
		       THRIFT_SSL_SOCKET_ERROR_TRANSPORT,
		       "Unable to create default SSL context");
	  return FALSE;
      }
  }

  return TRUE;
}


gboolean thrift_ssl_load_cert_from_file(ThriftSSLSocket *ssl_socket, const char *file_name)
{
  char error_buffer[255];
  if (!thrift_ssl_socket_openssl_initialized) {
      g_error("OpenSSL is not initialized yet");
      return FALSE;
  }
  int rc = SSL_CTX_load_verify_locations(ssl_socket->ctx, file_name, NULL);
  if (rc != 1) { /*verify authentication result*/
      ERR_error_string_n(ERR_get_error(), error_buffer, 254);
      g_warning("Load of certificates failed: %s!", error_buffer);
      return FALSE;
  }
  return TRUE;
}


gboolean thrift_ssl_load_cert_from_buffer(ThriftSSLSocket *ssl_socket, const char chain_certs[])
{
  gboolean retval = FALSE;
  /* Load chain of certs*/
  X509 *cacert=NULL;
  BIO *mem = BIO_new_mem_buf(chain_certs,strlen(chain_certs));
  X509_STORE *cert_store = SSL_CTX_get_cert_store(ssl_socket->ctx);

  if(cert_store!=NULL){
      int index = 0;
      while ((cacert = PEM_read_bio_X509(mem, NULL, 0, NULL))!=NULL) {
	  if(cacert) {
	      X509_STORE_add_cert(cert_store, cacert);
	      X509_free(cacert);
	      cacert=NULL;
	  } /* Free immediately */
	  index++;
      }
      retval=TRUE;
  }
  BIO_free(mem);
  return retval;
}

gboolean
thrift_ssl_socket_authorize(ThriftTransport * transport, GError **error)
{
  ThriftSocket *socket = THRIFT_SOCKET (transport);
  ThriftSSLSocket *ssl_socket = THRIFT_SSL_SOCKET (transport);
  ThriftSSLSocketClass *cls = THRIFT_SSL_SOCKET_GET_CLASS(ssl_socket);
  gboolean authorization_result = FALSE;

  if(cls!=NULL && ssl_socket->ssl!=NULL){
      int rc = SSL_get_verify_result(ssl_socket->ssl);
      if (rc != X509_V_OK) { /* verify authentication result */
	  if (rc == X509_V_ERR_DEPTH_ZERO_SELF_SIGNED_CERT && ssl_socket->allow_selfsigned) {
	      g_debug("The certificate is a self-signed certificate and configuration allows it");
	  } else {
	      g_set_error (error,
			   THRIFT_TRANSPORT_ERROR,
			   THRIFT_SSL_SOCKET_ERROR_SSL_CERT_VALIDATION_FAILED,
			   "The certificate verification failed: %s (%d)", X509_verify_cert_error_string(rc), rc);
	      return FALSE;
	  }
      }

      X509* cert = SSL_get_peer_certificate(ssl_socket->ssl);
      if (cert == NULL) {
	  if (SSL_get_verify_mode(ssl_socket->ssl) & SSL_VERIFY_FAIL_IF_NO_PEER_CERT) {
	      g_set_error (error,
			   THRIFT_TRANSPORT_ERROR,
			   THRIFT_SSL_SOCKET_ERROR_SSL_CERT_VALIDATION_FAILED,
			   "No certificate present. Are you connecting SSL server?");
	      return FALSE;
	  }
	  g_debug("No certificate required");
	  return TRUE;
      }

      /* certificate is present, since we don't support access manager we are done */
      if (cls->authorize_peer == NULL) {
	  X509_free(cert);
	  g_debug("Certificate presented but we're not checking it");
	  return TRUE;
      } else {
	  /* both certificate and access manager are present */
	  struct sockaddr_storage sa;
	  socklen_t saLength = sizeof(struct sockaddr_storage);
	  if (getpeername(socket->sd, (struct sockaddr*)&sa, &saLength) != 0) {
	      sa.ss_family = AF_UNSPEC;
	  }
	  authorization_result = cls->authorize_peer(transport, cert, &sa, error);
      }
      if(cert != NULL) {
	  X509_free(cert);
      }
  }

  return authorization_result;
}


/* initializes the instance */
static void
thrift_ssl_socket_init (ThriftSSLSocket *socket)
{
  GError *error = NULL;
  socket->ssl = NULL;
  socket->ctx = thrift_ssl_socket_context_initialize(SSLTLS, &error);
  if(socket->ctx == NULL) {
      g_info("The SSL context was not automatically initialized with protocol %d", SSLTLS);
      if(error!=NULL){
	  g_info("Reported reason %s", error->message);
	  g_error_free (error);
      }
  }
  socket->server = FALSE;
  socket->allow_selfsigned = FALSE;

}

/* destructor */
static void
thrift_ssl_socket_finalize (GObject *object)
{
  ThriftSSLSocket *socket = THRIFT_SSL_SOCKET (object);
  GError *error=NULL;
  if(socket!=NULL){
      g_debug("Instance %p destroyed", (void *)socket);
      if(socket->ssl != NULL)
	{
	  thrift_ssl_socket_close(THRIFT_TRANSPORT(object), &error);
	  socket->ssl=NULL;
	}

      if(socket->ctx!=NULL){
	  g_debug("Freeing the context for the instance");
	  SSL_CTX_free(socket->ctx);
	  socket->ctx=NULL;
      }
  }

  if (G_OBJECT_CLASS (thrift_ssl_socket_parent_class)->finalize)
    (*G_OBJECT_CLASS (thrift_ssl_socket_parent_class)->finalize) (object);
}

/* property accessor */
void
thrift_ssl_socket_get_property (GObject *object, guint property_id,
				GValue *value, GParamSpec *pspec)
{
  ThriftSSLSocket *socket = THRIFT_SSL_SOCKET (object);
  THRIFT_UNUSED_VAR (pspec);

  switch (property_id)
  {
    case PROP_THRIFT_SSL_SOCKET_CONTEXT:
      g_value_set_pointer (value, socket->ctx);
      break;
  }
}

/* property mutator */
void
thrift_ssl_socket_set_property (GObject *object, guint property_id,
				const GValue *value, GParamSpec *pspec)
{
  ThriftSSLSocket *socket = THRIFT_SSL_SOCKET (object);

  THRIFT_UNUSED_VAR (pspec);
  switch (property_id)
  {
    case PROP_THRIFT_SSL_SOCKET_CONTEXT:
      if(socket->ctx!=NULL){
	  g_debug("Freeing the context since we are setting a new one");
	  SSL_CTX_free(socket->ctx);
      }
      socket->ctx = g_value_get_pointer(value); /* We copy the context */
      break;

    case PROP_THRIFT_SSL_SELF_SIGNED:
      socket->allow_selfsigned = g_value_get_boolean(value);
      break;
    default:
      g_warning("Trying to set property %i that doesn't exists!", property_id);
      /*    thrift_socket_set_property(object, property_id, value, pspec); */
      break;
  }
}

void
thrift_ssl_socket_initialize_openssl(void)
{
  if(thrift_ssl_socket_openssl_initialized){
      return;
  }
  thrift_ssl_socket_openssl_initialized=TRUE;
  SSL_library_init();
  ERR_load_crypto_strings();
  SSL_load_error_strings();
  ERR_load_BIO_strings();

  /* Setup locking */
  g_debug("We setup %d threads locks", thrift_ssl_socket_static_thread_setup());

  /* dynamic locking
  CRYPTO_set_dynlock_create_callback(thrift_ssl_socket_dyn_lock_create_callback);
  CRYPTO_set_dynlock_lock_callback(thrift_ssl_socket_dyn_lock_callback);
  CRYPTO_set_dynlock_destroy_callback(thrift_ssl_socket_dyn_lock_destroy_callback);
   */
}


void thrift_ssl_socket_finalize_openssl(void)
{
  if (!thrift_ssl_socket_openssl_initialized) {
      return;
  }
  thrift_ssl_socket_openssl_initialized = FALSE;

  g_debug("We cleared %d threads locks", thrift_ssl_socket_static_thread_cleanup());
  /* Not supported
  CRYPTO_set_locking_callback(NULL);
  CRYPTO_set_dynlock_create_callback(NULL);
  CRYPTO_set_dynlock_lock_callback(NULL);
  CRYPTO_set_dynlock_destroy_callback(NULL);
   */
  ERR_free_strings();
  EVP_cleanup();
  CRYPTO_cleanup_all_ex_data();
  ERR_remove_state(0);
}


/* initializes the class */
static void
thrift_ssl_socket_class_init (ThriftSSLSocketClass *cls)
{
  ThriftTransportClass *ttc = THRIFT_TRANSPORT_CLASS (cls);
  GObjectClass *gobject_class = G_OBJECT_CLASS (cls);
  GParamSpec *param_spec = NULL;

  g_debug("Initialization of ThriftSSLSocketClass");
  /* setup accessors and mutators */
  gobject_class->get_property = thrift_ssl_socket_get_property;
  gobject_class->set_property = thrift_ssl_socket_set_property;
  param_spec = g_param_spec_pointer ("ssl_context",
				     "SSLContext",
				     "Set the SSL context for handshake with the remote host",
				     G_PARAM_READWRITE);
  g_object_class_install_property (gobject_class, PROP_THRIFT_SSL_SOCKET_CONTEXT,
				   param_spec);
  param_spec = g_param_spec_boolean ("ssl_accept_selfsigned",
				     "Accept Self Signed",
				     "Whether or not accept self signed certificate",
				     FALSE,
				     G_PARAM_READWRITE);
  g_object_class_install_property (gobject_class, PROP_THRIFT_SSL_SELF_SIGNED,
				   param_spec);
  /* Class methods */
  cls->handle_handshake = thrift_ssl_socket_handle_handshake;
  cls->create_ssl_context = thrift_ssl_socket_create_ssl_context;

  /* Override */
  gobject_class->finalize = thrift_ssl_socket_finalize;
  ttc->is_open = thrift_ssl_socket_is_open;
  ttc->peek = thrift_ssl_socket_peek;
  ttc->open = thrift_ssl_socket_open;
  ttc->close = thrift_ssl_socket_close;
  ttc->read = thrift_ssl_socket_read;
  ttc->read_end = thrift_ssl_socket_read_end;
  ttc->write = thrift_ssl_socket_write;
  ttc->write_end = thrift_ssl_socket_write_end;
  ttc->flush = thrift_ssl_socket_flush;
}


/*
 * Public API
 */
ThriftSSLSocket*
thrift_ssl_socket_new(ThriftSSLSocketProtocol ssl_protocol, GError **error)
{
  ThriftSSLSocket *thriftSSLSocket = NULL;
  SSL_CTX *ssl_context = NULL;
  /* Create the context */
  if((ssl_context=thrift_ssl_socket_context_initialize(ssl_protocol, error))==NULL){
      g_warning("We cannot initialize context for protocol %d", ssl_protocol);
      return thriftSSLSocket;
  }

  /* FIXME if the protocol is different? */
  thriftSSLSocket = g_object_new (THRIFT_TYPE_SSL_SOCKET, "ssl_context", ssl_context, NULL);
  return thriftSSLSocket;
}

ThriftSSLSocket*
thrift_ssl_socket_new_with_host(ThriftSSLSocketProtocol ssl_protocol, gchar *hostname, guint port, GError **error)
{
  ThriftSSLSocket *thriftSSLSocket = NULL;
  SSL_CTX *ssl_context = NULL;
  /* Create the context */
  if((ssl_context=thrift_ssl_socket_context_initialize(ssl_protocol, error))==NULL){
      /* FIXME Do error control */
      return thriftSSLSocket;
  }
  /* FIXME if the protocol is different? */
  thriftSSLSocket = g_object_new (THRIFT_TYPE_SSL_SOCKET, "ssl_context", ssl_context, "hostname", hostname, "port", port, NULL);
  return thriftSSLSocket;
}

void thrift_ssl_socket_set_manager(ThriftSSLSocket *ssl_socket, AUTHORIZATION_MANAGER_CALLBACK callback)
{
  ThriftSSLSocketClass *sslSocketClass = THRIFT_SSL_SOCKET_GET_CLASS (ssl_socket);
  if(sslSocketClass){
      sslSocketClass->authorize_peer = callback;
  }
}


SSL_CTX*
thrift_ssl_socket_context_initialize(ThriftSSLSocketProtocol ssl_protocol, GError **error)
{
  SSL_CTX* context = NULL;
  switch(ssl_protocol){
    case SSLTLS:
      context = SSL_CTX_new(SSLv23_method());
      break;
#ifndef OPENSSL_NO_SSL3
    case SSLv3:
      context = SSL_CTX_new(SSLv3_method());
      break;
#endif
    case TLSv1_0:
      context = SSL_CTX_new(TLSv1_method());
      break;
    case TLSv1_1:
      context = SSL_CTX_new(TLSv1_1_method());
      break;
    case TLSv1_2:
      context = SSL_CTX_new(TLSv1_2_method());
      break;
    default:
      g_set_error (error, THRIFT_TRANSPORT_ERROR,
		   THRIFT_SSL_SOCKET_ERROR_CIPHER_NOT_AVAILABLE,
		   "The SSL protocol is unknown for %d", ssl_protocol);
      return NULL;
      break;
  }

  if (context == NULL) {
      thrift_ssl_socket_get_error("No cipher overlay", THRIFT_SSL_SOCKET_ERROR_CIPHER_NOT_AVAILABLE, error);
      return NULL;
  }
  SSL_CTX_set_mode(context, SSL_MODE_AUTO_RETRY);

  /* Disable horribly insecure SSLv2 and SSLv3 protocols but allow a handshake
     with older clients so they get a graceful denial. */
  if (ssl_protocol == SSLTLS) {
      SSL_CTX_set_options(context, SSL_OP_NO_SSLv2);
      SSL_CTX_set_options(context, SSL_OP_NO_SSLv3);   /* THRIFT-3164 */
  }

  return context;
}

