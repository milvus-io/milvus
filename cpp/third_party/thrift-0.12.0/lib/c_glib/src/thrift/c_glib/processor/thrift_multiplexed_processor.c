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
#include <string.h>
#include <stdio.h>

#include <thrift/c_glib/thrift.h>
#include <thrift/c_glib/processor/thrift_processor.h>
#include <thrift/c_glib/processor/thrift_multiplexed_processor.h>
#include <thrift/c_glib/protocol/thrift_multiplexed_protocol.h>
#include <thrift/c_glib/protocol/thrift_stored_message_protocol.h>
#include <thrift/c_glib/thrift_application_exception.h>

G_DEFINE_TYPE(ThriftMultiplexedProcessor, thrift_multiplexed_processor, THRIFT_TYPE_PROCESSOR)


enum
{
  PROP_THRIFT_MULTIPLEXED_PROCESSOR_DEFAULT_SERVICE_NAME = 1,
  PROP_THRIFT_MULTIPLEXED_PROCESSOR_END
};

static GParamSpec *thrift_multiplexed_processor_obj_properties[PROP_THRIFT_MULTIPLEXED_PROCESSOR_END] = { NULL, };


static gboolean
thrift_multiplexed_processor_register_processor_impl(ThriftProcessor *processor, const gchar * multiplexed_processor_name, ThriftProcessor * multiplexed_processor , GError **error)
{
  ThriftMultiplexedProcessor *self = THRIFT_MULTIPLEXED_PROCESSOR(processor);
  g_hash_table_replace(self->multiplexed_services,
		       g_strdup(multiplexed_processor_name),
		       g_object_ref (multiplexed_processor));

  /* Make first registered become default */
  if(!self->default_processor_name){
      self->default_processor_name = g_strdup(multiplexed_processor_name);
  }
  return TRUE;
}


static gboolean
thrift_multiplexed_processor_process_impl (ThriftProcessor *processor, ThriftProtocol *in,
					   ThriftProtocol *out, GError **error)
{
  gboolean retval = FALSE;
  gboolean token_error = FALSE;
  ThriftApplicationException *xception;
  ThriftStoredMessageProtocol *stored_message_protocol = NULL;
  ThriftMessageType message_type;
  ThriftMultiplexedProcessor *self = THRIFT_MULTIPLEXED_PROCESSOR(processor);
  ThriftProcessor *multiplexed_processor = NULL;
  ThriftTransport *transport;
  char *token=NULL;
  int token_index=0;
  char *state=NULL;
  gchar *fname=NULL;
  gint32 seqid, result;

  /* FIXME It seems that previous processor is not managing error correctly */
  if(*error!=NULL) {
      g_debug ("thrift_multiplexed_processor: last error not removed: %s",
      		   *error != NULL ? (*error)->message : "(null)");
      g_clear_error (error);
  }


  THRIFT_PROTOCOL_GET_CLASS(in)->read_message_begin(in, &fname, &message_type, &seqid, error);

  if(!(message_type == T_CALL || message_type == T_ONEWAY)) {
      g_set_error (error,
		   THRIFT_MULTIPLEXED_PROCESSOR_ERROR,
		   THRIFT_MULTIPLEXED_PROCESSOR_ERROR_MESSAGE_TYPE,
		   "message type invalid for this processor");
  }else{
      /* Split by the token */
      for (token = strtok_r(fname, THRIFT_MULTIPLEXED_PROTOCOL_DEFAULT_SEPARATOR, &state),
	  token_index=0;
	  token != NULL && !token_error;
	  token = strtok_r(NULL, THRIFT_MULTIPLEXED_PROTOCOL_DEFAULT_SEPARATOR, &state),
	      token_index++)
	{
	  switch(token_index){
	    case 0:
	      /* It should be the service name */
	      multiplexed_processor = g_hash_table_lookup(self->multiplexed_services, token);
	      if(multiplexed_processor==NULL){
		  token_error=TRUE;
	      }
	      break;
	    case 1:
	      /* It should be the function name */
	      stored_message_protocol = g_object_new (THRIFT_TYPE_STORED_MESSAGE_PROTOCOL,
						      "protocol", in,
						      "name", token,
						      "type", message_type,
						      "seqid", seqid,
						      NULL);
	      break;
	    default:
	      g_set_error (error,
			   THRIFT_MULTIPLEXED_PROCESSOR_ERROR,
			   THRIFT_MULTIPLEXED_PROCESSOR_ERROR_MESSAGE_WRONGLY_MULTIPLEXED,
			   "the message has more tokens than expected!");
	      token_error=TRUE;
	      break;
	  }
	}
      /* Set default */
      if(!stored_message_protocol &&
	  !multiplexed_processor &&
	  token_index==1 && self->default_processor_name){
	  /* It should be the service name */
	  multiplexed_processor = g_hash_table_lookup(self->multiplexed_services, self->default_processor_name);
	  if(multiplexed_processor==NULL){
	      g_set_error (error,
			   THRIFT_MULTIPLEXED_PROCESSOR_ERROR,
			   THRIFT_MULTIPLEXED_PROCESSOR_ERROR_SERVICE_UNAVAILABLE,
			   "service %s not available on this processor",
			   self->default_processor_name);
	  }else{
	      /* Set the message name to the original name */
	      stored_message_protocol = g_object_new (THRIFT_TYPE_STORED_MESSAGE_PROTOCOL,
						      "protocol", in,
						      "name", fname,
						      "type", message_type,
						      "seqid", seqid,
						      NULL);
	  }

      }

      if(stored_message_protocol!=NULL && multiplexed_processor!=NULL){
	  retval = THRIFT_PROCESSOR_GET_CLASS (multiplexed_processor)->process (multiplexed_processor, (ThriftProtocol *) stored_message_protocol, out, error) ;
      }else{
	  if(!error)
	  g_set_error (error,
		       THRIFT_MULTIPLEXED_PROCESSOR_ERROR,
		       THRIFT_MULTIPLEXED_PROCESSOR_ERROR_SERVICE_UNAVAILABLE,
		       "service %s is not multiplexed in this processor",
		       fname);
      }


  }

  if(!retval){
      /* By default, return an application exception to the client indicating the
          method name is not recognized. */
      /* Copied from dispach processor */

      if ((thrift_protocol_skip (in, T_STRUCT, error) < 0) ||
	  (thrift_protocol_read_message_end (in, error) < 0))
	return retval;

      g_object_get (in, "transport", &transport, NULL);
      result = thrift_transport_read_end (transport, error);
      g_object_unref (transport);
      if (result < 0) {
	  /* We must free fname */
	  g_free(fname);
	  return retval;
      }

      if (thrift_protocol_write_message_begin (out,
					       fname,
					       T_EXCEPTION,
					       seqid,
					       error) < 0){
	  /* We must free fname */
	  g_free(fname);

	  return retval;
      }


      xception =
	  g_object_new (THRIFT_TYPE_APPLICATION_EXCEPTION,
			"type",    THRIFT_APPLICATION_EXCEPTION_ERROR_UNKNOWN_METHOD,
			"message", (*error)->message,
			NULL);
      result = thrift_struct_write (THRIFT_STRUCT (xception),
				    out,
				    error);
      g_object_unref (xception);
      if ((result < 0) ||
	  (thrift_protocol_write_message_end (out, error) < 0))
	return retval;

      g_object_get (out, "transport", &transport, NULL);
      retval =
	  ((thrift_transport_write_end (transport, error) >= 0) &&
	      (thrift_transport_flush (transport, error) >= 0));
      g_object_unref (transport);
  }else{
      /* The protocol now has a copy we can free it */
      g_free(fname);

  }

  /*
  FIXME This makes everything fail, I don't know why.
  if(stored_message_protocol!=NULL){
	  // After its use we must free it
	  g_object_unref(stored_message_protocol);
  }
  */
  return retval;
}

/* define the GError domain for Thrift transports */
GQuark
thrift_multiplexed_processor_error_quark (void)
{
  return g_quark_from_static_string (THRIFT_MULTIPLEXED_PROCESSOR_ERROR_DOMAIN);
}


static void
thrift_multiplexed_processor_set_property (GObject      *object,
    guint         property_id,
    const GValue *value,
    GParamSpec   *pspec)
{
  ThriftMultiplexedProcessor *self = THRIFT_MULTIPLEXED_PROCESSOR (object);

  switch (property_id)
  {
  case PROP_THRIFT_MULTIPLEXED_PROCESSOR_DEFAULT_SERVICE_NAME:
    self->default_processor_name = g_value_dup_string (value);
    break;

  default:
    /* We don't have any other property... */
    G_OBJECT_WARN_INVALID_PROPERTY_ID (object, property_id, pspec);
    break;
  }
}

static void
thrift_multiplexed_processor_get_property (GObject    *object,
    guint       property_id,
    GValue     *value,
    GParamSpec *pspec)
{
  ThriftMultiplexedProcessor *self = THRIFT_MULTIPLEXED_PROCESSOR (object);

  switch (property_id)
  {
  case PROP_THRIFT_MULTIPLEXED_PROCESSOR_DEFAULT_SERVICE_NAME:
    g_value_set_string (value, self->default_processor_name);
    break;

  default:
    /* We don't have any other property... */
    G_OBJECT_WARN_INVALID_PROPERTY_ID (object, property_id, pspec);
    break;
  }
}

/* destructor */
static void
thrift_multiplexed_processor_finalize (GObject *object)
{
  ThriftMultiplexedProcessor *self = THRIFT_MULTIPLEXED_PROCESSOR(object);

  /* Free our multiplexed hash table */
  g_hash_table_unref (self->multiplexed_services);
  self->multiplexed_services = NULL;

  if(self->default_processor_name){
      g_free(self->default_processor_name);
      self->default_processor_name=NULL;
  }

  /* Chain up to parent */
  if (G_OBJECT_CLASS (thrift_multiplexed_processor_parent_class)->finalize)
    (*G_OBJECT_CLASS (thrift_multiplexed_processor_parent_class)->finalize) (object);
}

/* class initializer for ThriftMultiplexedProcessor */
static void
thrift_multiplexed_processor_class_init (ThriftMultiplexedProcessorClass *cls)
{
  /* Override */
  THRIFT_PROCESSOR_CLASS(cls)->process = thrift_multiplexed_processor_process_impl;
  GObjectClass *gobject_class = G_OBJECT_CLASS (cls);

  /* Object methods */
  gobject_class->set_property = thrift_multiplexed_processor_set_property;
  gobject_class->get_property = thrift_multiplexed_processor_get_property;
  gobject_class->finalize = thrift_multiplexed_processor_finalize;

  /* Class methods */
  cls->register_processor = thrift_multiplexed_processor_register_processor_impl;


  thrift_multiplexed_processor_obj_properties[PROP_THRIFT_MULTIPLEXED_PROCESSOR_DEFAULT_SERVICE_NAME] =
      g_param_spec_string ("default",
          "Default service name the protocol points to where no multiplexed client used",
          "Set the default service name",
          NULL,
          (G_PARAM_READWRITE));

  g_object_class_install_properties (gobject_class,
				     PROP_THRIFT_MULTIPLEXED_PROCESSOR_END,
      thrift_multiplexed_processor_obj_properties);

}

static void
thrift_multiplexed_processor_init (ThriftMultiplexedProcessor *self)
{

  /* Create our multiplexed services hash table */
  self->multiplexed_services = g_hash_table_new_full (
      g_str_hash,
      g_str_equal,
      g_free,
      g_object_unref);
  self->default_processor_name = NULL;
}


gboolean
thrift_multiplexed_processor_register_processor(ThriftProcessor *processor, const gchar * multiplexed_processor_name, ThriftProcessor * multiplexed_processor , GError **error)
{
  return THRIFT_MULTIPLEXED_PROCESSOR_GET_CLASS(processor)->register_processor(processor, multiplexed_processor_name, multiplexed_processor, error);
}


