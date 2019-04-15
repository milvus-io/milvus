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
#include <glib.h>
#include <glib-object.h>

#include <thrift/c_glib/thrift.h>
#include <thrift/c_glib/protocol/thrift_protocol.h>
#include <thrift/c_glib/protocol/thrift_protocol_decorator.h>
#include <thrift/c_glib/protocol/thrift_stored_message_protocol.h>


enum
{
  PROP_THRIFT_STORED_MESSAGE_PROTOCOL_MESSAGE_NAME = 1,
  PROP_THRIFT_STORED_MESSAGE_PROTOCOL_MESSAGE_TYPE,
  PROP_THRIFT_STORED_MESSAGE_PROTOCOL_SEQUENCE_ID,
  PROP_THRIFT_STORED_MESSAGE_PROTOCOL_TRANSPORT, /* TODO ugly hack */
  PROP_THRIFT_STORED_MESSAGE_PROTOCOL_END
};

G_DEFINE_TYPE(ThriftStoredMessageProtocol, thrift_stored_message_protocol, THRIFT_TYPE_PROTOCOL_DECORATOR)


static GParamSpec *thrift_stored_message_protocol_obj_properties[PROP_THRIFT_STORED_MESSAGE_PROTOCOL_END] = { NULL, };

gint32
thrift_stored_message_protocol_read_message_begin (ThriftProtocol *protocol,
					   gchar **name,
					   ThriftMessageType *message_type,
					   gint32 *seqid, GError **error)
{
  gint32 ret = 0;
  g_return_val_if_fail (THRIFT_IS_STORED_MESSAGE_PROTOCOL (protocol), -1);
  THRIFT_UNUSED_VAR (error);

  ThriftStoredMessageProtocol *self = THRIFT_STORED_MESSAGE_PROTOCOL (protocol);

  /* We return the stored values on construction */
  *name = self->name;
  *message_type = self->mtype;
  *seqid = self->seqid;

  return ret;
}


static void
thrift_stored_message_protocol_set_property (GObject      *object,
					     guint         property_id,
					     const GValue *value,
					     GParamSpec   *pspec)
{
  ThriftStoredMessageProtocol *self = THRIFT_STORED_MESSAGE_PROTOCOL (object);

  switch (property_id)
  {
    case PROP_THRIFT_STORED_MESSAGE_PROTOCOL_MESSAGE_NAME:
      self->name = g_value_dup_string (value);
      break;
    case PROP_THRIFT_STORED_MESSAGE_PROTOCOL_MESSAGE_TYPE:
      self->mtype = g_value_get_int (value);
      break;
    case PROP_THRIFT_STORED_MESSAGE_PROTOCOL_SEQUENCE_ID:
      self->seqid = g_value_get_int (value);
      break;

    default:
      /* We don't have any other property... */
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, property_id, pspec);
      break;
  }
}

static void
thrift_stored_message_protocol_get_property (GObject    *object,
					     guint       property_id,
					     GValue     *value,
					     GParamSpec *pspec)
{
  ThriftStoredMessageProtocol *self = THRIFT_STORED_MESSAGE_PROTOCOL (object);
  ThriftProtocolDecorator *decorator = THRIFT_PROTOCOL_DECORATOR (object);
  ThriftTransport *transport=NULL;
  switch (property_id)
  {
    case PROP_THRIFT_STORED_MESSAGE_PROTOCOL_MESSAGE_NAME:
      g_value_set_string (value, self->name);
      break;
    case PROP_THRIFT_STORED_MESSAGE_PROTOCOL_TRANSPORT:
      /* FIXME Since we don't override properties in the decorator as it should
         we just override the properties that we know are used */
      g_object_get(decorator->concrete_protocol,pspec->name, &transport, NULL);
      g_value_set_pointer (value, transport);
      break;

    default:
      /* We don't have any other property... */
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, property_id, pspec);
      break;
  }
}


static void
thrift_stored_message_protocol_init (ThriftStoredMessageProtocol *protocol)
{
  protocol->name = NULL;
}

static void
thrift_stored_message_protocol_finalize (GObject *gobject)
{
  ThriftStoredMessageProtocol *self = THRIFT_STORED_MESSAGE_PROTOCOL (gobject);

  if (self->name) {
      g_free(self->name);
      self->name = NULL;
  }

  /* Always chain up to the parent class; as with dispose(), finalize()
   * is guaranteed to exist on the parent's class virtual function table
   */
  G_OBJECT_CLASS (thrift_stored_message_protocol_parent_class)->finalize(gobject);
}


/* initialize the class */
static void
thrift_stored_message_protocol_class_init (ThriftStoredMessageProtocolClass *klass)
{
  ThriftProtocolClass *cls = THRIFT_PROTOCOL_CLASS (klass);
  GObjectClass *object_class = G_OBJECT_CLASS (klass);

  cls->read_message_begin = thrift_stored_message_protocol_read_message_begin;

  object_class->set_property = thrift_stored_message_protocol_set_property;
  object_class->get_property = thrift_stored_message_protocol_get_property;
  object_class->finalize = thrift_stored_message_protocol_finalize;

  thrift_stored_message_protocol_obj_properties[PROP_THRIFT_STORED_MESSAGE_PROTOCOL_MESSAGE_NAME] =
      g_param_spec_string ("name",
			   "Service name the protocol points to",
			   "Set the service name",
			   NULL,
			   (G_PARAM_CONSTRUCT_ONLY | G_PARAM_READWRITE));
  thrift_stored_message_protocol_obj_properties[PROP_THRIFT_STORED_MESSAGE_PROTOCOL_MESSAGE_TYPE] =
      g_param_spec_int ("type",
			"Message type in the wire",
			"Set the message type in the wire",
			T_CALL, T_ONEWAY,
			T_CALL,
			(G_PARAM_CONSTRUCT_ONLY | G_PARAM_READWRITE));
  thrift_stored_message_protocol_obj_properties[PROP_THRIFT_STORED_MESSAGE_PROTOCOL_SEQUENCE_ID] =
      g_param_spec_int ("seqid",
			"Sequence id type in the wire",
			"Set the Sequence id in the wire",
			0, G_MAXINT,
			0,
			(G_PARAM_CONSTRUCT_ONLY | G_PARAM_READWRITE));

  /* TODO Ugly hack, in theory we must override all properties from underlaying
     protocol */
  thrift_stored_message_protocol_obj_properties[PROP_THRIFT_STORED_MESSAGE_PROTOCOL_TRANSPORT] =
      g_param_spec_pointer ("transport",
			"Transport on the underlaying implementation",
			"Transport of decorated protocol",
			G_PARAM_READABLE);



  g_object_class_install_properties (object_class,
				     PROP_THRIFT_STORED_MESSAGE_PROTOCOL_END,
				     thrift_stored_message_protocol_obj_properties);
}
