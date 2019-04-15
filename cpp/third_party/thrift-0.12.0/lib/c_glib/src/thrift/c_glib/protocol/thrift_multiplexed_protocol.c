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
#include <thrift/c_glib/protocol/thrift_multiplexed_protocol.h>


enum
{
  PROP_THRIFT_MULTIPLEXED_PROTOCOL_SERVICE_NAME = 1,
  PROP_THRIFT_MULTIPLEXED_PROTOCOL_END
};

G_DEFINE_TYPE(ThriftMultiplexedProtocol, thrift_multiplexed_protocol, THRIFT_TYPE_PROTOCOL_DECORATOR)


static GParamSpec *thrift_multiplexed_protocol_obj_properties[PROP_THRIFT_MULTIPLEXED_PROTOCOL_END] = { NULL, };

gint32
thrift_multiplexed_protocol_write_message_begin (ThriftMultiplexedProtocol *protocol,
    const gchar *name, const ThriftMessageType message_type,
    const gint32 seqid, GError **error)
{
  gint32 ret;
  gchar *service_name = NULL;
  g_return_val_if_fail (THRIFT_IS_MULTIPLEXED_PROTOCOL (protocol), -1);

  ThriftMultiplexedProtocol *self = THRIFT_MULTIPLEXED_PROTOCOL (protocol);
  ThriftMultiplexedProtocolClass *multiplexClass = THRIFT_MULTIPLEXED_PROTOCOL_GET_CLASS(self);

  if( (message_type == T_CALL || message_type == T_ONEWAY) && self->service_name != NULL) {
    service_name = g_strdup_printf("%s%s%s", self->service_name, THRIFT_MULTIPLEXED_PROTOCOL_DEFAULT_SEPARATOR, name);
  }else{
    service_name = g_strdup(name);
  }

  /* relay to the protocol_decorator */
  ret = thrift_protocol_decorator_write_message_begin(protocol, service_name, message_type, seqid, error);

  g_free(service_name);

  return ret;
}


static void
thrift_multiplexed_protocol_set_property (GObject      *object,
    guint         property_id,
    const GValue *value,
    GParamSpec   *pspec)
{
  ThriftMultiplexedProtocol *self = THRIFT_MULTIPLEXED_PROTOCOL (object);

  switch (property_id)
  {
  case PROP_THRIFT_MULTIPLEXED_PROTOCOL_SERVICE_NAME:
    self->service_name = g_value_dup_string (value);
    break;

  default:
    /* We don't have any other property... */
    G_OBJECT_WARN_INVALID_PROPERTY_ID (object, property_id, pspec);
    break;
  }
}

static void
thrift_multiplexed_protocol_get_property (GObject    *object,
    guint       property_id,
    GValue     *value,
    GParamSpec *pspec)
{
  ThriftMultiplexedProtocol *self = THRIFT_MULTIPLEXED_PROTOCOL (object);

  switch (property_id)
  {
  case PROP_THRIFT_MULTIPLEXED_PROTOCOL_SERVICE_NAME:
    g_value_set_string (value, self->service_name);
    break;

  default:
    /* We don't have any other property... */
    G_OBJECT_WARN_INVALID_PROPERTY_ID (object, property_id, pspec);
    break;
  }
}


static void
thrift_multiplexed_protocol_init (ThriftMultiplexedProtocol *protocol)
{
  protocol->service_name = NULL;
}

static void
thrift_multiplexed_protocol_finalize (GObject *gobject)
{
  ThriftMultiplexedProtocol *self = THRIFT_MULTIPLEXED_PROTOCOL (gobject);

  if (self->service_name) {
    g_free(self->service_name);
    self->service_name = NULL;
  }

  /* Always chain up to the parent class; as with dispose(), finalize()
   * is guaranteed to exist on the parent's class virtual function table
   */
  G_OBJECT_CLASS (thrift_multiplexed_protocol_parent_class)->finalize(gobject);
}


/* initialize the class */
static void
thrift_multiplexed_protocol_class_init (ThriftMultiplexedProtocolClass *klass)
{
  ThriftProtocolClass *cls = THRIFT_PROTOCOL_CLASS (klass);
  GObjectClass *object_class = G_OBJECT_CLASS (klass);

  cls->write_message_begin = thrift_multiplexed_protocol_write_message_begin;

  object_class->set_property = thrift_multiplexed_protocol_set_property;
  object_class->get_property = thrift_multiplexed_protocol_get_property;
  object_class->finalize = thrift_multiplexed_protocol_finalize;

  thrift_multiplexed_protocol_obj_properties[PROP_THRIFT_MULTIPLEXED_PROTOCOL_SERVICE_NAME] =
      g_param_spec_string ("service-name",
          "Service name the protocol points to",
          "Set the service name",
          NULL,
          (G_PARAM_CONSTRUCT_ONLY | G_PARAM_READWRITE));

  g_object_class_install_properties (object_class,
      PROP_THRIFT_MULTIPLEXED_PROTOCOL_END,
      thrift_multiplexed_protocol_obj_properties);
}
