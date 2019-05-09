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
#include <thrift/c_glib/protocol/thrift_binary_protocol.h>
#include <thrift/c_glib/protocol/thrift_protocol_decorator.h>

G_DEFINE_TYPE(ThriftProtocolDecorator, thrift_protocol_decorator, THRIFT_TYPE_PROTOCOL)


enum
{
  PROP_THRIFT_TYPE_PROTOCOL_DECORATOR_CONCRETE_PROTOCOL = 1,
  PROP_THRIFT_TYPE_PROTOCOL_DECORATOR_END
};

static GParamSpec *thrift_protocol_decorator_obj_properties[PROP_THRIFT_TYPE_PROTOCOL_DECORATOR_END] = { NULL, };





gint32
thrift_protocol_decorator_write_message_begin (ThriftProtocol *protocol,
                                     const gchar *name,
                                     const ThriftMessageType message_type,
                                     const gint32 seqid, GError **error)
{

  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);
  ThriftProtocolClass *proto = THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol);

  g_debug("Concrete protocol %p | %p", (void *)self->concrete_protocol, (void *)proto);

  return proto->write_message_begin (self->concrete_protocol, name,
                                    message_type, seqid,
                                    error);
}

gint32
thrift_protocol_decorator_write_message_end (ThriftProtocol *protocol, GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->write_message_end (self->concrete_protocol,
                                                                  error);
}

gint32
thrift_protocol_decorator_write_struct_begin (ThriftProtocol *protocol, const gchar *name,
                                    GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->write_struct_begin (self->concrete_protocol,
                                                   name, error);
}

gint32
thrift_protocol_decorator_write_struct_end (ThriftProtocol *protocol, GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->write_struct_end (self->concrete_protocol,
                                                                 error);
}

gint32
thrift_protocol_decorator_write_field_begin (ThriftProtocol *protocol,
                                   const gchar *name,
                                   const ThriftType field_type,
                                   const gint16 field_id,
                                   GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);
  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->write_field_begin (self->concrete_protocol,
                                                   name, field_type,
                                                   field_id, error);
}

gint32
thrift_protocol_decorator_write_field_end (ThriftProtocol *protocol, GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->write_field_end (self->concrete_protocol,
                                                                error);
}

gint32
thrift_protocol_decorator_write_field_stop (ThriftProtocol *protocol, GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->write_field_stop (self->concrete_protocol,
                                                                 error);
}

gint32
thrift_protocol_decorator_write_map_begin (ThriftProtocol *protocol,
                                 const ThriftType key_type,
                                 const ThriftType value_type,
                                 const guint32 size, GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->write_map_begin (self->concrete_protocol,
                                                   key_type, value_type,
                                                   size, error);
}

gint32
thrift_protocol_decorator_write_map_end (ThriftProtocol *protocol, GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->write_map_end (self->concrete_protocol,
                                                              error);
}

gint32
thrift_protocol_decorator_write_list_begin (ThriftProtocol *protocol,
                                  const ThriftType element_type,
                                  const guint32 size, GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->write_list_begin (self->concrete_protocol,
                                                   element_type, size,
                                                   error);
}

gint32
thrift_protocol_decorator_write_list_end (ThriftProtocol *protocol, GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->write_list_end (self->concrete_protocol,
                                                               error);
}

gint32
thrift_protocol_decorator_write_set_begin (ThriftProtocol *protocol,
                                 const ThriftType element_type,
                                 const guint32 size, GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->write_set_begin (self->concrete_protocol,
                                                   element_type, size,
                                                   error);
}

gint32
thrift_protocol_decorator_write_set_end (ThriftProtocol *protocol, GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->write_set_end (self->concrete_protocol,
                                                              error);
}

gint32
thrift_protocol_decorator_write_bool (ThriftProtocol *protocol,
                            const gboolean value, GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->write_bool (self->concrete_protocol, value,
                                                           error);
}

gint32
thrift_protocol_decorator_write_byte (ThriftProtocol *protocol, const gint8 value,
                            GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->write_byte (self->concrete_protocol, value,
                                                           error);
}

gint32
thrift_protocol_decorator_write_i16 (ThriftProtocol *protocol, const gint16 value,
                           GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->write_i16 (self->concrete_protocol, value,
                                                          error);
}

gint32
thrift_protocol_decorator_write_i32 (ThriftProtocol *protocol, const gint32 value,
                           GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->write_i32 (self->concrete_protocol, value,
                                                          error);
}

gint32
thrift_protocol_decorator_write_i64 (ThriftProtocol *protocol, const gint64 value,
                           GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->write_i64 (self->concrete_protocol, value,
                                                          error);
}

gint32
thrift_protocol_decorator_write_double (ThriftProtocol *protocol,
                              const gdouble value, GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->write_double (self->concrete_protocol,
                                                             value, error);
}

gint32
thrift_protocol_decorator_write_string (ThriftProtocol *protocol,
                              const gchar *str, GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->write_string (self->concrete_protocol, str,
                                                             error);
}

gint32
thrift_protocol_decorator_write_binary (ThriftProtocol *protocol, const gpointer buf,
                              const guint32 len, GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->write_binary (self->concrete_protocol, buf,
                                                             len, error);
}

gint32
thrift_protocol_decorator_read_message_begin (ThriftProtocol *protocol,
                                    gchar **name,
                                    ThriftMessageType *message_type,
                                    gint32 *seqid, GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->read_message_begin (self->concrete_protocol,
                                                   name, message_type,
                                                   seqid, error);
}

gint32
thrift_protocol_decorator_read_message_end (ThriftProtocol *protocol,
                                  GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->read_message_end (self->concrete_protocol,
                                                                 error);
}

gint32
thrift_protocol_decorator_read_struct_begin (ThriftProtocol *protocol,
                                   gchar **name,
                                   GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->read_struct_begin (self->concrete_protocol,
                                                                  name,
                                                                  error);
}

gint32
thrift_protocol_decorator_read_struct_end (ThriftProtocol *protocol, GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->read_struct_end (self->concrete_protocol,
                                                                error);
}

gint32
thrift_protocol_decorator_read_field_begin (ThriftProtocol *protocol,
                                  gchar **name,
                                  ThriftType *field_type,
                                  gint16 *field_id,
                                  GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->read_field_begin (self->concrete_protocol,
                                                                 name,
                                                                 field_type,
                                                                 field_id,
                                                                 error);
}

gint32
thrift_protocol_decorator_read_field_end (ThriftProtocol *protocol,
                                GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->read_field_end (self->concrete_protocol,
                                                               error);
}

gint32
thrift_protocol_decorator_read_map_begin (ThriftProtocol *protocol,
                                ThriftType *key_type,
                                ThriftType *value_type, guint32 *size,
                                GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->read_map_begin (self->concrete_protocol,
                                                               key_type,
                                                               value_type,
                                                               size,
                                                               error);
}

gint32
thrift_protocol_decorator_read_map_end (ThriftProtocol *protocol, GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->read_map_end (self->concrete_protocol,
                                                             error);
}

gint32
thrift_protocol_decorator_read_list_begin (ThriftProtocol *protocol,
                                 ThriftType *element_type,
                                 guint32 *size, GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->read_list_begin (self->concrete_protocol,
                                                                element_type,
                                                                size, error);
}

gint32
thrift_protocol_decorator_read_list_end (ThriftProtocol *protocol, GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->read_list_end (self->concrete_protocol,
                                                              error);
}

gint32
thrift_protocol_decorator_read_set_begin (ThriftProtocol *protocol,
                                ThriftType *element_type,
                                guint32 *size, GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->read_set_begin (self->concrete_protocol,
                                                               element_type,
                                                               size, error);
}

gint32
thrift_protocol_decorator_read_set_end (ThriftProtocol *protocol, GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->read_set_end (self->concrete_protocol,
                                                             error);
}

gint32
thrift_protocol_decorator_read_bool (ThriftProtocol *protocol, gboolean *value,
                           GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->read_bool (self->concrete_protocol, value,
                                                          error);
}

gint32
thrift_protocol_decorator_read_byte (ThriftProtocol *protocol, gint8 *value,
                           GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->read_byte (self->concrete_protocol, value,
                                                          error);
}

gint32
thrift_protocol_decorator_read_i16 (ThriftProtocol *protocol, gint16 *value,
                          GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->read_i16 (self->concrete_protocol, value,
                                                         error);
}

gint32
thrift_protocol_decorator_read_i32 (ThriftProtocol *protocol, gint32 *value,
                          GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->read_i32 (self->concrete_protocol, value,
                                                         error);
}

gint32
thrift_protocol_decorator_read_i64 (ThriftProtocol *protocol, gint64 *value,
                          GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->read_i64 (self->concrete_protocol, value,
                                                         error);
}

gint32
thrift_protocol_decorator_read_double (ThriftProtocol *protocol,
                             gdouble *value, GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->read_double (self->concrete_protocol, value,
                                                            error);
}

gint32
thrift_protocol_decorator_read_string (ThriftProtocol *protocol,
                             gchar **str, GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->read_string (self->concrete_protocol, str,
                                                            error);
}

gint32
thrift_protocol_decorator_read_binary (ThriftProtocol *protocol, gpointer *buf,
                             guint32 *len, GError **error)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (protocol);

  return THRIFT_PROTOCOL_GET_CLASS (self->concrete_protocol)->read_binary (self->concrete_protocol, buf,
                                                            len, error);
}


static void
thrift_protocol_decorator_set_property (GObject      *object,
    guint         property_id,
    const GValue *value,
    GParamSpec   *pspec)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (object);

  switch (property_id)
  {
  case PROP_THRIFT_TYPE_PROTOCOL_DECORATOR_CONCRETE_PROTOCOL:
    self->concrete_protocol = g_value_dup_object (value);
    break;

  default:
    /* We don't have any other property... */
    G_OBJECT_WARN_INVALID_PROPERTY_ID (object, property_id, pspec);
    break;
  }
}

static void
thrift_protocol_decorator_get_property (GObject    *object,
    guint       property_id,
    GValue     *value,
    GParamSpec *pspec)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (object);

  switch (property_id)
  {
  case PROP_THRIFT_TYPE_PROTOCOL_DECORATOR_CONCRETE_PROTOCOL:
    g_value_set_object (value, self->concrete_protocol);
    break;

  default:
    /* We don't have any other property... */
    G_OBJECT_WARN_INVALID_PROPERTY_ID (object, property_id, pspec);
    break;
  }
}


ThriftProtocol *
thrift_protocol_decorator_get_concrete_protocol(ThriftProtocolDecorator *protocol)
{
  ThriftProtocol *retval = NULL;
  if(!THRIFT_IS_PROTOCOL_DECORATOR(protocol)){
    g_warning("The type is not protocol decorator");
    return NULL;
  }
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR(protocol);
  g_debug("Getting concrete protocol from %p -> %p", (void *)self, (void *)self->concrete_protocol);

  return retval;
}


static void
thrift_protocol_decorator_init (ThriftProtocolDecorator *protocol)
{
  protocol->concrete_protocol = NULL;
}

static void
thrift_protocol_decorator_dispose (GObject *gobject)
{
  ThriftProtocolDecorator *self = THRIFT_PROTOCOL_DECORATOR (gobject);

  g_clear_object(&self->concrete_protocol);

  /* Always chain up to the parent class; there is no need to check if
   * the parent class implements the dispose() virtual function: it is
   * always guaranteed to do so
   */
  G_OBJECT_CLASS (thrift_protocol_decorator_parent_class)->dispose(gobject);
}

/* initialize the class */
static void
thrift_protocol_decorator_class_init (ThriftProtocolDecoratorClass *klass)
{
  ThriftProtocolClass *cls = THRIFT_PROTOCOL_CLASS (klass);
  GObjectClass *object_class = G_OBJECT_CLASS (klass);
  object_class->set_property = thrift_protocol_decorator_set_property;
  object_class->get_property = thrift_protocol_decorator_get_property;
  object_class->dispose = thrift_protocol_decorator_dispose;

  thrift_protocol_decorator_obj_properties[PROP_THRIFT_TYPE_PROTOCOL_DECORATOR_CONCRETE_PROTOCOL] =
      g_param_spec_object ("protocol",
          "Protocol",
          "Set the protocol to be implemented", THRIFT_TYPE_PROTOCOL,
          (G_PARAM_CONSTRUCT_ONLY | G_PARAM_READWRITE));

  g_object_class_install_properties (object_class,
      PROP_THRIFT_TYPE_PROTOCOL_DECORATOR_END,
      thrift_protocol_decorator_obj_properties);

  g_debug("Current decorator write_message_begin addr %p, new %p", cls->write_message_begin, thrift_protocol_decorator_write_message_begin);

  cls->write_message_begin = thrift_protocol_decorator_write_message_begin;
  cls->write_message_end = thrift_protocol_decorator_write_message_end;
  cls->write_struct_begin = thrift_protocol_decorator_write_struct_begin;
  cls->write_struct_end = thrift_protocol_decorator_write_struct_end;
  cls->write_field_begin = thrift_protocol_decorator_write_field_begin;
  cls->write_field_end = thrift_protocol_decorator_write_field_end;
  cls->write_field_stop = thrift_protocol_decorator_write_field_stop;
  cls->write_map_begin = thrift_protocol_decorator_write_map_begin;
  cls->write_map_end = thrift_protocol_decorator_write_map_end;
  cls->write_list_begin = thrift_protocol_decorator_write_list_begin;
  cls->write_list_end = thrift_protocol_decorator_write_list_end;
  cls->write_set_begin = thrift_protocol_decorator_write_set_begin;
  cls->write_set_end = thrift_protocol_decorator_write_set_end;
  cls->write_bool = thrift_protocol_decorator_write_bool;
  cls->write_byte = thrift_protocol_decorator_write_byte;
  cls->write_i16 = thrift_protocol_decorator_write_i16;
  cls->write_i32 = thrift_protocol_decorator_write_i32;
  cls->write_i64 = thrift_protocol_decorator_write_i64;
  cls->write_double = thrift_protocol_decorator_write_double;
  cls->write_string = thrift_protocol_decorator_write_string;
  cls->write_binary = thrift_protocol_decorator_write_binary;
  cls->read_message_begin = thrift_protocol_decorator_read_message_begin;
  cls->read_message_end = thrift_protocol_decorator_read_message_end;
  cls->read_struct_begin = thrift_protocol_decorator_read_struct_begin;
  cls->read_struct_end = thrift_protocol_decorator_read_struct_end;
  cls->read_field_begin = thrift_protocol_decorator_read_field_begin;
  cls->read_field_end = thrift_protocol_decorator_read_field_end;
  cls->read_map_begin = thrift_protocol_decorator_read_map_begin;
  cls->read_map_end = thrift_protocol_decorator_read_map_end;
  cls->read_list_begin = thrift_protocol_decorator_read_list_begin;
  cls->read_list_end = thrift_protocol_decorator_read_list_end;
  cls->read_set_begin = thrift_protocol_decorator_read_set_begin;
  cls->read_set_end = thrift_protocol_decorator_read_set_end;
  cls->read_bool = thrift_protocol_decorator_read_bool;
  cls->read_byte = thrift_protocol_decorator_read_byte;
  cls->read_i16 = thrift_protocol_decorator_read_i16;
  cls->read_i32 = thrift_protocol_decorator_read_i32;
  cls->read_i64 = thrift_protocol_decorator_read_i64;
  cls->read_double = thrift_protocol_decorator_read_double;
  cls->read_string = thrift_protocol_decorator_read_string;
  cls->read_binary = thrift_protocol_decorator_read_binary;
}
