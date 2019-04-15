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

#ifndef _THRIFT_MULTIPLEXED_PROTOCOL_H
#define _THRIFT_MULTIPLEXED_PROTOCOL_H

#include <glib-object.h>

#include <thrift/c_glib/protocol/thrift_protocol.h>
#include <thrift/c_glib/protocol/thrift_protocol_decorator.h>
#include <thrift/c_glib/transport/thrift_transport.h>

G_BEGIN_DECLS

/*! \file thrift_multiplexed_protocol.h
 *  \brief Multiplexed protocol implementation of a Thrift protocol.  Implements the
 *         ThriftProtocol interface.
 */

/* type macros */
#define THRIFT_TYPE_MULTIPLEXED_PROTOCOL (thrift_multiplexed_protocol_get_type ())
#define THRIFT_MULTIPLEXED_PROTOCOL(obj) (G_TYPE_CHECK_INSTANCE_CAST ((obj), THRIFT_TYPE_MULTIPLEXED_PROTOCOL, ThriftMultiplexedProtocol))
#define THRIFT_IS_MULTIPLEXED_PROTOCOL(obj) (G_TYPE_CHECK_INSTANCE_TYPE ((obj), THRIFT_TYPE_MULTIPLEXED_PROTOCOL))
#define THRIFT_MULTIPLEXED_PROTOCOL_CLASS(c) (G_TYPE_CHECK_CLASS_CAST ((c), THRIFT_TYPE_MULTIPLEXED_PROTOCOL, ThriftMultiplexedProtocolClass))
#define THRIFT_IS_MULTIPLEXED_PROTOCOL_CLASS(c) (G_TYPE_CHECK_CLASS_TYPE ((c), THRIFT_TYPE_MULTIPLEXED_PROTOCOL))
#define THRIFT_MULTIPLEXED_PROTOCOL_GET_CLASS(obj) (G_TYPE_INSTANCE_GET_CLASS ((obj), THRIFT_TYPE_MULTIPLEXED_PROTOCOL, ThriftMultiplexedProtocolClass))

/* constant */
#define THRIFT_MULTIPLEXED_PROTOCOL_DEFAULT_SEPARATOR ":"

typedef struct _ThriftMultiplexedProtocol ThriftMultiplexedProtocol;



/*!
 * Thrift Multiplexed Protocol instance.
 */
struct _ThriftMultiplexedProtocol
{
  ThriftProtocolDecorator parent;

  gchar *service_name;
};

typedef struct _ThriftMultiplexedProtocolClass ThriftMultiplexedProtocolClass;

/*!
 * Thrift Multiplexed Protocol class.
 */
struct _ThriftMultiplexedProtocolClass
{
  ThriftProtocolDecoratorClass parent;
};

/* used by THRIFT_TYPE_MULTIPLEXED_PROTOCOL */
GType thrift_multiplexed_protocol_get_type (void);

G_END_DECLS

#endif /* _THRIFT_MULTIPLEXED_PROTOCOL_H */
