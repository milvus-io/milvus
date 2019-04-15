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

#include <inttypes.h>
#include <string.h>
#include <unistd.h>

#include <thrift/c_glib/thrift.h>
#include <thrift/c_glib/thrift_application_exception.h>

#include "thrift_second_service_handler.h"

/* A handler that implements the TTestSecondServiceIf interface */

G_DEFINE_TYPE (SecondServiceHandler,
               second_service_handler,
	       T_TEST_TYPE_SECOND_SERVICE_HANDLER);


gboolean
second_service_handler_secondtest_string (TTestSecondServiceIf  *iface,
                                 gchar             **_return,
                                 const gchar        *thing,
                                 GError            **error)
{
  THRIFT_UNUSED_VAR (iface);
  THRIFT_UNUSED_VAR (error);
  gchar buffer[256];

  printf ("testSecondServiceMultiplexSecondTestString(\"%s\")\n", thing);
  snprintf(buffer, 255, "testString(\"%s\")", thing);
  *_return = g_strdup (buffer);

  return TRUE;
}

static void
second_service_handler_init (SecondServiceHandler *self)
{
  THRIFT_UNUSED_VAR (self);
}

static void
second_service_handler_class_init (SecondServiceHandlerClass *klass)
{
  TTestSecondServiceHandlerClass *base_class =
      T_TEST_SECOND_SERVICE_HANDLER_CLASS (klass);


  base_class->secondtest_string =
      second_service_handler_secondtest_string;

}
