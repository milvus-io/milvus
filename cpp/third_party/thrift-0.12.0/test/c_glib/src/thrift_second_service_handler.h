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

#ifndef _SECOND_SERVICE_HANDLER_H
#define _SECOND_SERVICE_HANDLER_H

#include <glib-object.h>
#include <stdio.h>

#include "../gen-c_glib/t_test_second_service.h"

G_BEGIN_DECLS

/* A handler that implements the TTestSecondServiceIf interface */

#define TYPE_SECOND_SERVICE_HANDLER (second_service_handler_get_type ())

#define SECOND_SERVICE_HANDLER(obj)                                \
  (G_TYPE_CHECK_INSTANCE_CAST ((obj),                           \
                               TYPE_SECOND_SERVICE_HANDLER,        \
                               SecondServiceHandler))
#define IS_SECOND_SERVICE_HANDLER(obj)                             \
  (G_TYPE_CHECK_INSTANCE_TYPE ((obj),                           \
                               TYPE_SECOND_SERVICE_HANDLER))
#define SECOND_SERVICE_HANDLER_CLASS(c)                    \
  (G_TYPE_CHECK_CLASS_CAST ((c),                        \
                            TYPE_SECOND_SERVICE_HANDLER,   \
                            SecondServiceHandlerClass))
#define IS_SECOND_SERVICE_HANDLER_CLASS(c)                 \
  (G_TYPE_CHECK_CLASS_TYPE ((c),                        \
                            TYPE_SECOND_SERVICE_HANDLER))
#define SECOND_SERVICE_HANDLER_GET_CLASS(obj)              \
  (G_TYPE_INSTANCE_GET_CLASS ((obj),                    \
                              TYPE_SECOND_SERVICE_HANDLER, \
                              SecondServiceHandlerClass))

typedef struct _SecondServiceHandler SecondServiceHandler;
typedef struct _SecondServiceHandlerClass SecondServiceHandlerClass;

struct _SecondServiceHandler {
  TTestSecondServiceHandler parent;
};

struct _SecondServiceHandlerClass {
  TTestSecondServiceHandlerClass parent;

};

/* Used by SECOND_SERVICE_HANDLER_GET_TYPE */
GType second_service_handler_get_type (void);

gboolean second_service_handler_blah_blah (TTestSecondServiceIf *iface, GError **error);
gboolean second_service_handler_secondtest_string          (TTestSecondServiceIf *iface, gchar ** _return, const gchar * thing, GError **error);

G_END_DECLS

#endif /* _SECOND_SERVICE_HANDLER_H */
