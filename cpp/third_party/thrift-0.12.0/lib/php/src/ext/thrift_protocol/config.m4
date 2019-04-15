dnl Copyright (C) 2009 Facebook
dnl Copying and distribution of this file, with or without modification,
dnl are permitted in any medium without royalty provided the copyright
dnl notice and this notice are preserved.
dnl
dnl Licensed to the Apache Software Foundation (ASF) under one
dnl or more contributor license agreements. See the NOTICE file
dnl distributed with this work for additional information
dnl regarding copyright ownership. The ASF licenses this file
dnl to you under the Apache License, Version 2.0 (the
dnl "License"); you may not use this file except in compliance
dnl with the License. You may obtain a copy of the License at
dnl
dnl  http://www.apache.org/licenses/LICENSE-2.0
dnl
dnl Unless required by applicable law or agreed to in writing,
dnl software distributed under the License is distributed on an
dnl "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
dnl KIND, either express or implied. See the License for the
dnl specific language governing permissions and limitations
dnl under the License.

PHP_ARG_ENABLE(thrift_protocol, whether to enable the thrift_protocol extension,
[  --enable-thrift_protocol	Enable the thrift_protocol extension])

if test "$PHP_THRIFT_PROTOCOL" != "no"; then
  PHP_REQUIRE_CXX()
  PHP_ADD_LIBRARY_WITH_PATH(stdc++, "", THRIFT_PROTOCOL_SHARED_LIBADD)
  PHP_SUBST(THRIFT_PROTOCOL_SHARED_LIBADD)
  CXXFLAGS="$CXXFLAGS -std=c++11"

  PHP_NEW_EXTENSION(thrift_protocol, php_thrift_protocol.cpp, $ext_shared)
fi

