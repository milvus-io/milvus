#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#


if WITH_TESTS
SUBDIRS = test
endif

if WITH_PHP_EXTENSION
%.so:
	cd src/ext/thrift_protocol/ && $(MAKE)

phpconfdir=$(PHP_CONFIG_PREFIX)
phpconf_DATA=thrift_protocol.ini

phpmoduledir = `php-config --extension-dir`
phpmodule_SCRIPTS = src/ext/thrift_protocol/modules/thrift_protocol.so

distclean-local:
	cd $(phpmodule_SCRIPTS) && $(PHPIZE) --clean

endif

phpdir = $(PHP_PREFIX)/
php_DATA = \
  lib/TMultiplexedProcessor.php

phpbasedir = $(phpdir)/Base
phpbase_DATA = \
  lib/Base/TBase.php

phpclassloaderdir = $(phpdir)/ClassLoader
phpclassloader_DATA = \
  lib/ClassLoader/ThriftClassLoader.php

phpexceptiondir = $(phpdir)/Exception
phpexception_DATA = \
  lib/Exception/TApplicationException.php \
  lib/Exception/TException.php \
  lib/Exception/TProtocolException.php \
  lib/Exception/TTransportException.php

phpfactorydir = $(phpdir)/Factory
phpfactory_DATA = \
  lib/Factory/TBinaryProtocolFactory.php \
  lib/Factory/TCompactProtocolFactory.php \
  lib/Factory/TJSONProtocolFactory.php \
  lib/Factory/TProtocolFactory.php \
  lib/Factory/TStringFuncFactory.php \
  lib/Factory/TTransportFactory.php

phpprotocoldir = $(phpdir)/Protocol
phpprotocol_DATA = \
  lib/Protocol/TBinaryProtocolAccelerated.php \
  lib/Protocol/TBinaryProtocol.php \
  lib/Protocol/TCompactProtocol.php \
  lib/Protocol/TJSONProtocol.php \
  lib/Protocol/TMultiplexedProtocol.php \
  lib/Protocol/TProtocol.php \
  lib/Protocol/TProtocolDecorator.php \
  lib/Protocol/TSimpleJSONProtocol.php

phpprotocoljsondir = $(phpprotocoldir)/JSON
phpprotocoljson_DATA = \
  lib/Protocol/JSON/BaseContext.php \
  lib/Protocol/JSON/ListContext.php \
  lib/Protocol/JSON/LookaheadReader.php \
  lib/Protocol/JSON/PairContext.php

phpprotocolsimplejsondir = $(phpprotocoldir)/SimpleJSON
phpprotocolsimplejson_DATA = \
  lib/Protocol/SimpleJSON/CollectionMapKeyException.php \
  lib/Protocol/SimpleJSON/Context.php \
  lib/Protocol/SimpleJSON/ListContext.php \
  lib/Protocol/SimpleJSON/MapContext.php \
  lib/Protocol/SimpleJSON/StructContext.php

phpserializerdir = $(phpdir)/Serializer
phpserializer_DATA = \
  lib/Serializer/TBinarySerializer.php

phpserverdir = $(phpdir)/Server
phpserver_DATA = \
  lib/Server/TServerSocket.php \
  lib/Server/TForkingServer.php \
  lib/Server/TServer.php \
  lib/Server/TServerTransport.php \
  lib/Server/TSimpleServer.php

phpstringfuncdir = $(phpdir)/StringFunc
phpstringfunc_DATA = \
  lib/StringFunc/Mbstring.php \
  lib/StringFunc/Core.php \
  lib/StringFunc/TStringFunc.php

phptransportdir = $(phpdir)/Transport
phptransport_DATA = \
  lib/Transport/TBufferedTransport.php \
  lib/Transport/TCurlClient.php \
  lib/Transport/TFramedTransport.php \
  lib/Transport/THttpClient.php \
  lib/Transport/TMemoryBuffer.php \
  lib/Transport/TNullTransport.php \
  lib/Transport/TPhpStream.php \
  lib/Transport/TSocket.php \
  lib/Transport/TSocketPool.php \
  lib/Transport/TTransport.php

phptypedir = $(phpdir)/Type
phptype_DATA = \
  lib/Type/TMessageType.php \
  lib/Type/TType.php \
  lib/Type/TConstant.php

EXTRA_DIST = \
  lib \
  src/autoload.php \
  src/ext/thrift_protocol/config.m4 \
  src/ext/thrift_protocol/config.w32 \
  src/ext/thrift_protocol/php_thrift_protocol.cpp \
  src/ext/thrift_protocol/php_thrift_protocol.h \
  src/ext/thrift_protocol/run-tests.php \
  src/Thrift.php \
  src/TStringUtils.php \
  coding_standards.md \
  thrift_protocol.ini \
  README.apache.md \
  README.md

MAINTAINERCLEANFILES = \
  Makefile \
  Makefile.in

