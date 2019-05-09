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

SUBDIRS = . 

THRIFT = $(top_builddir)/compiler/cpp/thrift

TESTDIR = Tests/Thrift.PublicInterfaces.Compile.Tests
GENDIR = $(TESTDIR)/gen-netcore

THRIFTCODE = \
			Thrift/Thrift.csproj \
			Thrift/ITAsyncProcessor.cs \
			Thrift/ITProcessorFactory.cs \
			Thrift/SingletonTProcessorFactory.cs \
			Thrift/TApplicationException.cs \
			Thrift/TBaseClient.cs \
			Thrift/TException.cs \
			Thrift/TMultiplexedProcessor.cs \
			Thrift/Collections/TCollections.cs \
			Thrift/Collections/THashSet.cs \
			Thrift/Properties/AssemblyInfo.cs \
			Thrift/Protocols/ITProtocolFactory.cs \
			Thrift/Protocols/TAbstractBase.cs \
			Thrift/Protocols/TBase.cs \
			Thrift/Protocols/TBinaryProtocol.cs \
			Thrift/Protocols/TCompactProtocol.cs \
			Thrift/Protocols/TJSONProtocol.cs \
			Thrift/Protocols/TMultiplexedProtocol.cs \
			Thrift/Protocols/TProtocol.cs \
			Thrift/Protocols/TProtocolDecorator.cs \
			Thrift/Protocols/TProtocolException.cs \
			Thrift/Protocols/Entities/TField.cs \
			Thrift/Protocols/Entities/TList.cs \
			Thrift/Protocols/Entities/TMap.cs \
			Thrift/Protocols/Entities/TMessage.cs \
			Thrift/Protocols/Entities/TMessageType.cs \
			Thrift/Protocols/Entities/TSet.cs \
			Thrift/Protocols/Entities/TStruct.cs \
			Thrift/Protocols/Entities/TType.cs \
			Thrift/Protocols/Utilities/TBase64Utils.cs \
			Thrift/Protocols/Utilities/TProtocolUtil.cs \
			Thrift/Server/AsyncBaseServer.cs \
			Thrift/Server/TBaseServer.cs \
			Thrift/Server/TServerEventHandler.cs \
			Thrift/Transports/TClientTransport.cs \
			Thrift/Transports/TServerTransport.cs \
			Thrift/Transports/TTransportException.cs \
			Thrift/Transports/TTransportFactory.cs \
			Thrift/Transports/Client/TBufferedClientTransport.cs \
			Thrift/Transports/Client/TFramedClientTransport.cs \
			Thrift/Transports/Client/THttpClientTransport.cs \
			Thrift/Transports/Client/TMemoryBufferClientTransport.cs \
			Thrift/Transports/Client/TNamedPipeClientTransport.cs \
			Thrift/Transports/Client/TSocketClientTransport.cs \
			Thrift/Transports/Client/TStreamClientTransport.cs \
			Thrift/Transports/Client/TTlsSocketClientTransport.cs \
			Thrift/Transports/Server/THttpServerTransport.cs \
			Thrift/Transports/Server/TNamedPipeServerTransport.cs \
			Thrift/Transports/Server/TServerFramedTransport.cs \
			Thrift/Transports/Server/TServerSocketTransport.cs \
			Thrift/Transports/Server/TTlsServerSocketTransport.cs 

all-local: \
	Thrift.dll

Thrift.dll: $(THRIFTCODE)
	$(MKDIR_P) $(GENDIR)
	$(THRIFT)  -gen netcore:wcf   -r  -out $(GENDIR)  $(TESTDIR)/CassandraTest.thrift
	$(THRIFT)  -gen netcore:wcf   -r  -out $(GENDIR)  $(top_srcdir)/test/ThriftTest.thrift
	$(THRIFT)  -gen netcore:wcf   -r  -out $(GENDIR)  $(top_srcdir)/contrib/fb303/if/fb303.thrift
	$(DOTNETCORE) --info
	$(DOTNETCORE) restore
	$(DOTNETCORE) build

clean-local:
	$(RM) Thrift.dll
	$(RM) -r $(GENDIR)
	$(RM) -r Thrift/bin
	$(RM) -r Thrift/obj
	$(RM) -r Tests/Thrift.PublicInterfaces.Compile.Tests/bin
	$(RM) -r Tests/Thrift.PublicInterfaces.Compile.Tests/obj

EXTRA_DIST = \
			 $(THRIFTCODE) \
			 Thrift.sln \
			 Tests \
			 README.md
			 
