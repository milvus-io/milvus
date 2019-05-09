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

GENDIR = ThriftTest/gen-netcore

THRIFTCODE = \
			ThriftTest/TestClient.cs \
			ThriftTest/TestServer.cs \
			ThriftTest/Properties/AssemblyInfo.cs \
			ThriftTest/Program.cs 

all-local: \
	ThriftTest/stage/ThriftTest.dll

ThriftTest/stage/ThriftTest.dll: $(THRIFTCODE)
	$(MKDIR_P) $(GENDIR)
	$(THRIFT)  -gen netcore:wcf   -r  -out $(GENDIR)  $(top_srcdir)/test/ThriftTest.thrift
	$(DOTNETCORE) --info
	$(DOTNETCORE) restore
	$(DOTNETCORE) build

precross: \
	ThriftTest/stage/ThriftTest.dll

clean-local:
	$(RM) ThriftTest.exe
	$(RM) -r $(GENDIR)
	$(RM) -r ThriftTest/bin
	$(RM) -r ThriftTest/obj

EXTRA_DIST = \
			 $(THRIFTCODE) \
			 ThriftTest.sln \
			 ThriftTest/ThriftTest.csproj \
			 ThriftTest/Properties/launchSettings.json \
			 build.cmd \
			 build.sh \
			 README.md
			 
