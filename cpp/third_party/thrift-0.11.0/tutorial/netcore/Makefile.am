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

GENDIR = Interfaces/gen-netcore

# Due to a known issue with "dotnet restore" the Thrift.dll dependency cannot be resolved from cmdline.
# The problem does NOT affect Visual Studio builds, only cmdline. 
# - For details see https://github.com/dotnet/cli/issues/3199 and related tickets.
# - Workaround is to temporarily copy the Thrift project into the solution
COPYCMD = cp  -u -p -r
	

THRIFTCODE = \
			Interfaces/Properties/AssemblyInfo.cs \
			Client/Properties/AssemblyInfo.cs \
			Client/Program.cs \
			Server/Properties/AssemblyInfo.cs \
			Server/Program.cs 

all-local: \
	Client.exe

Client.exe: $(THRIFTCODE)
	$(MKDIR_P) $(GENDIR)
	$(THRIFT)  -gen netcore:wcf   -r  -out $(GENDIR)  $(top_srcdir)/tutorial/tutorial.thrift
	$(DOTNETCORE) --info
	$(DOTNETCORE) restore
	$(DOTNETCORE) build

clean-local:
	$(RM) Client.exe
	$(RM) Server.exe
	$(RM) Interfaces.dll
	$(RM) -r $(GENDIR)
	$(RM) -r Client/bin
	$(RM) -r Client/obj
	$(RM) -r Server/bin
	$(RM) -r Server/obj
	$(RM) -r Interfaces/bin
	$(RM) -r Interfaces/obj

EXTRA_DIST = \
			 $(THRIFTCODE) \
			 Tutorial.sln \
			 Interfaces/Interfaces.csproj \
			 Client/Client.csproj \
			 Client/ThriftTest.pfx \
			 Client/Properties/launchSettings.json \
			 Server/Server.csproj \
			 Server/ThriftTest.pfx \
			 Server/Properties/launchSettings.json \
			 build.cmd \
			 build.sh \
			 README.md
			 
