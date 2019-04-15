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

# Default target is everything

ifndef thrift_home
thrift_home=../../
endif #thrift_home

target: all

ifndef boost_home
boost_home=/usr/local/include/boost-1_33_1
endif #boost_home
target: all

include_paths = $(thrift_home)/lib/cpp/src \
		$(boost_home)

include_flags = $(patsubst %,-I%, $(include_paths))

# Tools
ifndef THRIFT
THRIFT = ../../compiler/cpp/thrift
endif # THRIFT

CC     = g++
LD     = g++

# Compiler flags
LFL   =  -L$(thrift_home)/lib/cpp/.libs -lthrift
CCFL  = -Wall -O3 -g -I./gen-cpp $(include_flags)
CFL   = $(CCFL) $(LFL)

all: server client

stubs: ThreadsTest.thrift
	$(THRIFT) --gen cpp --gen py ThreadsTest.thrift

server: stubs
	$(CC) -o ThreadsServer $(CFL) ThreadsServer.cpp ./gen-cpp/ThreadsTest.cpp ./gen-cpp/ThreadsTest_types.cpp

client: stubs
	$(CC) -o ThreadsClient $(CFL) ThreadsClient.cpp ./gen-cpp/ThreadsTest.cpp ./gen-cpp/ThreadsTest_types.cpp

clean:
	$(RM) -r *.o ThreadsServer ThreadsClient gen-cpp gen-py
