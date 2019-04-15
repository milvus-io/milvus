THRIFT_DIR =../..
INCS_DIRS  =-I${THRIFT_DIR}/lib/cpp/src -I.
THRIFT_LIB =${THRIFT_DIR}/lib/cpp/.libs/libthrift.a
BOOST_LIB  =
CPP_DEFS   =
CPP_OPTS   =-Wall -O2

GEN_SRC    = gen-cpp/SampleService.cpp  \
             gen-cpp/Sample_types.cpp   \
             gen-cpp/SampleCallback.cpp \
             gen-cpp/Sample_constants.cpp
GEN_INC    = -Igen-cpp

default: server client
.PHONY: server client

server:
	g++ ${CPP_OPTS} ${CPP_DEFS} -o server/server ${GEN_INC} ${INCS_DIRS} server/server.cpp ${GEN_SRC} ${THRIFT_LIB} ${BOOST_LIB}

client:
	g++ ${CPP_OPTS} ${CPP_DEFS} -o client/client ${GEN_INC} ${INCS_DIRS} client/client.cpp ${GEN_SRC} ${THRIFT_LIB} ${BOOST_LIB}

clean:
	$(RM) -r client/client server/server

