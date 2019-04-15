#! /bin/sh

DIR="$( cd "$( dirname "$0" )" && pwd )"

mkdir -p $DIR/../test-compiled

COMPILEDDIR="$(cd $DIR && cd ../test-compiled && pwd)"
export NODE_PATH="${DIR}:${DIR}/../../nodejs/lib:${NODE_PATH}"

compile()
{
  #generating thrift code
  ${DIR}/../../../compiler/cpp/thrift -o ${DIR} --gen js:node,ts ${DIR}/../../../test/ThriftTest.thrift
  ${DIR}/../../../compiler/cpp/thrift -o ${COMPILEDDIR} --gen js:node,ts ${DIR}/../../../test/ThriftTest.thrift
}
compile

node ${COMPILEDDIR}/client.js $*
