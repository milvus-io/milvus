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

  tsc --outDir $COMPILEDDIR --project $DIR/tsconfig.json
}
compile

testServer()
{
  echo "start server $1"
  RET=0
  node ${COMPILEDDIR}/server.js $1 &
  SERVERPID=$!
  sleep 1
  echo "start client $1"
  node ${COMPILEDDIR}/client.js $1 || RET=1
  kill -2 $SERVERPID || RET=1
  return $RET
}

#integration tests

testServer || TESTOK=1
testServer --promise || TESTOK=1

exit $TESTOK
