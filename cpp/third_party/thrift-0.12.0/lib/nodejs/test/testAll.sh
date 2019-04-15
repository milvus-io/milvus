#! /bin/sh

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

if [ -n "${1}" ]; then
  COVER=${1};
fi

DIR="$( cd "$( dirname "$0" )" && pwd )"

ISTANBUL="$DIR/../../../node_modules/istanbul/lib/cli.js"

REPORT_PREFIX="${DIR}/../coverage/report"

COUNT=0

export NODE_PATH="${DIR}:${DIR}/../lib:${NODE_PATH}"

testServer()
{
  echo "  [ECMA $1] Testing $2 Client/Server with protocol $3 and transport $4 $5";
  RET=0
  if [ -n "${COVER}" ]; then
    ${ISTANBUL} cover ${DIR}/server.js --dir ${REPORT_PREFIX}${COUNT} --handle-sigint -- --type $2 -p $3 -t $4 $5 &
    COUNT=$((COUNT+1))
  else
    node ${DIR}/server.js --${1} --type $2 -p $3 -t $4 $5 &
  fi
  SERVERPID=$!
  sleep 0.1
  if [ -n "${COVER}" ]; then
    ${ISTANBUL} cover ${DIR}/client.js --dir ${REPORT_PREFIX}${COUNT} -- --${1} --type $2 -p $3 -t $4 $5 || RET=1
    COUNT=$((COUNT+1))
  else
    node ${DIR}/client.js --${1} --type $2 -p $3 -t $4 $5 || RET=1
  fi
  kill -2 $SERVERPID || RET=1
  wait $SERVERPID
  return $RET
}


TESTOK=0

#generating thrift code

${DIR}/../../../compiler/cpp/thrift -o ${DIR} --gen js:node ${DIR}/../../../test/ThriftTest.thrift
${DIR}/../../../compiler/cpp/thrift -o ${DIR} --gen js:node ${DIR}/../../../test/JsDeepConstructorTest.thrift
mkdir ${DIR}/gen-nodejs-es6
${DIR}/../../../compiler/cpp/thrift -out ${DIR}/gen-nodejs-es6 --gen js:node,es6 ${DIR}/../../../test/ThriftTest.thrift
${DIR}/../../../compiler/cpp/thrift -out ${DIR}/gen-nodejs-es6 --gen js:node,es6 ${DIR}/../../../test/JsDeepConstructorTest.thrift

#unit tests

node ${DIR}/binary.test.js || TESTOK=1
node ${DIR}/deep-constructor.test.js || TESTOK=1

#integration tests

for type in tcp multiplex websocket http
do
  for protocol in compact binary json
  do
    for transport in buffered framed
    do
      for ecma_version in es5 es6
      do
        testServer $ecma_version $type $protocol $transport || TESTOK=1
        testServer $ecma_version $type $protocol $transport --ssl || TESTOK=1
        testServer $ecma_version $type $protocol $transport --callback || TESTOK=1
      done
    done
  done
done


if [ -n "${COVER}" ]; then
  ${ISTANBUL} report --dir "${DIR}/../coverage" --include "${DIR}/../coverage/report*/coverage.json" lcov cobertura html
  rm -r ${DIR}/../coverage/report*/*
  rmdir ${DIR}/../coverage/report*
fi

exit $TESTOK
