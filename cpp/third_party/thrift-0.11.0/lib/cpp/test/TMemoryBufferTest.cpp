/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <boost/test/auto_unit_test.hpp>
#include <iostream>
#include <climits>
#include <vector>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/stdcxx.h>
#include <thrift/transport/TBufferTransports.h>
#include "gen-cpp/ThriftTest_types.h"

BOOST_AUTO_TEST_SUITE(TMemoryBufferTest)

using apache::thrift::protocol::TBinaryProtocol;
using apache::thrift::transport::TMemoryBuffer;
using apache::thrift::transport::TTransportException;
using apache::thrift::stdcxx::shared_ptr;
using std::cout;
using std::endl;
using std::string;

BOOST_AUTO_TEST_CASE(test_read_write_grow) {
  // Added to test the fix for THRIFT-1248
  TMemoryBuffer uut;
  const int maxSize = 65536;
  uint8_t verify[maxSize];
  std::vector<uint8_t> buf;
  buf.resize(maxSize);

  for (uint32_t i = 0; i < maxSize; ++i) {
    buf[i] = static_cast<uint8_t>(i);
  }

  for (uint32_t i = 1; i < maxSize; i *= 2) {
    uut.write(&buf[0], i);
  }

  for (uint32_t i = 1; i < maxSize; i *= 2) {
    uut.read(verify, i);
    BOOST_CHECK_EQUAL(0, ::memcmp(verify, &buf[0], i));
  }
}

BOOST_AUTO_TEST_CASE(test_roundtrip) {
  shared_ptr<TMemoryBuffer> strBuffer(new TMemoryBuffer());
  shared_ptr<TBinaryProtocol> binaryProtcol(new TBinaryProtocol(strBuffer));

  thrift::test::Xtruct a;
  a.i32_thing = 10;
  a.i64_thing = 30;
  a.string_thing = "holla back a";

  a.write(binaryProtcol.get());
  std::string serialized = strBuffer->getBufferAsString();

  shared_ptr<TMemoryBuffer> strBuffer2(new TMemoryBuffer());
  shared_ptr<TBinaryProtocol> binaryProtcol2(new TBinaryProtocol(strBuffer2));

  strBuffer2->resetBuffer((uint8_t*)serialized.data(), static_cast<uint32_t>(serialized.length()));
  thrift::test::Xtruct a2;
  a2.read(binaryProtcol2.get());

  BOOST_CHECK(a == a2);
}

BOOST_AUTO_TEST_CASE(test_readAppendToString) {
  string* str1 = new string("abcd1234");
  TMemoryBuffer buf((uint8_t*)str1->data(),
                    static_cast<uint32_t>(str1->length()),
                    TMemoryBuffer::COPY);

  string str3 = "wxyz", str4 = "6789";
  buf.readAppendToString(str3, 4);
  buf.readAppendToString(str4, INT_MAX);

  BOOST_CHECK(str3 == "wxyzabcd");
  BOOST_CHECK(str4 == "67891234");
}

BOOST_AUTO_TEST_CASE(test_exceptions) {
  char data[] = "foo\0bar";

  TMemoryBuffer buf1((uint8_t*)data, 7, TMemoryBuffer::OBSERVE);
  string str = buf1.getBufferAsString();
  BOOST_CHECK(str.length() == 7);

  buf1.resetBuffer();

  BOOST_CHECK_THROW(buf1.write((const uint8_t*)"foo", 3), TTransportException);

  TMemoryBuffer buf2((uint8_t*)data, 7, TMemoryBuffer::COPY);
  BOOST_CHECK_NO_THROW(buf2.write((const uint8_t*)"bar", 3));
}

BOOST_AUTO_TEST_CASE(test_default_maximum_buffer_size)
{
  BOOST_CHECK_EQUAL(std::numeric_limits<uint32_t>::max(), TMemoryBuffer().getMaxBufferSize());
}

BOOST_AUTO_TEST_CASE(test_default_buffer_size)
{
  BOOST_CHECK_EQUAL(1024, TMemoryBuffer().getBufferSize());
}

BOOST_AUTO_TEST_CASE(test_error_set_max_buffer_size_too_small)
{
  TMemoryBuffer buf;
  BOOST_CHECK_THROW(buf.setMaxBufferSize(buf.getBufferSize() - 1), TTransportException);
}

BOOST_AUTO_TEST_CASE(test_maximum_buffer_size)
{
  TMemoryBuffer buf;
  buf.setMaxBufferSize(8192);
  std::vector<uint8_t> small_buff(1);

  for (size_t i = 0; i < 8192; ++i)
  {
    buf.write(&small_buff[0], 1);
  }

  BOOST_CHECK_THROW(buf.write(&small_buff[0], 1), TTransportException);
}

BOOST_AUTO_TEST_CASE(test_memory_buffer_to_get_sizeof_objects)
{
  // This is a demonstration of how to use TMemoryBuffer to determine
  // the serialized size of a thrift object in the Binary protocol.
  // See THRIFT-3480

  shared_ptr<TMemoryBuffer> memBuffer(new TMemoryBuffer());
  shared_ptr<TBinaryProtocol> binaryProtcol(new TBinaryProtocol(memBuffer));

  thrift::test::Xtruct object;
  object.i32_thing = 10;
  object.i64_thing = 30;
  object.string_thing = "who's your daddy?";

  uint32_t size = object.write(binaryProtcol.get());
  BOOST_CHECK_EQUAL(47, size);
}

BOOST_AUTO_TEST_SUITE_END()
