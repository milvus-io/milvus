# encoding: UTF-8
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

require 'spec_helper'

describe Thrift::Bytes do
  if RUBY_VERSION >= '1.9'
    describe '.empty_byte_buffer' do
      it 'should create an empty buffer' do
        b = Thrift::Bytes.empty_byte_buffer
        expect(b.length).to eq(0)
        expect(b.encoding).to eq(Encoding::BINARY)
      end

      it 'should create an empty buffer of given size' do
        b = Thrift::Bytes.empty_byte_buffer 2
        expect(b.length).to eq(2)
        expect(b.getbyte(0)).to eq(0)
        expect(b.getbyte(1)).to eq(0)
        expect(b.encoding).to eq(Encoding::BINARY)
      end
    end

    describe '.force_binary_encoding' do
      it 'should change encoding' do
        e = 'STRING'.encode('UTF-8')
        expect(e.encoding).not_to eq(Encoding::BINARY)
        a = Thrift::Bytes.force_binary_encoding e
        expect(a.encoding).to eq(Encoding::BINARY)
      end
    end

    describe '.get_string_byte' do
      it 'should get the byte at index' do
        s = "\x41\x42"
        expect(Thrift::Bytes.get_string_byte(s, 0)).to eq(0x41)
        expect(Thrift::Bytes.get_string_byte(s, 1)).to eq(0x42)
      end
    end

    describe '.set_string_byte' do
      it 'should set byte value at index' do
        s = "\x41\x42"
        Thrift::Bytes.set_string_byte(s, 0, 0x43)
        expect(s.getbyte(0)).to eq(0x43)
        expect(s).to eq('CB')
      end
    end

    describe '.convert_to_utf8_byte_buffer' do
      it 'should convert UTF-8 String to byte buffer' do
        e = "\u20AC".encode('UTF-8') # a string with euro sign character U+20AC
        expect(e.length).to eq(1)

        a = Thrift::Bytes.convert_to_utf8_byte_buffer e
        expect(a.encoding).to eq(Encoding::BINARY)
        expect(a.length).to eq(3)
        expect(a.unpack('C*')).to eq([0xE2, 0x82, 0xAC])
      end

      it 'should convert ISO-8859-15 String to UTF-8 byte buffer' do
        # Assumptions
        e = "\u20AC".encode('ISO-8859-15') # a string with euro sign character U+20AC, then converted to ISO-8859-15
        expect(e.length).to eq(1)
        expect(e.unpack('C*')).to eq([0xA4]) # euro sign is a different code point in ISO-8859-15

        a = Thrift::Bytes.convert_to_utf8_byte_buffer e
        expect(a.encoding).to eq(Encoding::BINARY)
        expect(a.length).to eq(3)
        expect(a.unpack('C*')).to eq([0xE2, 0x82, 0xAC])
      end
    end

    describe '.convert_to_string' do
      it 'should convert UTF-8 byte buffer to a UTF-8 String' do
        e = [0xE2, 0x82, 0xAC].pack("C*")
        expect(e.encoding).to eq(Encoding::BINARY)
        a = Thrift::Bytes.convert_to_string e
        expect(a.encoding).to eq(Encoding::UTF_8)
        expect(a).to eq("\u20AC")
      end
    end

  else # RUBY_VERSION
    describe '.empty_byte_buffer' do
      it 'should create an empty buffer' do
        b = Thrift::Bytes.empty_byte_buffer
        expect(b.length).to eq(0)
      end

      it 'should create an empty buffer of given size' do
        b = Thrift::Bytes.empty_byte_buffer 2
        expect(b.length).to eq(2)
        expect(b[0]).to eq(0)
        expect(b[1]).to eq(0)
      end
    end

    describe '.force_binary_encoding' do
      it 'should be a no-op' do
        e = 'STRING'
        a = Thrift::Bytes.force_binary_encoding e
        expect(a).to eq(e)
        expect(a).to be(e)
      end
    end

    describe '.get_string_byte' do
      it 'should get the byte at index' do
        s = "\x41\x42"
        expect(Thrift::Bytes.get_string_byte(s, 0)).to eq(0x41)
        expect(Thrift::Bytes.get_string_byte(s, 1)).to eq(0x42)
      end
    end

    describe '.set_string_byte' do
      it 'should set byte value at index' do
        s = "\x41\x42"
        Thrift::Bytes.set_string_byte(s, 0, 0x43)
        expect(s[0]).to eq(0x43)
        expect(s).to eq('CB')
      end
    end

    describe '.convert_to_utf8_byte_buffer' do
      it 'should be a no-op' do
        e = 'STRING'
        a = Thrift::Bytes.convert_to_utf8_byte_buffer e
        expect(a).to eq(e)
        expect(a).to be(e)
      end
    end

    describe '.convert_to_string' do
      it 'should be a no-op' do
        e = 'STRING'
        a = Thrift::Bytes.convert_to_string e
        expect(a).to eq(e)
        expect(a).to be(e)
      end
    end
  end
end
