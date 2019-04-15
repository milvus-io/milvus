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

describe 'JsonProtocol' do

  describe Thrift::JsonProtocol do
    before(:each) do
      @trans = Thrift::MemoryBufferTransport.new
      @prot = Thrift::JsonProtocol.new(@trans)
    end

    it "should write json escaped char" do
      @prot.write_json_escape_char("\n")
      expect(@trans.read(@trans.available)).to eq('\u000a')

      @prot.write_json_escape_char(" ")
      expect(@trans.read(@trans.available)).to eq('\u0020')
    end

    it "should write json char" do
      @prot.write_json_char("\n")
      expect(@trans.read(@trans.available)).to eq('\\n')

      @prot.write_json_char(" ")
      expect(@trans.read(@trans.available)).to eq(' ')

      @prot.write_json_char("\\")
      expect(@trans.read(@trans.available)).to eq("\\\\")

      @prot.write_json_char("@")
      expect(@trans.read(@trans.available)).to eq('@')
    end

    it "should write json string" do
      @prot.write_json_string("this is a \\ json\nstring")
      expect(@trans.read(@trans.available)).to eq("\"this is a \\\\ json\\nstring\"")
    end

    it "should write json base64" do
      @prot.write_json_base64("this is a base64 string")
      expect(@trans.read(@trans.available)).to eq("\"dGhpcyBpcyBhIGJhc2U2NCBzdHJpbmc=\"")
    end

    it "should write json integer" do
      @prot.write_json_integer(45)
      expect(@trans.read(@trans.available)).to eq("45")

      @prot.write_json_integer(33000)
      expect(@trans.read(@trans.available)).to eq("33000")

      @prot.write_json_integer(3000000000)
      expect(@trans.read(@trans.available)).to eq("3000000000")

      @prot.write_json_integer(6000000000)
      expect(@trans.read(@trans.available)).to eq("6000000000")
    end

    it "should write json double" do
      @prot.write_json_double(12.3)
      expect(@trans.read(@trans.available)).to eq("12.3")

      @prot.write_json_double(-3.21)
      expect(@trans.read(@trans.available)).to eq("-3.21")

      @prot.write_json_double(((+1.0/0.0)/(+1.0/0.0)))
      expect(@trans.read(@trans.available)).to eq("\"NaN\"")

      @prot.write_json_double((+1.0/0.0))
      expect(@trans.read(@trans.available)).to eq("\"Infinity\"")

      @prot.write_json_double((-1.0/0.0))
      expect(@trans.read(@trans.available)).to eq("\"-Infinity\"")
    end

    it "should write json object start" do
      @prot.write_json_object_start
      expect(@trans.read(@trans.available)).to eq("{")
    end

    it "should write json object end" do
      @prot.write_json_object_end
      expect(@trans.read(@trans.available)).to eq("}")
    end

    it "should write json array start" do
      @prot.write_json_array_start
      expect(@trans.read(@trans.available)).to eq("[")
    end

    it "should write json array end" do
      @prot.write_json_array_end
      expect(@trans.read(@trans.available)).to eq("]")
    end

    it "should write message begin" do
      @prot.write_message_begin("name", 12, 32)
      expect(@trans.read(@trans.available)).to eq("[1,\"name\",12,32")
    end

    it "should write message end" do
      @prot.write_message_end
      expect(@trans.read(@trans.available)).to eq("]")
    end

    it "should write struct begin" do
      @prot.write_struct_begin("name")
      expect(@trans.read(@trans.available)).to eq("{")
    end

    it "should write struct end" do
      @prot.write_struct_end
      expect(@trans.read(@trans.available)).to eq("}")
    end

    it "should write field begin" do
      @prot.write_field_begin("name", Thrift::Types::STRUCT, 32)
      expect(@trans.read(@trans.available)).to eq("32{\"rec\"")
    end

    it "should write field end" do
      @prot.write_field_end
      expect(@trans.read(@trans.available)).to eq("}")
    end

    it "should write field stop" do
      @prot.write_field_stop
      expect(@trans.read(@trans.available)).to eq("")
    end

    it "should write map begin" do
      @prot.write_map_begin(Thrift::Types::STRUCT, Thrift::Types::LIST, 32)
      expect(@trans.read(@trans.available)).to eq("[\"rec\",\"lst\",32,{")
    end

    it "should write map end" do
      @prot.write_map_end
      expect(@trans.read(@trans.available)).to eq("}]")
    end

    it "should write list begin" do
      @prot.write_list_begin(Thrift::Types::STRUCT, 32)
      expect(@trans.read(@trans.available)).to eq("[\"rec\",32")
    end

    it "should write list end" do
      @prot.write_list_end
      expect(@trans.read(@trans.available)).to eq("]")
    end

    it "should write set begin" do
      @prot.write_set_begin(Thrift::Types::STRUCT, 32)
      expect(@trans.read(@trans.available)).to eq("[\"rec\",32")
    end

    it "should write set end" do
      @prot.write_set_end
      expect(@trans.read(@trans.available)).to eq("]")
    end

    it "should write bool" do
      @prot.write_bool(true)
      expect(@trans.read(@trans.available)).to eq("1")

      @prot.write_bool(false)
      expect(@trans.read(@trans.available)).to eq("0")
    end

    it "should write byte" do
      @prot.write_byte(100)
      expect(@trans.read(@trans.available)).to eq("100")
    end

    it "should write i16" do
      @prot.write_i16(1000)
      expect(@trans.read(@trans.available)).to eq("1000")
    end

    it "should write i32" do
      @prot.write_i32(3000000000)
      expect(@trans.read(@trans.available)).to eq("3000000000")
    end

    it "should write i64" do
      @prot.write_i64(6000000000)
      expect(@trans.read(@trans.available)).to eq("6000000000")
    end

    it "should write double" do
      @prot.write_double(1.23)
      expect(@trans.read(@trans.available)).to eq("1.23")

      @prot.write_double(-32.1)
      expect(@trans.read(@trans.available)).to eq("-32.1")

      @prot.write_double(((+1.0/0.0)/(+1.0/0.0)))
      expect(@trans.read(@trans.available)).to eq("\"NaN\"")

      @prot.write_double((+1.0/0.0))
      expect(@trans.read(@trans.available)).to eq("\"Infinity\"")

      @prot.write_double((-1.0/0.0))
      expect(@trans.read(@trans.available)).to eq("\"-Infinity\"")
    end

    if RUBY_VERSION >= '1.9'
      it 'should write string' do
        @prot.write_string('this is a test string')
        a = @trans.read(@trans.available)
        expect(a).to eq('"this is a test string"'.force_encoding(Encoding::BINARY))
        expect(a.encoding).to eq(Encoding::BINARY)
      end

      it 'should write string with unicode characters' do
        @prot.write_string("this is a test string with unicode characters: \u20AC \u20AD")
        a = @trans.read(@trans.available)
        expect(a).to eq("\"this is a test string with unicode characters: \u20AC \u20AD\"".force_encoding(Encoding::BINARY))
        expect(a.encoding).to eq(Encoding::BINARY)
      end
    else
      it 'should write string' do
        @prot.write_string('this is a test string')
        expect(@trans.read(@trans.available)).to eq('"this is a test string"')
      end
    end

    it "should write binary" do
      @prot.write_binary("this is a base64 string")
      expect(@trans.read(@trans.available)).to eq("\"dGhpcyBpcyBhIGJhc2U2NCBzdHJpbmc=\"")
    end

    it "should write long binary" do
      @prot.write_binary((0...256).to_a.pack('C*'))
      expect(@trans.read(@trans.available)).to eq("\"AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5fYGFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6e3x9fn+AgYKDhIWGh4iJiouMjY6PkJGSk5SVlpeYmZqbnJ2en6ChoqOkpaanqKmqq6ytrq+wsbKztLW2t7i5uru8vb6/wMHCw8TFxsfIycrLzM3Oz9DR0tPU1dbX2Nna29zd3t/g4eLj5OXm5+jp6uvs7e7v8PHy8/T19vf4+fr7/P3+/w==\"")
    end

    it "should get type name for type id" do
      expect {@prot.get_type_name_for_type_id(Thrift::Types::STOP)}.to raise_error(NotImplementedError)
      expect {@prot.get_type_name_for_type_id(Thrift::Types::VOID)}.to raise_error(NotImplementedError)
      expect(@prot.get_type_name_for_type_id(Thrift::Types::BOOL)).to eq("tf")
      expect(@prot.get_type_name_for_type_id(Thrift::Types::BYTE)).to eq("i8")
      expect(@prot.get_type_name_for_type_id(Thrift::Types::DOUBLE)).to eq("dbl")
      expect(@prot.get_type_name_for_type_id(Thrift::Types::I16)).to eq("i16")
      expect(@prot.get_type_name_for_type_id(Thrift::Types::I32)).to eq("i32")
      expect(@prot.get_type_name_for_type_id(Thrift::Types::I64)).to eq("i64")
      expect(@prot.get_type_name_for_type_id(Thrift::Types::STRING)).to eq("str")
      expect(@prot.get_type_name_for_type_id(Thrift::Types::STRUCT)).to eq("rec")
      expect(@prot.get_type_name_for_type_id(Thrift::Types::MAP)).to eq("map")
      expect(@prot.get_type_name_for_type_id(Thrift::Types::SET)).to eq("set")
      expect(@prot.get_type_name_for_type_id(Thrift::Types::LIST)).to eq("lst")
    end

    it "should get type id for type name" do
      expect {@prot.get_type_id_for_type_name("pp")}.to raise_error(NotImplementedError)
      expect(@prot.get_type_id_for_type_name("tf")).to eq(Thrift::Types::BOOL)
      expect(@prot.get_type_id_for_type_name("i8")).to eq(Thrift::Types::BYTE)
      expect(@prot.get_type_id_for_type_name("dbl")).to eq(Thrift::Types::DOUBLE)
      expect(@prot.get_type_id_for_type_name("i16")).to eq(Thrift::Types::I16)
      expect(@prot.get_type_id_for_type_name("i32")).to eq(Thrift::Types::I32)
      expect(@prot.get_type_id_for_type_name("i64")).to eq(Thrift::Types::I64)
      expect(@prot.get_type_id_for_type_name("str")).to eq(Thrift::Types::STRING)
      expect(@prot.get_type_id_for_type_name("rec")).to eq(Thrift::Types::STRUCT)
      expect(@prot.get_type_id_for_type_name("map")).to eq(Thrift::Types::MAP)
      expect(@prot.get_type_id_for_type_name("set")).to eq(Thrift::Types::SET)
      expect(@prot.get_type_id_for_type_name("lst")).to eq(Thrift::Types::LIST)
    end

    it "should read json syntax char" do
      @trans.write('F')
      expect {@prot.read_json_syntax_char('G')}.to raise_error(Thrift::ProtocolException)
      @trans.write('H')
      @prot.read_json_syntax_char('H')
    end

    it "should read json escape char" do
      @trans.write('0054')
      expect(@prot.read_json_escape_char).to eq('T')

      @trans.write("\"\\\"\"")
      expect(@prot.read_json_string(false)).to eq("\"")

      @trans.write("\"\\\\\"")
      expect(@prot.read_json_string(false)).to eq("\\")

      @trans.write("\"\\/\"")
      expect(@prot.read_json_string(false)).to eq("\/")

      @trans.write("\"\\b\"")
      expect(@prot.read_json_string(false)).to eq("\b")

      @trans.write("\"\\f\"")
      expect(@prot.read_json_string(false)).to eq("\f")

      @trans.write("\"\\n\"")
      expect(@prot.read_json_string(false)).to eq("\n")

      @trans.write("\"\\r\"")
      expect(@prot.read_json_string(false)).to eq("\r")

      @trans.write("\"\\t\"")
      expect(@prot.read_json_string(false)).to eq("\t")
    end

    it "should read json string" do
      @trans.write("\"\\P")
      expect {@prot.read_json_string(false)}.to raise_error(Thrift::ProtocolException)

      @trans.write("\"this is a test string\"")
      expect(@prot.read_json_string).to eq("this is a test string")
    end

    it "should read json base64" do
      @trans.write("\"dGhpcyBpcyBhIHRlc3Qgc3RyaW5n\"")
      expect(@prot.read_json_base64).to eq("this is a test string")
    end

    it "should is json numeric" do
      expect(@prot.is_json_numeric("A")).to eq(false)
      expect(@prot.is_json_numeric("+")).to eq(true)
      expect(@prot.is_json_numeric("-")).to eq(true)
      expect(@prot.is_json_numeric(".")).to eq(true)
      expect(@prot.is_json_numeric("0")).to eq(true)
      expect(@prot.is_json_numeric("1")).to eq(true)
      expect(@prot.is_json_numeric("2")).to eq(true)
      expect(@prot.is_json_numeric("3")).to eq(true)
      expect(@prot.is_json_numeric("4")).to eq(true)
      expect(@prot.is_json_numeric("5")).to eq(true)
      expect(@prot.is_json_numeric("6")).to eq(true)
      expect(@prot.is_json_numeric("7")).to eq(true)
      expect(@prot.is_json_numeric("8")).to eq(true)
      expect(@prot.is_json_numeric("9")).to eq(true)
      expect(@prot.is_json_numeric("E")).to eq(true)
      expect(@prot.is_json_numeric("e")).to eq(true)
    end

    it "should read json numeric chars" do
      @trans.write("1.453E45T")
      expect(@prot.read_json_numeric_chars).to eq("1.453E45")
    end

    it "should read json integer" do
      @trans.write("1.45\"\"")
      expect {@prot.read_json_integer}.to raise_error(Thrift::ProtocolException)
      @prot.read_string

      @trans.write("1453T")
      expect(@prot.read_json_integer).to eq(1453)
    end

    it "should read json double" do
      @trans.write("1.45e3e01\"\"")
      expect {@prot.read_json_double}.to raise_error(Thrift::ProtocolException)
      @prot.read_string

      @trans.write("\"1.453e01\"")
      expect {@prot.read_json_double}.to raise_error(Thrift::ProtocolException)

      @trans.write("1.453e01\"\"")
      expect(@prot.read_json_double).to eq(14.53)
      @prot.read_string

      @trans.write("\"NaN\"")
      expect(@prot.read_json_double.nan?).to eq(true)

      @trans.write("\"Infinity\"")
      expect(@prot.read_json_double).to eq(+1.0/0.0)

      @trans.write("\"-Infinity\"")
      expect(@prot.read_json_double).to eq(-1.0/0.0)
    end

    it "should read json object start" do
      @trans.write("{")
      expect(@prot.read_json_object_start).to eq(nil)
    end

    it "should read json object end" do
      @trans.write("}")
      expect(@prot.read_json_object_end).to eq(nil)
    end

    it "should read json array start" do
      @trans.write("[")
      expect(@prot.read_json_array_start).to eq(nil)
    end

    it "should read json array end" do
      @trans.write("]")
      expect(@prot.read_json_array_end).to eq(nil)
    end

    it "should read_message_begin" do
      @trans.write("[2,")
      expect {@prot.read_message_begin}.to raise_error(Thrift::ProtocolException)

      @trans.write("[1,\"name\",12,32\"\"")
      expect(@prot.read_message_begin).to eq(["name", 12, 32])
    end

    it "should read message end" do
      @trans.write("]")
      expect(@prot.read_message_end).to eq(nil)
    end

    it "should read struct begin" do
      @trans.write("{")
      expect(@prot.read_struct_begin).to eq(nil)
    end

    it "should read struct end" do
      @trans.write("}")
      expect(@prot.read_struct_end).to eq(nil)
    end

    it "should read field begin" do
      @trans.write("1{\"rec\"")
      expect(@prot.read_field_begin).to eq([nil, 12, 1])
    end

    it "should read field end" do
      @trans.write("}")
      expect(@prot.read_field_end).to eq(nil)
    end

    it "should read map begin" do
      @trans.write("[\"rec\",\"lst\",2,{")
      expect(@prot.read_map_begin).to eq([12, 15, 2])
    end

    it "should read map end" do
      @trans.write("}]")
      expect(@prot.read_map_end).to eq(nil)
    end

    it "should read list begin" do
      @trans.write("[\"rec\",2\"\"")
      expect(@prot.read_list_begin).to eq([12, 2])
    end

    it "should read list end" do
      @trans.write("]")
      expect(@prot.read_list_end).to eq(nil)
    end

    it "should read set begin" do
      @trans.write("[\"rec\",2\"\"")
      expect(@prot.read_set_begin).to eq([12, 2])
    end

    it "should read set end" do
      @trans.write("]")
      expect(@prot.read_set_end).to eq(nil)
    end

    it "should read bool" do
      @trans.write("0\"\"")
      expect(@prot.read_bool).to eq(false)
      @prot.read_string

      @trans.write("1\"\"")
      expect(@prot.read_bool).to eq(true)
    end

    it "should read byte" do
      @trans.write("60\"\"")
      expect(@prot.read_byte).to eq(60)
    end

    it "should read i16" do
      @trans.write("1000\"\"")
      expect(@prot.read_i16).to eq(1000)
    end

    it "should read i32" do
      @trans.write("3000000000\"\"")
      expect(@prot.read_i32).to eq(3000000000)
    end

    it "should read i64" do
      @trans.write("6000000000\"\"")
      expect(@prot.read_i64).to eq(6000000000)
    end

    it "should read double" do
      @trans.write("12.23\"\"")
      expect(@prot.read_double).to eq(12.23)
    end

    if RUBY_VERSION >= '1.9'
      it 'should read string' do
        @trans.write('"this is a test string"'.force_encoding(Encoding::BINARY))
        a = @prot.read_string
        expect(a).to eq('this is a test string')
        expect(a.encoding).to eq(Encoding::UTF_8)
      end

      it 'should read string with unicode characters' do
        @trans.write('"this is a test string with unicode characters: \u20AC \u20AD"'.force_encoding(Encoding::BINARY))
        a = @prot.read_string
        expect(a).to eq("this is a test string with unicode characters: \u20AC \u20AD")
        expect(a.encoding).to eq(Encoding::UTF_8)
      end
    else
      it 'should read string' do
        @trans.write('"this is a test string"')
        expect(@prot.read_string).to eq('this is a test string')
      end
    end

    it "should read binary" do
      @trans.write("\"dGhpcyBpcyBhIHRlc3Qgc3RyaW5n\"")
      expect(@prot.read_binary).to eq("this is a test string")
    end

    it "should read long binary" do
      @trans.write("\"AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5fYGFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6e3x9fn+AgYKDhIWGh4iJiouMjY6PkJGSk5SVlpeYmZqbnJ2en6ChoqOkpaanqKmqq6ytrq+wsbKztLW2t7i5uru8vb6/wMHCw8TFxsfIycrLzM3Oz9DR0tPU1dbX2Nna29zd3t/g4eLj5OXm5+jp6uvs7e7v8PHy8/T19vf4+fr7/P3+/w==\"")
      expect(@prot.read_binary.bytes.to_a).to eq((0...256).to_a)
    end
  
    it "should provide a reasonable to_s" do
      expect(@prot.to_s).to eq("json(memory)")
    end
  end

  describe Thrift::JsonProtocolFactory do
    it "should create a JsonProtocol" do
      expect(Thrift::JsonProtocolFactory.new.get_protocol(double("MockTransport"))).to be_instance_of(Thrift::JsonProtocol)
    end

    it "should provide a reasonable to_s" do
      expect(Thrift::JsonProtocolFactory.new.to_s).to eq("json")
    end
  end
end
