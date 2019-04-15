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

describe 'Exception' do

  describe Thrift::Exception do
    it "should have an accessible message" do
      e = Thrift::Exception.new("test message")
      expect(e.message).to eq("test message")
    end
  end

  describe Thrift::ApplicationException do
    it "should inherit from Thrift::Exception" do
      expect(Thrift::ApplicationException.superclass).to eq(Thrift::Exception)
    end

    it "should have an accessible type and message" do
      e = Thrift::ApplicationException.new
      expect(e.type).to eq(Thrift::ApplicationException::UNKNOWN)
      expect(e.message).to be_nil
      e = Thrift::ApplicationException.new(Thrift::ApplicationException::UNKNOWN_METHOD, "test message")
      expect(e.type).to eq(Thrift::ApplicationException::UNKNOWN_METHOD)
      expect(e.message).to eq("test message")
    end

    it "should read a struct off of a protocol" do
      prot = double("MockProtocol")
      expect(prot).to receive(:read_struct_begin).ordered
      expect(prot).to receive(:read_field_begin).exactly(3).times.and_return(
        ["message", Thrift::Types::STRING, 1],
        ["type", Thrift::Types::I32, 2],
        [nil, Thrift::Types::STOP, 0]
      )
      expect(prot).to receive(:read_string).ordered.and_return "test message"
      expect(prot).to receive(:read_i32).ordered.and_return Thrift::ApplicationException::BAD_SEQUENCE_ID
      expect(prot).to receive(:read_field_end).exactly(2).times
      expect(prot).to receive(:read_struct_end).ordered

      e = Thrift::ApplicationException.new
      e.read(prot)
      expect(e.message).to eq("test message")
      expect(e.type).to eq(Thrift::ApplicationException::BAD_SEQUENCE_ID)
    end

    it "should skip bad fields when reading a struct" do
      prot = double("MockProtocol")
      expect(prot).to receive(:read_struct_begin).ordered
      expect(prot).to receive(:read_field_begin).exactly(5).times.and_return(
        ["type", Thrift::Types::I32, 2],
        ["type", Thrift::Types::STRING, 2],
        ["message", Thrift::Types::MAP, 1],
        ["message", Thrift::Types::STRING, 3],
        [nil, Thrift::Types::STOP, 0]
      )
      expect(prot).to receive(:read_i32).and_return Thrift::ApplicationException::INVALID_MESSAGE_TYPE
      expect(prot).to receive(:skip).with(Thrift::Types::STRING).twice
      expect(prot).to receive(:skip).with(Thrift::Types::MAP)
      expect(prot).to receive(:read_field_end).exactly(4).times
      expect(prot).to receive(:read_struct_end).ordered

      e = Thrift::ApplicationException.new
      e.read(prot)
      expect(e.message).to be_nil
      expect(e.type).to eq(Thrift::ApplicationException::INVALID_MESSAGE_TYPE)
    end

    it "should write a Thrift::ApplicationException struct to the oprot" do
      prot = double("MockProtocol")
      expect(prot).to receive(:write_struct_begin).with("Thrift::ApplicationException").ordered
      expect(prot).to receive(:write_field_begin).with("message", Thrift::Types::STRING, 1).ordered
      expect(prot).to receive(:write_string).with("test message").ordered
      expect(prot).to receive(:write_field_begin).with("type", Thrift::Types::I32, 2).ordered
      expect(prot).to receive(:write_i32).with(Thrift::ApplicationException::UNKNOWN_METHOD).ordered
      expect(prot).to receive(:write_field_end).twice
      expect(prot).to receive(:write_field_stop).ordered
      expect(prot).to receive(:write_struct_end).ordered

      e = Thrift::ApplicationException.new(Thrift::ApplicationException::UNKNOWN_METHOD, "test message")
      e.write(prot)
    end

    it "should skip nil fields when writing to the oprot" do
      prot = double("MockProtocol")
      expect(prot).to receive(:write_struct_begin).with("Thrift::ApplicationException").ordered
      expect(prot).to receive(:write_field_begin).with("message", Thrift::Types::STRING, 1).ordered
      expect(prot).to receive(:write_string).with("test message").ordered
      expect(prot).to receive(:write_field_end).ordered
      expect(prot).to receive(:write_field_stop).ordered
      expect(prot).to receive(:write_struct_end).ordered

      e = Thrift::ApplicationException.new(nil, "test message")
      e.write(prot)

      prot = double("MockProtocol")
      expect(prot).to receive(:write_struct_begin).with("Thrift::ApplicationException").ordered
      expect(prot).to receive(:write_field_begin).with("type", Thrift::Types::I32, 2).ordered
      expect(prot).to receive(:write_i32).with(Thrift::ApplicationException::BAD_SEQUENCE_ID).ordered
      expect(prot).to receive(:write_field_end).ordered
      expect(prot).to receive(:write_field_stop).ordered
      expect(prot).to receive(:write_struct_end).ordered

      e = Thrift::ApplicationException.new(Thrift::ApplicationException::BAD_SEQUENCE_ID)
      e.write(prot)

      prot = double("MockProtocol")
      expect(prot).to receive(:write_struct_begin).with("Thrift::ApplicationException").ordered
      expect(prot).to receive(:write_field_stop).ordered
      expect(prot).to receive(:write_struct_end).ordered

      e = Thrift::ApplicationException.new(nil)
      e.write(prot)
    end
  end

  describe Thrift::ProtocolException do
    it "should have an accessible type" do
      prot = Thrift::ProtocolException.new(Thrift::ProtocolException::SIZE_LIMIT, "message")
      expect(prot.type).to eq(Thrift::ProtocolException::SIZE_LIMIT)
      expect(prot.message).to eq("message")
    end
  end
end
