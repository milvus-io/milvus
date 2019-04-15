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

describe 'Client' do

  class ClientSpec
    include Thrift::Client
  end

  before(:each) do
    @prot = double("MockProtocol")
    @client = ClientSpec.new(@prot)
  end

  describe Thrift::Client do
    it "should re-use iprot for oprot if not otherwise specified" do
      expect(@client.instance_variable_get(:'@iprot')).to eql(@prot)
      expect(@client.instance_variable_get(:'@oprot')).to eql(@prot)
    end

    it "should send a test message" do
      expect(@prot).to receive(:write_message_begin).with('testMessage', Thrift::MessageTypes::CALL, 0)
      mock_args = double('#<TestMessage_args:mock>')
      expect(mock_args).to receive(:foo=).with('foo')
      expect(mock_args).to receive(:bar=).with(42)
      expect(mock_args).to receive(:write).with(@prot)
      expect(@prot).to receive(:write_message_end)
      expect(@prot).to receive(:trans) do
        double('trans').tap do |trans|
          expect(trans).to receive(:flush)
        end
      end
      klass = double("TestMessage_args", :new => mock_args)
      @client.send_message('testMessage', klass, :foo => 'foo', :bar => 42)
    end

    it "should increment the sequence id when sending messages" do
      pending "it seems sequence ids are completely ignored right now"
      @prot.expect(:write_message_begin).with('testMessage',  Thrift::MessageTypes::CALL, 0).ordered
      @prot.expect(:write_message_begin).with('testMessage2', Thrift::MessageTypes::CALL, 1).ordered
      @prot.expect(:write_message_begin).with('testMessage3', Thrift::MessageTypes::CALL, 2).ordered
      @prot.stub!(:write_message_end)
      @prot.stub!(:trans).and_return double("trans").as_null_object
      @client.send_message('testMessage', double("args class").as_null_object)
      @client.send_message('testMessage2', double("args class").as_null_object)
      @client.send_message('testMessage3', double("args class").as_null_object)
    end

    it "should receive a test message" do
      expect(@prot).to receive(:read_message_begin).and_return [nil, Thrift::MessageTypes::CALL, 0]
      expect(@prot).to receive(:read_message_end)
      mock_klass = double("#<MockClass:mock>")
      expect(mock_klass).to receive(:read).with(@prot)
      @client.receive_message(double("MockClass", :new => mock_klass))
    end

    it "should handle received exceptions" do
      expect(@prot).to receive(:read_message_begin).and_return [nil, Thrift::MessageTypes::EXCEPTION, 0]
      expect(@prot).to receive(:read_message_end)
      expect(Thrift::ApplicationException).to receive(:new) do
        StandardError.new.tap do |mock_exc|
          expect(mock_exc).to receive(:read).with(@prot)
        end
      end
      expect { @client.receive_message(nil) }.to raise_error(StandardError)
    end

    it "should close the transport if an error occurs while sending a message" do
      allow(@prot).to receive(:write_message_begin)
      expect(@prot).not_to receive(:write_message_end)
      mock_args = double("#<TestMessage_args:mock>")
      expect(mock_args).to receive(:write).with(@prot).and_raise(StandardError)
      trans = double("MockTransport")
      allow(@prot).to receive(:trans).and_return(trans)
      expect(trans).to receive(:close)
      klass = double("TestMessage_args", :new => mock_args)
      expect { @client.send_message("testMessage", klass) }.to raise_error(StandardError)
    end
  end
end
