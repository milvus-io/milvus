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

describe 'Processor' do

  class ProcessorSpec
    include Thrift::Processor
  end

  describe Thrift::Processor do
    before(:each) do
      @processor = ProcessorSpec.new(double("MockHandler"))
      @prot = double("MockProtocol")
    end

    def mock_trans(obj)
      expect(obj).to receive(:trans).ordered do
        double("trans").tap do |trans|
          expect(trans).to receive(:flush).ordered
        end
      end
    end

    it "should call process_<message> when it receives that message" do
      expect(@prot).to receive(:read_message_begin).ordered.and_return ['testMessage', Thrift::MessageTypes::CALL, 17]
      expect(@processor).to receive(:process_testMessage).with(17, @prot, @prot).ordered
      expect(@processor.process(@prot, @prot)).to eq(true)
    end

    it "should raise an ApplicationException when the received message cannot be processed" do
      expect(@prot).to receive(:read_message_begin).ordered.and_return ['testMessage', Thrift::MessageTypes::CALL, 4]
      expect(@prot).to receive(:skip).with(Thrift::Types::STRUCT).ordered
      expect(@prot).to receive(:read_message_end).ordered
      expect(@prot).to receive(:write_message_begin).with('testMessage', Thrift::MessageTypes::EXCEPTION, 4).ordered
      e = double(Thrift::ApplicationException)
      expect(e).to receive(:write).with(@prot).ordered
      expect(Thrift::ApplicationException).to receive(:new).with(Thrift::ApplicationException::UNKNOWN_METHOD, "Unknown function testMessage").and_return(e)
      expect(@prot).to receive(:write_message_end).ordered
      mock_trans(@prot)
      @processor.process(@prot, @prot)
    end

    it "should pass args off to the args class" do
      args_class = double("MockArgsClass")
      args = double("#<MockArgsClass:mock>").tap do |args|
        expect(args).to receive(:read).with(@prot).ordered
      end
      expect(args_class).to receive(:new).and_return args
      expect(@prot).to receive(:read_message_end).ordered
      expect(@processor.read_args(@prot, args_class)).to eql(args)
    end

    it "should write out a reply when asked" do
      expect(@prot).to receive(:write_message_begin).with('testMessage', Thrift::MessageTypes::REPLY, 23).ordered
      result = double("MockResult")
      expect(result).to receive(:write).with(@prot).ordered
      expect(@prot).to receive(:write_message_end).ordered
      mock_trans(@prot)
      @processor.write_result(result, @prot, 'testMessage', 23)
    end
  end
end
