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
require File.expand_path("#{File.dirname(__FILE__)}/socket_spec_shared")

describe 'Thrift::ServerSocket' do

  describe Thrift::ServerSocket do
    before(:each) do
      @socket = Thrift::ServerSocket.new(1234)
    end

    it "should create a handle when calling listen" do
      expect(TCPServer).to receive(:new).with(nil, 1234)
      @socket.listen
    end

    it "should accept an optional host argument" do
      @socket = Thrift::ServerSocket.new('localhost', 1234)
      expect(TCPServer).to receive(:new).with('localhost', 1234)
      @socket.to_s == "server(localhost:1234)"
      @socket.listen
    end

    it "should create a Thrift::Socket to wrap accepted sockets" do
      handle = double("TCPServer")
      expect(TCPServer).to receive(:new).with(nil, 1234).and_return(handle)
      @socket.listen
      sock = double("sock")
      expect(handle).to receive(:accept).and_return(sock)
      trans = double("Socket")
      expect(Thrift::Socket).to receive(:new).and_return(trans)
      expect(trans).to receive(:handle=).with(sock)
      expect(@socket.accept).to eq(trans)
    end

    it "should close the handle when closed" do
      handle = double("TCPServer", :closed? => false)
      expect(TCPServer).to receive(:new).with(nil, 1234).and_return(handle)
      @socket.listen
      expect(handle).to receive(:close)
      @socket.close
    end

    it "should return nil when accepting if there is no handle" do
      expect(@socket.accept).to be_nil
    end

    it "should return true for closed? when appropriate" do
      handle = double("TCPServer", :closed? => false)
      allow(TCPServer).to receive(:new).and_return(handle)
      @socket.listen
      expect(@socket).not_to be_closed
      allow(handle).to receive(:close)
      @socket.close
      expect(@socket).to be_closed
      @socket.listen
      expect(@socket).not_to be_closed
      allow(handle).to receive(:closed?).and_return(true)
      expect(@socket).to be_closed
    end

    it "should provide a reasonable to_s" do
      expect(@socket.to_s).to eq("socket(:1234)")
    end
  end
end
