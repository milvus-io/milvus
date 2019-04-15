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

describe 'Socket' do

  describe Thrift::Socket do
    before(:each) do
      @socket = Thrift::Socket.new
      @handle = double("Handle", :closed? => false)
      allow(@handle).to receive(:close)
      allow(@handle).to receive(:connect_nonblock)
      allow(@handle).to receive(:setsockopt)
      allow(::Socket).to receive(:new).and_return(@handle)
    end

    it_should_behave_like "a socket"

    it "should raise a TransportException when it cannot open a socket" do
      expect(::Socket).to receive(:getaddrinfo).with("localhost", 9090, nil, ::Socket::SOCK_STREAM).and_return([[]])
      expect { @socket.open }.to raise_error(Thrift::TransportException) { |e| expect(e.type).to eq(Thrift::TransportException::NOT_OPEN) }
    end

    it "should open a ::Socket with default args" do
      expect(::Socket).to receive(:new).and_return(double("Handle", :connect_nonblock => true, :setsockopt => nil))
      expect(::Socket).to receive(:getaddrinfo).with("localhost", 9090, nil, ::Socket::SOCK_STREAM).and_return([[]])
      expect(::Socket).to receive(:sockaddr_in)
      @socket.to_s == "socket(localhost:9090)"
      @socket.open
    end

    it "should accept host/port options" do
      expect(::Socket).to receive(:new).and_return(double("Handle", :connect_nonblock => true, :setsockopt => nil))
      expect(::Socket).to receive(:getaddrinfo).with("my.domain", 1234, nil, ::Socket::SOCK_STREAM).and_return([[]])
      expect(::Socket).to receive(:sockaddr_in)
      @socket = Thrift::Socket.new('my.domain', 1234).open
      @socket.to_s == "socket(my.domain:1234)"
    end

    it "should accept an optional timeout" do
      allow(::Socket).to receive(:new)
      expect(Thrift::Socket.new('localhost', 8080, 5).timeout).to eq(5)
    end

    it "should provide a reasonable to_s" do
      allow(::Socket).to receive(:new)
      expect(Thrift::Socket.new('myhost', 8090).to_s).to eq("socket(myhost:8090)")
    end
  end
end
