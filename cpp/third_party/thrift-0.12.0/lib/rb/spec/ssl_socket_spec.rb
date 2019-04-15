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

describe 'SSLSocket' do

  describe Thrift::SSLSocket do
    before(:each) do
      @context = OpenSSL::SSL::SSLContext.new
      @socket = Thrift::SSLSocket.new
      @simple_socket_handle = double("Handle", :closed? => false)
      allow(@simple_socket_handle).to receive(:close)
      allow(@simple_socket_handle).to receive(:connect_nonblock)
      allow(@simple_socket_handle).to receive(:setsockopt)

      @handle = double(double("SSLHandle", :connect_nonblock => true, :post_connection_check => true), :closed? => false)
      allow(@handle).to receive(:connect_nonblock)
      allow(@handle).to receive(:close)
      allow(@handle).to receive(:post_connection_check)

      allow(::Socket).to receive(:new).and_return(@simple_socket_handle)
      allow(OpenSSL::SSL::SSLSocket).to receive(:new).and_return(@handle)
    end

    it_should_behave_like "a socket"

    it "should raise a TransportException when it cannot open a ssl socket" do
      expect(::Socket).to receive(:getaddrinfo).with("localhost", 9090, nil, ::Socket::SOCK_STREAM).and_return([[]])
      expect { @socket.open }.to raise_error(Thrift::TransportException) { |e| expect(e.type).to eq(Thrift::TransportException::NOT_OPEN) }
    end

    it "should open a ::Socket with default args" do
      expect(OpenSSL::SSL::SSLSocket).to receive(:new).with(@simple_socket_handle, nil).and_return(@handle)
      expect(@handle).to receive(:post_connection_check).with('localhost')
      @socket.open
    end

    it "should accept host/port options" do
      handle = double("Handle", :connect_nonblock => true, :setsockopt => nil)
      allow(::Socket).to receive(:new).and_return(handle)
      expect(::Socket).to receive(:getaddrinfo).with("my.domain", 1234, nil, ::Socket::SOCK_STREAM).and_return([[]])
      expect(::Socket).to receive(:sockaddr_in)
      expect(OpenSSL::SSL::SSLSocket).to receive(:new).with(handle, nil).and_return(@handle)
      expect(@handle).to receive(:post_connection_check).with('my.domain')
      Thrift::SSLSocket.new('my.domain', 1234, 6000, nil).open
    end

    it "should accept an optional timeout" do
      expect(Thrift::SSLSocket.new('localhost', 8080, 5).timeout).to eq(5)
    end

    it "should accept an optional context" do
      expect(Thrift::SSLSocket.new('localhost', 8080, 5, @context).ssl_context).to eq(@context)
    end

    it "should provide a reasonable to_s" do
      expect(Thrift::SSLSocket.new('myhost', 8090).to_s).to eq("ssl(socket(myhost:8090))")
    end
  end
end
