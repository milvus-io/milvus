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

describe 'UNIXSocket' do

  describe Thrift::UNIXSocket do
    before(:each) do
      @path = '/tmp/thrift_spec_socket'
      @socket = Thrift::UNIXSocket.new(@path)
      @handle = double("Handle", :closed? => false)
      allow(@handle).to receive(:close)
      allow(::UNIXSocket).to receive(:new).and_return(@handle)
    end

    it_should_behave_like "a socket"

    it "should raise a TransportException when it cannot open a socket" do
      expect(::UNIXSocket).to receive(:new).and_raise(StandardError)
      expect { @socket.open }.to raise_error(Thrift::TransportException) { |e| expect(e.type).to eq(Thrift::TransportException::NOT_OPEN) }
    end

    it "should accept an optional timeout" do
      allow(::UNIXSocket).to receive(:new)
      expect(Thrift::UNIXSocket.new(@path, 5).timeout).to eq(5)
    end
    
    it "should provide a reasonable to_s" do
      allow(::UNIXSocket).to receive(:new)
      expect(Thrift::UNIXSocket.new(@path).to_s).to eq("domain(#{@path})")
    end
  end

  describe Thrift::UNIXServerSocket do
    before(:each) do
      @path = '/tmp/thrift_spec_socket'
      @socket = Thrift::UNIXServerSocket.new(@path)
    end

    it "should create a handle when calling listen" do
      expect(UNIXServer).to receive(:new).with(@path)
      @socket.listen
    end

    it "should create a Thrift::UNIXSocket to wrap accepted sockets" do
      handle = double("UNIXServer")
      expect(UNIXServer).to receive(:new).with(@path).and_return(handle)
      @socket.listen
      sock = double("sock")
      expect(handle).to receive(:accept).and_return(sock)
      trans = double("UNIXSocket")
      expect(Thrift::UNIXSocket).to receive(:new).and_return(trans)
      expect(trans).to receive(:handle=).with(sock)
      expect(@socket.accept).to eq(trans)
    end

    it "should close the handle when closed" do
      handle = double("UNIXServer", :closed? => false)
      expect(UNIXServer).to receive(:new).with(@path).and_return(handle)
      @socket.listen
      expect(handle).to receive(:close)
      allow(File).to receive(:delete)
      @socket.close
    end

    it "should delete the socket when closed" do
      handle = double("UNIXServer", :closed? => false)
      expect(UNIXServer).to receive(:new).with(@path).and_return(handle)
      @socket.listen
      allow(handle).to receive(:close)
      expect(File).to receive(:delete).with(@path)
      @socket.close
    end

    it "should return nil when accepting if there is no handle" do
      expect(@socket.accept).to be_nil
    end

    it "should return true for closed? when appropriate" do
      handle = double("UNIXServer", :closed? => false)
      allow(UNIXServer).to receive(:new).and_return(handle)
      allow(File).to receive(:delete)
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
      expect(@socket.to_s).to eq("domain(#{@path})")
    end
  end
end
