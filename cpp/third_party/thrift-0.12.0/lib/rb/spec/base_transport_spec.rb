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

describe 'BaseTransport' do

  describe Thrift::TransportException do
    it "should make type accessible" do
      exc = Thrift::TransportException.new(Thrift::TransportException::ALREADY_OPEN, "msg")
      expect(exc.type).to eq(Thrift::TransportException::ALREADY_OPEN)
      expect(exc.message).to eq("msg")
    end
  end

  describe Thrift::BaseTransport do
    it "should read the specified size" do
      transport = Thrift::BaseTransport.new
      expect(transport).to receive(:read).with(40).ordered.and_return("10 letters")
      expect(transport).to receive(:read).with(30).ordered.and_return("fifteen letters")
      expect(transport).to receive(:read).with(15).ordered.and_return("more characters")
      expect(transport.read_all(40)).to eq("10 lettersfifteen lettersmore characters")
    end

    it "should stub out the rest of the methods" do
      # can't test for stubbiness, so just make sure they're defined
      [:open?, :open, :close, :read, :write, :flush].each do |sym|
        expect(Thrift::BaseTransport.method_defined?(sym)).to be_truthy
      end
    end

    it "should alias << to write" do
      expect(Thrift::BaseTransport.instance_method(:<<)).to eq(Thrift::BaseTransport.instance_method(:write))
    end
    
    it "should provide a reasonable to_s" do
      expect(Thrift::BaseTransport.new.to_s).to eq("base")
    end
  end

  describe Thrift::BaseServerTransport do
    it "should stub out its methods" do
      [:listen, :accept, :close].each do |sym|
        expect(Thrift::BaseServerTransport.method_defined?(sym)).to be_truthy
      end
    end
  end

  describe Thrift::BaseTransportFactory do
    it "should return the transport it's given" do
      transport = double("Transport")
      expect(Thrift::BaseTransportFactory.new.get_transport(transport)).to eql(transport)
    end
    
    it "should provide a reasonable to_s" do
      expect(Thrift::BaseTransportFactory.new.to_s).to eq("base")
    end
  end

  describe Thrift::BufferedTransport do
    it "should provide a to_s that describes the encapsulation" do
      trans = double("Transport")
      expect(trans).to receive(:to_s).and_return("mock")
      expect(Thrift::BufferedTransport.new(trans).to_s).to eq("buffered(mock)")
    end

    it "should pass through everything but write/flush/read" do
      trans = double("Transport")
      expect(trans).to receive(:open?).ordered.and_return("+ open?")
      expect(trans).to receive(:open).ordered.and_return("+ open")
      expect(trans).to receive(:flush).ordered # from the close
      expect(trans).to receive(:close).ordered.and_return("+ close")
      btrans = Thrift::BufferedTransport.new(trans)
      expect(btrans.open?).to eq("+ open?")
      expect(btrans.open).to eq("+ open")
      expect(btrans.close).to eq("+ close")
    end

    it "should buffer reads in chunks of #{Thrift::BufferedTransport::DEFAULT_BUFFER}" do
      trans = double("Transport")
      expect(trans).to receive(:read).with(Thrift::BufferedTransport::DEFAULT_BUFFER).and_return("lorum ipsum dolor emet")
      btrans = Thrift::BufferedTransport.new(trans)
      expect(btrans.read(6)).to eq("lorum ")
      expect(btrans.read(6)).to eq("ipsum ")
      expect(btrans.read(6)).to eq("dolor ")
      expect(btrans.read(6)).to eq("emet")
    end

    it "should buffer writes and send them on flush" do
      trans = double("Transport")
      btrans = Thrift::BufferedTransport.new(trans)
      btrans.write("one/")
      btrans.write("two/")
      btrans.write("three/")
      expect(trans).to receive(:write).with("one/two/three/").ordered
      expect(trans).to receive(:flush).ordered
      btrans.flush
    end

    it "should only send buffered data once" do
      trans = double("Transport")
      btrans = Thrift::BufferedTransport.new(trans)
      btrans.write("one/")
      btrans.write("two/")
      btrans.write("three/")
      expect(trans).to receive(:write).with("one/two/three/")
      allow(trans).to receive(:flush)
      btrans.flush
      # Nothing to flush with no data
      btrans.flush
    end

    it "should flush on close" do
      trans = double("Transport")
      expect(trans).to receive(:close)
      btrans = Thrift::BufferedTransport.new(trans)
      expect(btrans).to receive(:flush)
      btrans.close
    end

    it "should not write to socket if there's no data" do
      trans = double("Transport")
      expect(trans).to receive(:flush)
      btrans = Thrift::BufferedTransport.new(trans)
      btrans.flush
    end
  end

  describe Thrift::BufferedTransportFactory do
    it "should wrap the given transport in a BufferedTransport" do
      trans = double("Transport")
      btrans = double("BufferedTransport")
      expect(Thrift::BufferedTransport).to receive(:new).with(trans).and_return(btrans)
      expect(Thrift::BufferedTransportFactory.new.get_transport(trans)).to eq(btrans)
    end
    
    it "should provide a reasonable to_s" do
      expect(Thrift::BufferedTransportFactory.new.to_s).to eq("buffered")
    end
  end

  describe Thrift::FramedTransport do
    before(:each) do
      @trans = double("Transport")
    end

    it "should provide a to_s that describes the encapsulation" do
      trans = double("Transport")
      expect(trans).to receive(:to_s).and_return("mock")
      expect(Thrift::FramedTransport.new(trans).to_s).to eq("framed(mock)")
    end

    it "should pass through open?/open/close" do
      ftrans = Thrift::FramedTransport.new(@trans)
      expect(@trans).to receive(:open?).ordered.and_return("+ open?")
      expect(@trans).to receive(:open).ordered.and_return("+ open")
      expect(@trans).to receive(:close).ordered.and_return("+ close")
      expect(ftrans.open?).to eq("+ open?")
      expect(ftrans.open).to eq("+ open")
      expect(ftrans.close).to eq("+ close")
    end

    it "should pass through read when read is turned off" do
      ftrans = Thrift::FramedTransport.new(@trans, false, true)
      expect(@trans).to receive(:read).with(17).ordered.and_return("+ read")
      expect(ftrans.read(17)).to eq("+ read")
    end

    it "should pass through write/flush when write is turned off" do
      ftrans = Thrift::FramedTransport.new(@trans, true, false)
      expect(@trans).to receive(:write).with("foo").ordered.and_return("+ write")
      expect(@trans).to receive(:flush).ordered.and_return("+ flush")
      expect(ftrans.write("foo")).to eq("+ write")
      expect(ftrans.flush).to eq("+ flush")
    end

    it "should return a full frame if asked for >= the frame's length" do
      frame = "this is a frame"
      expect(@trans).to receive(:read_all).with(4).and_return("\000\000\000\017")
      expect(@trans).to receive(:read_all).with(frame.length).and_return(frame)
      expect(Thrift::FramedTransport.new(@trans).read(frame.length + 10)).to eq(frame)
    end

    it "should return slices of the frame when asked for < the frame's length" do
      frame = "this is a frame"
      expect(@trans).to receive(:read_all).with(4).and_return("\000\000\000\017")
      expect(@trans).to receive(:read_all).with(frame.length).and_return(frame)
      ftrans = Thrift::FramedTransport.new(@trans)
      expect(ftrans.read(4)).to eq("this")
      expect(ftrans.read(4)).to eq(" is ")
      expect(ftrans.read(16)).to eq("a frame")
    end

    it "should return nothing if asked for <= 0" do
      expect(Thrift::FramedTransport.new(@trans).read(-2)).to eq("")
    end

    it "should pull a new frame when the first is exhausted" do
      frame = "this is a frame"
      frame2 = "yet another frame"
      expect(@trans).to receive(:read_all).with(4).and_return("\000\000\000\017", "\000\000\000\021")
      expect(@trans).to receive(:read_all).with(frame.length).and_return(frame)
      expect(@trans).to receive(:read_all).with(frame2.length).and_return(frame2)
      ftrans = Thrift::FramedTransport.new(@trans)
      expect(ftrans.read(4)).to eq("this")
      expect(ftrans.read(8)).to eq(" is a fr")
      expect(ftrans.read(6)).to eq("ame")
      expect(ftrans.read(4)).to eq("yet ")
      expect(ftrans.read(16)).to eq("another frame")
    end

    it "should buffer writes" do
      ftrans = Thrift::FramedTransport.new(@trans)
      expect(@trans).not_to receive(:write)
      ftrans.write("foo")
      ftrans.write("bar")
      ftrans.write("this is a frame")
    end

    it "should write slices of the buffer" do
      ftrans = Thrift::FramedTransport.new(@trans)
      ftrans.write("foobar", 3)
      ftrans.write("barfoo", 1)
      allow(@trans).to receive(:flush)
      expect(@trans).to receive(:write).with("\000\000\000\004foob")
      ftrans.flush
    end

    it "should flush frames with a 4-byte header" do
      ftrans = Thrift::FramedTransport.new(@trans)
      expect(@trans).to receive(:write).with("\000\000\000\035one/two/three/this is a frame").ordered
      expect(@trans).to receive(:flush).ordered
      ftrans.write("one/")
      ftrans.write("two/")
      ftrans.write("three/")
      ftrans.write("this is a frame")
      ftrans.flush
    end

    it "should not flush the same buffered data twice" do
      ftrans = Thrift::FramedTransport.new(@trans)
      expect(@trans).to receive(:write).with("\000\000\000\007foo/bar")
      allow(@trans).to receive(:flush)
      ftrans.write("foo")
      ftrans.write("/bar")
      ftrans.flush
      expect(@trans).to receive(:write).with("\000\000\000\000")
      ftrans.flush
    end
  end

  describe Thrift::FramedTransportFactory do
    it "should wrap the given transport in a FramedTransport" do
      trans = double("Transport")
      expect(Thrift::FramedTransport).to receive(:new).with(trans)
      Thrift::FramedTransportFactory.new.get_transport(trans)
    end
    
    it "should provide a reasonable to_s" do
      expect(Thrift::FramedTransportFactory.new.to_s).to eq("framed")
    end
  end

  describe Thrift::MemoryBufferTransport do
    before(:each) do
      @buffer = Thrift::MemoryBufferTransport.new
    end

    it "should provide a reasonable to_s" do
      expect(@buffer.to_s).to eq("memory")
    end

    it "should accept a buffer on input and use it directly" do
      s = "this is a test"
      @buffer = Thrift::MemoryBufferTransport.new(s)
      expect(@buffer.read(4)).to eq("this")
      s.slice!(-4..-1)
      expect(@buffer.read(@buffer.available)).to eq(" is a ")
    end

    it "should always remain open" do
      expect(@buffer).to be_open
      @buffer.close
      expect(@buffer).to be_open
    end

    it "should respond to peek and available" do
      @buffer.write "some data"
      expect(@buffer.peek).to be_truthy
      expect(@buffer.available).to eq(9)
      @buffer.read(4)
      expect(@buffer.peek).to be_truthy
      expect(@buffer.available).to eq(5)
      @buffer.read(5)
      expect(@buffer.peek).to be_falsey
      expect(@buffer.available).to eq(0)
    end

    it "should be able to reset the buffer" do
      @buffer.write "test data"
      @buffer.reset_buffer("foobar")
      expect(@buffer.available).to eq(6)
      expect(@buffer.read(@buffer.available)).to eq("foobar")
      @buffer.reset_buffer
      expect(@buffer.available).to eq(0)
    end

    it "should copy the given string when resetting the buffer" do
      s = "this is a test"
      @buffer.reset_buffer(s)
      expect(@buffer.available).to eq(14)
      @buffer.read(10)
      expect(@buffer.available).to eq(4)
      expect(s).to eq("this is a test")
    end

    it "should return from read what was given in write" do
      @buffer.write "test data"
      expect(@buffer.read(4)).to eq("test")
      expect(@buffer.read(@buffer.available)).to eq(" data")
      @buffer.write "foo"
      @buffer.write " bar"
      expect(@buffer.read(@buffer.available)).to eq("foo bar")
    end

    it "should throw an EOFError when there isn't enough data in the buffer" do
      @buffer.reset_buffer("")
      expect{@buffer.read(1)}.to raise_error(EOFError)

      @buffer.reset_buffer("1234")
      expect{@buffer.read(5)}.to raise_error(EOFError)
    end
  end

  describe Thrift::IOStreamTransport do
    before(:each) do
      @input = double("Input", :closed? => false)
      @output = double("Output", :closed? => false)
      @trans = Thrift::IOStreamTransport.new(@input, @output)
    end

    it "should provide a reasonable to_s" do
      expect(@input).to receive(:to_s).and_return("mock_input")
      expect(@output).to receive(:to_s).and_return("mock_output")
      expect(@trans.to_s).to eq("iostream(input=mock_input,output=mock_output)")
    end

    it "should be open as long as both input or output are open" do
      expect(@trans).to be_open
      allow(@input).to receive(:closed?).and_return(true)
      expect(@trans).to be_open
      allow(@input).to receive(:closed?).and_return(false)
      allow(@output).to receive(:closed?).and_return(true)
      expect(@trans).to be_open
      allow(@input).to receive(:closed?).and_return(true)
      expect(@trans).not_to be_open
    end

    it "should pass through read/write to input/output" do
      expect(@input).to receive(:read).with(17).and_return("+ read")
      expect(@output).to receive(:write).with("foobar").and_return("+ write")
      expect(@trans.read(17)).to eq("+ read")
      expect(@trans.write("foobar")).to eq("+ write")
    end

    it "should close both input and output when closed" do
      expect(@input).to receive(:close)
      expect(@output).to receive(:close)
      @trans.close
    end
  end
end
