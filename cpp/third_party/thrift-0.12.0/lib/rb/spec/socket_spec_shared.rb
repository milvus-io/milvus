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

shared_examples_for "a socket" do
  it "should open a socket" do
    expect(@socket.open).to eq(@handle)
  end

  it "should be open whenever it has a handle" do
    expect(@socket).not_to be_open
    @socket.open
    expect(@socket).to be_open
    @socket.handle = nil
    expect(@socket).not_to be_open
    @socket.handle = @handle
    @socket.close
    expect(@socket).not_to be_open
  end

  it "should write data to the handle" do
    @socket.open
    expect(@handle).to receive(:write).with("foobar")
    @socket.write("foobar")
    expect(@handle).to receive(:write).with("fail").and_raise(StandardError)
    expect { @socket.write("fail") }.to raise_error(Thrift::TransportException) { |e| expect(e.type).to eq(Thrift::TransportException::NOT_OPEN) }
  end

  it "should raise an error when it cannot read from the handle" do
    @socket.open
    expect(@handle).to receive(:readpartial).with(17).and_raise(StandardError)
    expect { @socket.read(17) }.to raise_error(Thrift::TransportException) { |e| expect(e.type).to eq(Thrift::TransportException::NOT_OPEN) }
  end

  it "should return the data read when reading from the handle works" do
    @socket.open
    expect(@handle).to receive(:readpartial).with(17).and_return("test data")
    expect(@socket.read(17)).to eq("test data")
  end

  it "should declare itself as closed when it has an error" do
    @socket.open
    expect(@handle).to receive(:write).with("fail").and_raise(StandardError)
    expect(@socket).to be_open
    expect { @socket.write("fail") }.to raise_error(Thrift::TransportException) { |e| expect(e.type).to eq(Thrift::TransportException::NOT_OPEN) }
    expect(@socket).not_to be_open
  end

  it "should raise an error when the stream is closed" do
    @socket.open
    allow(@handle).to receive(:closed?).and_return(true)
    expect(@socket).not_to be_open
    expect { @socket.write("fail") }.to raise_error(IOError, "closed stream")
    expect { @socket.read(10) }.to raise_error(IOError, "closed stream")
  end

  it "should support the timeout accessor for read" do
    @socket.timeout = 3
    @socket.open
    expect(IO).to receive(:select).with([@handle], nil, nil, 3).and_return([[@handle], [], []])
    expect(@handle).to receive(:readpartial).with(17).and_return("test data")
    expect(@socket.read(17)).to eq("test data")
  end

  it "should support the timeout accessor for write" do
    @socket.timeout = 3
    @socket.open
    expect(IO).to receive(:select).with(nil, [@handle], nil, 3).twice.and_return([[], [@handle], []])
    expect(@handle).to receive(:write_nonblock).with("test data").and_return(4)
    expect(@handle).to receive(:write_nonblock).with(" data").and_return(5)
    expect(@socket.write("test data")).to eq(9)
  end

  it "should raise an error when read times out" do
    @socket.timeout = 0.5
    @socket.open
    expect(IO).to receive(:select).once {sleep(0.5); nil}
    expect { @socket.read(17) }.to raise_error(Thrift::TransportException) { |e| expect(e.type).to eq(Thrift::TransportException::TIMED_OUT) }
  end

  it "should raise an error when write times out" do
    @socket.timeout = 0.5
    @socket.open
    allow(IO).to receive(:select).with(nil, [@handle], nil, 0.5).and_return(nil)
    expect { @socket.write("test data") }.to raise_error(Thrift::TransportException) { |e| expect(e.type).to eq(Thrift::TransportException::TIMED_OUT) }
  end
end
