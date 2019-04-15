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

describe 'Server' do

  describe Thrift::BaseServer do
    before(:each) do
      @processor = double("Processor")
      @serverTrans = double("ServerTransport")
      @trans = double("BaseTransport")
      @prot = double("BaseProtocol")
      @server = described_class.new(@processor, @serverTrans, @trans, @prot)
    end

    it "should default to BaseTransportFactory and BinaryProtocolFactory when not specified" do
      @server = Thrift::BaseServer.new(double("Processor"), double("BaseServerTransport"))
      expect(@server.instance_variable_get(:'@transport_factory')).to be_an_instance_of(Thrift::BaseTransportFactory)
      expect(@server.instance_variable_get(:'@protocol_factory')).to be_an_instance_of(Thrift::BinaryProtocolFactory)
    end

    it "should not serve" do
      expect { @server.serve()}.to raise_error(NotImplementedError)
    end
    
    it "should provide a reasonable to_s" do
      expect(@serverTrans).to receive(:to_s).once.and_return("serverTrans")
      expect(@trans).to receive(:to_s).once.and_return("trans")
      expect(@prot).to receive(:to_s).once.and_return("prot")
      expect(@server.to_s).to eq("server(prot(trans(serverTrans)))")
    end
  end

  describe Thrift::SimpleServer do
    before(:each) do
      @processor = double("Processor")
      @serverTrans = double("ServerTransport")
      @trans = double("BaseTransport")
      @prot = double("BaseProtocol")
      @client = double("Client")
      @server = described_class.new(@processor, @serverTrans, @trans, @prot)
    end
    
    it "should provide a reasonable to_s" do
      expect(@serverTrans).to receive(:to_s).once.and_return("serverTrans")
      expect(@trans).to receive(:to_s).once.and_return("trans")
      expect(@prot).to receive(:to_s).once.and_return("prot")
      expect(@server.to_s).to eq("simple(server(prot(trans(serverTrans))))")
    end
    
    it "should serve in the main thread" do
      expect(@serverTrans).to receive(:listen).ordered
      expect(@serverTrans).to receive(:accept).exactly(3).times.and_return(@client)
      expect(@trans).to receive(:get_transport).exactly(3).times.with(@client).and_return(@trans)
      expect(@prot).to receive(:get_protocol).exactly(3).times.with(@trans).and_return(@prot)
      x = 0
      expect(@processor).to receive(:process).exactly(3).times.with(@prot, @prot) do
        case (x += 1)
        when 1 then raise Thrift::TransportException
        when 2 then raise Thrift::ProtocolException
        when 3 then throw :stop
        end
      end
      expect(@trans).to receive(:close).exactly(3).times
      expect(@serverTrans).to receive(:close).ordered
      expect { @server.serve }.to throw_symbol(:stop)
    end
  end

  describe Thrift::ThreadedServer do
    before(:each) do
      @processor = double("Processor")
      @serverTrans = double("ServerTransport")
      @trans = double("BaseTransport")
      @prot = double("BaseProtocol")
      @client = double("Client")
      @server = described_class.new(@processor, @serverTrans, @trans, @prot)
    end

    it "should provide a reasonable to_s" do
      expect(@serverTrans).to receive(:to_s).once.and_return("serverTrans")
      expect(@trans).to receive(:to_s).once.and_return("trans")
      expect(@prot).to receive(:to_s).once.and_return("prot")
      expect(@server.to_s).to eq("threaded(server(prot(trans(serverTrans))))")
    end
    
    it "should serve using threads" do
      expect(@serverTrans).to receive(:listen).ordered
      expect(@serverTrans).to receive(:accept).exactly(3).times.and_return(@client)
      expect(@trans).to receive(:get_transport).exactly(3).times.with(@client).and_return(@trans)
      expect(@prot).to receive(:get_protocol).exactly(3).times.with(@trans).and_return(@prot)
      expect(Thread).to receive(:new).with(@prot, @trans).exactly(3).times.and_yield(@prot, @trans)
      x = 0
      expect(@processor).to receive(:process).exactly(3).times.with(@prot, @prot) do
        case (x += 1)
        when 1 then raise Thrift::TransportException
        when 2 then raise Thrift::ProtocolException
        when 3 then throw :stop
        end
      end
      expect(@trans).to receive(:close).exactly(3).times
      expect(@serverTrans).to receive(:close).ordered
      expect { @server.serve }.to throw_symbol(:stop)
    end
  end

  describe Thrift::ThreadPoolServer do
    before(:each) do
      @processor = double("Processor")
      @server_trans = double("ServerTransport")
      @trans = double("BaseTransport")
      @prot = double("BaseProtocol")
      @client = double("Client")
      @server = described_class.new(@processor, @server_trans, @trans, @prot)
      sleep(0.15)
    end

    it "should provide a reasonable to_s" do
      expect(@server_trans).to receive(:to_s).once.and_return("server_trans")
      expect(@trans).to receive(:to_s).once.and_return("trans")
      expect(@prot).to receive(:to_s).once.and_return("prot")
      expect(@server.to_s).to eq("threadpool(server(prot(trans(server_trans))))")
    end
    
    it "should serve inside a thread" do
      exception_q = @server.instance_variable_get(:@exception_q)
      expect_any_instance_of(described_class).to receive(:serve) do 
        exception_q.push(StandardError.new('ERROR'))
      end
      expect { @server.rescuable_serve }.to(raise_error('ERROR'))
      sleep(0.15)
    end

    it "should avoid running the server twice when retrying rescuable_serve" do
      exception_q = @server.instance_variable_get(:@exception_q)
      expect_any_instance_of(described_class).to receive(:serve) do 
        exception_q.push(StandardError.new('ERROR1'))
        exception_q.push(StandardError.new('ERROR2'))
      end
      expect { @server.rescuable_serve }.to(raise_error('ERROR1'))
      expect { @server.rescuable_serve }.to(raise_error('ERROR2'))
    end

    it "should serve using a thread pool" do
      thread_q = double("SizedQueue")
      exception_q = double("Queue")
      @server.instance_variable_set(:@thread_q, thread_q)
      @server.instance_variable_set(:@exception_q, exception_q)
      expect(@server_trans).to receive(:listen).ordered
      expect(thread_q).to receive(:push).with(:token)
      expect(thread_q).to receive(:pop)
      expect(Thread).to receive(:new).and_yield
      expect(@server_trans).to receive(:accept).exactly(3).times.and_return(@client)
      expect(@trans).to receive(:get_transport).exactly(3).times.and_return(@trans)
      expect(@prot).to receive(:get_protocol).exactly(3).times.and_return(@prot)
      x = 0
      error = RuntimeError.new("Stopped")
      expect(@processor).to receive(:process).exactly(3).times.with(@prot, @prot) do
        case (x += 1)
        when 1 then raise Thrift::TransportException
        when 2 then raise Thrift::ProtocolException
        when 3 then raise error
        end
      end
      expect(@trans).to receive(:close).exactly(3).times
      expect(exception_q).to receive(:push).with(error).and_throw(:stop)
      expect(@server_trans).to receive(:close)
      expect { @server.serve }.to(throw_symbol(:stop))
    end
  end
end
