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

describe 'Thrift::HTTPClientTransport' do

  describe Thrift::HTTPClientTransport do
    before(:each) do
      @client = Thrift::HTTPClientTransport.new("http://my.domain.com/path/to/service?param=value")
    end
    
    it "should provide a reasonable to_s" do
      @client.to_s == "http://my.domain.com/path/to/service?param=value"
    end

    it "should always be open" do
      expect(@client).to be_open
      @client.close
      expect(@client).to be_open
    end

    it "should post via HTTP and return the results" do
      @client.write "a test"
      @client.write " frame"
      expect(Net::HTTP).to receive(:new).with("my.domain.com", 80) do
        double("Net::HTTP").tap do |http|
          expect(http).to receive(:use_ssl=).with(false)
          expect(http).to receive(:post).with("/path/to/service?param=value", "a test frame", {"Content-Type"=>"application/x-thrift"}) do
            double("Net::HTTPOK").tap do |response|
              expect(response).to receive(:body).and_return "data"
            end
          end
        end
      end
      @client.flush
      expect(@client.read(10)).to eq("data")
    end

    it "should send custom headers if defined" do
      @client.write "test"
      custom_headers = {"Cookie" => "Foo"}
      headers = {"Content-Type"=>"application/x-thrift"}.merge(custom_headers)

      @client.add_headers(custom_headers)
      expect(Net::HTTP).to receive(:new).with("my.domain.com", 80) do
        double("Net::HTTP").tap do |http|
          expect(http).to receive(:use_ssl=).with(false)
          expect(http).to receive(:post).with("/path/to/service?param=value", "test", headers) do
            double("Net::HTTPOK").tap do |response|
              expect(response).to receive(:body).and_return "data"
            end
          end
        end
      end
      @client.flush
    end

    it 'should reset the outbuf on HTTP failures' do
      @client.write "test"

      expect(Net::HTTP).to receive(:new).with("my.domain.com", 80) do
        double("Net::HTTP").tap do |http|
          expect(http).to receive(:use_ssl=).with(false)
          expect(http).to receive(:post).with("/path/to/service?param=value", "test", {"Content-Type"=>"application/x-thrift"}) { raise Net::ReadTimeout }
        end
      end

      @client.flush  rescue
      expect(@client.instance_variable_get(:@outbuf)).to eq(Thrift::Bytes.empty_byte_buffer)
    end

  end

  describe 'ssl enabled' do
    before(:each) do
      @service_path = "/path/to/service?param=value"
      @server_uri = "https://my.domain.com"
    end

    it "should use SSL for https" do
      client = Thrift::HTTPClientTransport.new("#{@server_uri}#{@service_path}")

      client.write "test"

      expect(Net::HTTP).to receive(:new).with("my.domain.com", 443) do
        double("Net::HTTP").tap do |http|
          expect(http).to receive(:use_ssl=).with(true)
          expect(http).to receive(:verify_mode=).with(OpenSSL::SSL::VERIFY_PEER)
          expect(http).to receive(:post).with(@service_path, "test",
              "Content-Type" => "application/x-thrift") do
            double("Net::HTTPOK").tap do |response|
              expect(response).to receive(:body).and_return "data"
            end
          end
        end
      end
      client.flush
      expect(client.read(4)).to eq("data")
    end

    it "should set SSL verify mode when specified" do
      client = Thrift::HTTPClientTransport.new("#{@server_uri}#{@service_path}",
          :ssl_verify_mode => OpenSSL::SSL::VERIFY_NONE)

      client.write "test"
      expect(Net::HTTP).to receive(:new).with("my.domain.com", 443) do
        double("Net::HTTP").tap do |http|
          expect(http).to receive(:use_ssl=).with(true)
          expect(http).to receive(:verify_mode=).with(OpenSSL::SSL::VERIFY_NONE)
          expect(http).to receive(:post).with(@service_path, "test",
              "Content-Type" => "application/x-thrift") do
            double("Net::HTTPOK").tap do |response|
              expect(response).to receive(:body).and_return "data"
            end
          end
        end
      end
      client.flush
      expect(client.read(4)).to eq("data")
    end
  end
end
