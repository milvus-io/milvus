#!/usr/bin/env ruby

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

$:.push File.dirname(__FILE__) + '/..'

require 'test_helper'
require 'thrift'
require 'thrift_test'
require 'thrift_test_types'

class SimpleHandler
  [:testVoid, :testString, :testBool, :testByte, :testI32, :testI64, :testDouble, :testBinary,
   :testStruct, :testMap, :testStringMap, :testSet, :testList, :testNest, :testEnum, :testTypedef,
   :testEnum, :testTypedef, :testMultiException].each do |meth|

    define_method(meth) do |thing|
      p meth
      p thing
      thing
    end

  end

  def testVoid()
  end

  def testInsanity(thing)
    return {
      1 => {
        2 => thing,
        3 => thing
      },
      2 => {
        6 => Thrift::Test::Insanity::new()
      }
    }
  end

  def testMapMap(thing)
    return {
      -4 => {
        -4 => -4,
        -3 => -3,
        -2 => -2,
        -1 => -1,
      },
      4 => {
        4 => 4,
        3 => 3,
        2 => 2,
        1 => 1,
      }
    }
  end

  def testMulti(arg0, arg1, arg2, arg3, arg4, arg5)
    return Thrift::Test::Xtruct.new({
      'string_thing' => 'Hello2',
      'byte_thing' => arg0,
      'i32_thing' => arg1,
      'i64_thing' => arg2,
    })
  end

  def testException(thing)
    if thing == "Xception"
      raise Thrift::Test::Xception, :errorCode => 1001, :message => thing
    elsif thing == "TException"
      raise Thrift::Exception, :message => thing
    else
      # no-op
    end
  end

  def testMultiException(arg0, arg1)
    if arg0 == "Xception2"
      raise Thrift::Test::Xception2, :errorCode => 2002, :struct_thing => ::Thrift::Test::Xtruct.new({ :string_thing => 'This is an Xception2' })
    elsif arg0 == "Xception"
      raise Thrift::Test::Xception, :errorCode => 1001, :message => 'This is an Xception'
    else
      return ::Thrift::Test::Xtruct.new({'string_thing' => arg1})
    end
  end

  def testOneway(arg0)
    sleep(arg0)
  end

end

domain_socket = nil
port = 9090
protocol = "binary"
@protocolFactory = nil
ssl = false
transport = "buffered"
@transportFactory = nil

ARGV.each do|a|
  if a == "--help"
    puts "Allowed options:"
    puts "\t -h [ --help ] \t produce help message"
    puts "\t--domain-socket arg (=) \t Unix domain socket path"
    puts "\t--port arg (=9090) \t Port number to listen \t not valid with domain-socket"
    puts "\t--protocol arg (=binary) \t protocol: accel, binary, compact, json"
    puts "\t--ssl \t use ssl \t not valid with domain-socket"
    puts "\t--transport arg (=buffered) transport: buffered, framed, http"
    exit
  elsif a.start_with?("--domain-socket")
    domain_socket = a.split("=")[1]
  elsif a.start_with?("--protocol")
    protocol = a.split("=")[1]
  elsif a == "--ssl"
    ssl = true
  elsif a.start_with?("--transport")
    transport = a.split("=")[1]
  elsif a.start_with?("--port")
    port = a.split("=")[1].to_i 
  end
end

if protocol == "binary" || protocol.to_s.strip.empty?
  @protocolFactory = Thrift::BinaryProtocolFactory.new
elsif protocol == "compact"
  @protocolFactory = Thrift::CompactProtocolFactory.new
elsif protocol == "json"
  @protocolFactory = Thrift::JsonProtocolFactory.new
elsif protocol == "accel"
  @protocolFactory = Thrift::BinaryProtocolAcceleratedFactory.new
else
  raise 'Unknown protocol type'
end

if transport == "buffered" || transport.to_s.strip.empty?
  @transportFactory = Thrift::BufferedTransportFactory.new
elsif transport == "framed"
  @transportFactory = Thrift::FramedTransportFactory.new
else
  raise 'Unknown transport type'
end

@handler = SimpleHandler.new
@processor = Thrift::Test::ThriftTest::Processor.new(@handler)
@transport = nil
if domain_socket.to_s.strip.empty?
  if ssl
    # the working directory for ruby crosstest is test/rb/gen-rb
    keysDir = File.join(File.dirname(File.dirname(Dir.pwd)), "keys")
    ctx = OpenSSL::SSL::SSLContext.new
    ctx.ca_file = File.join(keysDir, "CA.pem")
    ctx.cert = OpenSSL::X509::Certificate.new(File.open(File.join(keysDir, "server.crt")))
    ctx.cert_store = OpenSSL::X509::Store.new
    ctx.cert_store.add_file(File.join(keysDir, 'client.pem'))
    ctx.key = OpenSSL::PKey::RSA.new(File.open(File.join(keysDir, "server.key")))
    ctx.options = OpenSSL::SSL::OP_NO_SSLv2 | OpenSSL::SSL::OP_NO_SSLv3
    ctx.ssl_version = :SSLv23
    ctx.verify_mode = OpenSSL::SSL::VERIFY_PEER
    @transport = Thrift::SSLServerSocket.new(nil, port, ctx)
  else
    @transport = Thrift::ServerSocket.new(port)
  end
else
  @transport = Thrift::UNIXServerSocket.new(domain_socket)
end

@server = Thrift::ThreadedServer.new(@processor, @transport, @transportFactory, @protocolFactory)

puts "Starting TestServer #{@server.to_s}"
@server.serve
puts "done."
