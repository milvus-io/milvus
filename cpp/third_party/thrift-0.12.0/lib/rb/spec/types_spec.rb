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

describe Thrift::Types do

  before(:each) do
    Thrift.type_checking = true
  end

  after(:each) do
    Thrift.type_checking = false
  end

  context 'type checking' do
    it "should return the proper name for each type" do
      expect(Thrift.type_name(Thrift::Types::I16)).to eq("Types::I16")
      expect(Thrift.type_name(Thrift::Types::VOID)).to eq("Types::VOID")
      expect(Thrift.type_name(Thrift::Types::LIST)).to eq("Types::LIST")
      expect(Thrift.type_name(42)).to be_nil
    end

    it "should check types properly" do
      # lambda { Thrift.check_type(nil, Thrift::Types::STOP) }.should raise_error(Thrift::TypeError)
      expect { Thrift.check_type(3,              {:type => Thrift::Types::STOP},   :foo) }.to raise_error(Thrift::TypeError)
      expect { Thrift.check_type(nil,            {:type => Thrift::Types::VOID},   :foo) }.not_to raise_error
      expect { Thrift.check_type(3,              {:type => Thrift::Types::VOID},   :foo) }.to raise_error(Thrift::TypeError)
      expect { Thrift.check_type(true,           {:type => Thrift::Types::BOOL},   :foo) }.not_to raise_error
      expect { Thrift.check_type(3,              {:type => Thrift::Types::BOOL},   :foo) }.to raise_error(Thrift::TypeError)
      expect { Thrift.check_type(42,             {:type => Thrift::Types::BYTE},   :foo) }.not_to raise_error
      expect { Thrift.check_type(42,             {:type => Thrift::Types::I16},    :foo) }.not_to raise_error
      expect { Thrift.check_type(42,             {:type => Thrift::Types::I32},    :foo) }.not_to raise_error
      expect { Thrift.check_type(42,             {:type => Thrift::Types::I64},    :foo) }.not_to raise_error
      expect { Thrift.check_type(3.14,           {:type => Thrift::Types::I32},    :foo) }.to raise_error(Thrift::TypeError)
      expect { Thrift.check_type(3.14,           {:type => Thrift::Types::DOUBLE}, :foo) }.not_to raise_error
      expect { Thrift.check_type(3,              {:type => Thrift::Types::DOUBLE}, :foo) }.to raise_error(Thrift::TypeError)
      expect { Thrift.check_type("3",            {:type => Thrift::Types::STRING}, :foo) }.not_to raise_error
      expect { Thrift.check_type(3,              {:type => Thrift::Types::STRING}, :foo) }.to raise_error(Thrift::TypeError)
      hello = SpecNamespace::Hello.new
      expect { Thrift.check_type(hello,          {:type => Thrift::Types::STRUCT, :class => SpecNamespace::Hello}, :foo) }.not_to raise_error
      expect { Thrift.check_type("foo",          {:type => Thrift::Types::STRUCT}, :foo) }.to raise_error(Thrift::TypeError)
      field = {:type => Thrift::Types::MAP, :key => {:type => Thrift::Types::I32}, :value => {:type => Thrift::Types::STRING}}
      expect { Thrift.check_type({1 => "one"},   field,                            :foo) }.not_to raise_error
      expect { Thrift.check_type([1],            field,                            :foo) }.to raise_error(Thrift::TypeError)
      field = {:type => Thrift::Types::LIST, :element => {:type => Thrift::Types::I32}}
      expect { Thrift.check_type([1],            field,                            :foo) }.not_to raise_error
      expect { Thrift.check_type({:foo => 1},    field,                            :foo) }.to raise_error(Thrift::TypeError)
      field = {:type => Thrift::Types::SET, :element => {:type => Thrift::Types::I32}}
      expect { Thrift.check_type(Set.new([1,2]), field,                            :foo) }.not_to raise_error
      expect { Thrift.check_type([1,2],          field,                            :foo) }.to raise_error(Thrift::TypeError)
      expect { Thrift.check_type({:foo => true}, field,                            :foo) }.to raise_error(Thrift::TypeError)
    end

    it "should error out if nil is passed and skip_types is false" do
      expect { Thrift.check_type(nil, {:type => Thrift::Types::BOOL},   :foo, false) }.to raise_error(Thrift::TypeError)
      expect { Thrift.check_type(nil, {:type => Thrift::Types::BYTE},   :foo, false) }.to raise_error(Thrift::TypeError)
      expect { Thrift.check_type(nil, {:type => Thrift::Types::I16},    :foo, false) }.to raise_error(Thrift::TypeError)
      expect { Thrift.check_type(nil, {:type => Thrift::Types::I32},    :foo, false) }.to raise_error(Thrift::TypeError)
      expect { Thrift.check_type(nil, {:type => Thrift::Types::I64},    :foo, false) }.to raise_error(Thrift::TypeError)
      expect { Thrift.check_type(nil, {:type => Thrift::Types::DOUBLE}, :foo, false) }.to raise_error(Thrift::TypeError)
      expect { Thrift.check_type(nil, {:type => Thrift::Types::STRING}, :foo, false) }.to raise_error(Thrift::TypeError)
      expect { Thrift.check_type(nil, {:type => Thrift::Types::STRUCT}, :foo, false) }.to raise_error(Thrift::TypeError)
      expect { Thrift.check_type(nil, {:type => Thrift::Types::LIST},   :foo, false) }.to raise_error(Thrift::TypeError)
      expect { Thrift.check_type(nil, {:type => Thrift::Types::SET},    :foo, false) }.to raise_error(Thrift::TypeError)
      expect { Thrift.check_type(nil, {:type => Thrift::Types::MAP},    :foo, false) }.to raise_error(Thrift::TypeError)
    end

    it "should check element types on containers" do
      field = {:type => Thrift::Types::LIST, :element => {:type => Thrift::Types::I32}}
      expect { Thrift.check_type([1, 2], field, :foo) }.not_to raise_error
      expect { Thrift.check_type([1, nil, 2], field, :foo) }.to raise_error(Thrift::TypeError)
      field = {:type => Thrift::Types::MAP, :key => {:type => Thrift::Types::I32}, :value => {:type => Thrift::Types::STRING}}
      expect { Thrift.check_type({1 => "one", 2 => "two"}, field, :foo) }.not_to raise_error
      expect { Thrift.check_type({1 => "one", nil => "nil"}, field, :foo) }.to raise_error(Thrift::TypeError)
      expect { Thrift.check_type({1 => nil, 2 => "two"}, field, :foo) }.to raise_error(Thrift::TypeError)
      field = {:type => Thrift::Types::SET, :element => {:type => Thrift::Types::I32}}
      expect { Thrift.check_type(Set.new([1, 2]), field, :foo) }.not_to raise_error
      expect { Thrift.check_type(Set.new([1, nil, 2]), field, :foo) }.to raise_error(Thrift::TypeError)
      expect { Thrift.check_type(Set.new([1, 2.3, 2]), field, :foo) }.to raise_error(Thrift::TypeError)

      field = {:type => Thrift::Types::STRUCT, :class => SpecNamespace::Hello}
      expect { Thrift.check_type(SpecNamespace::BoolStruct, field, :foo) }.to raise_error(Thrift::TypeError)
    end

    it "should give the Thrift::TypeError a readable message" do
      msg = /Expected Types::STRING, received (Integer|Fixnum) for field foo/
      expect { Thrift.check_type(3, {:type => Thrift::Types::STRING}, :foo) }.to raise_error(Thrift::TypeError, msg)
      msg = /Expected Types::STRING, received (Integer|Fixnum) for field foo.element/
      field = {:type => Thrift::Types::LIST, :element => {:type => Thrift::Types::STRING}}
      expect { Thrift.check_type([3], field, :foo) }.to raise_error(Thrift::TypeError, msg)
      msg = "Expected Types::I32, received NilClass for field foo.element.key"
      field = {:type => Thrift::Types::LIST,
               :element => {:type => Thrift::Types::MAP,
                            :key => {:type => Thrift::Types::I32},
                            :value => {:type => Thrift::Types::I32}}}
      expect { Thrift.check_type([{nil => 3}], field, :foo) }.to raise_error(Thrift::TypeError, msg)
      msg = "Expected Types::I32, received NilClass for field foo.element.value"
      expect { Thrift.check_type([{1 => nil}], field, :foo) }.to raise_error(Thrift::TypeError, msg)
    end
  end
end
