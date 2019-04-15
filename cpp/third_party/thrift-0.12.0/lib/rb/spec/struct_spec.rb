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

describe 'Struct' do

  describe Thrift::Struct do
    it "should iterate over all fields properly" do
      fields = {}
      SpecNamespace::Foo.new.each_field { |fid,field_info| fields[fid] = field_info }
      expect(fields).to eq(SpecNamespace::Foo::FIELDS)
    end

    it "should initialize all fields to defaults" do
      validate_default_arguments(SpecNamespace::Foo.new)
    end

    it "should initialize all fields to defaults and accept a block argument" do
      SpecNamespace::Foo.new do |f|
        validate_default_arguments(f)
      end
    end

    def validate_default_arguments(object)
      expect(object.simple).to eq(53)
      expect(object.words).to eq("words")
      expect(object.hello).to eq(SpecNamespace::Hello.new(:greeting => 'hello, world!'))
      expect(object.ints).to eq([1, 2, 2, 3])
      expect(object.complex).to be_nil
      expect(object.shorts).to eq(Set.new([5, 17, 239]))
    end

    it "should not share default values between instances" do
      begin
        struct = SpecNamespace::Foo.new
        struct.ints << 17
        expect(SpecNamespace::Foo.new.ints).to eq([1,2,2,3])
      ensure
        # ensure no leakage to other tests
        SpecNamespace::Foo::FIELDS[4][:default] = [1,2,2,3]
      end
    end

    it "should properly initialize boolean values" do
      struct = SpecNamespace::BoolStruct.new(:yesno => false)
      expect(struct.yesno).to be_falsey
    end

    it "should have proper == semantics" do
      expect(SpecNamespace::Foo.new).not_to eq(SpecNamespace::Hello.new)
      expect(SpecNamespace::Foo.new).to eq(SpecNamespace::Foo.new)
      expect(SpecNamespace::Foo.new(:simple => 52)).not_to eq(SpecNamespace::Foo.new)
    end

    it "should print enum value names in inspect" do
      expect(SpecNamespace::StructWithSomeEnum.new(:some_enum => SpecNamespace::SomeEnum::ONE).inspect).to eq("<SpecNamespace::StructWithSomeEnum some_enum:ONE (0)>")

      expect(SpecNamespace::StructWithEnumMap.new(:my_map => {SpecNamespace::SomeEnum::ONE => [SpecNamespace::SomeEnum::TWO]}).inspect).to eq("<SpecNamespace::StructWithEnumMap my_map:{ONE (0): [TWO (1)]}>")
    end

    it "should pretty print binary fields" do
      expect(SpecNamespace::Foo2.new(:my_binary => "\001\002\003").inspect).to eq("<SpecNamespace::Foo2 my_binary:010203>")
    end

    it "should offer field? methods" do
      expect(SpecNamespace::Foo.new.opt_string?).to be_falsey
      expect(SpecNamespace::Foo.new(:simple => 52).simple?).to be_truthy
      expect(SpecNamespace::Foo.new(:my_bool => false).my_bool?).to be_truthy
      expect(SpecNamespace::Foo.new(:my_bool => true).my_bool?).to be_truthy
    end

    it "should be comparable" do
      s1 = SpecNamespace::StructWithSomeEnum.new(:some_enum => SpecNamespace::SomeEnum::ONE)
      s2 = SpecNamespace::StructWithSomeEnum.new(:some_enum => SpecNamespace::SomeEnum::TWO)

      expect(s1 <=> s2).to eq(-1)
      expect(s2 <=> s1).to eq(1)
      expect(s1 <=> s1).to eq(0)
      expect(s1 <=> SpecNamespace::StructWithSomeEnum.new()).to eq(-1)
    end

    it "should read itself off the wire" do
      struct = SpecNamespace::Foo.new
      prot = Thrift::BaseProtocol.new(double("transport"))
      expect(prot).to receive(:read_struct_begin).twice
      expect(prot).to receive(:read_struct_end).twice
      expect(prot).to receive(:read_field_begin).and_return(
        ['complex', Thrift::Types::MAP, 5], # Foo
        ['words', Thrift::Types::STRING, 2], # Foo
        ['hello', Thrift::Types::STRUCT, 3], # Foo
          ['greeting', Thrift::Types::STRING, 1], # Hello
          [nil, Thrift::Types::STOP, 0], # Hello
        ['simple', Thrift::Types::I32, 1], # Foo
        ['ints', Thrift::Types::LIST, 4], # Foo
        ['shorts', Thrift::Types::SET, 6], # Foo
        [nil, Thrift::Types::STOP, 0] # Hello
      )
      expect(prot).to receive(:read_field_end).exactly(7).times
      expect(prot).to receive(:read_map_begin).and_return(
        [Thrift::Types::I32, Thrift::Types::MAP, 2], # complex
          [Thrift::Types::STRING, Thrift::Types::DOUBLE, 2], # complex/1/value
          [Thrift::Types::STRING, Thrift::Types::DOUBLE, 1] # complex/2/value
      )
      expect(prot).to receive(:read_map_end).exactly(3).times
      expect(prot).to receive(:read_list_begin).and_return([Thrift::Types::I32, 4])
      expect(prot).to receive(:read_list_end)
      expect(prot).to receive(:read_set_begin).and_return([Thrift::Types::I16, 2])
      expect(prot).to receive(:read_set_end)
      expect(prot).to receive(:read_i32).and_return(
        1, 14,        # complex keys
        42,           # simple
        4, 23, 4, 29  # ints
      )
      expect(prot).to receive(:read_string).and_return("pi", "e", "feigenbaum", "apple banana", "what's up?")
      expect(prot).to receive(:read_double).and_return(Math::PI, Math::E, 4.669201609)
      expect(prot).to receive(:read_i16).and_return(2, 3)
      expect(prot).not_to receive(:skip)
      struct.read(prot)

      expect(struct.simple).to eq(42)
      expect(struct.complex).to eq({1 => {"pi" => Math::PI, "e" => Math::E}, 14 => {"feigenbaum" => 4.669201609}})
      expect(struct.hello).to eq(SpecNamespace::Hello.new(:greeting => "what's up?"))
      expect(struct.words).to eq("apple banana")
      expect(struct.ints).to eq([4, 23, 4, 29])
      expect(struct.shorts).to eq(Set.new([3, 2]))
    end

    it "should serialize false boolean fields correctly" do
      b = SpecNamespace::BoolStruct.new(:yesno => false)
      prot = Thrift::BinaryProtocol.new(Thrift::MemoryBufferTransport.new)
      expect(prot).to receive(:write_bool).with(false)
      b.write(prot)
    end

    it "should skip unexpected fields in structs and use default values" do
      struct = SpecNamespace::Foo.new
      prot = Thrift::BaseProtocol.new(double("transport"))
      expect(prot).to receive(:read_struct_begin)
      expect(prot).to receive(:read_struct_end)
      expect(prot).to receive(:read_field_begin).and_return(
        ['simple', Thrift::Types::I32, 1],
        ['complex', Thrift::Types::STRUCT, 5],
        ['thinz', Thrift::Types::MAP, 7],
        ['foobar', Thrift::Types::I32, 3],
        ['words', Thrift::Types::STRING, 2],
        [nil, Thrift::Types::STOP, 0]
      )
      expect(prot).to receive(:read_field_end).exactly(5).times
      expect(prot).to receive(:read_i32).and_return(42)
      expect(prot).to receive(:read_string).and_return("foobar")
      expect(prot).to receive(:skip).with(Thrift::Types::STRUCT)
      expect(prot).to receive(:skip).with(Thrift::Types::MAP)
      # prot.should_receive(:read_map_begin).and_return([Thrift::Types::I32, Thrift::Types::I32, 0])
      # prot.should_receive(:read_map_end)
      expect(prot).to receive(:skip).with(Thrift::Types::I32)
      struct.read(prot)

      expect(struct.simple).to eq(42)
      expect(struct.complex).to be_nil
      expect(struct.words).to eq("foobar")
      expect(struct.hello).to eq(SpecNamespace::Hello.new(:greeting => 'hello, world!'))
      expect(struct.ints).to eq([1, 2, 2, 3])
      expect(struct.shorts).to eq(Set.new([5, 17, 239]))
    end

    it "should write itself to the wire" do
      prot = Thrift::BaseProtocol.new(double("transport")) #mock("Protocol")
      expect(prot).to receive(:write_struct_begin).with("SpecNamespace::Foo")
      expect(prot).to receive(:write_struct_begin).with("SpecNamespace::Hello")
      expect(prot).to receive(:write_struct_end).twice
      expect(prot).to receive(:write_field_begin).with('ints', Thrift::Types::LIST, 4)
      expect(prot).to receive(:write_i32).with(1)
      expect(prot).to receive(:write_i32).with(2).twice
      expect(prot).to receive(:write_i32).with(3)
      expect(prot).to receive(:write_field_begin).with('complex', Thrift::Types::MAP, 5)
      expect(prot).to receive(:write_i32).with(5)
      expect(prot).to receive(:write_string).with('foo')
      expect(prot).to receive(:write_double).with(1.23)
      expect(prot).to receive(:write_field_begin).with('shorts', Thrift::Types::SET, 6)
      expect(prot).to receive(:write_i16).with(5)
      expect(prot).to receive(:write_i16).with(17)
      expect(prot).to receive(:write_i16).with(239)
      expect(prot).to receive(:write_field_stop).twice
      expect(prot).to receive(:write_field_end).exactly(6).times
      expect(prot).to receive(:write_field_begin).with('simple', Thrift::Types::I32, 1)
      expect(prot).to receive(:write_i32).with(53)
      expect(prot).to receive(:write_field_begin).with('hello', Thrift::Types::STRUCT, 3)
      expect(prot).to receive(:write_field_begin).with('greeting', Thrift::Types::STRING, 1)
      expect(prot).to receive(:write_string).with('hello, world!')
      expect(prot).to receive(:write_map_begin).with(Thrift::Types::I32, Thrift::Types::MAP, 1)
      expect(prot).to receive(:write_map_begin).with(Thrift::Types::STRING, Thrift::Types::DOUBLE, 1)
      expect(prot).to receive(:write_map_end).twice
      expect(prot).to receive(:write_list_begin).with(Thrift::Types::I32, 4)
      expect(prot).to receive(:write_list_end)
      expect(prot).to receive(:write_set_begin).with(Thrift::Types::I16, 3)
      expect(prot).to receive(:write_set_end)

      struct = SpecNamespace::Foo.new
      struct.words = nil
      struct.complex = {5 => {"foo" => 1.23}}
      struct.write(prot)
    end

    it "should raise an exception if presented with an unknown container" do
      # yeah this is silly, but I'm going for code coverage here
      struct = SpecNamespace::Foo.new
      expect { struct.send :write_container, nil, nil, {:type => "foo"} }.to raise_error(StandardError, "Not a container type: foo")
    end

    it "should support optional type-checking in Thrift::Struct.new" do
      Thrift.type_checking = true
      begin
        expect { SpecNamespace::Hello.new(:greeting => 3) }.to raise_error(Thrift::TypeError, /Expected Types::STRING, received (Integer|Fixnum) for field greeting/)
      ensure
        Thrift.type_checking = false
      end
      expect { SpecNamespace::Hello.new(:greeting => 3) }.not_to raise_error
    end

    it "should support optional type-checking in field accessors" do
      Thrift.type_checking = true
      begin
        hello = SpecNamespace::Hello.new
        expect { hello.greeting = 3 }.to raise_error(Thrift::TypeError, /Expected Types::STRING, received (Integer|Fixnum) for field greeting/)
      ensure
        Thrift.type_checking = false
      end
      expect { hello.greeting = 3 }.not_to raise_error
    end

    it "should raise an exception when unknown types are given to Thrift::Struct.new" do
      expect { SpecNamespace::Hello.new(:fish => 'salmon') }.to raise_error(Exception, "Unknown key given to SpecNamespace::Hello.new: fish")
    end

    it "should support `raise Xception, 'message'` for Exception structs" do
      begin
        raise SpecNamespace::Xception, "something happened"
      rescue Thrift::Exception => e
        expect(e.message).to eq("something happened")
        expect(e.code).to eq(1)
        # ensure it gets serialized properly, this is the really important part
        prot = Thrift::BaseProtocol.new(double("trans"))
        expect(prot).to receive(:write_struct_begin).with("SpecNamespace::Xception")
        expect(prot).to receive(:write_struct_end)
        expect(prot).to receive(:write_field_begin).with('message', Thrift::Types::STRING, 1)#, "something happened")
        expect(prot).to receive(:write_string).with("something happened")
        expect(prot).to receive(:write_field_begin).with('code', Thrift::Types::I32, 2)#, 1)
        expect(prot).to receive(:write_i32).with(1)
        expect(prot).to receive(:write_field_stop)
        expect(prot).to receive(:write_field_end).twice

        e.write(prot)
      end
    end

    it "should support the regular initializer for exception structs" do
      begin
        raise SpecNamespace::Xception, :message => "something happened", :code => 5
      rescue Thrift::Exception => e
        expect(e.message).to eq("something happened")
        expect(e.code).to eq(5)
        prot = Thrift::BaseProtocol.new(double("trans"))
        expect(prot).to receive(:write_struct_begin).with("SpecNamespace::Xception")
        expect(prot).to receive(:write_struct_end)
        expect(prot).to receive(:write_field_begin).with('message', Thrift::Types::STRING, 1)
        expect(prot).to receive(:write_string).with("something happened")
        expect(prot).to receive(:write_field_begin).with('code', Thrift::Types::I32, 2)
        expect(prot).to receive(:write_i32).with(5)
        expect(prot).to receive(:write_field_stop)
        expect(prot).to receive(:write_field_end).twice

        e.write(prot)
      end
    end
  end
end
