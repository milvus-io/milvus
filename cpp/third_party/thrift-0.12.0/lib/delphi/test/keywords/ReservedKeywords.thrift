/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// make sure generated code does not produce name collisions with predefined keywords
namespace delphi System

include "ReservedIncluded.thrift"


typedef i32 Cardinal
typedef string message
typedef list< map< Cardinal, message>> program

struct unit {
  1: Cardinal downto;
  2: program procedure;
}

typedef set< unit> units

exception exception1 {
  1: program message;
  2: unit array;
}

service constructor {
  unit Create(1: Cardinal asm; 2: message inherited) throws (1: exception1 label);
  units Destroy();
}

const Cardinal downto = +1
const Cardinal published = -1

enum keywords {
  record = 1,
  repeat = 2,
  deprecated = 3
}


struct Struct_lists {
  1: list<Struct_simple> init;
  2: list<Struct_simple> struc;
  3: list<Struct_simple> field;
  4: list<Struct_simple> field_;
  5: list<Struct_simple> tracker;
  6: list<Struct_simple> Self;
}

struct Struct_structs {
  1: Struct_simple init;
  2: Struct_simple struc;
  3: Struct_simple field;
  4: Struct_simple field_;
  5: Struct_simple tracker;
  6: Struct_simple Self;
}

struct Struct_simple {
  1: bool init;
  2: bool struc;
  3: bool field;
  4: bool field_;
  5: bool tracker;
  6: bool Self;
}

struct Struct_strings {
  1: string init;
  2: string struc;
  3: string field;
  4: string field_;
  5: string tracker;
  6: string Self;
}

struct Struct_binary {
  1: binary init;
  2: binary struc;
  3: binary field;
  4: binary field_;
  5: binary tracker;
  6: binary Self;
}


typedef i32 IProtocol 
typedef i32 ITransport
typedef i32 IFace
typedef i32 IAsync
typedef i32 System
typedef i32 SysUtils
typedef i32 Generics
typedef i32 Thrift

struct Struct_Thrift_Names {
  1: IProtocol   IProtocol
  2: ITransport  ITransport
  3: IFace       IFace
  4: IAsync      IAsync
  5: System      System
  6: SysUtils    SysUtils
  7: Generics    Generics
  8: Thrift      Thrift
}


enum Thrift4554_Enum {
  Foo = 0,
  Bar = 1,
  Baz = 2,
}

struct Thrift4554_Struct {
  1 : optional double MinValue
  2 : optional double MaxValue
  3 : optional bool Integer  // causes issue
  4 : optional Thrift4554_Enum Foo
}


// EOF
