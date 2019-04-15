(*
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
 *)

{$SCOPEDENUMS ON}

unit Thrift.Exception;

interface

uses
  Classes, SysUtils;

type
  // base class for all Thrift exceptions
  TException = class( SysUtils.Exception)
  public
    function Message : string;        // hide inherited property: allow read, but prevent accidental writes
    procedure UpdateMessageProperty;  // update inherited message property with toString()
  end;




implementation

{ TException }

function TException.Message;
// allow read (exception summary), but prevent accidental writes
// read will return the exception summary
begin
  result := Self.ToString;
end;

procedure TException.UpdateMessageProperty;
// Update the inherited Message property to better conform to standard behaviour.
// Nice benefit: The IDE is now able to show the exception message again.
begin
  inherited Message := Self.ToString;  // produces a summary text
end;




end.

