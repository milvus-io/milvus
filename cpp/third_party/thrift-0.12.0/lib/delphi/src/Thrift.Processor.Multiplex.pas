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

unit Thrift.Processor.Multiplex;


interface

uses
  SysUtils,
  Generics.Collections,
  Thrift,
  Thrift.Protocol,
  Thrift.Protocol.Multiplex;

{ TMultiplexedProcessor is a TProcessor allowing a single TServer to provide multiple services.
  To do so, you instantiate the processor and then register additional processors with it,
  as shown in the following example:


     TMultiplexedProcessor processor = new TMultiplexedProcessor();

     processor.registerProcessor(
         "Calculator",
         new Calculator.Processor(new CalculatorHandler()));

     processor.registerProcessor(
         "WeatherReport",
         new WeatherReport.Processor(new WeatherReportHandler()));

     TServerTransport t = new TServerSocket(9090);
     TSimpleServer server = new TSimpleServer(processor, t);

     server.serve();
}


type
  IMultiplexedProcessor = interface( IProcessor)
    ['{807F9D19-6CF4-4789-840E-93E87A12EB63}']
    // Register a service with this TMultiplexedProcessor.  This allows us
    // to broker requests to individual services by using the service name
    // to select them at request time.
    procedure RegisterProcessor( const serviceName : String; const processor : IProcessor; const asDefault : Boolean = FALSE);
  end;


  TMultiplexedProcessorImpl = class( TInterfacedObject, IMultiplexedProcessor, IProcessor)
  private type
    // Our goal was to work with any protocol.  In order to do that, we needed
    // to allow them to call readMessageBegin() and get a TMessage in exactly
    // the standard format, without the service name prepended to TMessage.name.
    TStoredMessageProtocol = class( TProtocolDecorator)
    private
      FMessageBegin : TThriftMessage;
    public
      constructor Create( const protocol : IProtocol; const aMsgBegin : TThriftMessage);
      function ReadMessageBegin: TThriftMessage; override;
    end;

  private
    FServiceProcessorMap : TDictionary<String, IProcessor>;
    FDefaultProcessor : IProcessor;

    procedure Error( const oprot : IProtocol; const msg : TThriftMessage;
                     extype : TApplicationExceptionSpecializedClass; const etxt : string);

  public
    constructor Create;
    destructor Destroy;  override;

    // Register a service with this TMultiplexedProcessorImpl.  This allows us
    // to broker requests to individual services by using the service name
    // to select them at request time.
    procedure RegisterProcessor( const serviceName : String; const processor : IProcessor; const asDefault : Boolean = FALSE);

    { This implementation of process performs the following steps:
      - Read the beginning of the message.
      - Extract the service name from the message.
      - Using the service name to locate the appropriate processor.
      - Dispatch to the processor, with a decorated instance of TProtocol
        that allows readMessageBegin() to return the original TMessage.

      An exception is thrown if the message type is not CALL or ONEWAY
      or if the service is unknown (or not properly registered).
    }
    function Process( const iprot, oprot: IProtocol; const events : IProcessorEvents = nil): Boolean;
  end;


implementation

constructor TMultiplexedProcessorImpl.TStoredMessageProtocol.Create( const protocol : IProtocol; const aMsgBegin : TThriftMessage);
begin
  inherited Create( protocol);
  FMessageBegin := aMsgBegin;
end;


function TMultiplexedProcessorImpl.TStoredMessageProtocol.ReadMessageBegin: TThriftMessage;
begin
  result := FMessageBegin;
end;


constructor TMultiplexedProcessorImpl.Create;
begin
  inherited Create;
  FServiceProcessorMap := TDictionary<string,IProcessor>.Create;
end;


destructor TMultiplexedProcessorImpl.Destroy;
begin
  try
    FreeAndNil( FServiceProcessorMap);
  finally
    inherited Destroy;
  end;
end;


procedure TMultiplexedProcessorImpl.RegisterProcessor( const serviceName : String; const processor : IProcessor; const asDefault : Boolean);
begin
  FServiceProcessorMap.Add( serviceName, processor);

  if asDefault then begin
    if FDefaultProcessor = nil
    then FDefaultProcessor := processor
    else raise TApplicationExceptionInternalError.Create('Only one default service allowed');
  end;
end;


procedure TMultiplexedProcessorImpl.Error( const oprot : IProtocol; const msg : TThriftMessage;
                                           extype : TApplicationExceptionSpecializedClass;
                                           const etxt : string);
var appex  : TApplicationException;
    newMsg : TThriftMessage;
begin
  appex := extype.Create(etxt);
  try
    Init( newMsg, msg.Name, TMessageType.Exception, msg.SeqID);

    oprot.WriteMessageBegin(newMsg);
    appex.Write(oprot);
    oprot.WriteMessageEnd();
    oprot.Transport.Flush();

  finally
    appex.Free;
  end;
end;


function TMultiplexedProcessorImpl.Process(const iprot, oprot : IProtocol; const events : IProcessorEvents = nil): Boolean;
var msg, newMsg : TThriftMessage;
    idx         : Integer;
    sService    : string;
    processor   : IProcessor;
    protocol    : IProtocol;
const
  ERROR_INVALID_MSGTYPE   = 'Message must be "call" or "oneway"';
  ERROR_INCOMPATIBLE_PROT = 'No service name found in "%s". Client is expected to use TMultiplexProtocol.';
  ERROR_UNKNOWN_SERVICE   = 'Service "%s" is not registered with MultiplexedProcessor';
begin
  // Use the actual underlying protocol (e.g. TBinaryProtocol) to read the message header.
  // This pulls the message "off the wire", which we'll deal with at the end of this method.
  msg := iprot.readMessageBegin();
  if not (msg.Type_ in [TMessageType.Call, TMessageType.Oneway]) then begin
    Error( oprot, msg,
           TApplicationExceptionInvalidMessageType,
           ERROR_INVALID_MSGTYPE);
    Exit( FALSE);
  end;

  // Extract the service name
  // use FDefaultProcessor as fallback if there is no separator
  idx := Pos( TMultiplexedProtocol.SEPARATOR, msg.Name);
  if idx > 0 then begin

    // Create a new TMessage, something that can be consumed by any TProtocol
    sService := Copy( msg.Name, 1, idx-1);
    if not FServiceProcessorMap.TryGetValue( sService, processor)
    then begin
      Error( oprot, msg,
             TApplicationExceptionInternalError,
             Format(ERROR_UNKNOWN_SERVICE,[sService]));
      Exit( FALSE);
    end;

    // Create a new TMessage, removing the service name
    Inc( idx, Length(TMultiplexedProtocol.SEPARATOR));
    Init( newMsg, Copy( msg.Name, idx, MAXINT), msg.Type_, msg.SeqID);

  end
  else if FDefaultProcessor <> nil then begin
    processor := FDefaultProcessor;
    newMsg    := msg;  // no need to change

  end
  else begin
    Error( oprot, msg,
           TApplicationExceptionInvalidProtocol,
           Format(ERROR_INCOMPATIBLE_PROT,[msg.Name]));
    Exit( FALSE);
  end;

  // Dispatch processing to the stored processor
  protocol := TStoredMessageProtocol.Create( iprot, newMsg);
  result   := processor.process( protocol, oprot, events);
end;


end.
