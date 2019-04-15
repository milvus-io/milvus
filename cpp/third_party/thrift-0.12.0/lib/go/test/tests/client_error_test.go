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

package tests

import (
	"context"
	"errors"
	"errortest"
	"testing"
	"thrift"

	"github.com/golang/mock/gomock"
)

// TestCase: Comprehensive call and reply workflow in the client.
// Setup mock to fail at a certain position. Return true if position exists otherwise false.
func prepareClientCallReply(protocol *MockTProtocol, failAt int, failWith error) bool {
	var err error = nil

	if failAt == 0 {
		err = failWith
	}
	last := protocol.EXPECT().WriteMessageBegin("testStruct", thrift.CALL, int32(1)).Return(err)
	if failAt == 0 {
		return true
	}
	if failAt == 1 {
		err = failWith
	}
	last = protocol.EXPECT().WriteStructBegin("testStruct_args").Return(err).After(last)
	if failAt == 1 {
		return true
	}
	if failAt == 2 {
		err = failWith
	}
	last = protocol.EXPECT().WriteFieldBegin("thing", thrift.TType(thrift.STRUCT), int16(1)).Return(err).After(last)
	if failAt == 2 {
		return true
	}
	if failAt == 3 {
		err = failWith
	}
	last = protocol.EXPECT().WriteStructBegin("TestStruct").Return(err).After(last)
	if failAt == 3 {
		return true
	}
	if failAt == 4 {
		err = failWith
	}
	last = protocol.EXPECT().WriteFieldBegin("m", thrift.TType(thrift.MAP), int16(1)).Return(err).After(last)
	if failAt == 4 {
		return true
	}
	if failAt == 5 {
		err = failWith
	}
	last = protocol.EXPECT().WriteMapBegin(thrift.TType(thrift.STRING), thrift.TType(thrift.STRING), 0).Return(err).After(last)
	if failAt == 5 {
		return true
	}
	if failAt == 6 {
		err = failWith
	}
	last = protocol.EXPECT().WriteMapEnd().Return(err).After(last)
	if failAt == 6 {
		return true
	}
	if failAt == 7 {
		err = failWith
	}
	last = protocol.EXPECT().WriteFieldEnd().Return(err).After(last)
	if failAt == 7 {
		return true
	}
	if failAt == 8 {
		err = failWith
	}
	last = protocol.EXPECT().WriteFieldBegin("l", thrift.TType(thrift.LIST), int16(2)).Return(err).After(last)
	if failAt == 8 {
		return true
	}
	if failAt == 9 {
		err = failWith
	}
	last = protocol.EXPECT().WriteListBegin(thrift.TType(thrift.STRING), 0).Return(err).After(last)
	if failAt == 9 {
		return true
	}
	if failAt == 10 {
		err = failWith
	}
	last = protocol.EXPECT().WriteListEnd().Return(err).After(last)
	if failAt == 10 {
		return true
	}
	if failAt == 11 {
		err = failWith
	}
	last = protocol.EXPECT().WriteFieldEnd().Return(err).After(last)
	if failAt == 11 {
		return true
	}
	if failAt == 12 {
		err = failWith
	}

	last = protocol.EXPECT().WriteFieldBegin("s", thrift.TType(thrift.SET), int16(3)).Return(err).After(last)
	if failAt == 12 {
		return true
	}
	if failAt == 13 {
		err = failWith
	}
	last = protocol.EXPECT().WriteSetBegin(thrift.TType(thrift.STRING), 0).Return(err).After(last)
	if failAt == 13 {
		return true
	}
	if failAt == 14 {
		err = failWith
	}
	last = protocol.EXPECT().WriteSetEnd().Return(err).After(last)
	if failAt == 14 {
		return true
	}
	if failAt == 15 {
		err = failWith
	}
	last = protocol.EXPECT().WriteFieldEnd().Return(err).After(last)
	if failAt == 15 {
		return true
	}
	if failAt == 16 {
		err = failWith
	}
	last = protocol.EXPECT().WriteFieldBegin("i", thrift.TType(thrift.I32), int16(4)).Return(err).After(last)
	if failAt == 16 {
		return true
	}
	if failAt == 17 {
		err = failWith
	}
	last = protocol.EXPECT().WriteI32(int32(3)).Return(err).After(last)
	if failAt == 17 {
		return true
	}
	if failAt == 18 {
		err = failWith
	}
	last = protocol.EXPECT().WriteFieldEnd().Return(err).After(last)
	if failAt == 18 {
		return true
	}
	if failAt == 19 {
		err = failWith
	}
	last = protocol.EXPECT().WriteFieldStop().Return(err).After(last)
	if failAt == 19 {
		return true
	}
	if failAt == 20 {
		err = failWith
	}
	last = protocol.EXPECT().WriteStructEnd().Return(err).After(last)
	if failAt == 20 {
		return true
	}
	if failAt == 21 {
		err = failWith
	}
	last = protocol.EXPECT().WriteFieldEnd().Return(err).After(last)
	if failAt == 21 {
		return true
	}
	if failAt == 22 {
		err = failWith
	}
	last = protocol.EXPECT().WriteFieldStop().Return(err).After(last)
	if failAt == 22 {
		return true
	}
	if failAt == 23 {
		err = failWith
	}
	last = protocol.EXPECT().WriteStructEnd().Return(err).After(last)
	if failAt == 23 {
		return true
	}
	if failAt == 24 {
		err = failWith
	}
	last = protocol.EXPECT().WriteMessageEnd().Return(err).After(last)
	if failAt == 24 {
		return true
	}
	if failAt == 25 {
		err = failWith
	}
	last = protocol.EXPECT().Flush(context.Background()).Return(err).After(last)
	if failAt == 25 {
		return true
	}
	if failAt == 26 {
		err = failWith
	}
	last = protocol.EXPECT().ReadMessageBegin().Return("testStruct", thrift.REPLY, int32(1), err).After(last)
	if failAt == 26 {
		return true
	}
	if failAt == 27 {
		err = failWith
	}
	last = protocol.EXPECT().ReadStructBegin().Return("testStruct_args", err).After(last)
	if failAt == 27 {
		return true
	}
	if failAt == 28 {
		err = failWith
	}
	last = protocol.EXPECT().ReadFieldBegin().Return("_", thrift.TType(thrift.STRUCT), int16(0), err).After(last)
	if failAt == 28 {
		return true
	}
	if failAt == 29 {
		err = failWith
	}
	last = protocol.EXPECT().ReadStructBegin().Return("TestStruct", err).After(last)
	if failAt == 29 {
		return true
	}
	if failAt == 30 {
		err = failWith
	}
	last = protocol.EXPECT().ReadFieldBegin().Return("m", thrift.TType(thrift.MAP), int16(1), err).After(last)
	if failAt == 30 {
		return true
	}
	if failAt == 31 {
		err = failWith
	}
	last = protocol.EXPECT().ReadMapBegin().Return(thrift.TType(thrift.STRING), thrift.TType(thrift.STRING), 0, err).After(last)
	if failAt == 31 {
		return true
	}
	if failAt == 32 {
		err = failWith
	}
	last = protocol.EXPECT().ReadMapEnd().Return(err).After(last)
	if failAt == 32 {
		return true
	}
	if failAt == 33 {
		err = failWith
	}
	last = protocol.EXPECT().ReadFieldEnd().Return(err).After(last)
	if failAt == 33 {
		return true
	}
	if failAt == 34 {
		err = failWith
	}
	last = protocol.EXPECT().ReadFieldBegin().Return("l", thrift.TType(thrift.LIST), int16(2), err).After(last)
	if failAt == 34 {
		return true
	}
	if failAt == 35 {
		err = failWith
	}
	last = protocol.EXPECT().ReadListBegin().Return(thrift.TType(thrift.STRING), 0, err).After(last)
	if failAt == 35 {
		return true
	}
	if failAt == 36 {
		err = failWith
	}
	last = protocol.EXPECT().ReadListEnd().Return(err).After(last)
	if failAt == 36 {
		return true
	}
	if failAt == 37 {
		err = failWith
	}
	last = protocol.EXPECT().ReadFieldEnd().Return(err).After(last)
	if failAt == 37 {
		return true
	}
	if failAt == 38 {
		err = failWith
	}
	last = protocol.EXPECT().ReadFieldBegin().Return("s", thrift.TType(thrift.SET), int16(3), err).After(last)
	if failAt == 38 {
		return true
	}
	if failAt == 39 {
		err = failWith
	}
	last = protocol.EXPECT().ReadSetBegin().Return(thrift.TType(thrift.STRING), 0, err).After(last)
	if failAt == 39 {
		return true
	}
	if failAt == 40 {
		err = failWith
	}
	last = protocol.EXPECT().ReadSetEnd().Return(err).After(last)
	if failAt == 40 {
		return true
	}
	if failAt == 41 {
		err = failWith
	}
	last = protocol.EXPECT().ReadFieldEnd().Return(err).After(last)
	if failAt == 41 {
		return true
	}
	if failAt == 42 {
		err = failWith
	}
	last = protocol.EXPECT().ReadFieldBegin().Return("i", thrift.TType(thrift.I32), int16(4), err).After(last)
	if failAt == 42 {
		return true
	}
	if failAt == 43 {
		err = failWith
	}
	last = protocol.EXPECT().ReadI32().Return(int32(3), err).After(last)
	if failAt == 43 {
		return true
	}
	if failAt == 44 {
		err = failWith
	}
	last = protocol.EXPECT().ReadFieldEnd().Return(err).After(last)
	if failAt == 44 {
		return true
	}
	if failAt == 45 {
		err = failWith
	}
	last = protocol.EXPECT().ReadFieldBegin().Return("_", thrift.TType(thrift.STOP), int16(5), err).After(last)
	if failAt == 45 {
		return true
	}
	if failAt == 46 {
		err = failWith
	}
	last = protocol.EXPECT().ReadStructEnd().Return(err).After(last)
	if failAt == 46 {
		return true
	}
	if failAt == 47 {
		err = failWith
	}
	last = protocol.EXPECT().ReadFieldEnd().Return(err).After(last)
	if failAt == 47 {
		return true
	}
	if failAt == 48 {
		err = failWith
	}
	last = protocol.EXPECT().ReadFieldBegin().Return("_", thrift.TType(thrift.STOP), int16(1), err).After(last)
	if failAt == 48 {
		return true
	}
	if failAt == 49 {
		err = failWith
	}
	last = protocol.EXPECT().ReadStructEnd().Return(err).After(last)
	if failAt == 49 {
		return true
	}
	if failAt == 50 {
		err = failWith
	}
	last = protocol.EXPECT().ReadMessageEnd().Return(err).After(last)
	if failAt == 50 {
		return true
	}
	return false
}

// TestCase: Comprehensive call and reply workflow in the client.
// Expecting TTransportError on fail.
func TestClientReportTTransportErrors(t *testing.T) {
	mockCtrl := gomock.NewController(t)

	thing := errortest.NewTestStruct()
	thing.M = make(map[string]string)
	thing.L = make([]string, 0)
	thing.S = make([]string, 0)
	thing.I = 3

	err := thrift.NewTTransportException(thrift.TIMED_OUT, "test")
	for i := 0; ; i++ {
		protocol := NewMockTProtocol(mockCtrl)
		if !prepareClientCallReply(protocol, i, err) {
			return
		}
		client := errortest.NewErrorTestClient(thrift.NewTStandardClient(protocol, protocol))
		_, retErr := client.TestStruct(defaultCtx, thing)
		mockCtrl.Finish()
		mockCtrl = gomock.NewController(t)
		err2, ok := retErr.(thrift.TTransportException)
		if !ok {
			t.Fatal("Expected a TTrasportException")
		}

		if err2.TypeId() != thrift.TIMED_OUT {
			t.Fatal("Expected TIMED_OUT error")
		}
	}
}

// TestCase: Comprehensive call and reply workflow in the client.
// Expecting TTransportError on fail.
// Similar to TestClientReportTTransportErrors, but using legacy client constructor.
func TestClientReportTTransportErrorsLegacy(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	transport := thrift.NewTMemoryBuffer()
	thing := errortest.NewTestStruct()
	thing.M = make(map[string]string)
	thing.L = make([]string, 0)
	thing.S = make([]string, 0)
	thing.I = 3

	err := thrift.NewTTransportException(thrift.TIMED_OUT, "test")
	for i := 0; ; i++ {
		protocol := NewMockTProtocol(mockCtrl)
		if !prepareClientCallReply(protocol, i, err) {
			return
		}
		client := errortest.NewErrorTestClientProtocol(transport, protocol, protocol)
		_, retErr := client.TestStruct(defaultCtx, thing)
		mockCtrl.Finish()
		mockCtrl = gomock.NewController(t)
		err2, ok := retErr.(thrift.TTransportException)
		if !ok {
			t.Fatal("Expected a TTrasportException")
		}

		if err2.TypeId() != thrift.TIMED_OUT {
			t.Fatal("Expected TIMED_OUT error")
		}
	}
}

// TestCase: Comprehensive call and reply workflow in the client.
// Expecting TTProtocolErrors on fail.
func TestClientReportTProtocolErrors(t *testing.T) {
	mockCtrl := gomock.NewController(t)

	thing := errortest.NewTestStruct()
	thing.M = make(map[string]string)
	thing.L = make([]string, 0)
	thing.S = make([]string, 0)
	thing.I = 3

	err := thrift.NewTProtocolExceptionWithType(thrift.INVALID_DATA, errors.New("test"))
	for i := 0; ; i++ {
		protocol := NewMockTProtocol(mockCtrl)
		if !prepareClientCallReply(protocol, i, err) {
			return
		}
		client := errortest.NewErrorTestClient(thrift.NewTStandardClient(protocol, protocol))
		_, retErr := client.TestStruct(defaultCtx, thing)
		mockCtrl.Finish()
		mockCtrl = gomock.NewController(t)
		err2, ok := retErr.(thrift.TProtocolException)
		if !ok {
			t.Fatal("Expected a TProtocolException")
		}
		if err2.TypeId() != thrift.INVALID_DATA {
			t.Fatal("Expected INVALID_DATA error")
		}
	}
}

// TestCase: Comprehensive call and reply workflow in the client.
// Expecting TTProtocolErrors on fail.
// Similar to TestClientReportTProtocolErrors, but using legacy client constructor.
func TestClientReportTProtocolErrorsLegacy(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	transport := thrift.NewTMemoryBuffer()
	thing := errortest.NewTestStruct()
	thing.M = make(map[string]string)
	thing.L = make([]string, 0)
	thing.S = make([]string, 0)
	thing.I = 3

	err := thrift.NewTProtocolExceptionWithType(thrift.INVALID_DATA, errors.New("test"))
	for i := 0; ; i++ {
		protocol := NewMockTProtocol(mockCtrl)
		if !prepareClientCallReply(protocol, i, err) {
			return
		}
		client := errortest.NewErrorTestClientProtocol(transport, protocol, protocol)
		_, retErr := client.TestStruct(defaultCtx, thing)
		mockCtrl.Finish()
		mockCtrl = gomock.NewController(t)
		err2, ok := retErr.(thrift.TProtocolException)
		if !ok {
			t.Fatal("Expected a TProtocolException")
		}
		if err2.TypeId() != thrift.INVALID_DATA {
			t.Fatal("Expected INVALID_DATA error")
		}
	}
}

// TestCase: call and reply with exception workflow in the client.
// Setup mock to fail at a certain position. Return true if position exists otherwise false.
func prepareClientCallException(protocol *MockTProtocol, failAt int, failWith error) bool {
	var err error = nil

	// No need to test failure in this block, because it is covered in other test cases
	last := protocol.EXPECT().WriteMessageBegin("testString", thrift.CALL, int32(1))
	last = protocol.EXPECT().WriteStructBegin("testString_args").After(last)
	last = protocol.EXPECT().WriteFieldBegin("s", thrift.TType(thrift.STRING), int16(1)).After(last)
	last = protocol.EXPECT().WriteString("test").After(last)
	last = protocol.EXPECT().WriteFieldEnd().After(last)
	last = protocol.EXPECT().WriteFieldStop().After(last)
	last = protocol.EXPECT().WriteStructEnd().After(last)
	last = protocol.EXPECT().WriteMessageEnd().After(last)
	last = protocol.EXPECT().Flush(context.Background()).After(last)

	// Reading the exception, might fail.
	if failAt == 0 {
		err = failWith
	}
	last = protocol.EXPECT().ReadMessageBegin().Return("testString", thrift.EXCEPTION, int32(1), err).After(last)
	if failAt == 0 {
		return true
	}
	if failAt == 1 {
		err = failWith
	}
	last = protocol.EXPECT().ReadStructBegin().Return("TApplicationException", err).After(last)
	if failAt == 1 {
		return true
	}
	if failAt == 2 {
		err = failWith
	}
	last = protocol.EXPECT().ReadFieldBegin().Return("message", thrift.TType(thrift.STRING), int16(1), err).After(last)
	if failAt == 2 {
		return true
	}
	if failAt == 3 {
		err = failWith
	}
	last = protocol.EXPECT().ReadString().Return("test", err).After(last)
	if failAt == 3 {
		return true
	}
	if failAt == 4 {
		err = failWith
	}
	last = protocol.EXPECT().ReadFieldEnd().Return(err).After(last)
	if failAt == 4 {
		return true
	}
	if failAt == 5 {
		err = failWith
	}
	last = protocol.EXPECT().ReadFieldBegin().Return("type", thrift.TType(thrift.I32), int16(2), err).After(last)
	if failAt == 5 {
		return true
	}
	if failAt == 6 {
		err = failWith
	}
	last = protocol.EXPECT().ReadI32().Return(int32(thrift.PROTOCOL_ERROR), err).After(last)
	if failAt == 6 {
		return true
	}
	if failAt == 7 {
		err = failWith
	}
	last = protocol.EXPECT().ReadFieldEnd().Return(err).After(last)
	if failAt == 7 {
		return true
	}
	if failAt == 8 {
		err = failWith
	}
	last = protocol.EXPECT().ReadFieldBegin().Return("_", thrift.TType(thrift.STOP), int16(2), err).After(last)
	if failAt == 8 {
		return true
	}
	if failAt == 9 {
		err = failWith
	}
	last = protocol.EXPECT().ReadStructEnd().Return(err).After(last)
	if failAt == 9 {
		return true
	}
	if failAt == 10 {
		err = failWith
	}
	last = protocol.EXPECT().ReadMessageEnd().Return(err).After(last)
	if failAt == 10 {
		return true
	}

	return false
}

// TestCase: call and reply with exception workflow in the client.
func TestClientCallException(t *testing.T) {
	mockCtrl := gomock.NewController(t)

	err := thrift.NewTTransportException(thrift.TIMED_OUT, "test")
	for i := 0; ; i++ {
		protocol := NewMockTProtocol(mockCtrl)
		willComplete := !prepareClientCallException(protocol, i, err)

		client := errortest.NewErrorTestClient(thrift.NewTStandardClient(protocol, protocol))
		_, retErr := client.TestString(defaultCtx, "test")
		mockCtrl.Finish()
		mockCtrl = gomock.NewController(t)

		if !willComplete {
			err2, ok := retErr.(thrift.TTransportException)
			if !ok {
				t.Fatal("Expected a TTransportException")
			}
			if err2.TypeId() != thrift.TIMED_OUT {
				t.Fatal("Expected TIMED_OUT error")
			}
		} else {
			err2, ok := retErr.(thrift.TApplicationException)
			if !ok {
				t.Fatal("Expected a TApplicationException")
			}
			if err2.TypeId() != thrift.PROTOCOL_ERROR {
				t.Fatal("Expected PROTOCOL_ERROR error")
			}
			break
		}
	}
}

// TestCase: call and reply with exception workflow in the client.
// Similar to TestClientCallException, but using legacy client constructor.
func TestClientCallExceptionLegacy(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	transport := thrift.NewTMemoryBuffer()
	err := thrift.NewTTransportException(thrift.TIMED_OUT, "test")
	for i := 0; ; i++ {
		protocol := NewMockTProtocol(mockCtrl)
		willComplete := !prepareClientCallException(protocol, i, err)

		client := errortest.NewErrorTestClientProtocol(transport, protocol, protocol)
		_, retErr := client.TestString(defaultCtx, "test")
		mockCtrl.Finish()
		mockCtrl = gomock.NewController(t)

		if !willComplete {
			err2, ok := retErr.(thrift.TTransportException)
			if !ok {
				t.Fatal("Expected a TTransportException")
			}
			if err2.TypeId() != thrift.TIMED_OUT {
				t.Fatal("Expected TIMED_OUT error")
			}
		} else {
			err2, ok := retErr.(thrift.TApplicationException)
			if !ok {
				t.Fatal("Expected a TApplicationException")
			}
			if err2.TypeId() != thrift.PROTOCOL_ERROR {
				t.Fatal("Expected PROTOCOL_ERROR error")
			}
			break
		}
	}
}

// TestCase: Mismatching sequence id has been received in the client.
func TestClientSeqIdMismatch(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	protocol := NewMockTProtocol(mockCtrl)
	gomock.InOrder(
		protocol.EXPECT().WriteMessageBegin("testString", thrift.CALL, int32(1)),
		protocol.EXPECT().WriteStructBegin("testString_args"),
		protocol.EXPECT().WriteFieldBegin("s", thrift.TType(thrift.STRING), int16(1)),
		protocol.EXPECT().WriteString("test"),
		protocol.EXPECT().WriteFieldEnd(),
		protocol.EXPECT().WriteFieldStop(),
		protocol.EXPECT().WriteStructEnd(),
		protocol.EXPECT().WriteMessageEnd(),
		protocol.EXPECT().Flush(context.Background()),
		protocol.EXPECT().ReadMessageBegin().Return("testString", thrift.REPLY, int32(2), nil),
	)

	client := errortest.NewErrorTestClient(thrift.NewTStandardClient(protocol, protocol))
	_, err := client.TestString(defaultCtx, "test")
	mockCtrl.Finish()
	appErr, ok := err.(thrift.TApplicationException)
	if !ok {
		t.Fatal("Expected TApplicationException")
	}
	if appErr.TypeId() != thrift.BAD_SEQUENCE_ID {
		t.Fatal("Expected BAD_SEQUENCE_ID error")
	}
}

// TestCase: Mismatching sequence id has been received in the client.
// Similar to TestClientSeqIdMismatch, but using legacy client constructor.
func TestClientSeqIdMismatchLegeacy(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	transport := thrift.NewTMemoryBuffer()
	protocol := NewMockTProtocol(mockCtrl)
	gomock.InOrder(
		protocol.EXPECT().WriteMessageBegin("testString", thrift.CALL, int32(1)),
		protocol.EXPECT().WriteStructBegin("testString_args"),
		protocol.EXPECT().WriteFieldBegin("s", thrift.TType(thrift.STRING), int16(1)),
		protocol.EXPECT().WriteString("test"),
		protocol.EXPECT().WriteFieldEnd(),
		protocol.EXPECT().WriteFieldStop(),
		protocol.EXPECT().WriteStructEnd(),
		protocol.EXPECT().WriteMessageEnd(),
		protocol.EXPECT().Flush(context.Background()),
		protocol.EXPECT().ReadMessageBegin().Return("testString", thrift.REPLY, int32(2), nil),
	)

	client := errortest.NewErrorTestClientProtocol(transport, protocol, protocol)
	_, err := client.TestString(defaultCtx, "test")
	mockCtrl.Finish()
	appErr, ok := err.(thrift.TApplicationException)
	if !ok {
		t.Fatal("Expected TApplicationException")
	}
	if appErr.TypeId() != thrift.BAD_SEQUENCE_ID {
		t.Fatal("Expected BAD_SEQUENCE_ID error")
	}
}

// TestCase: Wrong method name has been received in the client.
func TestClientWrongMethodName(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	protocol := NewMockTProtocol(mockCtrl)
	gomock.InOrder(
		protocol.EXPECT().WriteMessageBegin("testString", thrift.CALL, int32(1)),
		protocol.EXPECT().WriteStructBegin("testString_args"),
		protocol.EXPECT().WriteFieldBegin("s", thrift.TType(thrift.STRING), int16(1)),
		protocol.EXPECT().WriteString("test"),
		protocol.EXPECT().WriteFieldEnd(),
		protocol.EXPECT().WriteFieldStop(),
		protocol.EXPECT().WriteStructEnd(),
		protocol.EXPECT().WriteMessageEnd(),
		protocol.EXPECT().Flush(context.Background()),
		protocol.EXPECT().ReadMessageBegin().Return("unknown", thrift.REPLY, int32(1), nil),
	)

	client := errortest.NewErrorTestClient(thrift.NewTStandardClient(protocol, protocol))
	_, err := client.TestString(defaultCtx, "test")
	mockCtrl.Finish()
	appErr, ok := err.(thrift.TApplicationException)
	if !ok {
		t.Fatal("Expected TApplicationException")
	}
	if appErr.TypeId() != thrift.WRONG_METHOD_NAME {
		t.Fatal("Expected WRONG_METHOD_NAME error")
	}
}

// TestCase: Wrong method name has been received in the client.
// Similar to TestClientWrongMethodName, but using legacy client constructor.
func TestClientWrongMethodNameLegacy(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	transport := thrift.NewTMemoryBuffer()
	protocol := NewMockTProtocol(mockCtrl)
	gomock.InOrder(
		protocol.EXPECT().WriteMessageBegin("testString", thrift.CALL, int32(1)),
		protocol.EXPECT().WriteStructBegin("testString_args"),
		protocol.EXPECT().WriteFieldBegin("s", thrift.TType(thrift.STRING), int16(1)),
		protocol.EXPECT().WriteString("test"),
		protocol.EXPECT().WriteFieldEnd(),
		protocol.EXPECT().WriteFieldStop(),
		protocol.EXPECT().WriteStructEnd(),
		protocol.EXPECT().WriteMessageEnd(),
		protocol.EXPECT().Flush(context.Background()),
		protocol.EXPECT().ReadMessageBegin().Return("unknown", thrift.REPLY, int32(1), nil),
	)

	client := errortest.NewErrorTestClientProtocol(transport, protocol, protocol)
	_, err := client.TestString(defaultCtx, "test")
	mockCtrl.Finish()
	appErr, ok := err.(thrift.TApplicationException)
	if !ok {
		t.Fatal("Expected TApplicationException")
	}
	if appErr.TypeId() != thrift.WRONG_METHOD_NAME {
		t.Fatal("Expected WRONG_METHOD_NAME error")
	}
}

// TestCase: Wrong message type has been received in the client.
func TestClientWrongMessageType(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	protocol := NewMockTProtocol(mockCtrl)
	gomock.InOrder(
		protocol.EXPECT().WriteMessageBegin("testString", thrift.CALL, int32(1)),
		protocol.EXPECT().WriteStructBegin("testString_args"),
		protocol.EXPECT().WriteFieldBegin("s", thrift.TType(thrift.STRING), int16(1)),
		protocol.EXPECT().WriteString("test"),
		protocol.EXPECT().WriteFieldEnd(),
		protocol.EXPECT().WriteFieldStop(),
		protocol.EXPECT().WriteStructEnd(),
		protocol.EXPECT().WriteMessageEnd(),
		protocol.EXPECT().Flush(context.Background()),
		protocol.EXPECT().ReadMessageBegin().Return("testString", thrift.INVALID_TMESSAGE_TYPE, int32(1), nil),
	)

	client := errortest.NewErrorTestClient(thrift.NewTStandardClient(protocol, protocol))
	_, err := client.TestString(defaultCtx, "test")
	mockCtrl.Finish()
	appErr, ok := err.(thrift.TApplicationException)
	if !ok {
		t.Fatal("Expected TApplicationException")
	}
	if appErr.TypeId() != thrift.INVALID_MESSAGE_TYPE_EXCEPTION {
		t.Fatal("Expected INVALID_MESSAGE_TYPE_EXCEPTION error")
	}
}

// TestCase: Wrong message type has been received in the client.
// Similar to TestClientWrongMessageType, but using legacy client constructor.
func TestClientWrongMessageTypeLegacy(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	transport := thrift.NewTMemoryBuffer()
	protocol := NewMockTProtocol(mockCtrl)
	gomock.InOrder(
		protocol.EXPECT().WriteMessageBegin("testString", thrift.CALL, int32(1)),
		protocol.EXPECT().WriteStructBegin("testString_args"),
		protocol.EXPECT().WriteFieldBegin("s", thrift.TType(thrift.STRING), int16(1)),
		protocol.EXPECT().WriteString("test"),
		protocol.EXPECT().WriteFieldEnd(),
		protocol.EXPECT().WriteFieldStop(),
		protocol.EXPECT().WriteStructEnd(),
		protocol.EXPECT().WriteMessageEnd(),
		protocol.EXPECT().Flush(context.Background()),
		protocol.EXPECT().ReadMessageBegin().Return("testString", thrift.INVALID_TMESSAGE_TYPE, int32(1), nil),
	)

	client := errortest.NewErrorTestClientProtocol(transport, protocol, protocol)
	_, err := client.TestString(defaultCtx, "test")
	mockCtrl.Finish()
	appErr, ok := err.(thrift.TApplicationException)
	if !ok {
		t.Fatal("Expected TApplicationException")
	}
	if appErr.TypeId() != thrift.INVALID_MESSAGE_TYPE_EXCEPTION {
		t.Fatal("Expected INVALID_MESSAGE_TYPE_EXCEPTION error")
	}
}
