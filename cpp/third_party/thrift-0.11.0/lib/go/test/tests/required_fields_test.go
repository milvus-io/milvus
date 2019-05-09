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
	"github.com/golang/mock/gomock"
	"optionalfieldstest"
	"testing"
	"thrift"
)

func TestStructReadRequiredFields(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	protocol := NewMockTProtocol(mockCtrl)
	testStruct := optionalfieldstest.NewStructC()

	// None of required fields are set
	gomock.InOrder(
		protocol.EXPECT().ReadStructBegin().Return("StructC", nil),
		protocol.EXPECT().ReadFieldBegin().Return("_", thrift.TType(thrift.STOP), int16(1), nil),
		protocol.EXPECT().ReadStructEnd().Return(nil),
	)

	err := testStruct.Read(protocol)
	mockCtrl.Finish()
	if err == nil {
		t.Fatal("Expected read to fail")
	}
	err2, ok := err.(thrift.TProtocolException)
	if !ok {
		t.Fatal("Expected a TProtocolException")
	}
	if err2.TypeId() != thrift.INVALID_DATA {
		t.Fatal("Expected INVALID_DATA TProtocolException")
	}

	// One of the required fields is set
	gomock.InOrder(
		protocol.EXPECT().ReadStructBegin().Return("StructC", nil),
		protocol.EXPECT().ReadFieldBegin().Return("I", thrift.TType(thrift.I32), int16(2), nil),
		protocol.EXPECT().ReadI32().Return(int32(1), nil),
		protocol.EXPECT().ReadFieldEnd().Return(nil),
		protocol.EXPECT().ReadFieldBegin().Return("_", thrift.TType(thrift.STOP), int16(1), nil),
		protocol.EXPECT().ReadStructEnd().Return(nil),
	)

	err = testStruct.Read(protocol)
	mockCtrl.Finish()
	if err == nil {
		t.Fatal("Expected read to fail")
	}
	err2, ok = err.(thrift.TProtocolException)
	if !ok {
		t.Fatal("Expected a TProtocolException")
	}
	if err2.TypeId() != thrift.INVALID_DATA {
		t.Fatal("Expected INVALID_DATA TProtocolException")
	}

	// Both of the required fields are set
	gomock.InOrder(
		protocol.EXPECT().ReadStructBegin().Return("StructC", nil),
		protocol.EXPECT().ReadFieldBegin().Return("i", thrift.TType(thrift.I32), int16(2), nil),
		protocol.EXPECT().ReadI32().Return(int32(1), nil),
		protocol.EXPECT().ReadFieldEnd().Return(nil),
		protocol.EXPECT().ReadFieldBegin().Return("s2", thrift.TType(thrift.STRING), int16(4), nil),
		protocol.EXPECT().ReadString().Return("test", nil),
		protocol.EXPECT().ReadFieldEnd().Return(nil),
		protocol.EXPECT().ReadFieldBegin().Return("_", thrift.TType(thrift.STOP), int16(1), nil),
		protocol.EXPECT().ReadStructEnd().Return(nil),
	)

	err = testStruct.Read(protocol)
	mockCtrl.Finish()
	if err != nil {
		t.Fatal("Expected read to succeed")
	}
}
