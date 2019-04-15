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
	"bytes"
	gomock "github.com/golang/mock/gomock"
	"optionalfieldstest"
	"testing"
	"thrift"
)

func TestIsSetReturnFalseOnCreation(t *testing.T) {
	ao := optionalfieldstest.NewAllOptional()
	if ao.IsSetS() {
		t.Errorf("Optional field S is set on initialization")
	}
	if ao.IsSetI() {
		t.Errorf("Optional field I is set on initialization")
	}
	if ao.IsSetB() {
		t.Errorf("Optional field B is set on initialization")
	}
	if ao.IsSetS2() {
		t.Errorf("Optional field S2 is set on initialization")
	}
	if ao.IsSetI2() {
		t.Errorf("Optional field I2 is set on initialization")
	}
	if ao.IsSetB2() {
		t.Errorf("Optional field B2 is set on initialization")
	}
	if ao.IsSetAa() {
		t.Errorf("Optional field Aa is set on initialization")
	}
	if ao.IsSetL() {
		t.Errorf("Optional field L is set on initialization")
	}
	if ao.IsSetL2() {
		t.Errorf("Optional field L2 is set on initialization")
	}
	if ao.IsSetM() {
		t.Errorf("Optional field M is set on initialization")
	}
	if ao.IsSetM2() {
		t.Errorf("Optional field M2 is set on initialization")
	}
	if ao.IsSetBin() {
		t.Errorf("Optional field Bin is set on initialization")
	}
	if ao.IsSetBin2() {
		t.Errorf("Optional field Bin2 is set on initialization")
	}
}

func TestDefaultValuesOnCreation(t *testing.T) {
	ao := optionalfieldstest.NewAllOptional()
	if ao.GetS() != "DEFAULT" {
		t.Errorf("Unexpected default value %#v for field S", ao.GetS())
	}
	if ao.GetI() != 42 {
		t.Errorf("Unexpected default value %#v for field I", ao.GetI())
	}
	if ao.GetB() != false {
		t.Errorf("Unexpected default value %#v for field B", ao.GetB())
	}
	if ao.GetS2() != "" {
		t.Errorf("Unexpected default value %#v for field S2", ao.GetS2())
	}
	if ao.GetI2() != 0 {
		t.Errorf("Unexpected default value %#v for field I2", ao.GetI2())
	}
	if ao.GetB2() != false {
		t.Errorf("Unexpected default value %#v for field B2", ao.GetB2())
	}
	if l := ao.GetL(); len(l) != 0 {
		t.Errorf("Unexpected default value %#v for field L", l)
	}
	if l := ao.GetL2(); len(l) != 2 || l[0] != 1 || l[1] != 2 {
		t.Errorf("Unexpected default value %#v for field L2", l)
	}
	//FIXME: should we return empty map here?
	if m := ao.GetM(); m != nil {
		t.Errorf("Unexpected default value %#v for field M", m)
	}
	if m := ao.GetM2(); len(m) != 2 || m[1] != 2 || m[3] != 4 {
		t.Errorf("Unexpected default value %#v for field M2", m)
	}
	if bv := ao.GetBin(); bv != nil {
		t.Errorf("Unexpected default value %#v for field Bin", bv)
	}
	if bv := ao.GetBin2(); !bytes.Equal(bv, []byte("asdf")) {
		t.Errorf("Unexpected default value %#v for field Bin2", bv)
	}
}

func TestInitialValuesOnCreation(t *testing.T) {
	ao := optionalfieldstest.NewAllOptional()
	if ao.S != "DEFAULT" {
		t.Errorf("Unexpected initial value %#v for field S", ao.S)
	}
	if ao.I != 42 {
		t.Errorf("Unexpected initial value %#v for field I", ao.I)
	}
	if ao.B != false {
		t.Errorf("Unexpected initial value %#v for field B", ao.B)
	}
	if ao.S2 != nil {
		t.Errorf("Unexpected initial value %#v for field S2", ao.S2)
	}
	if ao.I2 != nil {
		t.Errorf("Unexpected initial value %#v for field I2", ao.I2)
	}
	if ao.B2 != nil {
		t.Errorf("Unexpected initial value %#v for field B2", ao.B2)
	}
	if ao.L != nil || len(ao.L) != 0 {
		t.Errorf("Unexpected initial value %#v for field L", ao.L)
	}
	if ao.L2 != nil {
		t.Errorf("Unexpected initial value %#v for field L2", ao.L2)
	}
	if ao.M != nil {
		t.Errorf("Unexpected initial value %#v for field M", ao.M)
	}
	if ao.M2 != nil {
		t.Errorf("Unexpected initial value %#v for field M2", ao.M2)
	}
	if ao.Bin != nil || len(ao.Bin) != 0 {
		t.Errorf("Unexpected initial value %#v for field Bin", ao.Bin)
	}
	if !bytes.Equal(ao.Bin2, []byte("asdf")) {
		t.Errorf("Unexpected initial value %#v for field Bin2", ao.Bin2)
	}
}

func TestIsSetReturnTrueAfterUpdate(t *testing.T) {
	ao := optionalfieldstest.NewAllOptional()
	ao.S = "somevalue"
	ao.I = 123
	ao.B = true
	ao.Aa = optionalfieldstest.NewStructA()
	if !ao.IsSetS() {
		t.Errorf("Field S should be set")
	}
	if !ao.IsSetI() {
		t.Errorf("Field I should be set")
	}
	if !ao.IsSetB() {
		t.Errorf("Field B should be set")
	}
	if !ao.IsSetAa() {
		t.Errorf("Field aa should be set")
	}
}

func TestListNotEmpty(t *testing.T) {
	ao := optionalfieldstest.NewAllOptional()
	ao.L = []int64{1, 2, 3}
	if !ao.IsSetL() {
		t.Errorf("Field L should be set")
	}
}

//Make sure that optional fields are not being serialized
func TestNoOptionalUnsetFieldsOnWire(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	proto := NewMockTProtocol(mockCtrl)
	gomock.InOrder(
		proto.EXPECT().WriteStructBegin("all_optional").Return(nil),
		proto.EXPECT().WriteFieldStop().Return(nil),
		proto.EXPECT().WriteStructEnd().Return(nil),
	)
	ao := optionalfieldstest.NewAllOptional()
	ao.Write(proto)
}

func TestNoSetToDefaultFieldsOnWire(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	proto := NewMockTProtocol(mockCtrl)
	gomock.InOrder(
		proto.EXPECT().WriteStructBegin("all_optional").Return(nil),
		proto.EXPECT().WriteFieldStop().Return(nil),
		proto.EXPECT().WriteStructEnd().Return(nil),
	)
	ao := optionalfieldstest.NewAllOptional()
	ao.I = 42
	ao.Write(proto)
}

//Make sure that only one field is being serialized when set to non-default
func TestOneISetFieldOnWire(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	proto := NewMockTProtocol(mockCtrl)
	gomock.InOrder(
		proto.EXPECT().WriteStructBegin("all_optional").Return(nil),
		proto.EXPECT().WriteFieldBegin("i", thrift.TType(thrift.I64), int16(2)).Return(nil),
		proto.EXPECT().WriteI64(int64(123)).Return(nil),
		proto.EXPECT().WriteFieldEnd().Return(nil),
		proto.EXPECT().WriteFieldStop().Return(nil),
		proto.EXPECT().WriteStructEnd().Return(nil),
	)
	ao := optionalfieldstest.NewAllOptional()
	ao.I = 123
	ao.Write(proto)
}

func TestOneLSetFieldOnWire(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	proto := NewMockTProtocol(mockCtrl)
	gomock.InOrder(
		proto.EXPECT().WriteStructBegin("all_optional").Return(nil),
		proto.EXPECT().WriteFieldBegin("l", thrift.TType(thrift.LIST), int16(9)).Return(nil),
		proto.EXPECT().WriteListBegin(thrift.TType(thrift.I64), 2).Return(nil),
		proto.EXPECT().WriteI64(int64(1)).Return(nil),
		proto.EXPECT().WriteI64(int64(2)).Return(nil),
		proto.EXPECT().WriteListEnd().Return(nil),
		proto.EXPECT().WriteFieldEnd().Return(nil),
		proto.EXPECT().WriteFieldStop().Return(nil),
		proto.EXPECT().WriteStructEnd().Return(nil),
	)
	ao := optionalfieldstest.NewAllOptional()
	ao.L = []int64{1, 2}
	ao.Write(proto)
}

func TestOneBinSetFieldOnWire(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	proto := NewMockTProtocol(mockCtrl)
	gomock.InOrder(
		proto.EXPECT().WriteStructBegin("all_optional").Return(nil),
		proto.EXPECT().WriteFieldBegin("bin", thrift.TType(thrift.STRING), int16(13)).Return(nil),
		proto.EXPECT().WriteBinary([]byte("somebytestring")).Return(nil),
		proto.EXPECT().WriteFieldEnd().Return(nil),
		proto.EXPECT().WriteFieldStop().Return(nil),
		proto.EXPECT().WriteStructEnd().Return(nil),
	)
	ao := optionalfieldstest.NewAllOptional()
	ao.Bin = []byte("somebytestring")
	ao.Write(proto)
}

func TestOneEmptyBinSetFieldOnWire(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	proto := NewMockTProtocol(mockCtrl)
	gomock.InOrder(
		proto.EXPECT().WriteStructBegin("all_optional").Return(nil),
		proto.EXPECT().WriteFieldBegin("bin", thrift.TType(thrift.STRING), int16(13)).Return(nil),
		proto.EXPECT().WriteBinary([]byte{}).Return(nil),
		proto.EXPECT().WriteFieldEnd().Return(nil),
		proto.EXPECT().WriteFieldStop().Return(nil),
		proto.EXPECT().WriteStructEnd().Return(nil),
	)
	ao := optionalfieldstest.NewAllOptional()
	ao.Bin = []byte{}
	ao.Write(proto)
}
