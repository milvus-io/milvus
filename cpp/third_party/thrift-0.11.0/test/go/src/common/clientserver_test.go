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

package common

import (
	"errors"
	"gen/thrifttest"
	"reflect"
	"sync"
	"testing"
	"thrift"

	"github.com/golang/mock/gomock"
)

type test_unit struct {
	host          string
	port          int64
	domain_socket string
	transport     string
	protocol      string
	ssl           bool
}

var units = []test_unit{
	{"127.0.0.1", 9095, "", "", "binary", false},
	{"127.0.0.1", 9091, "", "", "compact", false},
	{"127.0.0.1", 9092, "", "", "binary", true},
	{"127.0.0.1", 9093, "", "", "compact", true},
}

func TestAllConnection(t *testing.T) {
	certPath = "../../../keys"
	wg := &sync.WaitGroup{}
	wg.Add(len(units))
	for _, unit := range units {
		go func(u test_unit) {
			defer wg.Done()
			doUnit(t, &u)
		}(unit)
	}
	wg.Wait()
}

func doUnit(t *testing.T, unit *test_unit) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	handler := NewMockThriftTest(ctrl)

	processor, serverTransport, transportFactory, protocolFactory, err := GetServerParams(unit.host, unit.port, unit.domain_socket, unit.transport, unit.protocol, unit.ssl, "../../../keys", handler)

	server := thrift.NewTSimpleServer4(processor, serverTransport, transportFactory, protocolFactory)
	if err = server.Listen(); err != nil {
		t.Errorf("Unable to start server: %v", err)
		return
	}
	go server.AcceptLoop()
	defer server.Stop()
	client, trans, err := StartClient(unit.host, unit.port, unit.domain_socket, unit.transport, unit.protocol, unit.ssl)
	if err != nil {
		t.Errorf("Unable to start client: %v", err)
		return
	}
	defer trans.Close()
	callEverythingWithMock(t, client, handler)
}

var rmapmap = map[int32]map[int32]int32{
	-4: map[int32]int32{-4: -4, -3: -3, -2: -2, -1: -1},
	4:  map[int32]int32{4: 4, 3: 3, 2: 2, 1: 1},
}

var xxs = &thrifttest.Xtruct{
	StringThing: "Hello2",
	ByteThing:   42,
	I32Thing:    4242,
	I64Thing:    424242,
}

var xcept = &thrifttest.Xception{ErrorCode: 1001, Message: "some"}

func callEverythingWithMock(t *testing.T, client *thrifttest.ThriftTestClient, handler *MockThriftTest) {
	gomock.InOrder(
		handler.EXPECT().TestVoid(gomock.Any()),
		handler.EXPECT().TestString(gomock.Any(), "thing").Return("thing", nil),
		handler.EXPECT().TestBool(gomock.Any(), true).Return(true, nil),
		handler.EXPECT().TestBool(gomock.Any(), false).Return(false, nil),
		handler.EXPECT().TestByte(gomock.Any(), int8(42)).Return(int8(42), nil),
		handler.EXPECT().TestI32(gomock.Any(), int32(4242)).Return(int32(4242), nil),
		handler.EXPECT().TestI64(gomock.Any(), int64(424242)).Return(int64(424242), nil),
		// TODO: add TestBinary()
		handler.EXPECT().TestDouble(gomock.Any(), float64(42.42)).Return(float64(42.42), nil),
		handler.EXPECT().TestStruct(gomock.Any(), &thrifttest.Xtruct{StringThing: "thing", ByteThing: 42, I32Thing: 4242, I64Thing: 424242}).Return(&thrifttest.Xtruct{StringThing: "thing", ByteThing: 42, I32Thing: 4242, I64Thing: 424242}, nil),
		handler.EXPECT().TestNest(gomock.Any(), &thrifttest.Xtruct2{StructThing: &thrifttest.Xtruct{StringThing: "thing", ByteThing: 42, I32Thing: 4242, I64Thing: 424242}}).Return(&thrifttest.Xtruct2{StructThing: &thrifttest.Xtruct{StringThing: "thing", ByteThing: 42, I32Thing: 4242, I64Thing: 424242}}, nil),
		handler.EXPECT().TestMap(gomock.Any(), map[int32]int32{1: 2, 3: 4, 5: 42}).Return(map[int32]int32{1: 2, 3: 4, 5: 42}, nil),
		handler.EXPECT().TestStringMap(gomock.Any(), map[string]string{"a": "2", "b": "blah", "some": "thing"}).Return(map[string]string{"a": "2", "b": "blah", "some": "thing"}, nil),
		handler.EXPECT().TestSet(gomock.Any(), []int32{1, 2, 42}).Return([]int32{1, 2, 42}, nil),
		handler.EXPECT().TestList(gomock.Any(), []int32{1, 2, 42}).Return([]int32{1, 2, 42}, nil),
		handler.EXPECT().TestEnum(gomock.Any(), thrifttest.Numberz_TWO).Return(thrifttest.Numberz_TWO, nil),
		handler.EXPECT().TestTypedef(gomock.Any(), thrifttest.UserId(42)).Return(thrifttest.UserId(42), nil),
		handler.EXPECT().TestMapMap(gomock.Any(), int32(42)).Return(rmapmap, nil),
		// TODO: not testing insanity
		handler.EXPECT().TestMulti(gomock.Any(), int8(42), int32(4242), int64(424242), map[int16]string{1: "blah", 2: "thing"}, thrifttest.Numberz_EIGHT, thrifttest.UserId(24)).Return(xxs, nil),
		handler.EXPECT().TestException(gomock.Any(), "some").Return(xcept),
		handler.EXPECT().TestException(gomock.Any(), "TException").Return(errors.New("Just random exception")),
		handler.EXPECT().TestMultiException(gomock.Any(), "Xception", "ignoreme").Return(nil, &thrifttest.Xception{ErrorCode: 1001, Message: "This is an Xception"}),
		handler.EXPECT().TestMultiException(gomock.Any(), "Xception2", "ignoreme").Return(nil, &thrifttest.Xception2{ErrorCode: 2002, StructThing: &thrifttest.Xtruct{StringThing: "This is an Xception2"}}),
		handler.EXPECT().TestOneway(gomock.Any(), int32(2)).Return(nil),
		handler.EXPECT().TestVoid(gomock.Any()),
	)
	var err error
	if err = client.TestVoid(defaultCtx); err != nil {
		t.Errorf("Unexpected error in TestVoid() call: ", err)
	}

	thing, err := client.TestString(defaultCtx, "thing")
	if err != nil {
		t.Errorf("Unexpected error in TestString() call: ", err)
	}
	if thing != "thing" {
		t.Errorf("Unexpected TestString() result, expected 'thing' got '%s' ", thing)
	}

	bl, err := client.TestBool(defaultCtx, true)
	if err != nil {
		t.Errorf("Unexpected error in TestBool() call: ", err)
	}
	if !bl {
		t.Errorf("Unexpected TestBool() result expected true, got %f ", bl)
	}
	bl, err = client.TestBool(defaultCtx, false)
	if err != nil {
		t.Errorf("Unexpected error in TestBool() call: ", err)
	}
	if bl {
		t.Errorf("Unexpected TestBool() result expected false, got %f ", bl)
	}

	b, err := client.TestByte(defaultCtx, 42)
	if err != nil {
		t.Errorf("Unexpected error in TestByte() call: ", err)
	}
	if b != 42 {
		t.Errorf("Unexpected TestByte() result expected 42, got %d ", b)
	}

	i32, err := client.TestI32(defaultCtx, 4242)
	if err != nil {
		t.Errorf("Unexpected error in TestI32() call: ", err)
	}
	if i32 != 4242 {
		t.Errorf("Unexpected TestI32() result expected 4242, got %d ", i32)
	}

	i64, err := client.TestI64(defaultCtx, 424242)
	if err != nil {
		t.Errorf("Unexpected error in TestI64() call: ", err)
	}
	if i64 != 424242 {
		t.Errorf("Unexpected TestI64() result expected 424242, got %d ", i64)
	}

	d, err := client.TestDouble(defaultCtx, 42.42)
	if err != nil {
		t.Errorf("Unexpected error in TestDouble() call: ", err)
	}
	if d != 42.42 {
		t.Errorf("Unexpected TestDouble() result expected 42.42, got %f ", d)
	}

	// TODO: add TestBinary() call

	xs := thrifttest.NewXtruct()
	xs.StringThing = "thing"
	xs.ByteThing = 42
	xs.I32Thing = 4242
	xs.I64Thing = 424242
	xsret, err := client.TestStruct(defaultCtx, xs)
	if err != nil {
		t.Errorf("Unexpected error in TestStruct() call: ", err)
	}
	if *xs != *xsret {
		t.Errorf("Unexpected TestStruct() result expected %#v, got %#v ", xs, xsret)
	}

	x2 := thrifttest.NewXtruct2()
	x2.StructThing = xs
	x2ret, err := client.TestNest(defaultCtx, x2)
	if err != nil {
		t.Errorf("Unexpected error in TestNest() call: ", err)
	}
	if !reflect.DeepEqual(x2, x2ret) {
		t.Errorf("Unexpected TestNest() result expected %#v, got %#v ", x2, x2ret)
	}

	m := map[int32]int32{1: 2, 3: 4, 5: 42}
	mret, err := client.TestMap(defaultCtx, m)
	if err != nil {
		t.Errorf("Unexpected error in TestMap() call: ", err)
	}
	if !reflect.DeepEqual(m, mret) {
		t.Errorf("Unexpected TestMap() result expected %#v, got %#v ", m, mret)
	}

	sm := map[string]string{"a": "2", "b": "blah", "some": "thing"}
	smret, err := client.TestStringMap(defaultCtx, sm)
	if err != nil {
		t.Errorf("Unexpected error in TestStringMap() call: ", err)
	}
	if !reflect.DeepEqual(sm, smret) {
		t.Errorf("Unexpected TestStringMap() result expected %#v, got %#v ", sm, smret)
	}

	s := []int32{1, 2, 42}
	sret, err := client.TestSet(defaultCtx, s)
	if err != nil {
		t.Errorf("Unexpected error in TestSet() call: ", err)
	}
	// Sets can be in any order, but Go slices are ordered, so reflect.DeepEqual won't work.
	stemp := map[int32]struct{}{}
	for _, val := range s {
		stemp[val] = struct{}{}
	}
	for _, val := range sret {
		if _, ok := stemp[val]; !ok {
			t.Fatalf("Unexpected TestSet() result expected %#v, got %#v ", s, sret)
		}
	}

	l := []int32{1, 2, 42}
	lret, err := client.TestList(defaultCtx, l)
	if err != nil {
		t.Errorf("Unexpected error in TestList() call: ", err)
	}
	if !reflect.DeepEqual(l, lret) {
		t.Errorf("Unexpected TestList() result expected %#v, got %#v ", l, lret)
	}

	eret, err := client.TestEnum(defaultCtx, thrifttest.Numberz_TWO)
	if err != nil {
		t.Errorf("Unexpected error in TestEnum() call: ", err)
	}
	if eret != thrifttest.Numberz_TWO {
		t.Errorf("Unexpected TestEnum() result expected %#v, got %#v ", thrifttest.Numberz_TWO, eret)
	}

	tret, err := client.TestTypedef(defaultCtx, thrifttest.UserId(42))
	if err != nil {
		t.Errorf("Unexpected error in TestTypedef() call: ", err)
	}
	if tret != thrifttest.UserId(42) {
		t.Errorf("Unexpected TestTypedef() result expected %#v, got %#v ", thrifttest.UserId(42), tret)
	}

	mapmap, err := client.TestMapMap(defaultCtx, 42)
	if err != nil {
		t.Errorf("Unexpected error in TestMapmap() call: ", err)
	}
	if !reflect.DeepEqual(mapmap, rmapmap) {
		t.Errorf("Unexpected TestMapmap() result expected %#v, got %#v ", rmapmap, mapmap)
	}

	xxsret, err := client.TestMulti(defaultCtx, 42, 4242, 424242, map[int16]string{1: "blah", 2: "thing"}, thrifttest.Numberz_EIGHT, thrifttest.UserId(24))
	if err != nil {
		t.Errorf("Unexpected error in TestMulti() call: %v", err)
	}
	if !reflect.DeepEqual(xxs, xxsret) {
		t.Errorf("Unexpected TestMulti() result expected %#v, got %#v ", xxs, xxsret)
	}

	err = client.TestException(defaultCtx, "some")
	if err == nil {
		t.Errorf("Expecting exception in TestException() call")
	}
	if !reflect.DeepEqual(err, xcept) {
		t.Errorf("Unexpected TestException() result expected %#v, got %#v ", xcept, err)
	}

	// TODO: connection is being closed on this
	err = client.TestException(defaultCtx, "TException")
	if err == nil {
		t.Error("expected exception got nil")
	} else if tex, ok := err.(thrift.TApplicationException); !ok {
		t.Errorf("Unexpected TestException() result expected ApplicationError, got %T ", err)
	} else if tex.TypeId() != thrift.INTERNAL_ERROR {
		t.Errorf("expected internal_error got %v", tex.TypeId())
	}

	ign, err := client.TestMultiException(defaultCtx, "Xception", "ignoreme")
	if ign != nil || err == nil {
		t.Errorf("Expecting exception in TestMultiException() call")
	}
	if !reflect.DeepEqual(err, &thrifttest.Xception{ErrorCode: 1001, Message: "This is an Xception"}) {
		t.Errorf("Unexpected TestMultiException() %#v ", err)
	}

	ign, err = client.TestMultiException(defaultCtx, "Xception2", "ignoreme")
	if ign != nil || err == nil {
		t.Errorf("Expecting exception in TestMultiException() call")
	}
	expecting := &thrifttest.Xception2{ErrorCode: 2002, StructThing: &thrifttest.Xtruct{StringThing: "This is an Xception2"}}

	if !reflect.DeepEqual(err, expecting) {
		t.Errorf("Unexpected TestMultiException() %#v ", err)
	}

	err = client.TestOneway(defaultCtx, 2)
	if err != nil {
		t.Errorf("Unexpected error in TestOneway() call: ", err)
	}

	//Make sure the connection still alive
	if err = client.TestVoid(defaultCtx); err != nil {
		t.Errorf("Unexpected error in TestVoid() call: ", err)
	}
}
