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

package thrift

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

type ProtocolFactory interface {
	GetProtocol(t TTransport) TProtocol
}

func compareStructs(m, m1 MyTestStruct) (bool, error) {
	switch {
	case m.On != m1.On:
		return false, errors.New("Boolean not equal")
	case m.B != m1.B:
		return false, errors.New("Byte not equal")
	case m.Int16 != m1.Int16:
		return false, errors.New("Int16 not equal")
	case m.Int32 != m1.Int32:
		return false, errors.New("Int32 not equal")
	case m.Int64 != m1.Int64:
		return false, errors.New("Int64 not equal")
	case m.D != m1.D:
		return false, errors.New("Double not equal")
	case m.St != m1.St:
		return false, errors.New("String not equal")

	case len(m.Bin) != len(m1.Bin):
		return false, errors.New("Binary size not equal")
	case len(m.Bin) == len(m1.Bin):
		for i := range m.Bin {
			if m.Bin[i] != m1.Bin[i] {
				return false, errors.New("Binary not equal")
			}
		}
	case len(m.StringMap) != len(m1.StringMap):
		return false, errors.New("StringMap size not equal")
	case len(m.StringList) != len(m1.StringList):
		return false, errors.New("StringList size not equal")
	case len(m.StringSet) != len(m1.StringSet):
		return false, errors.New("StringSet size not equal")

	case m.E != m1.E:
		return false, errors.New("MyTestEnum not equal")

	default:
		return true, nil

	}
	return true, nil
}

func ProtocolTest1(test *testing.T, pf ProtocolFactory) (bool, error) {
	t := NewTSerializer()
	t.Protocol = pf.GetProtocol(t.Transport)
	var m = MyTestStruct{}
	m.On = true
	m.B = int8(0)
	m.Int16 = 1
	m.Int32 = 2
	m.Int64 = 3
	m.D = 4.1
	m.St = "Test"
	m.Bin = make([]byte, 10)
	m.StringMap = make(map[string]string, 5)
	m.StringList = make([]string, 5)
	m.StringSet = make(map[string]struct{}, 5)
	m.E = 2

	s, err := t.WriteString(context.Background(), &m)
	if err != nil {
		return false, errors.New(fmt.Sprintf("Unable to Serialize struct\n\t %s", err))
	}

	t1 := NewTDeserializer()
	t1.Protocol = pf.GetProtocol(t1.Transport)
	var m1 = MyTestStruct{}
	if err = t1.ReadString(&m1, s); err != nil {
		return false, errors.New(fmt.Sprintf("Unable to Deserialize struct\n\t %s", err))

	}

	return compareStructs(m, m1)

}

func ProtocolTest2(test *testing.T, pf ProtocolFactory) (bool, error) {
	t := NewTSerializer()
	t.Protocol = pf.GetProtocol(t.Transport)
	var m = MyTestStruct{}
	m.On = false
	m.B = int8(0)
	m.Int16 = 1
	m.Int32 = 2
	m.Int64 = 3
	m.D = 4.1
	m.St = "Test"
	m.Bin = make([]byte, 10)
	m.StringMap = make(map[string]string, 5)
	m.StringList = make([]string, 5)
	m.StringSet = make(map[string]struct{}, 5)
	m.E = 2

	s, err := t.WriteString(context.Background(), &m)
	if err != nil {
		return false, errors.New(fmt.Sprintf("Unable to Serialize struct\n\t %s", err))

	}

	t1 := NewTDeserializer()
	t1.Protocol = pf.GetProtocol(t1.Transport)
	var m1 = MyTestStruct{}
	if err = t1.ReadString(&m1, s); err != nil {
		return false, errors.New(fmt.Sprintf("Unable to Deserialize struct\n\t %s", err))

	}

	return compareStructs(m, m1)

}

func TestSerializer(t *testing.T) {

	var protocol_factories map[string]ProtocolFactory
	protocol_factories = make(map[string]ProtocolFactory)
	protocol_factories["Binary"] = NewTBinaryProtocolFactoryDefault()
	protocol_factories["Compact"] = NewTCompactProtocolFactory()
	//protocol_factories["SimpleJSON"] = NewTSimpleJSONProtocolFactory() - write only, can't be read back by design
	protocol_factories["JSON"] = NewTJSONProtocolFactory()

	var tests map[string]func(*testing.T, ProtocolFactory) (bool, error)
	tests = make(map[string]func(*testing.T, ProtocolFactory) (bool, error))
	tests["Test 1"] = ProtocolTest1
	tests["Test 2"] = ProtocolTest2
	//tests["Test 3"] = ProtocolTest3 // Example of how to add additional tests

	for name, pf := range protocol_factories {

		for test, f := range tests {

			if s, err := f(t, pf); !s || err != nil {
				t.Errorf("%s Failed for %s protocol\n\t %s", test, name, err)
			}

		}
	}

}
