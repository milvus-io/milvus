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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
)

func TestWriteSimpleJSONProtocolBool(t *testing.T) {
	thetype := "boolean"
	trans := NewTMemoryBuffer()
	p := NewTSimpleJSONProtocol(trans)
	for _, value := range BOOL_VALUES {
		if e := p.WriteBool(value); e != nil {
			t.Fatalf("Unable to write %s value %v due to error: %s", thetype, value, e.Error())
		}
		if e := p.Flush(); e != nil {
			t.Fatalf("Unable to write %s value %v due to error flushing: %s", thetype, value, e.Error())
		}
		s := trans.String()
		if s != fmt.Sprint(value) {
			t.Fatalf("Bad value for %s %v: %s", thetype, value, s)
		}
		v := false
		if err := json.Unmarshal([]byte(s), &v); err != nil || v != value {
			t.Fatalf("Bad json-decoded value for %s %v, wrote: '%s', expected: '%v'", thetype, value, s, v)
		}
		trans.Reset()
	}
	trans.Close()
}

func TestReadSimpleJSONProtocolBool(t *testing.T) {
	thetype := "boolean"
	for _, value := range BOOL_VALUES {
		trans := NewTMemoryBuffer()
		p := NewTSimpleJSONProtocol(trans)
		if value {
			trans.Write(JSON_TRUE)
		} else {
			trans.Write(JSON_FALSE)
		}
		trans.Flush()
		s := trans.String()
		v, e := p.ReadBool()
		if e != nil {
			t.Fatalf("Unable to read %s value %v due to error: %s", thetype, value, e.Error())
		}
		if v != value {
			t.Fatalf("Bad value for %s value %v, wrote: %v, received: %v", thetype, value, s, v)
		}
		if err := json.Unmarshal([]byte(s), &v); err != nil || v != value {
			t.Fatalf("Bad json-decoded value for %s %v, wrote: '%s', expected: '%v'", thetype, value, s, v)
		}
		trans.Reset()
		trans.Close()
	}
}

func TestWriteSimpleJSONProtocolByte(t *testing.T) {
	thetype := "byte"
	trans := NewTMemoryBuffer()
	p := NewTSimpleJSONProtocol(trans)
	for _, value := range BYTE_VALUES {
		if e := p.WriteByte(value); e != nil {
			t.Fatalf("Unable to write %s value %v due to error: %s", thetype, value, e.Error())
		}
		if e := p.Flush(); e != nil {
			t.Fatalf("Unable to write %s value %v due to error flushing: %s", thetype, value, e.Error())
		}
		s := trans.String()
		if s != fmt.Sprint(value) {
			t.Fatalf("Bad value for %s %v: %s", thetype, value, s)
		}
		v := int8(0)
		if err := json.Unmarshal([]byte(s), &v); err != nil || v != value {
			t.Fatalf("Bad json-decoded value for %s %v, wrote: '%s', expected: '%v'", thetype, value, s, v)
		}
		trans.Reset()
	}
	trans.Close()
}

func TestReadSimpleJSONProtocolByte(t *testing.T) {
	thetype := "byte"
	for _, value := range BYTE_VALUES {
		trans := NewTMemoryBuffer()
		p := NewTSimpleJSONProtocol(trans)
		trans.WriteString(strconv.Itoa(int(value)))
		trans.Flush()
		s := trans.String()
		v, e := p.ReadByte()
		if e != nil {
			t.Fatalf("Unable to read %s value %v due to error: %s", thetype, value, e.Error())
		}
		if v != value {
			t.Fatalf("Bad value for %s value %v, wrote: %v, received: %v", thetype, value, s, v)
		}
		if err := json.Unmarshal([]byte(s), &v); err != nil || v != value {
			t.Fatalf("Bad json-decoded value for %s %v, wrote: '%s', expected: '%v'", thetype, value, s, v)
		}
		trans.Reset()
		trans.Close()
	}
}

func TestWriteSimpleJSONProtocolI16(t *testing.T) {
	thetype := "int16"
	trans := NewTMemoryBuffer()
	p := NewTSimpleJSONProtocol(trans)
	for _, value := range INT16_VALUES {
		if e := p.WriteI16(value); e != nil {
			t.Fatalf("Unable to write %s value %v due to error: %s", thetype, value, e.Error())
		}
		if e := p.Flush(); e != nil {
			t.Fatalf("Unable to write %s value %v due to error flushing: %s", thetype, value, e.Error())
		}
		s := trans.String()
		if s != fmt.Sprint(value) {
			t.Fatalf("Bad value for %s %v: %s", thetype, value, s)
		}
		v := int16(0)
		if err := json.Unmarshal([]byte(s), &v); err != nil || v != value {
			t.Fatalf("Bad json-decoded value for %s %v, wrote: '%s', expected: '%v'", thetype, value, s, v)
		}
		trans.Reset()
	}
	trans.Close()
}

func TestReadSimpleJSONProtocolI16(t *testing.T) {
	thetype := "int16"
	for _, value := range INT16_VALUES {
		trans := NewTMemoryBuffer()
		p := NewTSimpleJSONProtocol(trans)
		trans.WriteString(strconv.Itoa(int(value)))
		trans.Flush()
		s := trans.String()
		v, e := p.ReadI16()
		if e != nil {
			t.Fatalf("Unable to read %s value %v due to error: %s", thetype, value, e.Error())
		}
		if v != value {
			t.Fatalf("Bad value for %s value %v, wrote: %v, received: %v", thetype, value, s, v)
		}
		if err := json.Unmarshal([]byte(s), &v); err != nil || v != value {
			t.Fatalf("Bad json-decoded value for %s %v, wrote: '%s', expected: '%v'", thetype, value, s, v)
		}
		trans.Reset()
		trans.Close()
	}
}

func TestWriteSimpleJSONProtocolI32(t *testing.T) {
	thetype := "int32"
	trans := NewTMemoryBuffer()
	p := NewTSimpleJSONProtocol(trans)
	for _, value := range INT32_VALUES {
		if e := p.WriteI32(value); e != nil {
			t.Fatalf("Unable to write %s value %v due to error: %s", thetype, value, e.Error())
		}
		if e := p.Flush(); e != nil {
			t.Fatalf("Unable to write %s value %v due to error flushing: %s", thetype, value, e.Error())
		}
		s := trans.String()
		if s != fmt.Sprint(value) {
			t.Fatalf("Bad value for %s %v: %s", thetype, value, s)
		}
		v := int32(0)
		if err := json.Unmarshal([]byte(s), &v); err != nil || v != value {
			t.Fatalf("Bad json-decoded value for %s %v, wrote: '%s', expected: '%v'", thetype, value, s, v)
		}
		trans.Reset()
	}
	trans.Close()
}

func TestReadSimpleJSONProtocolI32(t *testing.T) {
	thetype := "int32"
	for _, value := range INT32_VALUES {
		trans := NewTMemoryBuffer()
		p := NewTSimpleJSONProtocol(trans)
		trans.WriteString(strconv.Itoa(int(value)))
		trans.Flush()
		s := trans.String()
		v, e := p.ReadI32()
		if e != nil {
			t.Fatalf("Unable to read %s value %v due to error: %s", thetype, value, e.Error())
		}
		if v != value {
			t.Fatalf("Bad value for %s value %v, wrote: %v, received: %v", thetype, value, s, v)
		}
		if err := json.Unmarshal([]byte(s), &v); err != nil || v != value {
			t.Fatalf("Bad json-decoded value for %s %v, wrote: '%s', expected: '%v'", thetype, value, s, v)
		}
		trans.Reset()
		trans.Close()
	}
}

func TestReadSimpleJSONProtocolI32Null(t *testing.T) {
	thetype := "int32"
	value := "null"

	trans := NewTMemoryBuffer()
	p := NewTSimpleJSONProtocol(trans)
	trans.WriteString(value)
	trans.Flush()
	s := trans.String()
	v, e := p.ReadI32()

	if e != nil {
		t.Fatalf("Unable to read %s value %v due to error: %s", thetype, value, e.Error())
	}
	if v != 0 {
		t.Fatalf("Bad value for %s value %v, wrote: %v, received: %v", thetype, value, s, v)
	}
	trans.Reset()
	trans.Close()
}

func TestWriteSimpleJSONProtocolI64(t *testing.T) {
	thetype := "int64"
	trans := NewTMemoryBuffer()
	p := NewTSimpleJSONProtocol(trans)
	for _, value := range INT64_VALUES {
		if e := p.WriteI64(value); e != nil {
			t.Fatalf("Unable to write %s value %v due to error: %s", thetype, value, e.Error())
		}
		if e := p.Flush(); e != nil {
			t.Fatalf("Unable to write %s value %v due to error flushing: %s", thetype, value, e.Error())
		}
		s := trans.String()
		if s != fmt.Sprint(value) {
			t.Fatalf("Bad value for %s %v: %s", thetype, value, s)
		}
		v := int64(0)
		if err := json.Unmarshal([]byte(s), &v); err != nil || v != value {
			t.Fatalf("Bad json-decoded value for %s %v, wrote: '%s', expected: '%v'", thetype, value, s, v)
		}
		trans.Reset()
	}
	trans.Close()
}

func TestReadSimpleJSONProtocolI64(t *testing.T) {
	thetype := "int64"
	for _, value := range INT64_VALUES {
		trans := NewTMemoryBuffer()
		p := NewTSimpleJSONProtocol(trans)
		trans.WriteString(strconv.FormatInt(value, 10))
		trans.Flush()
		s := trans.String()
		v, e := p.ReadI64()
		if e != nil {
			t.Fatalf("Unable to read %s value %v due to error: %s", thetype, value, e.Error())
		}
		if v != value {
			t.Fatalf("Bad value for %s value %v, wrote: %v, received: %v", thetype, value, s, v)
		}
		if err := json.Unmarshal([]byte(s), &v); err != nil || v != value {
			t.Fatalf("Bad json-decoded value for %s %v, wrote: '%s', expected: '%v'", thetype, value, s, v)
		}
		trans.Reset()
		trans.Close()
	}
}

func TestReadSimpleJSONProtocolI64Null(t *testing.T) {
	thetype := "int32"
	value := "null"

	trans := NewTMemoryBuffer()
	p := NewTSimpleJSONProtocol(trans)
	trans.WriteString(value)
	trans.Flush()
	s := trans.String()
	v, e := p.ReadI64()

	if e != nil {
		t.Fatalf("Unable to read %s value %v due to error: %s", thetype, value, e.Error())
	}
	if v != 0 {
		t.Fatalf("Bad value for %s value %v, wrote: %v, received: %v", thetype, value, s, v)
	}
	trans.Reset()
	trans.Close()
}

func TestWriteSimpleJSONProtocolDouble(t *testing.T) {
	thetype := "double"
	trans := NewTMemoryBuffer()
	p := NewTSimpleJSONProtocol(trans)
	for _, value := range DOUBLE_VALUES {
		if e := p.WriteDouble(value); e != nil {
			t.Fatalf("Unable to write %s value %v due to error: %s", thetype, value, e.Error())
		}
		if e := p.Flush(); e != nil {
			t.Fatalf("Unable to write %s value %v due to error flushing: %s", thetype, value, e.Error())
		}
		s := trans.String()
		if math.IsInf(value, 1) {
			if s != jsonQuote(JSON_INFINITY) {
				t.Fatalf("Bad value for %s %v, wrote: %v, expected: %v", thetype, value, s, jsonQuote(JSON_INFINITY))
			}
		} else if math.IsInf(value, -1) {
			if s != jsonQuote(JSON_NEGATIVE_INFINITY) {
				t.Fatalf("Bad value for %s %v, wrote: %v, expected: %v", thetype, value, s, jsonQuote(JSON_NEGATIVE_INFINITY))
			}
		} else if math.IsNaN(value) {
			if s != jsonQuote(JSON_NAN) {
				t.Fatalf("Bad value for %s %v, wrote: %v, expected: %v", thetype, value, s, jsonQuote(JSON_NAN))
			}
		} else {
			if s != fmt.Sprint(value) {
				t.Fatalf("Bad value for %s %v: %s", thetype, value, s)
			}
			v := float64(0)
			if err := json.Unmarshal([]byte(s), &v); err != nil || v != value {
				t.Fatalf("Bad json-decoded value for %s %v, wrote: '%s', expected: '%v'", thetype, value, s, v)
			}
		}
		trans.Reset()
	}
	trans.Close()
}

func TestReadSimpleJSONProtocolDouble(t *testing.T) {
	thetype := "double"
	for _, value := range DOUBLE_VALUES {
		trans := NewTMemoryBuffer()
		p := NewTSimpleJSONProtocol(trans)
		n := NewNumericFromDouble(value)
		trans.WriteString(n.String())
		trans.Flush()
		s := trans.String()
		v, e := p.ReadDouble()
		if e != nil {
			t.Fatalf("Unable to read %s value %v due to error: %s", thetype, value, e.Error())
		}
		if math.IsInf(value, 1) {
			if !math.IsInf(v, 1) {
				t.Fatalf("Bad value for %s %v, wrote: %v, received: %v", thetype, value, s, v)
			}
		} else if math.IsInf(value, -1) {
			if !math.IsInf(v, -1) {
				t.Fatalf("Bad value for %s %v, wrote: %v, received: %v", thetype, value, s, v)
			}
		} else if math.IsNaN(value) {
			if !math.IsNaN(v) {
				t.Fatalf("Bad value for %s %v, wrote: %v, received: %v", thetype, value, s, v)
			}
		} else {
			if v != value {
				t.Fatalf("Bad value for %s value %v, wrote: %v, received: %v", thetype, value, s, v)
			}
			if err := json.Unmarshal([]byte(s), &v); err != nil || v != value {
				t.Fatalf("Bad json-decoded value for %s %v, wrote: '%s', expected: '%v'", thetype, value, s, v)
			}
		}
		trans.Reset()
		trans.Close()
	}
}

func TestWriteSimpleJSONProtocolString(t *testing.T) {
	thetype := "string"
	trans := NewTMemoryBuffer()
	p := NewTSimpleJSONProtocol(trans)
	for _, value := range STRING_VALUES {
		if e := p.WriteString(value); e != nil {
			t.Fatalf("Unable to write %s value %v due to error: %s", thetype, value, e.Error())
		}
		if e := p.Flush(); e != nil {
			t.Fatalf("Unable to write %s value %v due to error flushing: %s", thetype, value, e.Error())
		}
		s := trans.String()
		if s[0] != '"' || s[len(s)-1] != '"' {
			t.Fatalf("Bad value for %s '%v', wrote '%v', expected: %v", thetype, value, s, fmt.Sprint("\"", value, "\""))
		}
		v := new(string)
		if err := json.Unmarshal([]byte(s), v); err != nil || *v != value {
			t.Fatalf("Bad json-decoded value for %s %v, wrote: '%s', expected: '%v'", thetype, value, s, *v)
		}
		trans.Reset()
	}
	trans.Close()
}

func TestReadSimpleJSONProtocolString(t *testing.T) {
	thetype := "string"
	for _, value := range STRING_VALUES {
		trans := NewTMemoryBuffer()
		p := NewTSimpleJSONProtocol(trans)
		trans.WriteString(jsonQuote(value))
		trans.Flush()
		s := trans.String()
		v, e := p.ReadString()
		if e != nil {
			t.Fatalf("Unable to read %s value %v due to error: %s", thetype, value, e.Error())
		}
		if v != value {
			t.Fatalf("Bad value for %s value %v, wrote: %v, received: %v", thetype, value, s, v)
		}
		v1 := new(string)
		if err := json.Unmarshal([]byte(s), v1); err != nil || *v1 != value {
			t.Fatalf("Bad json-decoded value for %s %v, wrote: '%s', expected: '%v'", thetype, value, s, *v1)
		}
		trans.Reset()
		trans.Close()
	}
}
func TestReadSimpleJSONProtocolStringNull(t *testing.T) {
	thetype := "string"
	value := "null"

	trans := NewTMemoryBuffer()
	p := NewTSimpleJSONProtocol(trans)
	trans.WriteString(value)
	trans.Flush()
	s := trans.String()
	v, e := p.ReadString()
	if e != nil {
		t.Fatalf("Unable to read %s value %v due to error: %s", thetype, value, e.Error())
	}
	if v != "" {
		t.Fatalf("Bad value for %s value %v, wrote: %v, received: %v", thetype, value, s, v)
	}
	trans.Reset()
	trans.Close()
}

func TestWriteSimpleJSONProtocolBinary(t *testing.T) {
	thetype := "binary"
	value := protocol_bdata
	b64value := make([]byte, base64.StdEncoding.EncodedLen(len(protocol_bdata)))
	base64.StdEncoding.Encode(b64value, value)
	b64String := string(b64value)
	trans := NewTMemoryBuffer()
	p := NewTSimpleJSONProtocol(trans)
	if e := p.WriteBinary(value); e != nil {
		t.Fatalf("Unable to write %s value %v due to error: %s", thetype, value, e.Error())
	}
	if e := p.Flush(); e != nil {
		t.Fatalf("Unable to write %s value %v due to error flushing: %s", thetype, value, e.Error())
	}
	s := trans.String()
	if s != fmt.Sprint("\"", b64String, "\"") {
		t.Fatalf("Bad value for %s %v\n  wrote: %v\nexpected: %v", thetype, value, s, "\""+b64String+"\"")
	}
	v1 := new(string)
	if err := json.Unmarshal([]byte(s), v1); err != nil || *v1 != b64String {
		t.Fatalf("Bad json-decoded value for %s %v, wrote: '%s', expected: '%v'", thetype, value, s, *v1)
	}
	trans.Close()
}

func TestReadSimpleJSONProtocolBinary(t *testing.T) {
	thetype := "binary"
	value := protocol_bdata
	b64value := make([]byte, base64.StdEncoding.EncodedLen(len(protocol_bdata)))
	base64.StdEncoding.Encode(b64value, value)
	b64String := string(b64value)
	trans := NewTMemoryBuffer()
	p := NewTSimpleJSONProtocol(trans)
	trans.WriteString(jsonQuote(b64String))
	trans.Flush()
	s := trans.String()
	v, e := p.ReadBinary()
	if e != nil {
		t.Fatalf("Unable to read %s value %v due to error: %s", thetype, value, e.Error())
	}
	if len(v) != len(value) {
		t.Fatalf("Bad value for %s value length %v, wrote: %v, received length: %v", thetype, len(value), s, len(v))
	}
	for i := 0; i < len(v); i++ {
		if v[i] != value[i] {
			t.Fatalf("Bad value for %s at index %d value %v, wrote: %v, received: %v", thetype, i, value[i], s, v[i])
		}
	}
	v1 := new(string)
	if err := json.Unmarshal([]byte(s), v1); err != nil || *v1 != b64String {
		t.Fatalf("Bad json-decoded value for %s %v, wrote: '%s', expected: '%v'", thetype, value, s, *v1)
	}
	trans.Reset()
	trans.Close()
}

func TestReadSimpleJSONProtocolBinaryNull(t *testing.T) {
	thetype := "binary"
	value := "null"

	trans := NewTMemoryBuffer()
	p := NewTSimpleJSONProtocol(trans)
	trans.WriteString(value)
	trans.Flush()
	s := trans.String()
	b, e := p.ReadBinary()
	v := string(b)

	if e != nil {
		t.Fatalf("Unable to read %s value %v due to error: %s", thetype, value, e.Error())
	}
	if v != "" {
		t.Fatalf("Bad value for %s value %v, wrote: %v, received: %v", thetype, value, s, v)
	}
	trans.Reset()
	trans.Close()
}

func TestWriteSimpleJSONProtocolList(t *testing.T) {
	thetype := "list"
	trans := NewTMemoryBuffer()
	p := NewTSimpleJSONProtocol(trans)
	p.WriteListBegin(TType(DOUBLE), len(DOUBLE_VALUES))
	for _, value := range DOUBLE_VALUES {
		if e := p.WriteDouble(value); e != nil {
			t.Fatalf("Unable to write %s value %v due to error: %s", thetype, value, e.Error())
		}
	}
	p.WriteListEnd()
	if e := p.Flush(); e != nil {
		t.Fatalf("Unable to write %s due to error flushing: %s", thetype, e.Error())
	}
	str := trans.String()
	str1 := new([]interface{})
	err := json.Unmarshal([]byte(str), str1)
	if err != nil {
		t.Fatalf("Unable to decode %s, wrote: %s", thetype, str)
	}
	l := *str1
	if len(l) < 2 {
		t.Fatalf("List must be at least of length two to include metadata")
	}
	if int(l[0].(float64)) != DOUBLE {
		t.Fatal("Invalid type for list, expected: ", DOUBLE, ", but was: ", l[0])
	}
	if int(l[1].(float64)) != len(DOUBLE_VALUES) {
		t.Fatal("Invalid length for list, expected: ", len(DOUBLE_VALUES), ", but was: ", l[1])
	}
	for k, value := range DOUBLE_VALUES {
		s := l[k+2]
		if math.IsInf(value, 1) {
			if s.(string) != JSON_INFINITY {
				t.Fatalf("Bad value for %s at index %v %v, wrote: %q, expected: %q, originally wrote: %q", thetype, k, value, s, jsonQuote(JSON_INFINITY), str)
			}
		} else if math.IsInf(value, 0) {
			if s.(string) != JSON_NEGATIVE_INFINITY {
				t.Fatalf("Bad value for %s at index %v %v, wrote: %q, expected: %q, originally wrote: %q", thetype, k, value, s, jsonQuote(JSON_NEGATIVE_INFINITY), str)
			}
		} else if math.IsNaN(value) {
			if s.(string) != JSON_NAN {
				t.Fatalf("Bad value for %s at index %v  %v, wrote: %q, expected: %q, originally wrote: %q", thetype, k, value, s, jsonQuote(JSON_NAN), str)
			}
		} else {
			if s.(float64) != value {
				t.Fatalf("Bad json-decoded value for %s %v, wrote: '%s'", thetype, value, s)
			}
		}
		trans.Reset()
	}
	trans.Close()
}

func TestWriteSimpleJSONProtocolSet(t *testing.T) {
	thetype := "set"
	trans := NewTMemoryBuffer()
	p := NewTSimpleJSONProtocol(trans)
	p.WriteSetBegin(TType(DOUBLE), len(DOUBLE_VALUES))
	for _, value := range DOUBLE_VALUES {
		if e := p.WriteDouble(value); e != nil {
			t.Fatalf("Unable to write %s value %v due to error: %s", thetype, value, e.Error())
		}
	}
	p.WriteSetEnd()
	if e := p.Flush(); e != nil {
		t.Fatalf("Unable to write %s due to error flushing: %s", thetype, e.Error())
	}
	str := trans.String()
	str1 := new([]interface{})
	err := json.Unmarshal([]byte(str), str1)
	if err != nil {
		t.Fatalf("Unable to decode %s, wrote: %s", thetype, str)
	}
	l := *str1
	if len(l) < 2 {
		t.Fatalf("Set must be at least of length two to include metadata")
	}
	if int(l[0].(float64)) != DOUBLE {
		t.Fatal("Invalid type for set, expected: ", DOUBLE, ", but was: ", l[0])
	}
	if int(l[1].(float64)) != len(DOUBLE_VALUES) {
		t.Fatal("Invalid length for set, expected: ", len(DOUBLE_VALUES), ", but was: ", l[1])
	}
	for k, value := range DOUBLE_VALUES {
		s := l[k+2]
		if math.IsInf(value, 1) {
			if s.(string) != JSON_INFINITY {
				t.Fatalf("Bad value for %s at index %v %v, wrote: %q, expected: %q, originally wrote: %q", thetype, k, value, s, jsonQuote(JSON_INFINITY), str)
			}
		} else if math.IsInf(value, 0) {
			if s.(string) != JSON_NEGATIVE_INFINITY {
				t.Fatalf("Bad value for %s at index %v %v, wrote: %q, expected: %q, originally wrote: %q", thetype, k, value, s, jsonQuote(JSON_NEGATIVE_INFINITY), str)
			}
		} else if math.IsNaN(value) {
			if s.(string) != JSON_NAN {
				t.Fatalf("Bad value for %s at index %v  %v, wrote: %q, expected: %q, originally wrote: %q", thetype, k, value, s, jsonQuote(JSON_NAN), str)
			}
		} else {
			if s.(float64) != value {
				t.Fatalf("Bad json-decoded value for %s %v, wrote: '%s'", thetype, value, s)
			}
		}
		trans.Reset()
	}
	trans.Close()
}

func TestWriteSimpleJSONProtocolMap(t *testing.T) {
	thetype := "map"
	trans := NewTMemoryBuffer()
	p := NewTSimpleJSONProtocol(trans)
	p.WriteMapBegin(TType(I32), TType(DOUBLE), len(DOUBLE_VALUES))
	for k, value := range DOUBLE_VALUES {
		if e := p.WriteI32(int32(k)); e != nil {
			t.Fatalf("Unable to write %s key int32 value %v due to error: %s", thetype, k, e.Error())
		}
		if e := p.WriteDouble(value); e != nil {
			t.Fatalf("Unable to write %s value float64 value %v due to error: %s", thetype, value, e.Error())
		}
	}
	p.WriteMapEnd()
	if e := p.Flush(); e != nil {
		t.Fatalf("Unable to write %s due to error flushing: %s", thetype, e.Error())
	}
	str := trans.String()
	if str[0] != '[' || str[len(str)-1] != ']' {
		t.Fatalf("Bad value for %s, wrote: %q, in go: %q", thetype, str, DOUBLE_VALUES)
	}
	l := strings.Split(str[1:len(str)-1], ",")
	if len(l) < 3 {
		t.Fatal("Expected list of at least length 3 for map for metadata, but was of length ", len(l))
	}
	expectedKeyType, _ := strconv.Atoi(l[0])
	expectedValueType, _ := strconv.Atoi(l[1])
	expectedSize, _ := strconv.Atoi(l[2])
	if expectedKeyType != I32 {
		t.Fatal("Expected map key type ", I32, ", but was ", l[0])
	}
	if expectedValueType != DOUBLE {
		t.Fatal("Expected map value type ", DOUBLE, ", but was ", l[1])
	}
	if expectedSize != len(DOUBLE_VALUES) {
		t.Fatal("Expected map size of ", len(DOUBLE_VALUES), ", but was ", l[2])
	}
	for k, value := range DOUBLE_VALUES {
		strk := l[k*2+3]
		strv := l[k*2+4]
		ik, err := strconv.Atoi(strk)
		if err != nil {
			t.Fatalf("Bad value for %s index %v, wrote: %v, expected: %v, error: %s", thetype, k, strk, string(k), err.Error())
		}
		if ik != k {
			t.Fatalf("Bad value for %s index %v, wrote: %v, expected: %v", thetype, k, strk, k)
		}
		s := strv
		if math.IsInf(value, 1) {
			if s != jsonQuote(JSON_INFINITY) {
				t.Fatalf("Bad value for %s at index %v %v, wrote: %v, expected: %v", thetype, k, value, s, jsonQuote(JSON_INFINITY))
			}
		} else if math.IsInf(value, 0) {
			if s != jsonQuote(JSON_NEGATIVE_INFINITY) {
				t.Fatalf("Bad value for %s at index %v %v, wrote: %v, expected: %v", thetype, k, value, s, jsonQuote(JSON_NEGATIVE_INFINITY))
			}
		} else if math.IsNaN(value) {
			if s != jsonQuote(JSON_NAN) {
				t.Fatalf("Bad value for %s at index %v  %v, wrote: %v, expected: %v", thetype, k, value, s, jsonQuote(JSON_NAN))
			}
		} else {
			expected := strconv.FormatFloat(value, 'g', 10, 64)
			if s != expected {
				t.Fatalf("Bad value for %s at index %v %v, wrote: %v, expected %v", thetype, k, value, s, expected)
			}
			v := float64(0)
			if err := json.Unmarshal([]byte(s), &v); err != nil || v != value {
				t.Fatalf("Bad json-decoded value for %s %v, wrote: '%s', expected: '%v'", thetype, value, s, v)
			}
		}
		trans.Reset()
	}
	trans.Close()
}
