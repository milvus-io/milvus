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
	"encoding"
	"encoding/json"
	"testing"
	"thrifttest"
)

func TestEnumIsTextMarshaller(t *testing.T) {
	one := thrifttest.Numberz_ONE
	var tm encoding.TextMarshaler = one
	b, err := tm.MarshalText()
	if err != nil {
		t.Fatalf("Unexpected error from MarshalText: %s", err)
	}
	if string(b) != one.String() {
		t.Errorf("MarshalText(%s) = %s, expected = %s", one, b, one)
	}
}

func TestEnumIsTextUnmarshaller(t *testing.T) {
	var tm encoding.TextUnmarshaler = thrifttest.NumberzPtr(thrifttest.Numberz_TWO)
	err := tm.UnmarshalText([]byte("TWO"))
	if err != nil {
		t.Fatalf("Unexpected error from UnmarshalText(TWO): %s", err)
	}
	if *(tm.(*thrifttest.Numberz)) != thrifttest.Numberz_TWO {
		t.Errorf("UnmarshalText(TWO) = %s", tm)
	}

	err = tm.UnmarshalText([]byte("NAN"))
	if err == nil {
		t.Errorf("Error from UnmarshalText(NAN)")
	}
}

func TestJSONMarshalUnmarshal(t *testing.T) {
	s1 := thrifttest.StructB{
		Aa: &thrifttest.StructA{S: "Aa"},
		Ab: &thrifttest.StructA{S: "Ab"},
	}

	b, err := json.Marshal(s1)
	if err != nil {
		t.Fatalf("Unexpected error from json.Marshal: %s", err)
	}

	s2 := thrifttest.StructB{}
	err = json.Unmarshal(b, &s2)
	if err != nil {
		t.Fatalf("Unexpected error from json.Unmarshal: %s", err)
	}

	if *s1.Aa != *s2.Aa || *s1.Ab != *s2.Ab {
		t.Logf("s1 = %+v", s1)
		t.Logf("s2 = %+v", s2)
		t.Errorf("json: Unmarshal(Marshal(s)) != s")
	}
}
