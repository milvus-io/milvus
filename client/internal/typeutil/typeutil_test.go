// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package typeutil

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func TestConcurrentMapBasic(t *testing.T) {
	m := NewConcurrentMap[string, int]()

	// missing key returns the zero value and ok=false
	if _, ok := m.Get("a"); ok {
		t.Fatal("expected missing key to report ok=false")
	}

	// insert then get
	m.Insert("a", 1)
	if v, ok := m.Get("a"); !ok || v != 1 {
		t.Fatalf("expected 1, got %d ok=%v", v, ok)
	}

	// insert overwrites
	m.Insert("a", 2)
	if v, ok := m.Get("a"); !ok || v != 2 {
		t.Fatalf("expected 2, got %d ok=%v", v, ok)
	}

	// Remove deletes the key
	m.Remove("a")
	if _, ok := m.Get("a"); ok {
		t.Fatal("expected key removed")
	}
}

func TestSetNewAndComplement(t *testing.T) {
	a := NewSet(1, 2, 3, 4)
	b := NewSet(3, 4, 5)

	c := a.Complement(b)
	if len(c) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(c))
	}
	for _, e := range []int{1, 2} {
		if _, ok := c[e]; !ok {
			t.Fatalf("expected %d in complement", e)
		}
	}
	for _, e := range []int{3, 4, 5} {
		if _, ok := c[e]; ok {
			t.Fatalf("did not expect %d in complement", e)
		}
	}

	// duplicate elements are deduplicated
	s := NewSet("a", "a", "b")
	if len(s) != 2 {
		t.Fatalf("expected 2 elements after dedup, got %d", len(s))
	}
}

func TestFloat32ArrayToBytes(t *testing.T) {
	values := []float32{1.0, -2.5, 3.75}
	b := Float32ArrayToBytes(values)
	if len(b) != 4*len(values) {
		t.Fatalf("expected %d bytes, got %d", 4*len(values), len(b))
	}
	for i, v := range values {
		if got := math.Float32frombits(binary.LittleEndian.Uint32(b[i*4:])); got != v {
			t.Fatalf("expected %v, got %v", v, got)
		}
	}
}

func TestFloat16RoundTrip(t *testing.T) {
	// values exactly representable in float16
	values := []float32{1.0, 0.5, 2.0, -4.0, 0.0, 1.5, 100.0}
	b := Float32ArrayToFloat16Bytes(values)
	if len(b) != 2*len(values) {
		t.Fatalf("expected %d bytes, got %d", 2*len(values), len(b))
	}
	got := Float16BytesToFloat32Vector(b)
	if len(got) != len(values) {
		t.Fatalf("expected %d values, got %d", len(values), len(got))
	}
	for i, v := range values {
		if got[i] != v {
			t.Fatalf("float16 round trip: expected %v, got %v", v, got[i])
		}
	}
}

func TestBFloat16RoundTrip(t *testing.T) {
	// values whose low 16 mantissa bits are zero are exact in bfloat16
	values := []float32{1.0, -2.0, 128.0, 0.5, 0.0, -0.25, 65536.0}
	b := Float32ArrayToBFloat16Bytes(values)
	if len(b) != 2*len(values) {
		t.Fatalf("expected %d bytes, got %d", 2*len(values), len(b))
	}
	got := BFloat16BytesToFloat32Vector(b)
	for i, v := range values {
		if got[i] != v {
			t.Fatalf("bfloat16 round trip: expected %v, got %v", v, got[i])
		}
	}

	// a non-exact value is truncated to its top 16 bits
	pi := float32(math.Pi)
	truncated := BFloat16BytesToFloat32Vector(Float32ArrayToBFloat16Bytes([]float32{pi}))[0]
	expected := math.Float32frombits(math.Float32bits(pi) & 0xFFFF0000)
	if truncated != expected {
		t.Fatalf("expected truncated %v, got %v", expected, truncated)
	}
}

func TestInt8ArrayToBytes(t *testing.T) {
	values := []int8{-1, 0, 1, 127, -128}
	b := Int8ArrayToBytes(values)
	expected := []byte{0xFF, 0x00, 0x01, 0x7F, 0x80}
	if !bytes.Equal(b, expected) {
		t.Fatalf("expected %v, got %v", expected, b)
	}
}

func TestIsVectorType(t *testing.T) {
	vectorTypes := []schemapb.DataType{
		schemapb.DataType_BinaryVector,
		schemapb.DataType_FloatVector,
		schemapb.DataType_Float16Vector,
		schemapb.DataType_BFloat16Vector,
		schemapb.DataType_SparseFloatVector,
		schemapb.DataType_Int8Vector,
		schemapb.DataType_ArrayOfVector,
	}
	for _, dt := range vectorTypes {
		if !IsVectorType(dt) {
			t.Fatalf("expected %s to be a vector type", dt)
		}
	}

	scalarTypes := []schemapb.DataType{
		schemapb.DataType_Int64,
		schemapb.DataType_VarChar,
		schemapb.DataType_Float,
		schemapb.DataType_Bool,
		schemapb.DataType_JSON,
		schemapb.DataType_Array,
	}
	for _, dt := range scalarTypes {
		if IsVectorType(dt) {
			t.Fatalf("expected %s to not be a vector type", dt)
		}
	}
}
