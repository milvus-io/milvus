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

package storage

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVarCharFieldValue(t *testing.T) {
	pk := NewVarCharFieldValue("milvus")

	testPk := NewVarCharFieldValue("milvus")

	// test GE
	assert.Equal(t, true, pk.GE(testPk))
	// test LE
	assert.Equal(t, true, pk.LE(testPk))
	// test EQ
	assert.Equal(t, true, pk.EQ(testPk))

	err := testPk.SetValue(1.0)
	assert.Error(t, err)

	// test GT
	err = testPk.SetValue("bivlus")
	assert.NoError(t, err)
	assert.Equal(t, true, pk.GT(testPk))
	assert.Equal(t, false, testPk.GT(pk))

	// test LT
	err = testPk.SetValue("mivlut")
	assert.NoError(t, err)
	assert.Equal(t, true, pk.LT(testPk))
	assert.Equal(t, false, testPk.LT(pk))

	t.Run("unmarshal", func(t *testing.T) {
		blob, err := json.Marshal(pk)
		assert.NoError(t, err)

		unmarshalledPk := &VarCharFieldValue{}
		err = json.Unmarshal(blob, unmarshalledPk)
		assert.NoError(t, err)
		assert.Equal(t, pk.Value, unmarshalledPk.Value)
	})
}

func TestInt64FieldValue(t *testing.T) {
	pk := NewInt64FieldValue(100)

	testPk := NewInt64FieldValue(100)
	// test GE
	assert.Equal(t, true, pk.GE(testPk))
	assert.Equal(t, true, testPk.GE(pk))
	// test LE
	assert.Equal(t, true, pk.LE(testPk))
	assert.Equal(t, true, testPk.LE(pk))
	// test EQ
	assert.Equal(t, true, pk.EQ(testPk))

	err := testPk.SetValue(1.0)
	assert.Error(t, err)

	// test GT
	err = testPk.SetValue(int64(10))
	assert.NoError(t, err)
	assert.Equal(t, true, pk.GT(testPk))
	assert.Equal(t, false, testPk.GT(pk))
	assert.Equal(t, true, pk.GE(testPk))
	assert.Equal(t, false, testPk.GE(pk))

	// test LT
	err = testPk.SetValue(int64(200))
	assert.NoError(t, err)
	assert.Equal(t, true, pk.LT(testPk))
	assert.Equal(t, false, testPk.LT(pk))
	assert.Equal(t, true, pk.LE(testPk))
	assert.Equal(t, false, testPk.LE(pk))

	t.Run("unmarshal", func(t *testing.T) {
		blob, err := json.Marshal(pk)
		assert.NoError(t, err)

		unmarshalledPk := &Int64FieldValue{}
		err = json.Unmarshal(blob, unmarshalledPk)
		assert.NoError(t, err)
		assert.Equal(t, pk.Value, unmarshalledPk.Value)
	})
}

func TestInt8FieldValue(t *testing.T) {
	pk := NewInt8FieldValue(20)

	testPk := NewInt8FieldValue(20)
	// test GE
	assert.Equal(t, true, pk.GE(testPk))
	assert.Equal(t, true, testPk.GE(pk))
	// test LE
	assert.Equal(t, true, pk.LE(testPk))
	assert.Equal(t, true, testPk.LE(pk))
	// test EQ
	assert.Equal(t, true, pk.EQ(testPk))

	err := testPk.SetValue(1.0)
	assert.Error(t, err)

	// test GT
	err = testPk.SetValue(int8(10))
	assert.NoError(t, err)
	assert.Equal(t, true, pk.GT(testPk))
	assert.Equal(t, false, testPk.GT(pk))
	assert.Equal(t, true, pk.GE(testPk))
	assert.Equal(t, false, testPk.GE(pk))

	// test LT
	err = testPk.SetValue(int8(30))
	assert.NoError(t, err)
	assert.Equal(t, true, pk.LT(testPk))
	assert.Equal(t, false, testPk.LT(pk))
	assert.Equal(t, true, pk.LE(testPk))
	assert.Equal(t, false, testPk.LE(pk))

	t.Run("unmarshal", func(t *testing.T) {
		blob, err := json.Marshal(pk)
		assert.NoError(t, err)

		unmarshalledPk := &Int8FieldValue{}
		err = json.Unmarshal(blob, unmarshalledPk)
		assert.NoError(t, err)
		assert.Equal(t, pk.Value, unmarshalledPk.Value)
	})
}

func TestInt16FieldValue(t *testing.T) {
	pk := NewInt16FieldValue(100)

	testPk := NewInt16FieldValue(100)
	// test GE
	assert.Equal(t, true, pk.GE(testPk))
	assert.Equal(t, true, testPk.GE(pk))
	// test LE
	assert.Equal(t, true, pk.LE(testPk))
	assert.Equal(t, true, testPk.LE(pk))
	// test EQ
	assert.Equal(t, true, pk.EQ(testPk))

	err := testPk.SetValue(1.0)
	assert.Error(t, err)

	// test GT
	err = testPk.SetValue(int16(10))
	assert.NoError(t, err)
	assert.Equal(t, true, pk.GT(testPk))
	assert.Equal(t, false, testPk.GT(pk))
	assert.Equal(t, true, pk.GE(testPk))
	assert.Equal(t, false, testPk.GE(pk))

	// test LT
	err = testPk.SetValue(int16(200))
	assert.NoError(t, err)
	assert.Equal(t, true, pk.LT(testPk))
	assert.Equal(t, false, testPk.LT(pk))
	assert.Equal(t, true, pk.LE(testPk))
	assert.Equal(t, false, testPk.LE(pk))

	t.Run("unmarshal", func(t *testing.T) {
		blob, err := json.Marshal(pk)
		assert.NoError(t, err)

		unmarshalledPk := &Int16FieldValue{}
		err = json.Unmarshal(blob, unmarshalledPk)
		assert.NoError(t, err)
		assert.Equal(t, pk.Value, unmarshalledPk.Value)
	})
}

func TestInt32FieldValue(t *testing.T) {
	pk := NewInt32FieldValue(100)

	testPk := NewInt32FieldValue(100)
	// test GE
	assert.Equal(t, true, pk.GE(testPk))
	assert.Equal(t, true, testPk.GE(pk))
	// test LE
	assert.Equal(t, true, pk.LE(testPk))
	assert.Equal(t, true, testPk.LE(pk))
	// test EQ
	assert.Equal(t, true, pk.EQ(testPk))

	err := testPk.SetValue(1.0)
	assert.Error(t, err)

	// test GT
	err = testPk.SetValue(int32(10))
	assert.NoError(t, err)
	assert.Equal(t, true, pk.GT(testPk))
	assert.Equal(t, false, testPk.GT(pk))
	assert.Equal(t, true, pk.GE(testPk))
	assert.Equal(t, false, testPk.GE(pk))

	// test LT
	err = testPk.SetValue(int32(200))
	assert.NoError(t, err)
	assert.Equal(t, true, pk.LT(testPk))
	assert.Equal(t, false, testPk.LT(pk))
	assert.Equal(t, true, pk.LE(testPk))
	assert.Equal(t, false, testPk.LE(pk))

	t.Run("unmarshal", func(t *testing.T) {
		blob, err := json.Marshal(pk)
		assert.NoError(t, err)

		unmarshalledPk := &Int32FieldValue{}
		err = json.Unmarshal(blob, unmarshalledPk)
		assert.NoError(t, err)
		assert.Equal(t, pk.Value, unmarshalledPk.Value)
	})
}

func TestFloatFieldValue(t *testing.T) {
	pk := NewFloatFieldValue(100)

	testPk := NewFloatFieldValue(100)
	// test GE
	assert.Equal(t, true, pk.GE(testPk))
	assert.Equal(t, true, testPk.GE(pk))
	// test LE
	assert.Equal(t, true, pk.LE(testPk))
	assert.Equal(t, true, testPk.LE(pk))
	// test EQ
	assert.Equal(t, true, pk.EQ(testPk))

	err := testPk.SetValue(float32(1.0))
	assert.NoError(t, err)
	// test GT
	err = testPk.SetValue(float32(10))
	assert.NoError(t, err)
	assert.Equal(t, true, pk.GT(testPk))
	assert.Equal(t, false, testPk.GT(pk))
	assert.Equal(t, true, pk.GE(testPk))
	assert.Equal(t, false, testPk.GE(pk))
	// test LT
	err = testPk.SetValue(float32(200))
	assert.NoError(t, err)
	assert.Equal(t, true, pk.LT(testPk))
	assert.Equal(t, false, testPk.LT(pk))
	assert.Equal(t, true, pk.LE(testPk))
	assert.Equal(t, false, testPk.LE(pk))

	t.Run("unmarshal", func(t *testing.T) {
		blob, err := json.Marshal(pk)
		assert.NoError(t, err)

		unmarshalledPk := &FloatFieldValue{}
		err = json.Unmarshal(blob, unmarshalledPk)
		assert.NoError(t, err)
		assert.Equal(t, pk.Value, unmarshalledPk.Value)
	})
}

func TestDoubleFieldValue(t *testing.T) {
	pk := NewDoubleFieldValue(100)

	testPk := NewDoubleFieldValue(100)
	// test GE
	assert.Equal(t, true, pk.GE(testPk))
	assert.Equal(t, true, testPk.GE(pk))
	// test LE
	assert.Equal(t, true, pk.LE(testPk))
	assert.Equal(t, true, testPk.LE(pk))
	// test EQ
	assert.Equal(t, true, pk.EQ(testPk))
	// test GT
	err := testPk.SetValue(float64(10))
	assert.NoError(t, err)
	assert.Equal(t, true, pk.GT(testPk))
	assert.Equal(t, false, testPk.GT(pk))
	assert.Equal(t, true, pk.GE(testPk))
	assert.Equal(t, false, testPk.GE(pk))
	// test LT
	err = testPk.SetValue(float64(200))
	assert.NoError(t, err)
	assert.Equal(t, true, pk.LT(testPk))
	assert.Equal(t, false, testPk.LT(pk))
	assert.Equal(t, true, pk.LE(testPk))
	assert.Equal(t, false, testPk.LE(pk))

	t.Run("unmarshal", func(t *testing.T) {
		blob, err := json.Marshal(pk)
		assert.NoError(t, err)

		unmarshalledPk := &DoubleFieldValue{}
		err = json.Unmarshal(blob, unmarshalledPk)
		assert.NoError(t, err)
		assert.Equal(t, pk.Value, unmarshalledPk.Value)
	})
}

func TestFieldValueSize(t *testing.T) {
	vcf := NewVarCharFieldValue("milvus")
	assert.Equal(t, int64(56), vcf.Size())

	stf := NewStringFieldValue("milvus")
	assert.Equal(t, int64(56), stf.Size())

	int8f := NewInt8FieldValue(100)
	assert.Equal(t, int64(2), int8f.Size())

	int16f := NewInt16FieldValue(100)
	assert.Equal(t, int64(4), int16f.Size())

	int32f := NewInt32FieldValue(100)
	assert.Equal(t, int64(8), int32f.Size())

	int64f := NewInt64FieldValue(100)
	assert.Equal(t, int64(16), int64f.Size())

	floatf := NewFloatFieldValue(float32(10.7))
	assert.Equal(t, int64(8), floatf.Size())

	doublef := NewDoubleFieldValue(float64(10.7))
	assert.Equal(t, int64(16), doublef.Size())
}

func TestFloatVectorFieldValue(t *testing.T) {
	pk := NewFloatVectorFieldValue([]float32{1.0, 2.0, 3.0, 4.0})

	t.Run("unmarshal", func(t *testing.T) {
		blob, err := json.Marshal(pk)
		assert.NoError(t, err)

		unmarshalledPk := &FloatVectorFieldValue{}
		err = json.Unmarshal(blob, unmarshalledPk)
		assert.NoError(t, err)
		assert.Equal(t, pk.Value, unmarshalledPk.Value)
	})
}
