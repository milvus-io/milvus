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

package kv

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockTestKV struct {
	data map[string][]byte
}

func newMockTestKV() ValueKV {
	return &mockTestKV{
		data: make(map[string][]byte),
	}
}

func (m *mockTestKV) Save(key string, value Value) error {
	m.data[key] = value.Serialize()
	return nil
}

func (m *mockTestKV) Load(key string) (Value, error) {
	d, ok := m.data[key]
	if !ok {
		return nil, errors.New("not found")
	}
	return BytesValue(d), nil
}

func TestTKV(t *testing.T) {
	tkv := newMockTestKV()
	bv := BytesValue([]byte{1, 2, 3})
	sv := StringValue("test")
	tkv.Save("1", bv)
	tkv.Save("2", sv)

	v, err := tkv.Load("1")
	assert.NoError(t, err)

	assert.EqualValues(t, bv.Serialize(), v.Serialize())

	v, err = tkv.Load("2")
	assert.NoError(t, err)
	assert.EqualValues(t, sv.String(), v.String())
}
