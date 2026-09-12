// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package column

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v3/entity"
)

func TestSlice2Scalar_UUID(t *testing.T) {
	uuids := []string{
		"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
		"550e8400-e29b-41d4-a716-446655440000",
	}
	sf := slice2Scalar(uuids, entity.FieldTypeUUID)
	require.NotNil(t, sf)
	assert.NotNil(t, sf.GetStringData())
	assert.Equal(t, uuids, sf.GetStringData().GetData())
}

func TestSlice2Scalar_UUID_WrongType(t *testing.T) {
	// Passing ints as UUID should cause an ok=false path (panic or nil)
	defer func() {
		r := recover()
		assert.NotNil(t, r, "expected panic for wrong type")
	}()
	slice2Scalar([]int{1, 2}, entity.FieldTypeUUID)
}

func TestValues2FieldData_UUID(t *testing.T) {
	uuids := []string{
		"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
		"550e8400-e29b-41d4-a716-446655440000",
	}
	fd := values2FieldData(uuids, entity.FieldTypeUUID, 0)
	require.NotNil(t, fd)
	require.NotNil(t, fd.GetScalars())
	require.NotNil(t, fd.GetScalars().GetStringData())
	assert.Equal(t, uuids, fd.GetScalars().GetStringData().GetData())
}

func TestValues2Scalars_UUID(t *testing.T) {
	uuids := []string{
		"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
		"550e8400-e29b-41d4-a716-446655440000",
	}
	scalars := values2Scalars(uuids, entity.FieldTypeUUID)
	require.NotNil(t, scalars)
	require.NotNil(t, scalars.GetStringData())
	assert.Equal(t, uuids, scalars.GetStringData().GetData())
}
