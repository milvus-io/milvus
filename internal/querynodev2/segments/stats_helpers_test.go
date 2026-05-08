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

package segments

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
)

func TestSumInt64Field(t *testing.T) {
	results := []*segcorepb.RetrieveResults{
		{AllRetrieveCount: 10},
		{AllRetrieveCount: 20},
		{AllRetrieveCount: 30},
	}
	sum := sumInt64Field(results, func(r *segcorepb.RetrieveResults) int64 { return r.GetAllRetrieveCount() })
	assert.Equal(t, int64(60), sum)
}

func TestSumInt64Field_WithNil(t *testing.T) {
	results := []*segcorepb.RetrieveResults{
		{AllRetrieveCount: 10},
		nil,
		{AllRetrieveCount: 30},
	}
	sum := sumInt64Field(results, func(r *segcorepb.RetrieveResults) int64 { return r.GetAllRetrieveCount() })
	assert.Equal(t, int64(40), sum)
}

func TestSumInt64Field_Empty(t *testing.T) {
	sum := sumInt64Field(nil, func(r *segcorepb.RetrieveResults) int64 { return r.GetAllRetrieveCount() })
	assert.Equal(t, int64(0), sum)
}

func TestAnyFieldTrue(t *testing.T) {
	results := []*segcorepb.RetrieveResults{
		{HasMoreResult: false},
		{HasMoreResult: true},
		{HasMoreResult: false},
	}
	assert.True(t, anyFieldTrue(results, func(r *segcorepb.RetrieveResults) bool { return r.GetHasMoreResult() }))
}

func TestAnyFieldTrue_AllFalse(t *testing.T) {
	results := []*segcorepb.RetrieveResults{
		{HasMoreResult: false},
		{HasMoreResult: false},
	}
	assert.False(t, anyFieldTrue(results, func(r *segcorepb.RetrieveResults) bool { return r.GetHasMoreResult() }))
}

func TestAnyFieldTrue_WithNil(t *testing.T) {
	results := []*segcorepb.RetrieveResults{
		nil,
		{HasMoreResult: true},
	}
	assert.True(t, anyFieldTrue(results, func(r *segcorepb.RetrieveResults) bool { return r.GetHasMoreResult() }))
}

func TestAnyFieldTrue_Empty(t *testing.T) {
	assert.False(t, anyFieldTrue(nil, func(r *segcorepb.RetrieveResults) bool { return r.GetHasMoreResult() }))
}

func TestSumInt64FieldInternal(t *testing.T) {
	results := []*internalpb.RetrieveResults{
		{AllRetrieveCount: 5},
		{AllRetrieveCount: 15},
	}
	sum := sumInt64FieldInternal(results, func(r *internalpb.RetrieveResults) int64 { return r.GetAllRetrieveCount() })
	assert.Equal(t, int64(20), sum)
}

func TestSumInt64FieldInternal_WithNil(t *testing.T) {
	results := []*internalpb.RetrieveResults{
		nil,
		{AllRetrieveCount: 15},
		nil,
	}
	sum := sumInt64FieldInternal(results, func(r *internalpb.RetrieveResults) int64 { return r.GetAllRetrieveCount() })
	assert.Equal(t, int64(15), sum)
}

func TestSumInt64FieldInternal_Empty(t *testing.T) {
	sum := sumInt64FieldInternal(nil, func(r *internalpb.RetrieveResults) int64 { return r.GetAllRetrieveCount() })
	assert.Equal(t, int64(0), sum)
}

func TestAnyFieldTrueInternal(t *testing.T) {
	results := []*internalpb.RetrieveResults{
		{HasMoreResult: false},
		{HasMoreResult: true},
	}
	assert.True(t, anyFieldTrueInternal(results, func(r *internalpb.RetrieveResults) bool { return r.GetHasMoreResult() }))
}

func TestAnyFieldTrueInternal_AllFalse(t *testing.T) {
	results := []*internalpb.RetrieveResults{
		{HasMoreResult: false},
	}
	assert.False(t, anyFieldTrueInternal(results, func(r *internalpb.RetrieveResults) bool { return r.GetHasMoreResult() }))
}

func TestAnyFieldTrueInternal_WithNil(t *testing.T) {
	results := []*internalpb.RetrieveResults{
		nil,
		{HasMoreResult: false},
	}
	assert.False(t, anyFieldTrueInternal(results, func(r *internalpb.RetrieveResults) bool { return r.GetHasMoreResult() }))
}

func TestAnyFieldTrueInternal_Empty(t *testing.T) {
	assert.False(t, anyFieldTrueInternal(nil, func(r *internalpb.RetrieveResults) bool { return r.GetHasMoreResult() }))
}
