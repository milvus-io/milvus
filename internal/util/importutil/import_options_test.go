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

package importutil

import (
	"math"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/stretchr/testify/assert"
)

func Test_ValidateOptions(t *testing.T) {

	assert.NoError(t, ValidateOptions([]*commonpb.KeyValuePair{}))
	assert.NoError(t, ValidateOptions([]*commonpb.KeyValuePair{
		{Key: "start_ts", Value: "1666007457"},
		{Key: "end_ts", Value: "1666007459"},
	}))
	assert.NoError(t, ValidateOptions([]*commonpb.KeyValuePair{
		{Key: "start_ts", Value: "0"},
		{Key: "end_ts", Value: "0"},
	}))
	assert.NoError(t, ValidateOptions([]*commonpb.KeyValuePair{
		{Key: "start_ts", Value: "0"},
		{Key: "end_ts", Value: "1666007457"},
	}))
	assert.Error(t, ValidateOptions([]*commonpb.KeyValuePair{
		{Key: "start_ts", Value: "-1"},
		{Key: "end_ts", Value: "-1"},
	}))
	assert.Error(t, ValidateOptions([]*commonpb.KeyValuePair{
		{Key: "start_ts", Value: "2"},
		{Key: "end_ts", Value: "1"},
	}))
	assert.Error(t, ValidateOptions([]*commonpb.KeyValuePair{
		{Key: "start_ts", Value: "3.14"},
		{Key: "end_ts", Value: "1666007457"},
	}))
	assert.Error(t, ValidateOptions([]*commonpb.KeyValuePair{
		{Key: "start_ts", Value: "1666007457"},
		{Key: "end_ts", Value: "3.14"},
	}))
}

func Test_ParseTSFromOptions(t *testing.T) {
	var tsStart uint64
	var tsEnd uint64
	var err error

	tsStart, tsEnd, err = ParseTSFromOptions([]*commonpb.KeyValuePair{})
	assert.Equal(t, uint64(0), tsStart)
	assert.Equal(t, uint64(0), math.MaxUint64-tsEnd)
	assert.NoError(t, err)

	tsStart, tsEnd, err = ParseTSFromOptions([]*commonpb.KeyValuePair{
		{Key: "start_ts", Value: "0"},
		{Key: "end_ts", Value: "0"},
	})
	assert.Equal(t, uint64(0), tsStart)
	assert.Equal(t, uint64(0), tsEnd)
	assert.NoError(t, err)

	tsStart, tsEnd, err = ParseTSFromOptions([]*commonpb.KeyValuePair{
		{Key: "start_ts", Value: "0"},
		{Key: "end_ts", Value: "1666007457"},
	})
	assert.Equal(t, uint64(0), tsStart)
	assert.Equal(t, uint64(436733858807808), tsEnd)
	assert.NoError(t, err)

	tsStart, tsEnd, err = ParseTSFromOptions([]*commonpb.KeyValuePair{
		{Key: "start_ts", Value: "2"},
		{Key: "end_ts", Value: "1"},
	})
	assert.Equal(t, uint64(0), tsStart)
	assert.Equal(t, uint64(0), tsEnd)
	assert.Error(t, err)
}

func Test_IsBackup(t *testing.T) {
	isBackup := IsBackup([]*commonpb.KeyValuePair{
		{Key: "backup", Value: "true"},
	})
	assert.Equal(t, true, isBackup)
	isBackup2 := IsBackup([]*commonpb.KeyValuePair{
		{Key: "backup", Value: "True"},
	})
	assert.Equal(t, true, isBackup2)
	falseBackup := IsBackup([]*commonpb.KeyValuePair{
		{Key: "backup", Value: "false"},
	})
	assert.Equal(t, false, falseBackup)
	noBackup := IsBackup([]*commonpb.KeyValuePair{
		{Key: "backup", Value: "false"},
	})
	assert.Equal(t, false, noBackup)
}
