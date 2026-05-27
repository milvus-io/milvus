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

package growingsource

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestUseGrowingSourceFlushOrdinaryFieldsFollowDefaultSwitch(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Reset(paramtable.Get().CommonCfg.EnableGrowingSourceFlush.Key)
	t.Cleanup(func() {
		paramtable.Get().Reset(paramtable.Get().CommonCfg.EnableGrowingSourceFlush.Key)
	})

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64},
			{FieldID: 101, DataType: schemapb.DataType_FloatVector},
			{FieldID: 102, DataType: schemapb.DataType_JSON},
		},
	}
	require.True(t, UseGrowingSourceFlush(schema))

	paramtable.Get().Save(paramtable.Get().CommonCfg.EnableGrowingSourceFlush.Key, "false")
	require.False(t, UseGrowingSourceFlush(schema))
}

func TestUseGrowingSourceFlushTextFieldsIgnoreDefaultSwitch(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().CommonCfg.EnableGrowingSourceFlush.Key, "false")
	t.Cleanup(func() {
		paramtable.Get().Reset(paramtable.Get().CommonCfg.EnableGrowingSourceFlush.Key)
	})

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64},
			{FieldID: 101, DataType: schemapb.DataType_Text},
		},
	}
	require.True(t, HasTextField(schema))
	require.True(t, UseGrowingSourceFlush(schema))
}

func TestUseGrowingSourceFlushNilAndEmptySchema(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().CommonCfg.EnableGrowingSourceFlush.Key, "false")
	t.Cleanup(func() {
		paramtable.Get().Reset(paramtable.Get().CommonCfg.EnableGrowingSourceFlush.Key)
	})

	require.False(t, HasTextField(nil))
	require.False(t, UseGrowingSourceFlush(nil))
	require.False(t, HasTextField(&schemapb.CollectionSchema{}))
	require.False(t, UseGrowingSourceFlush(&schemapb.CollectionSchema{}))

	paramtable.Get().Save(paramtable.Get().CommonCfg.EnableGrowingSourceFlush.Key, "true")
	require.False(t, HasTextField(nil))
	require.True(t, UseGrowingSourceFlush(nil))
	require.False(t, HasTextField(&schemapb.CollectionSchema{}))
	require.True(t, UseGrowingSourceFlush(&schemapb.CollectionSchema{}))
}
