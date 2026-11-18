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

package collectionutil

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestApplySettingsPreservesOverridesAndInput(t *testing.T) {
	paramtable.Init()
	raw := &schemapb.CollectionSchema{
		Version: 2,
		Properties: []*commonpb.KeyValuePair{
			{Key: common.MmapEnabledKey, Value: "true"},
			{Key: common.WarmupScalarFieldKey, Value: common.WarmupSync},
			{Key: common.WarmupVectorFieldKey, Value: common.WarmupDisable},
		},
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_VarChar},
			{FieldID: 101, DataType: schemapb.DataType_FloatVector},
			{FieldID: 102, DataType: schemapb.DataType_Int64, TypeParams: []*commonpb.KeyValuePair{
				{Key: common.MmapEnabledKey, Value: "false"},
				{Key: common.WarmupKey, Value: common.WarmupDisable},
			}},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{
			{FieldID: 200, TypeParams: []*commonpb.KeyValuePair{
				{Key: common.MmapEnabledKey, Value: "false"},
				{Key: common.WarmupKey, Value: common.WarmupDisable},
			}, Fields: []*schemapb.FieldSchema{
				{FieldID: 201, DataType: schemapb.DataType_Int64},
				{FieldID: 202, DataType: schemapb.DataType_Int64, TypeParams: []*commonpb.KeyValuePair{
					{Key: common.MmapEnabledKey, Value: "true"},
					{Key: common.WarmupKey, Value: common.WarmupSync},
				}},
			}},
			{FieldID: 300, Fields: []*schemapb.FieldSchema{{FieldID: 301, DataType: schemapb.DataType_Int64}}},
		},
	}
	original := proto.Clone(raw)
	// Schema properties already win in the merge; field defaults must use the
	// same values even when separately supplied collection properties differ.
	effective := ApplySettings(raw, []*commonpb.KeyValuePair{
		{Key: common.MmapEnabledKey, Value: "false"},
		{Key: common.WarmupScalarFieldKey, Value: common.WarmupDisable},
	})
	require.True(t, proto.Equal(original, raw), "normalization must not mutate the shared input")
	require.True(t, proto.Equal(effective, ApplySettings(raw, raw.GetProperties())))
	require.True(t, proto.Equal(effective, ApplySettings(effective, effective.GetProperties())), "normalization must be idempotent")
	assertField := func(field *schemapb.FieldSchema, mmap bool, warmup string) {
		t.Helper()
		actualMmap, exists := common.IsMmapDataEnabled(field.GetTypeParams()...)
		require.True(t, exists)
		require.Equal(t, mmap, actualMmap)
		actualWarmup, exists := common.GetWarmupPolicy(field.GetTypeParams()...)
		require.True(t, exists)
		require.Equal(t, warmup, actualWarmup)
	}
	assertField(effective.Fields[0], true, common.WarmupSync)
	assertField(effective.Fields[1], true, common.WarmupDisable)
	assertField(effective.Fields[2], false, common.WarmupDisable)
	assertField(effective.StructArrayFields[0].Fields[0], false, common.WarmupDisable)
	assertField(effective.StructArrayFields[0].Fields[1], true, common.WarmupSync)
	assertField(effective.StructArrayFields[1].Fields[0], true, common.WarmupSync)
}

func TestApplySettingsAutoWarmupParity(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().QueryCoordCfg.AutoWarmupForNonPKIsolationCollection.Key
	require.NoError(t, paramtable.Get().Save(key, "true"))
	t.Cleanup(func() { paramtable.Get().Reset(key) })
	for _, tc := range []struct {
		name       string
		properties []*commonpb.KeyValuePair
		autoWarmup bool
	}{
		{name: "non PKI", autoWarmup: true},
		{name: "PKI", properties: []*commonpb.KeyValuePair{{Key: common.PartitionKeyIsolationKey, Value: "true"}}},
		{name: "invalid PKI", properties: []*commonpb.KeyValuePair{{Key: common.PartitionKeyIsolationKey, Value: "invalid"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
				{FieldID: 100, DataType: schemapb.DataType_Int64},
				{FieldID: 101, DataType: schemapb.DataType_FloatVector},
			}}
			qc := ApplySettings(raw, tc.properties)
			raw.Properties = tc.properties
			qn := ApplySettings(raw, raw.GetProperties())
			require.True(t, proto.Equal(qc, qn))
			require.True(t, proto.Equal(qc, ApplySettings(qc, qc.GetProperties())))
			warmup, exists := common.GetWarmupPolicy(qn.Fields[0].GetTypeParams()...)
			require.Equal(t, tc.autoWarmup, exists)
			if exists {
				require.Equal(t, common.WarmupSync, warmup)
			}
			_, exists = common.GetWarmupPolicy(qn.Fields[1].GetTypeParams()...)
			require.False(t, exists, "auto warmup excludes vector data")
			_, exists = common.GetWarmupPolicyByKey(common.WarmupVectorIndexKey, qn.GetProperties()...)
			require.Equal(t, tc.autoWarmup, exists)
		})
	}
}
