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

package loadmgr

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestSchemaChangePreloadsSurviveRecovery(t *testing.T) {
	for _, test := range []struct {
		name              string
		initial, expected []*viewpb.LoadFieldConfig
	}{
		{"partial preloads", []*viewpb.LoadFieldConfig{{FieldId: 100}}, []*viewpb.LoadFieldConfig{{FieldId: 100}, {FieldId: 102}, {FieldId: 201}}},
		{"empty preloads", nil, []*viewpb.LoadFieldConfig{{FieldId: 102}, {FieldId: 201}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()
			hooks := new(versionedTestHooks)
			deps := versionedTestDependencies(t, hooks)
			store, err := RecoverLoadConfigStore(ctx, deps)
			require.NoError(t, err)
			old := &schemapb.CollectionSchema{Version: 1, Fields: []*schemapb.FieldSchema{{FieldID: 100}, {FieldID: 101}}}
			target := proto.Clone(old).(*schemapb.CollectionSchema)
			target.Version++
			skip := []*commonpb.KeyValuePair{{Key: common.FieldSkipLoadKey, Value: "true"}}
			target.Fields = append(target.Fields, &schemapb.FieldSchema{FieldID: 102}, &schemapb.FieldSchema{FieldID: 103, TypeParams: skip})
			target.StructArrayFields = []*schemapb.StructArrayFieldSchema{{FieldID: 200, Fields: []*schemapb.FieldSchema{{FieldID: 201}, {FieldID: 202, TypeParams: skip}}}}
			require.NoError(t, store.Put(ctx, &LoadConfig{CollectionID: 1, RequiredFields: RequiredFieldsForSchema(old), LoadFields: test.initial}))
			initialVersion := store.GetConfigVersion(1)

			// A failed catalog write must leave both scopes and the selected load
			// version unchanged so the original ALTER can retry the same delta.
			hooks.beforeSave = func(*viewpb.CollectionLoadConfig) error { return context.Canceled }
			require.ErrorIs(t, store.ApplySchema(ctx, 1, old, target, nil), context.Canceled)
			require.Equal(t, initialVersion, store.GetConfigVersion(1))
			requireFieldConfigsEqual(t, test.initial, store.GetConfig(1).LoadFields)
			requireFieldConfigsEqual(t, RequiredFieldsForSchema(old), store.GetConfig(1).RequiredFields)
			hooks.beforeSave = nil
			require.NoError(t, store.ApplySchema(ctx, 1, old, target, nil))
			updatedVersion := store.GetConfigVersion(1)
			require.Greater(t, updatedVersion, initialVersion)

			// Recover through the actual versioned catalog path. Old exclusions
			// stay excluded, additions use schema defaults, and skip_load fields
			// stay in required scope without being added to eager preparation.
			recovered, err := RecoverLoadConfigStore(ctx, deps)
			require.NoError(t, err)
			current := recovered.GetConfig(1)
			require.Equal(t, updatedVersion, current.Version)
			requireFieldConfigsEqual(t, test.expected, current.LoadFields)
			requireFieldConfigsEqual(t, RequiredFieldsForSchema(target), current.RequiredFields)
			previous, err := recovered.GetLoadInfo(ctx, 1, initialVersion)
			require.NoError(t, err)
			requireFieldConfigsEqual(t, test.initial, previous.LoadFields)
			requireFieldConfigsEqual(t, RequiredFieldsForSchema(old), previous.RequiredFields)

			// Retrying before public schema publication and replaying after it
			// must both preserve the recovered version and preload selection.
			require.NoError(t, recovered.ApplySchema(ctx, 1, old, target, nil))
			require.NoError(t, recovered.ApplySchema(ctx, 1, target, target, nil))
			require.Equal(t, updatedVersion, recovered.GetConfigVersion(1))
			requireFieldConfigsEqual(t, test.expected, recovered.GetConfig(1).LoadFields)
			recovered.StopPlanning(1)
			require.NoError(t, recovered.ApplySchema(ctx, 1, old, target, nil))
			require.False(t, recovered.Contains(1), "schema replay cannot reload a released collection")
		})
	}
}
