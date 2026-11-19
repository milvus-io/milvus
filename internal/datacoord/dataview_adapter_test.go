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

package datacoord

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/dataview"
	datacoordkv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

func TestDataViewAdapterRequiresInitializedManager(t *testing.T) {
	server := &Server{}
	_, err := server.CreateCollectionDataView(context.Background(), 1, []string{"ch-1"})
	require.Error(t, err)
	_, err = server.CreateCollectionDataViewWithTransformStarts(t.Context(), dataViewCreationResult(7))
	require.Error(t, err)
	require.Error(t, server.DropCollectionDataView(context.Background(), 1))
}

func TestCreateCollectionDataViewPreservesSchemaVersion(t *testing.T) {
	for _, version := range []int32{0, 7} {
		t.Run(strconv.Itoa(int(version)), func(t *testing.T) {
			catalog := datacoordkv.NewCatalog(meta.NewMetaMemoryKV(), "", "")
			manager := recoverCreationTestDataViews(t, catalog)
			server := &Server{dataViewManager: manager}
			result := dataViewCreationResult(version)
			_, err := server.CreateCollectionDataViewWithTransformStarts(t.Context(), result)
			require.NoError(t, err)
			ref, err := manager.Latest(t.Context(), 1)
			require.NoError(t, err)
			defer ref.Deref()
			require.Equal(t, version, ref.DataView().GetSchemaVersion())
			require.Equal(t, uint64(100), ref.DataView().Shards[0].GrowingTransformStartAfterTimetick)
			require.Len(t, ref.DataView().Shards, 2, "control channel is excluded from the DataView")
			require.Equal(t, uint64(101), ref.DataView().Shards[1].GrowingTransformStartAfterTimetick)
			// A duplicate creation must preserve a later schema and its initial frontiers.
			_, err = manager.AdvanceSchema(t.Context(), 1, version+1, nil)
			require.NoError(t, err)
			result.Results["v1"].TimeTick = 200
			_, err = server.CreateCollectionDataViewWithTransformStarts(t.Context(), result)
			require.NoError(t, err)
			recovered := recoverCreationTestDataViews(t, catalog)
			after, err := recovered.Latest(t.Context(), 1)
			require.NoError(t, err)
			defer after.Deref()
			require.Equal(t, version+1, after.DataView().GetSchemaVersion())
			require.Equal(t, uint64(100), after.DataView().Shards[0].GrowingTransformStartAfterTimetick)
			require.Equal(t, version, ref.DataView().GetSchemaVersion(), "an old reference is immutable")
		})
	}
}

func TestCreateCollectionDataViewLegacyZeroVersion(t *testing.T) {
	manager := recoverCreationTestDataViews(t, datacoordkv.NewCatalog(meta.NewMetaMemoryKV(), "", ""))
	server := &Server{dataViewManager: manager}
	_, err := server.CreateCollectionDataView(t.Context(), 1, []string{"v1"})
	require.NoError(t, err)
	ref, err := manager.Latest(t.Context(), 1)
	require.NoError(t, err)
	defer ref.Deref()
	require.Zero(t, ref.DataView().GetSchemaVersion())
}

func dataViewCreationResult(version int32) message.BroadcastResultCreateCollectionMessageV1 {
	msg := message.NewCreateCollectionMessageBuilderV1().
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 1}).
		WithBody(&message.CreateCollectionRequest{
			CollectionSchema: &schemapb.CollectionSchema{Version: version}, VirtualChannelNames: []string{"v1", "v2"},
		}).WithBroadcast([]string{"v1", "v2"}).MustBuildBroadcast()
	msg = message.WithBroadcastControlChannel(msg, funcutil.GetControlChannel("p0"))
	return message.BroadcastResultCreateCollectionMessageV1{
		Message: message.MustAsBroadcastCreateCollectionMessageV1(msg),
		Results: map[string]*message.AppendResult{
			"v1": {TimeTick: 100}, "v2": {TimeTick: 101}, funcutil.GetControlChannel("p0"): {TimeTick: 1000},
		},
	}
}

func recoverCreationTestDataViews(t *testing.T, catalog dataview.Catalog) dataview.Manager {
	t.Helper()
	manager, err := dataview.RecoverManager(t.Context(), catalog, func(context.Context, int64) (bool, error) {
		return true, nil
	}, nil, nil, nil)
	require.NoError(t, err)
	return manager
}
