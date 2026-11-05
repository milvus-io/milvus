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

package rootcoord

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	kvrootcoord "github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestRestoreCollectionVisibilityAfterDurableCompletion(t *testing.T) {
	collection := &model.Collection{CollectionID: 10, DBID: 1, DBName: "default", Name: "restored", State: etcdpb.CollectionState_CollectionRestoring}
	mt := &MetaTable{catalog: &kvrootcoord.Catalog{}, collID2Meta: map[int64]*model.Collection{10: collection}}
	_, err := mt.GetCollectionByID(t.Context(), "default", 10, typeutil.MaxTimestamp, false)
	require.ErrorIs(t, err, merr.ErrCollectionNotFound)
	internal, err := mt.GetCollectionByID(t.Context(), "default", 10, typeutil.MaxTimestamp, true)
	require.NoError(t, err)
	require.Equal(t, etcdpb.CollectionState_CollectionRestoring, internal.State)
	patch := mockey.Mock((*kvrootcoord.Catalog).AlterCollection).Return(merr.ErrServiceUnavailable).Build()
	require.ErrorIs(t, mt.CompleteRestoreCollection(t.Context(), 10), merr.ErrServiceUnavailable)
	require.False(t, mt.collID2Meta[10].Available())
	patch.UnPatch()
	patch = mockey.Mock((*kvrootcoord.Catalog).AlterCollection).Return(nil).Build()
	defer patch.UnPatch()
	require.NoError(t, mt.CompleteRestoreCollection(t.Context(), 10))
	require.NoError(t, mt.CompleteRestoreCollection(t.Context(), 10))
	require.Equal(t, 1, mt.availableCollectionCount)
	require.Equal(t, etcdpb.CollectionState_CollectionRestoring, collection.State, "published metadata is immutable")
	_, err = mt.GetCollectionByID(t.Context(), "default", 10, typeutil.MaxTimestamp, false)
	require.NoError(t, err)
}
