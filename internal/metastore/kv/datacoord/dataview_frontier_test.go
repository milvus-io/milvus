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
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestDataViewFrontierIsNotPersisted(t *testing.T) {
	for _, composite := range []bool{false, true} {
		name := "snapshot"
		if composite {
			name = "flush_transaction"
		}
		t.Run(name, func(t *testing.T) {
			store := etcdkv.NewEtcdKV(nil, "frontier-test")
			values := make(map[string]string)
			save := mockey.Mock(mockey.GetMethod(store, "Save")).To(func(_ context.Context, key, value string) error {
				values[key] = value
				return nil
			}).Build()
			defer save.UnPatch()
			limit := mockey.Mock(mockey.GetMethod(store, "MaxTxnOps")).Return(128).Build()
			defer limit.UnPatch()
			multi := mockey.Mock(mockey.GetMethod(store, "MultiSaveAndRemove")).To(func(_ context.Context, saves map[string]string, _ []string, _ ...predicates.Predicate) error {
				for key, value := range saves {
					values[key] = value
				}
				return nil
			}).Build()
			defer multi.UnPatch()
			view := &viewpb.DataViewOfCollection{
				CollectionId: 100,
				DataVersion:  &viewpb.DataVersion{StreamingVersion: 2, CompactVersion: 1},
				Shards: []*viewpb.DataViewOfShard{
					{
						Vchannel:                    "v1",
						TransformStartAfterTimetick: 100,
						Partitions:                  []*viewpb.DataViewOfPartition{{PartitionId: 10, SegmentIds: []int64{101}, SegmentManifestVersions: []int64{7}}},
					},
					{Vchannel: "v2", TransformStartAfterTimetick: 200},
				},
			}
			original := proto.Clone(view)
			catalog := NewCatalog(store, "", "")
			if composite {
				require.NoError(t, catalog.Update(context.Background(), metastore.SaveDataView(view)))
			} else {
				require.NoError(t, catalog.SaveDataView(context.Background(), view))
			}
			require.True(t, proto.Equal(original, view), "persistence must not mutate a published runtime snapshot")
			require.Len(t, values, 1)
			persisted := &viewpb.DataViewOfCollection{}
			require.NoError(t, proto.Unmarshal([]byte(values[buildDataViewVersionKey(100, 2, 1)]), persisted))
			for _, shard := range persisted.GetShards() {
				require.Zero(t, shard.GetTransformStartAfterTimetick())
			}
			expected := proto.Clone(view).(*viewpb.DataViewOfCollection)
			for _, shard := range expected.GetShards() {
				shard.TransformStartAfterTimetick = 0
			}
			require.True(t, proto.Equal(expected, persisted), "membership and data revisions must survive persistence")
		})
	}
}
