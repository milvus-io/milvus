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
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type dataViewMetaKV struct {
	kv.MetaKv
	values map[string]string
}

func newDataViewMetaKV() *dataViewMetaKV {
	return &dataViewMetaKV{values: make(map[string]string)}
}

func (m *dataViewMetaKV) Save(_ context.Context, key, value string) error {
	m.values[key] = value
	return nil
}

func (m *dataViewMetaKV) Remove(_ context.Context, key string) error {
	delete(m.values, key)
	return nil
}

func (m *dataViewMetaKV) RemoveWithPrefix(_ context.Context, prefix string) error {
	for key := range m.values {
		if strings.HasPrefix(key, prefix) {
			delete(m.values, key)
		}
	}
	return nil
}

func (m *dataViewMetaKV) LoadWithPrefix(_ context.Context, prefix string) ([]string, []string, error) {
	keys := make([]string, 0)
	for key := range m.values {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	values := make([]string, 0, len(keys))
	for _, key := range keys {
		values = append(values, m.values[key])
	}
	return keys, values, nil
}

func (m *dataViewMetaKV) WalkWithPrefix(_ context.Context, prefix string, _ int, fn func([]byte, []byte) error) error {
	keys, values, _ := m.LoadWithPrefix(context.Background(), prefix)
	for i, key := range keys {
		if err := fn([]byte(key), []byte(values[i])); err != nil {
			return err
		}
	}
	return nil
}

func TestDataViewCatalogLifecycle(t *testing.T) {
	ctx := context.Background()
	store := newDataViewMetaKV()
	catalog := NewCatalog(store, "", "")
	dataView := &viewpb.DataViewOfCollection{
		CollectionId: 100,
		DataVersion:  &viewpb.DataVersion{StreamingVersion: 2, CompactVersion: 1},
		Shards: []*viewpb.DataViewOfShard{{
			Vchannel: "ch-1",
			Partitions: []*viewpb.DataViewOfPartition{{
				PartitionId: 10,
				SegmentIds:  []int64{101, 102},
			}},
		}},
	}

	require.NoError(t, catalog.SaveDataView(ctx, dataView))
	views, err := catalog.ListDataViews(ctx, 100)
	require.NoError(t, err)
	require.Len(t, views, 1)
	require.True(t, proto.Equal(dataView, views[0]))

	require.NoError(t, catalog.MarkDataViewCollectionDropped(ctx, 100))
	dropped, err := catalog.ListDroppedDataViewCollections(ctx)
	require.NoError(t, err)
	require.Equal(t, []int64{100}, dropped)
	allViews, err := catalog.ListAllDataViews(ctx)
	require.NoError(t, err)
	require.Len(t, allViews, 1, "drop markers are not DataView payloads")

	require.NoError(t, catalog.DropDataView(ctx, 100, dataView.GetDataVersion()))
	views, err = catalog.ListDataViews(ctx, 100)
	require.NoError(t, err)
	require.Empty(t, views)
	require.NoError(t, catalog.UnmarkDataViewCollectionDropped(ctx, 100))
}
