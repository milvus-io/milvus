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
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/externalrefresh"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metacache/metacachetest"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestServerHandler_GetCollectionReadsSource(t *testing.T) {
	ms := metacache.NewMetaStore(nil)
	live := &meta.CollectionInfo{ID: 1}
	dropping := &meta.CollectionInfo{ID: 2, State: etcdpb.CollectionState_CollectionDropping}
	ms.PutCollection(live)
	ms.PutCollection(dropping)
	h := newServerHandler(&Server{meta: &meta.Meta{MetaStore: ms}})

	tests := []struct {
		name string
		id   int64
		want *meta.CollectionInfo
		err  error
	}{
		{"live", 1, live, nil},
		{"dropping is returned with its state", 2, dropping, nil},
		{"missing is not found", 3, nil, merr.ErrCollectionNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := h.GetCollection(context.Background(), tc.id)
			if tc.err != nil {
				assert.ErrorIs(t, err, tc.err)
				assert.Nil(t, got)
				return
			}
			require.NoError(t, err)
			assert.Same(t, tc.want, got)
		})
	}
}

func TestServer_WatchCollectionMetrics(t *testing.T) {
	ms := metacache.NewMetaStore(nil)
	source := metacachetest.Collections(ms)
	source.Put(&meta.CollectionInfo{ID: 1})
	s := &Server{metaStore: ms}

	numCollections := func() float64 {
		return testutil.ToFloat64(metrics.DataCoordNumCollections.WithLabelValues())
	}
	s.watchCollectionMetrics()
	assert.Equal(t, float64(1), numCollections(), "seeded at registration")

	source.Put(&meta.CollectionInfo{ID: 2})
	assert.Equal(t, float64(2), numCollections())

	labels := []string{"db", fmt.Sprint(2), "coll", "Flushed"}
	metrics.DataCoordNumStoredRows.WithLabelValues(labels...).Set(10)
	source.Remove(2)
	assert.Equal(t, float64(1), numCollections())
	assert.False(t, metrics.DataCoordNumStoredRows.DeleteLabelValues(labels...),
		"per-collection series are dropped on removal")

	// Dropping collections are not counted.
	source.Put(&meta.CollectionInfo{ID: 1, State: etcdpb.CollectionState_CollectionDropping})
	assert.Equal(t, float64(0), numCollections())

	// The hooks follow a replacement source and ignore the old one.
	replacement := metacache.NewCollections()
	ms.SetCollectionSource(replacement)
	replacement.Put(&meta.CollectionInfo{ID: 3})
	replacement.Put(&meta.CollectionInfo{ID: 4})
	assert.Equal(t, float64(2), numCollections())
	source.Put(&meta.CollectionInfo{ID: 5})
	assert.Equal(t, float64(2), numCollections())
}

// Every alter enqueues the collection's stats builds; only one that
// publishes a new external source/spec also enqueues its index builds.
func TestWatchCollectionAlters(t *testing.T) {
	spec := `{"format":"parquet"}`
	for _, tc := range []struct {
		name    string
		updated *schemapb.CollectionSchema
		notify  bool
	}{
		{"same source", externalrefresh.ExternalRefreshIndexSchema("s3://bucket/a", spec), false},
		{"new source", externalrefresh.ExternalRefreshIndexSchema("s3://bucket/b", spec), true},
		{"new spec", externalrefresh.ExternalRefreshIndexSchema("s3://bucket/a", `{"format":"orc"}`), true},
		{"not external", &schemapb.CollectionSchema{Name: "external_collection"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ms := metacache.NewMetaStore(nil)
			ms.PutCollection(&meta.CollectionInfo{ID: 100, Schema: externalrefresh.ExternalRefreshIndexSchema("s3://bucket/a", spec)})
			notifyIndexChan := make(chan UniqueID, 1)
			notifyStatsChan := make(chan UniqueID, 1)
			s := &Server{metaStore: ms, notifyIndexChan: notifyIndexChan, notifyStatsChan: notifyStatsChan}
			s.watchCollectionAlters()

			ms.PutCollection(&meta.CollectionInfo{ID: 100, Schema: tc.updated})
			require.Len(t, notifyStatsChan, 1)
			assert.Equal(t, UniqueID(100), <-notifyStatsChan)
			if tc.notify {
				require.Len(t, notifyIndexChan, 1)
				assert.Equal(t, UniqueID(100), <-notifyIndexChan)
			} else {
				assert.Empty(t, notifyIndexChan)
			}
		})
	}
}
