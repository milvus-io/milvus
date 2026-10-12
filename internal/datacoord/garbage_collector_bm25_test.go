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
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
)

func TestGarbageCollectorBM25OrphanFiles(t *testing.T) {
	for _, local := range []bool{true, false} {
		name := "remote complete keys"
		if local {
			name = "local filesystem"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			root := "files"
			if local {
				root = t.TempDir()
			}
			key := func(suffix string) string { return path.Join(root, "bm25_stats", suffix) }
			cases := []struct {
				suffix string
				keep   bool
				fresh  bool
			}{
				{"100/10/200/101/1000", true, false}, // compressed metadata
				{"100/10/200/101/1", true, false},    // compound stats
				{"100/10/200/102/1", false, false},   // same LogID, different field
				{"100/10/200/101/999", false, false}, // superseded stats
				{"100/10/200/101/777", true, false},  // explicit LogPath overrides LogID
				{"100/10/200/101/1002", false, false},
				{"101/10/200/101/1000", false, false}, // same segment/log IDs, wrong collection
				{"100/11/200/101/1000", false, false}, // same segment/log IDs, wrong partition
				{"100/10/201/101/1000", false, false}, // absent segment
				{"100/10/202/101/1000", true, true},   // fresh orphan
				{"100/10/205/101/1000", true, false},  // snapshot-protected absent segment
				{"100/10/300/101/999", true, false},   // registered V3 skipped
			}
			segment := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID: 200, CollectionID: 100, PartitionID: 10,
				State: commonpb.SegmentState_Flushed,
				Bm25Statslogs: []*datapb.FieldBinlog{{FieldID: 101, Binlogs: []*datapb.Binlog{
					{LogID: 1000}, {LogID: 1}, {LogID: 1002, LogPath: key("100/10/200/101/777")},
				}}},
			}}
			mt := &meta{segments: &SegmentsInfo{segments: map[int64]*SegmentInfo{
				200: segment,
				300: {SegmentInfo: &datapb.SegmentInfo{ID: 300, CollectionID: 100, PartitionID: 10, StorageVersion: storage.StorageV3}},
			}}, snapshotMeta: &snapshotMeta{}}
			blocked := mockey.Mock((*snapshotMeta).IsSegmentGCBlocked).To(
				func(_ *snapshotMeta, _ int64, segmentID int64) bool { return segmentID == 205 },
			).Build()
			defer blocked.UnPatch()
			pool := conc.NewPool[struct{}](1)
			t.Cleanup(pool.Release)
			gc := &garbageCollector{meta: mt, option: GcOption{missingTolerance: time.Hour, removeObjectPool: pool}}
			old := time.Now().Add(-2 * time.Hour)
			if local {
				cm := storage.NewLocalChunkManager(objectstorage.RootPath(root))
				gc.option.cli = cm
				for _, tc := range cases {
					file := key(tc.suffix)
					require.NoError(t, cm.Write(ctx, file, []byte(tc.suffix)))
					if !tc.fresh {
						require.NoError(t, os.Chtimes(file, old, old))
					}
				}
				gc.recycleUnusedBinlogFiles(ctx)
				for _, tc := range cases {
					exists, err := cm.Exist(ctx, key(tc.suffix))
					require.NoError(t, err)
					assert.Equal(t, tc.keep, exists, tc.suffix)
				}
				// GC must not decompress or otherwise rewrite authoritative metadata.
				assert.Empty(t, segment.GetBm25Statslogs()[0].GetBinlogs()[0].GetLogPath())
				return
			}

			cm := mocks.NewChunkManager(t)
			gc.option.cli = cm
			cm.EXPECT().RootPath().Return(root)
			files := make(map[string]*storage.ChunkObjectInfo)
			for _, tc := range cases {
				modified := old
				if tc.fresh {
					modified = time.Now()
				}
				files[key(tc.suffix)] = &storage.ChunkObjectInfo{FilePath: key(tc.suffix), ModifyTime: modified}
			}
			cm.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, true, mock.Anything).RunAndReturn(
				func(_ context.Context, prefix string, _ bool, walk storage.ChunkObjectWalkFunc) error {
					// Snapshot the listing: the deletion worker may run concurrently.
					listed := make([]*storage.ChunkObjectInfo, 0)
					for file, info := range files {
						if strings.HasPrefix(file, strings.TrimSuffix(prefix, "/")+"/") {
							listed = append(listed, info)
						}
					}
					for _, info := range listed {
						if !walk(info) {
							break
						}
					}
					return nil
				})
			retryFile := key("100/10/201/101/1000")
			cm.EXPECT().Remove(mock.Anything, retryFile).Return(errors.New("temporary delete failure")).Once()
			for _, tc := range cases {
				if tc.keep || key(tc.suffix) == retryFile {
					continue
				}
				cm.EXPECT().Remove(mock.Anything, key(tc.suffix)).RunAndReturn(func(_ context.Context, file string) error {
					delete(files, file)
					return nil
				}).Once()
			}
			gc.recycleUnusedBinlogFiles(ctx)
			require.Contains(t, files, retryFile)
			cm.EXPECT().Remove(mock.Anything, retryFile).RunAndReturn(func(_ context.Context, file string) error {
				delete(files, file)
				return nil
			}).Once()
			gc.recycleUnusedBinlogFiles(ctx)
			for _, tc := range cases {
				_, exists := files[key(tc.suffix)]
				assert.Equal(t, tc.keep, exists, tc.suffix)
			}
		})
	}
}
