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

package index

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestMeta_GetHasUnindexTaskSegments(t *testing.T) {
	var (
		collID    = UniqueID(1)
		partID    = UniqueID(2)
		segID     = UniqueID(1000)
		indexID   = UniqueID(100)
		fieldID   = UniqueID(10)
		indexName = "default_idx"
	)
	segments := map[UniqueID]*meta.SegmentInfo{
		segID: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:            segID,
				CollectionID:  collID,
				PartitionID:   partID,
				InsertChannel: "",
				NumOfRows:     1025,
				State:         commonpb.SegmentState_Flushed,
			},
		},
		segID + 1: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:            segID + 1,
				CollectionID:  collID,
				PartitionID:   partID,
				InsertChannel: "",
				NumOfRows:     1025,
				State:         commonpb.SegmentState_Growing,
			},
		},
		segID + 2: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:            segID + 2,
				CollectionID:  collID,
				PartitionID:   partID,
				InsertChannel: "",
				NumOfRows:     1025,
				State:         commonpb.SegmentState_Dropped,
			},
		},
	}
	im := &IndexMeta{
		SegmentBuildInfo: NewSegmentIndexBuildInfo(),
		SegmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
		Indexes: map[UniqueID]map[UniqueID]*model.Index{
			collID: {
				indexID: {
					TenantID:        "",
					CollectionID:    collID,
					FieldID:         fieldID,
					IndexID:         indexID,
					IndexName:       indexName,
					IsDeleted:       false,
					CreateTime:      0,
					TypeParams:      nil,
					IndexParams:     nil,
					IsAutoIndex:     false,
					UserIndexParams: nil,
				},
				indexID + 1: {
					TenantID:        "",
					CollectionID:    collID,
					FieldID:         fieldID + 1,
					IndexID:         indexID + 1,
					IndexName:       indexName + "_1",
					IsDeleted:       false,
					CreateTime:      0,
					TypeParams:      nil,
					IndexParams:     nil,
					IsAutoIndex:     false,
					UserIndexParams: nil,
				},
			},
		},
	}
	m := &meta.Meta{
		Segments: meta.NewSegmentsInfo(metacache.NewMetaStore(nil)),
	}
	for id, segment := range segments {
		m.Segments.SetSegment(id, segment)
	}
	indexInspector := &IndexInspector{
		meta:      m,
		indexMeta: im,
	}

	t.Run("normal", func(t *testing.T) {
		segments := indexInspector.getUnIndexTaskSegments(context.TODO())
		assert.Equal(t, 1, len(segments))
		assert.Equal(t, segID, segments[0].ID)

		im.SegmentIndexes.Insert(segID, typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]())
		im.updateSegmentIndex(&model.SegmentIndex{
			CollectionID: collID,
			SegmentID:    segID,
			IndexID:      indexID + 2,
			IndexState:   commonpb.IndexState_Finished,
		})
		assert.Equal(t, 1, len(segments))
		assert.Equal(t, segID, segments[0].ID)
	})

	t.Run("segment partial field with index", func(t *testing.T) {
		im.updateSegmentIndex(&model.SegmentIndex{
			CollectionID: collID,
			SegmentID:    segID,
			IndexID:      indexID,
			IndexState:   commonpb.IndexState_Finished,
		})

		segments := indexInspector.getUnIndexTaskSegments(context.TODO())
		assert.Equal(t, 1, len(segments))
		assert.Equal(t, segID, segments[0].ID)
	})

	t.Run("segment all vector field with index", func(t *testing.T) {
		im.updateSegmentIndex(&model.SegmentIndex{
			CollectionID: collID,
			SegmentID:    segID,
			IndexID:      indexID + 1,
			IndexState:   commonpb.IndexState_Finished,
		})

		segments := indexInspector.getUnIndexTaskSegments(context.TODO())
		assert.Equal(t, 0, len(segments))
	})
}

// Test_checkFMIndexEngineVersion covers the shared FMINDEX rolling-upgrade gate
// used by BOTH Server.CreateIndex and snapshotManager.RestoreIndexes (so a
// snapshot restore cannot bypass it). MinScalarIndexVersionForFMINDEX is 5:
// resolved version 4 must be rejected, 5 accepted; non-FMINDEX always passes.
func Test_checkFMIndexEngineVersion(t *testing.T) {
	fmParams := []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "FMINDEX"}}
	invertedParams := []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "INVERTED"}}

	t.Run("fmindex below min version rejected", func(t *testing.T) {
		err := CheckFMIndexEngineVersion(fmParams, common.MinScalarIndexVersionForFMINDEX-1)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrServiceNotReady)
		assert.Contains(t, err.Error(), "FMINDEX requires scalar index engine version")
		assert.True(t, merr.Status(err).GetRetriable())
	})

	t.Run("fmindex at min version accepted", func(t *testing.T) {
		assert.NoError(t, CheckFMIndexEngineVersion(fmParams, common.MinScalarIndexVersionForFMINDEX))
	})

	t.Run("fmindex above min version accepted", func(t *testing.T) {
		assert.NoError(t, CheckFMIndexEngineVersion(fmParams, common.MinScalarIndexVersionForFMINDEX+1))
	})

	t.Run("non-fmindex always accepted regardless of version", func(t *testing.T) {
		assert.NoError(t, CheckFMIndexEngineVersion(invertedParams, 0))
	})
}
