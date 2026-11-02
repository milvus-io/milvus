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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// The segment size target is maxSize (or the DiskANN size) capped at maxRows
// rows of the schema's estimated row width. It binds only for narrow rows.
func TestGetExpectedSegmentSizeRowCap(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	require.NoError(t, params.Save(params.DataCoordCfg.SegmentMaxSize.Key, "1024"))
	require.NoError(t, params.Save(params.DataCoordCfg.DiskSegmentMaxSize.Key, "2048"))
	require.NoError(t, params.Save(params.DataCoordCfg.SegmentMaxRows.Key, "4000000"))
	t.Cleanup(func() {
		params.Reset(params.DataCoordCfg.SegmentMaxSize.Key)
		params.Reset(params.DataCoordCfg.DiskSegmentMaxSize.Key)
		params.Reset(params.DataCoordCfg.SegmentMaxRows.Key)
	})
	const (
		collectionID = int64(1)
		mb           = int64(1024 * 1024)
	)
	pk := &schemapb.FieldSchema{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}
	narrow := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{pk, {FieldID: 101, Name: "age", DataType: schemapb.DataType_Int32}}}
	wide := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{pk, {
		FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "128"}},
	}}}
	lowDim := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{pk, {
		FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "32"}},
	}}}
	// 8 + 1024 + 16 = 1048 B x 4 M rows = 4.2 GB, above the DiskANN target too
	veryWide := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{pk, {
		FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "256"}},
	}}}
	noIndex := &meta{indexMeta: &indexMeta{indexes: map[UniqueID]map[UniqueID]*model.Index{}}}
	diskIndex := &meta{indexMeta: &indexMeta{indexes: map[UniqueID]map[UniqueID]*model.Index{collectionID: {
		10: {CollectionID: collectionID, FieldID: 101, IndexID: 10, IndexParams: []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "DISKANN"}}},
	}}}}

	t.Run("narrow rows are capped by maxRows", func(t *testing.T) {
		require.Equal(t, int64(28*4000000), getExpectedSegmentSize(noIndex, collectionID, narrow))
		require.Equal(t, typeutil.EstimateRowWidth(narrow)*4000000, getExpectedSegmentSize(noIndex, collectionID, narrow))
	})
	t.Run("wide rows keep maxSize", func(t *testing.T) {
		require.Equal(t, 1024*mb, getExpectedSegmentSize(noIndex, collectionID, wide))
	})
	t.Run("low dimensional vector is capped below maxSize", func(t *testing.T) {
		// 8 + 128 + 16 = 152 B x 4 M = 608 MB
		require.Equal(t, int64(152*4000000), getExpectedSegmentSize(noIndex, collectionID, lowDim))
	})
	t.Run("disk index base is capped too", func(t *testing.T) {
		require.Equal(t, 2048*mb, getExpectedSegmentSize(diskIndex, collectionID, veryWide))
		// 536 B x 4 M = 2.144 GB sits just under the 2 GiB DiskANN target
		require.Equal(t, int64(536*4000000), getExpectedSegmentSize(diskIndex, collectionID, wide))
		require.Equal(t, int64(152*4000000), getExpectedSegmentSize(diskIndex, collectionID, lowDim))
	})
	t.Run("maxRows 0 disables the cap", func(t *testing.T) {
		require.NoError(t, params.Save(params.DataCoordCfg.SegmentMaxRows.Key, "0"))
		defer params.Save(params.DataCoordCfg.SegmentMaxRows.Key, "4000000")
		require.Equal(t, 1024*mb, getExpectedSegmentSize(noIndex, collectionID, lowDim))
		require.Equal(t, 2048*mb, getExpectedSegmentSize(diskIndex, collectionID, lowDim))
	})
	t.Run("nil meta falls back to maxSize", func(t *testing.T) {
		require.Equal(t, int64(28*4000000), getExpectedSegmentSize(nil, collectionID, narrow))
		require.Equal(t, 1024*mb, getExpectedSegmentSize(nil, collectionID, wide))
	})
}
