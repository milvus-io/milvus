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

package segments

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestDropHotLoadedSortedPrimaryKeyIndex(t *testing.T) {
	paramtable.Init()
	initcore.InitLocalChunkManager(filepath.Join(paramtable.Get().LocalStorageCfg.Path.GetValue(), typeutil.QueryNodeRole))
	initcore.InitMmapManager(paramtable.Get(), 1)
	initcore.InitTieredStorage(paramtable.Get())
	ctx := context.Background()
	for _, pkType := range []schemapb.DataType{schemapb.DataType_Int64, schemapb.DataType_VarChar} {
		t.Run(pkType.String(), func(t *testing.T) {
			schema := mock_segcore.GenTestCollectionSchema("sorted-pk-index", pkType, true)
			pkField, err := typeutil.GetPrimaryFieldSchema(schema)
			require.NoError(t, err)
			collection, err := NewCollection(100, schema, nil, &querypb.LoadMetaInfo{})
			require.NoError(t, err)
			t.Cleanup(func() { DeleteCollection(collection) })
			segment, err := NewSegment(ctx, collection, NewManager().Segment, SegmentTypeSealed, 0, &querypb.SegmentLoadInfo{
				CollectionID:  100,
				PartitionID:   10,
				SegmentID:     1,
				InsertChannel: "by-dev-rootcoord-dml_0_100v0",
				Level:         datapb.SegmentLevel_Legacy,
				IsSorted:      true,
			})
			require.NoError(t, err)
			t.Cleanup(func() { segment.Release(ctx) })
			local := segment.(*LocalSegment)
			indexA := &querypb.FieldIndexInfo{FieldID: pkField.GetFieldID(), IndexID: 1000, EnableIndex: true}
			indexB := &querypb.FieldIndexInfo{FieldID: pkField.GetFieldID(), IndexID: 2000, EnableIndex: true}

			// Exercise the real sorted-PK load path: it skips native loading but
			// must leave metadata that DropIndex can remove by the index ID.
			require.NoError(t, local.LoadIndex(ctx, indexA, pkType))
			require.Len(t, local.Indexes(), 1)
			require.NoError(t, local.DropIndex(ctx, 1000))
			require.Empty(t, local.Indexes(), "stale A would block the checker's replacement load")

			require.NoError(t, local.LoadIndex(ctx, indexB, pkType))
			require.NotNil(t, local.GetIndexByID(2000))
			require.True(t, local.GetIndexByID(2000).IsLoaded)
			require.NoError(t, local.DropIndex(ctx, 1000))
			require.Len(t, local.Indexes(), 1, "a delayed A drop must preserve B")
			require.NoError(t, local.DropIndex(ctx, 2000))
			require.Empty(t, local.Indexes())
		})
	}
}
