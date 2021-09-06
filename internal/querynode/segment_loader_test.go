// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package querynode

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/assert"
)

func TestSegmentLoader_CheckSegmentMemory(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	collectionID := UniqueID(0)
	segmentID := UniqueID(0)

	genSegmentLoader := func() *segmentLoader {
		replica := newCollectionReplica(nil)
		err := replica.addCollection(collectionID, genTestCollectionSchema(collectionID, false, 128))
		assert.NoError(t, err)
		loader := newSegmentLoader(ctx, nil, nil, replica, nil)
		return loader
	}

	genSegmentLoadInfo := func() *querypb.SegmentLoadInfo {
		return &querypb.SegmentLoadInfo{
			SegmentID:    segmentID,
			PartitionID:  UniqueID(0),
			CollectionID: collectionID,
			NumOfRows:    1000,
		}
	}

	t.Run("valid test", func(t *testing.T) {
		loader := genSegmentLoader()
		err := loader.checkSegmentMemory([]*querypb.SegmentLoadInfo{genSegmentLoadInfo()})
		assert.NoError(t, err)
	})

	t.Run("test no collection", func(t *testing.T) {
		loader := genSegmentLoader()
		loader.historicalReplica.freeAll()
		err := loader.checkSegmentMemory([]*querypb.SegmentLoadInfo{genSegmentLoadInfo()})
		assert.Error(t, err)
	})

	t.Run("test OOM", func(t *testing.T) {
		totalRAM := metricsinfo.GetMemoryCount()

		loader := genSegmentLoader()
		col, err := loader.historicalReplica.getCollectionByID(collectionID)
		assert.NoError(t, err)

		sizePerRecord, err := typeutil.EstimateSizePerRecord(col.schema)
		assert.NoError(t, err)

		info := genSegmentLoadInfo()
		info.NumOfRows = int64(totalRAM / uint64(sizePerRecord))
		err = loader.checkSegmentMemory([]*querypb.SegmentLoadInfo{info})
		assert.Error(t, err)
	})
}
