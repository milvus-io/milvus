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

package querycoord

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/stretchr/testify/assert"
)

var globalMetaTestDir = "/tmp/milvus_test/global_meta"

func TestGlobalMetaBroker_RootCoord(t *testing.T) {
	refreshParams()
	ctx, cancel := context.WithCancel(context.Background())
	rootCoord := newRootCoordMock(ctx)
	rootCoord.createCollection(defaultCollectionID)
	rootCoord.createPartition(defaultCollectionID, defaultPartitionID)

	cm := storage.NewLocalChunkManager(storage.RootPath(globalMetaTestDir))
	defer cm.RemoveWithPrefix("")
	handler, err := newGlobalMetaBroker(ctx, rootCoord, nil, nil, cm)
	assert.Nil(t, err)

	t.Run("successCase", func(t *testing.T) {
		err = handler.releaseDQLMessageStream(ctx, defaultCollectionID)
		assert.Nil(t, err)
		enableIndex, _, err := handler.getIndexBuildID(ctx, defaultCollectionID, defaultSegmentID)
		assert.Nil(t, err)
		_, err = handler.showPartitionIDs(ctx, defaultCollectionID)
		assert.Nil(t, err)
		assert.Equal(t, false, enableIndex)
	})

	t.Run("returnError", func(t *testing.T) {
		rootCoord.returnError = true
		err = handler.releaseDQLMessageStream(ctx, defaultCollectionID)
		assert.Error(t, err)
		_, _, err = handler.getIndexBuildID(ctx, defaultCollectionID, defaultSegmentID)
		assert.Error(t, err)
		_, err = handler.showPartitionIDs(ctx, defaultCollectionID)
		assert.Error(t, err)
		rootCoord.returnError = false
	})

	t.Run("returnGrpcError", func(t *testing.T) {
		rootCoord.returnGrpcError = true
		err = handler.releaseDQLMessageStream(ctx, defaultCollectionID)
		assert.Error(t, err)
		_, _, err = handler.getIndexBuildID(ctx, defaultCollectionID, defaultSegmentID)
		assert.Error(t, err)
		_, err = handler.showPartitionIDs(ctx, defaultCollectionID)
		assert.Error(t, err)
		rootCoord.returnGrpcError = false
	})

	cancel()
}

func TestGlobalMetaBroker_DataCoord(t *testing.T) {
	refreshParams()
	ctx, cancel := context.WithCancel(context.Background())
	dataCoord := newDataCoordMock(ctx)

	cm := storage.NewLocalChunkManager(storage.RootPath(globalMetaTestDir))
	defer cm.RemoveWithPrefix("")
	handler, err := newGlobalMetaBroker(ctx, nil, dataCoord, nil, cm)
	assert.Nil(t, err)

	t.Run("successCase", func(t *testing.T) {
		_, _, err = handler.getRecoveryInfo(ctx, defaultCollectionID, defaultPartitionID)
		assert.Nil(t, err)
		_, err = handler.getSegmentStates(ctx, defaultSegmentID)
		assert.Nil(t, err)
	})

	t.Run("returnError", func(t *testing.T) {
		dataCoord.returnError = true
		_, _, err = handler.getRecoveryInfo(ctx, defaultCollectionID, defaultPartitionID)
		assert.Error(t, err)
		_, err = handler.getSegmentStates(ctx, defaultSegmentID)
		assert.Error(t, err)
		dataCoord.returnError = false
	})

	t.Run("returnGrpcError", func(t *testing.T) {
		dataCoord.returnGrpcError = true
		_, _, err = handler.getRecoveryInfo(ctx, defaultCollectionID, defaultPartitionID)
		assert.Error(t, err)
		_, err = handler.getSegmentStates(ctx, defaultSegmentID)
		assert.Error(t, err)
		dataCoord.returnGrpcError = false
	})

	cancel()
}

func TestGlobalMetaBroker_IndexCoord(t *testing.T) {
	refreshParams()
	ctx, cancel := context.WithCancel(context.Background())
	rootCoord := newRootCoordMock(ctx)
	rootCoord.enableIndex = true
	rootCoord.createCollection(defaultCollectionID)
	rootCoord.createPartition(defaultCollectionID, defaultPartitionID)
	indexCoord, err := newIndexCoordMock(globalMetaTestDir)
	assert.Nil(t, err)

	cm := storage.NewLocalChunkManager(storage.RootPath(globalMetaTestDir))
	defer cm.RemoveWithPrefix("")
	handler, err := newGlobalMetaBroker(ctx, rootCoord, nil, indexCoord, cm)
	assert.Nil(t, err)

	t.Run("successCase", func(t *testing.T) {
		indexFilePathInfos, err := handler.getIndexFilePaths(ctx, int64(100))
		assert.Nil(t, err)
		assert.Equal(t, 1, len(indexFilePathInfos))
		indexInfos, err := handler.getIndexInfo(ctx, defaultCollectionID, defaultSegmentID, genDefaultCollectionSchema(false))
		assert.Nil(t, err)
		assert.Equal(t, 1, len(indexInfos))
	})

	t.Run("returnError", func(t *testing.T) {
		indexCoord.returnError = true
		indexFilePathInfos, err := handler.getIndexFilePaths(ctx, int64(100))
		assert.Error(t, err)
		assert.Nil(t, indexFilePathInfos)
		indexInfos, err := handler.getIndexInfo(ctx, defaultCollectionID, defaultSegmentID, genDefaultCollectionSchema(false))
		assert.Error(t, err)
		assert.Nil(t, indexInfos)
		indexCoord.returnError = false
	})

	t.Run("returnGrpcError", func(t *testing.T) {
		indexCoord.returnGrpcError = true
		indexFilePathInfos, err := handler.getIndexFilePaths(ctx, int64(100))
		assert.Error(t, err)
		assert.Nil(t, indexFilePathInfos)
		indexInfos, err := handler.getIndexInfo(ctx, defaultCollectionID, defaultSegmentID, genDefaultCollectionSchema(false))
		assert.Error(t, err)
		assert.Nil(t, indexInfos)
		indexCoord.returnGrpcError = false
	})

	cancel()
}
