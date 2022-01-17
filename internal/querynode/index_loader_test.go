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

package querynode

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
)

func TestIndexLoader_setIndexInfo(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("test setIndexInfo", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)
		loader := node.loader
		assert.NotNil(t, loader)

		segment, err := genSimpleSealedSegment()
		assert.NoError(t, err)

		loader.indexLoader.rootCoord = newMockRootCoord()
		loader.indexLoader.indexCoord = newMockIndexCoord()

		info, err := loader.indexLoader.getIndexInfo(defaultCollectionID, segment)
		assert.NoError(t, err)
		loader.indexLoader.setIndexInfo(segment, info)
	})

	t.Run("test nil root and index", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)
		loader := node.loader
		assert.NotNil(t, loader)

		segment, err := genSimpleSealedSegment()
		assert.NoError(t, err)

		info, err := loader.indexLoader.getIndexInfo(defaultCollectionID, segment)
		assert.NoError(t, err)
		loader.indexLoader.setIndexInfo(segment, info)
	})
}

func TestIndexLoader_getIndexBinlog(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("test getIndexBinlog", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)
		loader := node.loader
		assert.NotNil(t, loader)

		paths, err := generateIndex(defaultSegmentID)
		assert.NoError(t, err)

		_, _, _, err = loader.indexLoader.getIndexBinlog(paths)
		assert.NoError(t, err)
	})

	t.Run("test invalid path", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)
		loader := node.loader
		assert.NotNil(t, loader)

		_, _, _, err = loader.indexLoader.getIndexBinlog([]string{""})
		assert.Error(t, err)
	})
}

func TestIndexLoader_printIndexParams(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)
	loader := node.loader
	assert.NotNil(t, loader)

	indexKV := []*commonpb.KeyValuePair{
		{
			Key:   "test-key-0",
			Value: "test-value-0",
		},
	}
	loader.indexLoader.printIndexParams(indexKV)
}

func TestIndexLoader_loadIndex(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("test loadIndex", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)
		loader := node.loader
		assert.NotNil(t, loader)

		segment, err := genSimpleSealedSegment()
		assert.NoError(t, err)

		loader.indexLoader.rootCoord = newMockRootCoord()
		loader.indexLoader.indexCoord = newMockIndexCoord()

		info, err := loader.indexLoader.getIndexInfo(defaultCollectionID, segment)
		assert.NoError(t, err)
		loader.indexLoader.setIndexInfo(segment, info)

		err = loader.indexLoader.loadIndex(segment, simpleVecField.id)
		assert.NoError(t, err)
	})

	t.Run("test get indexinfo with empty indexFilePath", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)
		loader := node.loader
		assert.NotNil(t, loader)

		segment, err := genSimpleSealedSegment()
		assert.NoError(t, err)

		loader.indexLoader.rootCoord = newMockRootCoord()
		ic := newMockIndexCoord()
		ic.idxFileInfo.IndexFilePaths = []string{}

		loader.indexLoader.indexCoord = ic

		_, err = loader.indexLoader.getIndexInfo(defaultCollectionID, segment)
		assert.Error(t, err)
	})

	//t.Run("test get index failed", func(t *testing.T) {
	//	historical, err := genSimpleHistorical(ctx)
	//	assert.NoError(t, err)
	//
	//	segment, err := genSimpleSealedSegment()
	//	assert.NoError(t, err)
	//
	//	historical.loader.indexLoader.rootCoord = newMockRootCoord()
	//	historical.loader.indexLoader.indexCoord = newMockIndexCoord()
	//
	//	err = historical.loader.indexLoader.loadIndex(segment, rowIDFieldID)
	//	assert.Error(t, err)
	//})

	t.Run("test checkIndexReady failed", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)
		loader := node.loader
		assert.NotNil(t, loader)

		segment, err := genSimpleSealedSegment()
		assert.NoError(t, err)

		loader.indexLoader.rootCoord = newMockRootCoord()
		loader.indexLoader.indexCoord = newMockIndexCoord()

		info, err := loader.indexLoader.getIndexInfo(defaultCollectionID, segment)
		assert.NoError(t, err)

		vecFieldID := UniqueID(101)
		info.setFieldID(vecFieldID)
		loader.indexLoader.setIndexInfo(segment, info)

		segment.indexInfos[vecFieldID].setReadyLoad(false)
		err = loader.indexLoader.loadIndex(segment, vecFieldID)
		assert.Error(t, err)
	})
}
