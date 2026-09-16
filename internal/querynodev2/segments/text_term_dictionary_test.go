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
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"

	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/util/textindex"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func newGrowingTextTermDictionary(t *testing.T) *segmentTextTermDictionary {
	t.Helper()
	paramtable.Init()
	params := paramtable.Get()
	oldLocalStoragePath := params.LocalStorageCfg.Path.SwapTempValue(t.TempDir())
	t.Cleanup(func() {
		params.LocalStorageCfg.Path.SwapTempValue(oldLocalStoragePath)
	})
	localDataRootPath := filepath.Join(params.LocalStorageCfg.Path.GetValue(), typeutil.QueryNodeRole)
	require.NoError(t, initcore.InitLocalChunkManager(localDataRootPath))
	require.NoError(t, initcore.InitMmapManager(params, 1))
	schema := mock_segcore.GenTestCollectionSchema(
		"text-term-dictionary",
		schemapb.DataType_Int64,
		true,
	)
	collection, err := segcore.CreateCCollection(&segcore.CreateCCollectionRequest{
		CollectionID: 1,
		Schema:       schema,
	})
	require.NoError(t, err)
	t.Cleanup(collection.Release)
	segment, err := segcore.CreateCSegment(&segcore.CreateCSegmentRequest{
		Collection:  collection,
		SegmentID:   1,
		SegmentType: segcore.SegmentTypeGrowing,
	})
	require.NoError(t, err)
	dictionary := newSegmentTextTermDictionary(unsafe.Pointer(segment.RawPointer()))
	t.Cleanup(func() {
		dictionary.close()
		segment.Release()
	})
	return dictionary
}

func TestSegmentTextTermDictionaryDeduplicatesGrowingTerms(t *testing.T) {
	dictionary := newGrowingTextTermDictionary(t)
	require.NoError(t, dictionary.add([]*msgpb.TextTermBatch{{
		InputFieldId: 101,
		Terms:        [][]byte{[]byte("fuzzy"), []byte("milvus"), []byte("fuzzy")},
	}, {
		InputFieldId: 101,
		Terms:        [][]byte{[]byte("milvus"), []byte("search")},
	}}))

	dictionary.mu.RLock()
	assert.EqualValues(t, 3, dictionary.termCount)
	dictionary.mu.RUnlock()
	assert.Positive(t, dictionary.memoryBytes())
}

func TestSegmentTextTermDictionaryRejectsInvalidTermMetadata(t *testing.T) {
	dictionary := newGrowingTextTermDictionary(t)
	require.NoError(t, dictionary.add([]*msgpb.TextTermBatch{{InputFieldId: 101}}))
	require.Error(t, dictionary.add([]*msgpb.TextTermBatch{{
		InputFieldId: 101,
		Terms:        [][]byte{{0xff}},
	}}))
}

func TestSegmentTextTermDictionaryImportsCommittedBaseAndGrowingDelta(t *testing.T) {
	artifact, err := textindex.BuildTextFst([][]byte{[]byte("committed")})
	require.NoError(t, err)
	reader, err := textindex.LoadTextFstBytes(artifact.Data)
	require.NoError(t, err)

	dictionary := newGrowingTextTermDictionary(t)
	recovery := &loadedTextTermDictionary{
		readers:   map[int64][]*textindex.FstReader{101: {reader}},
		heapBytes: reader.DataSize(),
	}
	require.NoError(t, dictionary.importLoaded(recovery))
	require.NoError(t, dictionary.add([]*msgpb.TextTermBatch{{
		InputFieldId: 101,
		Terms:        [][]byte{[]byte("replayed")},
	}}))

	dictionary.mu.RLock()
	assert.Nil(t, dictionary.loaded)
	assert.EqualValues(t, 2, dictionary.termCount)
	dictionary.mu.RUnlock()
	assert.Positive(t, dictionary.memoryBytes())
}

func TestSegmentTextTermDictionaryImportRemovesMappedCache(t *testing.T) {
	cacheDir := filepath.Join(t.TempDir(), "segment-cache")
	require.NoError(t, os.MkdirAll(cacheDir, 0o700))
	dictionary := newGrowingTextTermDictionary(t)
	require.NoError(t, dictionary.importLoaded(&loadedTextTermDictionary{
		readers:  make(map[int64][]*textindex.FstReader),
		cacheDir: cacheDir,
	}))
	_, err := os.Stat(cacheDir)
	assert.ErrorIs(t, err, os.ErrNotExist)
	require.NoError(t, dictionary.add([]*msgpb.TextTermBatch{{
		InputFieldId: 101,
		Terms:        [][]byte{[]byte("growing")},
	}}))

	dictionary.close()
	assert.Zero(t, dictionary.memoryBytes())
}

func TestSegmentTextTermDictionaryImportFailureReleasesReadersAndCache(t *testing.T) {
	artifact, err := textindex.BuildTextFst([][]byte{[]byte("committed")})
	require.NoError(t, err)
	reader, err := textindex.LoadTextFstBytes(artifact.Data)
	require.NoError(t, err)
	reader.Close()
	cacheDir := filepath.Join(t.TempDir(), "segment-cache")
	require.NoError(t, os.MkdirAll(cacheDir, 0o700))

	dictionary := newGrowingTextTermDictionary(t)
	err = dictionary.importLoaded(&loadedTextTermDictionary{
		readers:  map[int64][]*textindex.FstReader{101: {reader}},
		cacheDir: cacheDir,
	})
	require.Error(t, err)
	_, statErr := os.Stat(cacheDir)
	assert.ErrorIs(t, statErr, os.ErrNotExist)
	assert.Zero(t, dictionary.memoryBytes())
}
