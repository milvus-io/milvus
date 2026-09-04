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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/textindex"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func fuzzyBM25LoadTestSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar}},
		Functions: []*schemapb.FunctionSchema{{
			Type:          schemapb.FunctionType_BM25,
			InputFieldIds: []int64{101},
			Params:        []*commonpb.KeyValuePair{{Key: common.EnableFuzzyKey, Value: "true"}},
		}},
	}
}

func textTermLoadInfo(path string, artifact *textindex.FstArtifact) *querypb.SegmentLoadInfo {
	return &querypb.SegmentLoadInfo{
		SegmentID: 1,
		BinlogPaths: []*datapb.FieldBinlog{{
			FieldID: 101,
			Binlogs: []*datapb.Binlog{{TimestampTo: 100}},
		}},
		TextLogV2: []*datapb.FieldBinlog{{
			FieldID: 101,
			Format:  textindex.MilvusTextFstFormat,
			Binlogs: []*datapb.Binlog{{
				LogPath:     path,
				LogSize:     int64(len(artifact.Data)),
				MemorySize:  int64(len(artifact.Data)),
				EntriesNum:  artifact.TermCount,
				TimestampTo: 100,
			}},
		}},
	}
}

func TestBuildLoadedTextTermDictionaryHeapAndMmap(t *testing.T) {
	paramtable.Init()
	root := t.TempDir()
	paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, t.TempDir())
	t.Cleanup(func() {
		paramtable.Get().Reset(paramtable.Get().LocalStorageCfg.Path.Key)
		paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.MmapTextLogV2.Key)
	})
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(root))
	loader := &segmentLoader{cm: cm}
	artifact, err := textindex.BuildTextFst([][]byte{[]byte("fuzzy"), []byte("milvus")})
	require.NoError(t, err)
	remotePath := filepath.Join(root, "text-term/1.fst")
	require.NoError(t, cm.Write(context.Background(), remotePath, artifact.Data))
	loadInfo := textTermLoadInfo(remotePath, artifact)

	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.MmapTextLogV2.Key, "false")
	heap, err := loader.buildLoadedTextTermDictionary(context.Background(), fuzzyBM25LoadTestSchema(), loadInfo)
	require.NoError(t, err)
	require.Len(t, heap.readers[101], 1)
	assert.False(t, heap.readers[101][0].IsMemoryMapped())
	assert.EqualValues(t, len(artifact.Data), heap.heapBytes)
	heap.close()

	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.MmapTextLogV2.Key, "true")
	mapped, err := loader.buildLoadedTextTermDictionary(context.Background(), fuzzyBM25LoadTestSchema(), loadInfo)
	require.NoError(t, err)
	require.Len(t, mapped.readers[101], 1)
	assert.True(t, mapped.readers[101][0].IsMemoryMapped())
	assert.Zero(t, mapped.heapBytes)
	require.DirExists(t, mapped.cacheDir)
	cacheDir := mapped.cacheDir
	mapped.close()
	_, err = os.Stat(cacheDir)
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestBuildLoadedTextTermDictionaryFailsClosed(t *testing.T) {
	paramtable.Init()
	root := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(root))
	loader := &segmentLoader{cm: cm}
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.MmapTextLogV2.Key, "false")
	t.Cleanup(func() {
		paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.MmapTextLogV2.Key)
	})

	_, err := loader.buildLoadedTextTermDictionary(context.Background(), fuzzyBM25LoadTestSchema(), &querypb.SegmentLoadInfo{SegmentID: 1})
	require.Error(t, err)

	artifact, err := textindex.BuildTextFst([][]byte{[]byte("fuzzy")})
	require.NoError(t, err)
	artifact.Data[len(artifact.Data)-1] ^= 0xff
	remotePath := filepath.Join(root, "text-term/corrupt.fst")
	require.NoError(t, cm.Write(context.Background(), remotePath, artifact.Data))
	_, err = loader.buildLoadedTextTermDictionary(context.Background(), fuzzyBM25LoadTestSchema(), textTermLoadInfo(remotePath, artifact))
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
}

func TestBuildLoadedTextTermDictionaryValidatesManifestAggregateSize(t *testing.T) {
	paramtable.Init()
	root := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(root))
	loader := &segmentLoader{cm: cm}
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.MmapTextLogV2.Key, "false")
	t.Cleanup(func() {
		paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.MmapTextLogV2.Key)
	})

	first, err := textindex.BuildTextFst([][]byte{[]byte("fuzzy")})
	require.NoError(t, err)
	second, err := textindex.BuildTextFst([][]byte{[]byte("milvus")})
	require.NoError(t, err)
	firstPath := filepath.Join(root, "text-term/first.fst")
	secondPath := filepath.Join(root, "text-term/second.fst")
	require.NoError(t, cm.Write(context.Background(), firstPath, first.Data))
	require.NoError(t, cm.Write(context.Background(), secondPath, second.Data))

	actualSize := int64(len(first.Data) + len(second.Data))
	loadInfo := &querypb.SegmentLoadInfo{
		SegmentID: 1,
		BinlogPaths: []*datapb.FieldBinlog{{
			FieldID: 101,
			Binlogs: []*datapb.Binlog{{TimestampTo: 100}},
		}},
		TextLogV2: []*datapb.FieldBinlog{{
			FieldID: 101,
			Format:  textindex.MilvusTextFstFormat,
			Binlogs: []*datapb.Binlog{
				{
					LogPath:     firstPath,
					LogSize:     actualSize + 1,
					MemorySize:  actualSize + 1,
					TimestampTo: 100,
				},
				{
					LogPath:     secondPath,
					TimestampTo: 100,
				},
			},
		}},
	}

	_, err = loader.buildLoadedTextTermDictionary(context.Background(), fuzzyBM25LoadTestSchema(), loadInfo)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	assert.Contains(t, err.Error(), "aggregate size mismatch")
}

func TestValidateTextTermLoadMetadataRejectsMissingResourceMetadata(t *testing.T) {
	artifact, err := textindex.BuildTextFst([][]byte{[]byte("fuzzy")})
	require.NoError(t, err)
	loadInfo := textTermLoadInfo("", artifact)
	_, err = validateTextTermLoadMetadata(fuzzyBM25FieldIDs(fuzzyBM25LoadTestSchema()), loadInfo)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)

	loadInfo = textTermLoadInfo(filepath.Join(t.TempDir(), "text-term/1.fst"), artifact)
	loadInfo.TextLogV2[0].Binlogs[0].LogSize = 0
	_, err = validateTextTermLoadMetadata(fuzzyBM25FieldIDs(fuzzyBM25LoadTestSchema()), loadInfo)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
}

func TestValidateTextTermLoadMetadataRejectsMissingOrStaleCoverage(t *testing.T) {
	artifact, err := textindex.BuildTextFst([][]byte{[]byte("fuzzy")})
	require.NoError(t, err)
	enabled := fuzzyBM25FieldIDs(fuzzyBM25LoadTestSchema())
	path := filepath.Join(t.TempDir(), "text-term/1.fst")

	t.Run("missing fragment coverage", func(t *testing.T) {
		loadInfo := textTermLoadInfo(path, artifact)
		loadInfo.TextLogV2[0].Binlogs[0].TimestampTo = 0

		_, err := validateTextTermLoadMetadata(enabled, loadInfo)
		require.ErrorIs(t, err, merr.ErrDataIntegrity)
		assert.Contains(t, err.Error(), "fragment coverage is missing")
	})

	t.Run("coverage behind readable data", func(t *testing.T) {
		loadInfo := textTermLoadInfo(path, artifact)
		loadInfo.BinlogPaths[0].Binlogs[0].TimestampTo++

		_, err := validateTextTermLoadMetadata(enabled, loadInfo)
		require.ErrorIs(t, err, merr.ErrDataIntegrity)
		assert.Contains(t, err.Error(), "coverage 100 is behind data coverage 101")
	})
}

func TestResolveTextLogV2SkipsL0Segment(t *testing.T) {
	loader := &segmentLoader{}
	err := loader.resolveTextLogV2(context.Background(), fuzzyBM25LoadTestSchema(), []*querypb.SegmentLoadInfo{{
		SegmentID: 1,
		Level:     datapb.SegmentLevel_L0,
	}})
	require.NoError(t, err)
}

func TestBuildLoadedTextTermDictionaryPreservesMissingObjectError(t *testing.T) {
	paramtable.Init()
	loader := &segmentLoader{cm: storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))}
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.MmapTextLogV2.Key, "false")
	t.Cleanup(func() {
		paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.MmapTextLogV2.Key)
	})
	artifact, err := textindex.BuildTextFst([][]byte{[]byte("fuzzy")})
	require.NoError(t, err)

	_, err = loader.buildLoadedTextTermDictionary(
		context.Background(), fuzzyBM25LoadTestSchema(), textTermLoadInfo(filepath.Join(t.TempDir(), "text-term/missing.fst"), artifact))
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrIoKeyNotFound)
	assert.NotErrorIs(t, err, merr.ErrDataIntegrity)
}
