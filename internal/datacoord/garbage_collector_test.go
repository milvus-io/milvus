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
	"bytes"
	"context"
	"path"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	kvmocks "github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
)

func Test_garbageCollector_basic(t *testing.T) {
	bucketName := `datacoord-ut` + strings.ToLower(funcutil.RandomString(8))
	rootPath := `gc` + funcutil.RandomString(8)
	//TODO change to Params
	cli, _, _, _, _, err := initUtOSSEnv(bucketName, rootPath, 0)
	require.NoError(t, err)

	meta, err := newMemoryMeta()
	assert.NoError(t, err)

	t.Run("normal gc", func(t *testing.T) {
		gc := newGarbageCollector(meta, newMockHandler(), GcOption{
			cli:              cli,
			enabled:          true,
			checkInterval:    time.Millisecond * 10,
			missingTolerance: time.Hour * 24,
			dropTolerance:    time.Hour * 24,
		})
		gc.start()

		time.Sleep(time.Millisecond * 20)
		assert.NotPanics(t, func() {
			gc.close()
		})
	})

	t.Run("with nil cli", func(t *testing.T) {
		gc := newGarbageCollector(meta, newMockHandler(), GcOption{
			cli:              nil,
			enabled:          true,
			checkInterval:    time.Millisecond * 10,
			missingTolerance: time.Hour * 24,
			dropTolerance:    time.Hour * 24,
		})
		assert.NotPanics(t, func() {
			gc.start()
		})

		assert.NotPanics(t, func() {
			gc.close()
		})
	})

}

func validateMinioPrefixElements(t *testing.T, cli *minio.Client, bucketName string, prefix string, elements []string) {
	var current []string
	for info := range cli.ListObjects(context.TODO(), bucketName, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		current = append(current, info.Key)
	}
	assert.ElementsMatch(t, elements, current)
}

func Test_garbageCollector_scan(t *testing.T) {
	bucketName := `datacoord-ut` + strings.ToLower(funcutil.RandomString(8))
	rootPath := `gc` + funcutil.RandomString(8)
	//TODO change to Params
	cli, inserts, stats, delta, others, err := initUtOSSEnv(bucketName, rootPath, 4)
	require.NoError(t, err)

	meta, err := newMemoryMeta()
	assert.NoError(t, err)

	t.Run("key is reference", func(t *testing.T) {
		gc := newGarbageCollector(meta, newMockHandler(), GcOption{
			cli:              cli,
			enabled:          true,
			checkInterval:    time.Minute * 30,
			missingTolerance: time.Hour * 24,
			dropTolerance:    time.Hour * 24,
		})
		gc.scan()

		validateMinioPrefixElements(t, cli.Client, bucketName, path.Join(rootPath, insertLogPrefix), inserts)
		validateMinioPrefixElements(t, cli.Client, bucketName, path.Join(rootPath, statsLogPrefix), stats)
		validateMinioPrefixElements(t, cli.Client, bucketName, path.Join(rootPath, deltaLogPrefix), delta)
		validateMinioPrefixElements(t, cli.Client, bucketName, path.Join(rootPath, `indexes`), others)
		gc.close()
	})

	t.Run("missing all but save tolerance", func(t *testing.T) {
		gc := newGarbageCollector(meta, newMockHandler(), GcOption{
			cli:              cli,
			enabled:          true,
			checkInterval:    time.Minute * 30,
			missingTolerance: time.Hour * 24,
			dropTolerance:    time.Hour * 24,
		})
		gc.scan()

		validateMinioPrefixElements(t, cli.Client, bucketName, path.Join(rootPath, insertLogPrefix), inserts)
		validateMinioPrefixElements(t, cli.Client, bucketName, path.Join(rootPath, statsLogPrefix), stats)
		validateMinioPrefixElements(t, cli.Client, bucketName, path.Join(rootPath, deltaLogPrefix), delta)
		validateMinioPrefixElements(t, cli.Client, bucketName, path.Join(rootPath, `indexes`), others)

		gc.close()
	})
	t.Run("hit, no gc", func(t *testing.T) {
		segment := buildSegment(1, 10, 100, "ch", false)
		segment.State = commonpb.SegmentState_Flushed
		segment.Binlogs = []*datapb.FieldBinlog{getFieldBinlogPaths(0, inserts[0])}
		segment.Statslogs = []*datapb.FieldBinlog{getFieldBinlogPaths(0, stats[0])}
		segment.Deltalogs = []*datapb.FieldBinlog{getFieldBinlogPaths(0, delta[0])}
		err = meta.AddSegment(segment)
		require.NoError(t, err)

		gc := newGarbageCollector(meta, newMockHandler(), GcOption{
			cli:              cli,
			enabled:          true,
			checkInterval:    time.Minute * 30,
			missingTolerance: time.Hour * 24,
			dropTolerance:    time.Hour * 24,
		})
		gc.start()
		gc.scan()
		validateMinioPrefixElements(t, cli.Client, bucketName, path.Join(rootPath, insertLogPrefix), inserts)
		validateMinioPrefixElements(t, cli.Client, bucketName, path.Join(rootPath, statsLogPrefix), stats)
		validateMinioPrefixElements(t, cli.Client, bucketName, path.Join(rootPath, deltaLogPrefix), delta)
		validateMinioPrefixElements(t, cli.Client, bucketName, path.Join(rootPath, `indexes`), others)

		gc.close()
	})

	t.Run("dropped gc one", func(t *testing.T) {
		segment := buildSegment(1, 10, 100, "ch", false)
		segment.State = commonpb.SegmentState_Dropped
		segment.DroppedAt = uint64(time.Now().Add(-time.Hour).UnixNano())
		segment.Binlogs = []*datapb.FieldBinlog{getFieldBinlogPaths(0, inserts[0])}
		segment.Statslogs = []*datapb.FieldBinlog{getFieldBinlogPaths(0, stats[0])}
		segment.Deltalogs = []*datapb.FieldBinlog{getFieldBinlogPaths(0, delta[0])}

		err = meta.AddSegment(segment)
		require.NoError(t, err)

		gc := newGarbageCollector(meta, newMockHandler(), GcOption{
			cli:              cli,
			enabled:          true,
			checkInterval:    time.Minute * 30,
			missingTolerance: time.Hour * 24,
			dropTolerance:    0,
		})
		gc.clearEtcd()
		validateMinioPrefixElements(t, cli.Client, bucketName, path.Join(rootPath, insertLogPrefix), inserts[1:])
		validateMinioPrefixElements(t, cli.Client, bucketName, path.Join(rootPath, statsLogPrefix), stats[1:])
		validateMinioPrefixElements(t, cli.Client, bucketName, path.Join(rootPath, deltaLogPrefix), delta[1:])
		validateMinioPrefixElements(t, cli.Client, bucketName, path.Join(rootPath, `indexes`), others)

		gc.close()
	})
	t.Run("missing gc all", func(t *testing.T) {
		gc := newGarbageCollector(meta, newMockHandler(), GcOption{
			cli:              cli,
			enabled:          true,
			checkInterval:    time.Minute * 30,
			missingTolerance: 0,
			dropTolerance:    0,
		})
		gc.start()
		gc.scan()
		gc.clearEtcd()

		// bad path shall remains since datacoord cannot determine file is garbage or not if path is not valid
		validateMinioPrefixElements(t, cli.Client, bucketName, path.Join(rootPath, insertLogPrefix), inserts[1:2])
		validateMinioPrefixElements(t, cli.Client, bucketName, path.Join(rootPath, statsLogPrefix), stats[1:2])
		validateMinioPrefixElements(t, cli.Client, bucketName, path.Join(rootPath, deltaLogPrefix), delta[1:2])
		validateMinioPrefixElements(t, cli.Client, bucketName, path.Join(rootPath, `indexes`), others)

		gc.close()
	})

	t.Run("list object with error", func(t *testing.T) {
		gc := newGarbageCollector(meta, newMockHandler(), GcOption{
			cli:              cli,
			enabled:          true,
			checkInterval:    time.Minute * 30,
			missingTolerance: 0,
			dropTolerance:    0,
		})
		gc.start()
		gc.scan()

		// bad path shall remains since datacoord cannot determine file is garbage or not if path is not valid
		validateMinioPrefixElements(t, cli.Client, bucketName, path.Join(rootPath, insertLogPrefix), inserts[1:2])
		validateMinioPrefixElements(t, cli.Client, bucketName, path.Join(rootPath, statsLogPrefix), stats[1:2])
		validateMinioPrefixElements(t, cli.Client, bucketName, path.Join(rootPath, deltaLogPrefix), delta[1:2])
		validateMinioPrefixElements(t, cli.Client, bucketName, path.Join(rootPath, `indexes`), others)

		gc.close()
	})

	cleanupOSS(cli.Client, bucketName, rootPath)
}

// initialize unit test sso env
func initUtOSSEnv(bucket, root string, n int) (mcm *storage.MinioChunkManager, inserts []string, stats []string, delta []string, other []string, err error) {
	Params.Init()
	cli, err := minio.New(Params.MinioCfg.Address.GetValue(), &minio.Options{
		Creds:  credentials.NewStaticV4(Params.MinioCfg.AccessKeyID.GetValue(), Params.MinioCfg.SecretAccessKey.GetValue(), ""),
		Secure: Params.MinioCfg.UseSSL.GetAsBool(),
	})
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	has, err := cli.BucketExists(context.TODO(), bucket)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	if !has {
		err = cli.MakeBucket(context.TODO(), bucket, minio.MakeBucketOptions{})
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
	}
	inserts = make([]string, 0, n)
	stats = make([]string, 0, n)
	delta = make([]string, 0, n)
	other = make([]string, 0, n)

	content := []byte("test")
	for i := 0; i < n; i++ {
		reader := bytes.NewReader(content)
		// collID/partID/segID/fieldID/fileName
		// [str]/id/id/string/string

		var token string
		if i == 1 {
			token = path.Join(strconv.Itoa(i), strconv.Itoa(i), "error-seg-id", funcutil.RandomString(8), funcutil.RandomString(8))
		} else {
			token = path.Join(strconv.Itoa(1+i), strconv.Itoa(10+i), strconv.Itoa(100+i), funcutil.RandomString(8), funcutil.RandomString(8))
		}
		// insert
		filePath := path.Join(root, insertLogPrefix, token)
		info, err := cli.PutObject(context.TODO(), bucket, filePath, reader, int64(len(content)), minio.PutObjectOptions{})
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
		inserts = append(inserts, info.Key)
		// stats
		filePath = path.Join(root, statsLogPrefix, token)
		info, err = cli.PutObject(context.TODO(), bucket, filePath, reader, int64(len(content)), minio.PutObjectOptions{})
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
		stats = append(stats, info.Key)

		// delta
		if i == 1 {
			token = path.Join(strconv.Itoa(i), strconv.Itoa(i), "error-seg-id", funcutil.RandomString(8))
		} else {
			token = path.Join(strconv.Itoa(1+i), strconv.Itoa(10+i), strconv.Itoa(100+i), funcutil.RandomString(8))
		}
		filePath = path.Join(root, deltaLogPrefix, token)
		info, err = cli.PutObject(context.TODO(), bucket, filePath, reader, int64(len(content)), minio.PutObjectOptions{})
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
		delta = append(delta, info.Key)

		// other
		filePath = path.Join(root, `indexes`, token)
		info, err = cli.PutObject(context.TODO(), bucket, filePath, reader, int64(len(content)), minio.PutObjectOptions{})
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
		other = append(other, info.Key)
	}
	mcm = &storage.MinioChunkManager{
		Client: cli,
	}
	mcm.SetVar(bucket, root)
	return mcm, inserts, stats, delta, other, nil
}

func cleanupOSS(cli *minio.Client, bucket, root string) {
	ch := cli.ListObjects(context.TODO(), bucket, minio.ListObjectsOptions{Prefix: root, Recursive: true})
	cli.RemoveObjects(context.TODO(), bucket, ch, minio.RemoveObjectsOptions{})
	cli.RemoveBucket(context.TODO(), bucket)
}

func createMetaForRecycleUnusedIndexes(catalog metastore.DataCoordCatalog) *meta {
	var (
		ctx    = context.Background()
		collID = UniqueID(100)
		//partID = UniqueID(200)
		fieldID = UniqueID(300)
		indexID = UniqueID(400)
	)
	return &meta{
		RWMutex:      sync.RWMutex{},
		ctx:          ctx,
		catalog:      catalog,
		collections:  nil,
		segments:     nil,
		channelCPs:   nil,
		chunkManager: nil,
		indexes: map[UniqueID]map[UniqueID]*model.Index{
			collID: {
				indexID: {
					TenantID:        "",
					CollectionID:    collID,
					FieldID:         fieldID,
					IndexID:         indexID,
					IndexName:       "_default_idx",
					IsDeleted:       false,
					CreateTime:      10,
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
					IndexName:       "_default_idx_101",
					IsDeleted:       true,
					CreateTime:      0,
					TypeParams:      nil,
					IndexParams:     nil,
					IsAutoIndex:     false,
					UserIndexParams: nil,
				},
			},
			collID + 1: {
				indexID + 10: {
					TenantID:        "",
					CollectionID:    collID + 1,
					FieldID:         fieldID + 10,
					IndexID:         indexID + 10,
					IndexName:       "index",
					IsDeleted:       true,
					CreateTime:      10,
					TypeParams:      nil,
					IndexParams:     nil,
					IsAutoIndex:     false,
					UserIndexParams: nil,
				},
			},
		},
		buildID2SegmentIndex: nil,
	}
}

func TestGarbageCollector_recycleUnusedIndexes(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.On("DropIndex",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)
		gc := &garbageCollector{
			meta: createMetaForRecycleUnusedIndexes(catalog),
		}
		gc.recycleUnusedIndexes()
	})

	t.Run("fail", func(t *testing.T) {
		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.On("DropIndex",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(errors.New("fail"))
		gc := &garbageCollector{
			meta: createMetaForRecycleUnusedIndexes(catalog),
		}
		gc.recycleUnusedIndexes()
	})
}

func createMetaForRecycleUnusedSegIndexes(catalog metastore.DataCoordCatalog) *meta {
	var (
		ctx    = context.Background()
		collID = UniqueID(100)
		partID = UniqueID(200)
		//fieldID = UniqueID(300)
		indexID = UniqueID(400)
		segID   = UniqueID(500)
	)
	return &meta{
		RWMutex:     sync.RWMutex{},
		ctx:         ctx,
		catalog:     catalog,
		collections: nil,
		segments: &SegmentsInfo{
			segments: map[UniqueID]*SegmentInfo{
				segID: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:            segID,
						CollectionID:  collID,
						PartitionID:   partID,
						InsertChannel: "",
						NumOfRows:     1026,
						State:         commonpb.SegmentState_Flushed,
					},
					segmentIndexes: map[UniqueID]*model.SegmentIndex{
						indexID: {
							SegmentID:     segID,
							CollectionID:  collID,
							PartitionID:   partID,
							NumRows:       1026,
							IndexID:       indexID,
							BuildID:       buildID,
							NodeID:        1,
							IndexVersion:  1,
							IndexState:    commonpb.IndexState_Finished,
							FailReason:    "",
							IsDeleted:     false,
							CreateTime:    10,
							IndexFileKeys: []string{"file1", "file2"},
							IndexSize:     0,
							WriteHandoff:  false,
						},
					},
				},
				segID + 1: {
					SegmentInfo: nil,
					segmentIndexes: map[UniqueID]*model.SegmentIndex{
						indexID: {
							SegmentID:     segID + 1,
							CollectionID:  collID,
							PartitionID:   partID,
							NumRows:       1026,
							IndexID:       indexID,
							BuildID:       buildID + 1,
							NodeID:        1,
							IndexVersion:  1,
							IndexState:    commonpb.IndexState_Finished,
							FailReason:    "",
							IsDeleted:     false,
							CreateTime:    10,
							IndexFileKeys: []string{"file1", "file2"},
							IndexSize:     0,
							WriteHandoff:  false,
						},
					},
				},
			},
		},
		channelCPs:   nil,
		chunkManager: nil,
		indexes:      map[UniqueID]map[UniqueID]*model.Index{},
		buildID2SegmentIndex: map[UniqueID]*model.SegmentIndex{
			buildID: {
				SegmentID:     segID,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       1026,
				IndexID:       indexID,
				BuildID:       buildID,
				NodeID:        1,
				IndexVersion:  1,
				IndexState:    commonpb.IndexState_Finished,
				FailReason:    "",
				IsDeleted:     false,
				CreateTime:    10,
				IndexFileKeys: []string{"file1", "file2"},
				IndexSize:     0,
				WriteHandoff:  false,
			},
			buildID + 1: {
				SegmentID:     segID + 1,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       1026,
				IndexID:       indexID,
				BuildID:       buildID + 1,
				NodeID:        1,
				IndexVersion:  1,
				IndexState:    commonpb.IndexState_Finished,
				FailReason:    "",
				IsDeleted:     false,
				CreateTime:    10,
				IndexFileKeys: []string{"file1", "file2"},
				IndexSize:     0,
				WriteHandoff:  false,
			},
		},
	}
}

func TestGarbageCollector_recycleUnusedSegIndexes(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.On("DropSegmentIndex",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)
		gc := &garbageCollector{
			meta: createMetaForRecycleUnusedSegIndexes(catalog),
		}
		gc.recycleUnusedSegIndexes()
	})

	t.Run("fail", func(t *testing.T) {
		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.On("DropSegmentIndex",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(errors.New("fail"))
		gc := &garbageCollector{
			meta: createMetaForRecycleUnusedSegIndexes(catalog),
		}
		gc.recycleUnusedSegIndexes()
	})
}

func createMetaTableForRecycleUnusedIndexFiles(catalog *datacoord.Catalog) *meta {
	var (
		ctx    = context.Background()
		collID = UniqueID(100)
		partID = UniqueID(200)
		//fieldID = UniqueID(300)
		indexID = UniqueID(400)
		segID   = UniqueID(500)
		buildID = UniqueID(600)
	)
	return &meta{
		RWMutex:     sync.RWMutex{},
		ctx:         ctx,
		catalog:     catalog,
		collections: nil,
		segments: &SegmentsInfo{
			segments: map[UniqueID]*SegmentInfo{
				segID: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:            segID,
						CollectionID:  collID,
						PartitionID:   partID,
						InsertChannel: "",
						NumOfRows:     1026,
						State:         commonpb.SegmentState_Flushed,
					},
					segmentIndexes: map[UniqueID]*model.SegmentIndex{
						indexID: {
							SegmentID:     segID,
							CollectionID:  collID,
							PartitionID:   partID,
							NumRows:       1026,
							IndexID:       indexID,
							BuildID:       buildID,
							NodeID:        1,
							IndexVersion:  1,
							IndexState:    commonpb.IndexState_Finished,
							FailReason:    "",
							IsDeleted:     false,
							CreateTime:    10,
							IndexFileKeys: []string{"file1", "file2"},
							IndexSize:     0,
							WriteHandoff:  false,
						},
					},
				},
				segID + 1: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:            segID + 1,
						CollectionID:  collID,
						PartitionID:   partID,
						InsertChannel: "",
						NumOfRows:     1026,
						State:         commonpb.SegmentState_Flushed,
					},
					segmentIndexes: map[UniqueID]*model.SegmentIndex{
						indexID: {
							SegmentID:     segID + 1,
							CollectionID:  collID,
							PartitionID:   partID,
							NumRows:       1026,
							IndexID:       indexID,
							BuildID:       buildID + 1,
							NodeID:        1,
							IndexVersion:  1,
							IndexState:    commonpb.IndexState_InProgress,
							FailReason:    "",
							IsDeleted:     false,
							CreateTime:    10,
							IndexFileKeys: nil,
							IndexSize:     0,
							WriteHandoff:  false,
						},
					},
				},
			},
		},
		indexes: map[UniqueID]map[UniqueID]*model.Index{
			collID: {
				indexID: {
					TenantID:        "",
					CollectionID:    collID,
					FieldID:         fieldID,
					IndexID:         indexID,
					IndexName:       "_default_idx",
					IsDeleted:       false,
					CreateTime:      10,
					TypeParams:      nil,
					IndexParams:     nil,
					IsAutoIndex:     false,
					UserIndexParams: nil,
				},
			},
		},
		buildID2SegmentIndex: map[UniqueID]*model.SegmentIndex{
			buildID: {
				SegmentID:     segID,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       1026,
				IndexID:       indexID,
				BuildID:       buildID,
				NodeID:        1,
				IndexVersion:  1,
				IndexState:    commonpb.IndexState_Finished,
				FailReason:    "",
				IsDeleted:     false,
				CreateTime:    10,
				IndexFileKeys: []string{"file1", "file2"},
				IndexSize:     0,
				WriteHandoff:  false,
			},
			buildID + 1: {
				SegmentID:     segID + 1,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       1026,
				IndexID:       indexID,
				BuildID:       buildID + 1,
				NodeID:        1,
				IndexVersion:  1,
				IndexState:    commonpb.IndexState_InProgress,
				FailReason:    "",
				IsDeleted:     false,
				CreateTime:    10,
				IndexFileKeys: nil,
				IndexSize:     0,
				WriteHandoff:  false,
			},
		},
	}
}

func TestGarbageCollector_recycleUnusedIndexFiles(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		cm := &mocks.ChunkManager{}
		cm.EXPECT().RootPath().Return("root")
		cm.EXPECT().ListWithPrefix(mock.Anything, mock.Anything, mock.Anything).Return([]string{"a/b/c/", "a/b/600/", "a/b/601/", "a/b/602/"}, nil, nil)
		cm.EXPECT().RemoveWithPrefix(mock.Anything, mock.Anything).Return(nil)
		cm.EXPECT().Remove(mock.Anything, mock.Anything).Return(nil)
		gc := &garbageCollector{
			meta: createMetaTableForRecycleUnusedIndexFiles(&datacoord.Catalog{MetaKv: kvmocks.NewMetaKv(t)}),
			option: GcOption{
				cli: cm,
			},
		}
		gc.recycleUnusedIndexFiles()
	})

	t.Run("list fail", func(t *testing.T) {
		cm := &mocks.ChunkManager{}
		cm.EXPECT().RootPath().Return("root")
		cm.EXPECT().ListWithPrefix(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil, errors.New("error"))
		gc := &garbageCollector{
			meta: createMetaTableForRecycleUnusedIndexFiles(&datacoord.Catalog{MetaKv: kvmocks.NewMetaKv(t)}),
			option: GcOption{
				cli: cm,
			},
		}
		gc.recycleUnusedIndexFiles()
	})

	t.Run("remove fail", func(t *testing.T) {
		cm := &mocks.ChunkManager{}
		cm.EXPECT().RootPath().Return("root")
		cm.EXPECT().Remove(mock.Anything, mock.Anything).Return(errors.New("error"))
		cm.EXPECT().ListWithPrefix(mock.Anything, mock.Anything, mock.Anything).Return([]string{"a/b/c/", "a/b/600/", "a/b/601/", "a/b/602/"}, nil, nil)
		cm.EXPECT().RemoveWithPrefix(mock.Anything, mock.Anything).Return(nil)
		gc := &garbageCollector{
			meta: createMetaTableForRecycleUnusedIndexFiles(&datacoord.Catalog{MetaKv: kvmocks.NewMetaKv(t)}),
			option: GcOption{
				cli: cm,
			},
		}
		gc.recycleUnusedIndexFiles()
	})

	t.Run("remove with prefix fail", func(t *testing.T) {
		cm := &mocks.ChunkManager{}
		cm.EXPECT().RootPath().Return("root")
		cm.EXPECT().Remove(mock.Anything, mock.Anything).Return(errors.New("error"))
		cm.EXPECT().ListWithPrefix(mock.Anything, mock.Anything, mock.Anything).Return([]string{"a/b/c/", "a/b/600/", "a/b/601/", "a/b/602/"}, nil, nil)
		cm.EXPECT().RemoveWithPrefix(mock.Anything, mock.Anything).Return(errors.New("error"))
		gc := &garbageCollector{
			meta: createMetaTableForRecycleUnusedIndexFiles(&datacoord.Catalog{MetaKv: kvmocks.NewMetaKv(t)}),
			option: GcOption{
				cli: cm,
			},
		}
		gc.recycleUnusedIndexFiles()
	})
}

func TestGarbageCollector_clearETCD(t *testing.T) {
	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.On("ChannelExists",
		mock.Anything,
		mock.Anything,
	).Return(false)
	catalog.On("DropChannelCheckpoint",
		mock.Anything,
		mock.Anything,
	).Return(nil)
	catalog.On("CreateSegmentIndex",
		mock.Anything,
		mock.Anything,
	).Return(nil)
	catalog.On("AlterSegmentIndexes",
		mock.Anything,
		mock.Anything,
	).Return(nil)
	catalog.On("DropSegment",
		mock.Anything,
		mock.Anything,
	).Return(nil)

	m := &meta{
		catalog: catalog,
		channelCPs: map[string]*msgpb.MsgPosition{
			"dmlChannel": {
				Timestamp: 1000,
			},
		},
		segments: &SegmentsInfo{
			map[UniqueID]*SegmentInfo{
				segID: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:            segID,
						CollectionID:  collID,
						PartitionID:   partID,
						InsertChannel: "",
						NumOfRows:     5000,
						State:         commonpb.SegmentState_Dropped,
						MaxRowNum:     65536,
						DroppedAt:     0,
					},
					segmentIndexes: map[UniqueID]*model.SegmentIndex{
						indexID: {
							SegmentID:     segID,
							CollectionID:  collID,
							PartitionID:   partID,
							NumRows:       5000,
							IndexID:       indexID,
							BuildID:       buildID,
							NodeID:        0,
							IndexVersion:  1,
							IndexState:    commonpb.IndexState_Finished,
							FailReason:    "",
							IsDeleted:     false,
							CreateTime:    0,
							IndexFileKeys: []string{"file1", "file2"},
							IndexSize:     1024,
							WriteHandoff:  false,
						},
					},
				},
				segID + 1: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:            segID + 1,
						CollectionID:  collID,
						PartitionID:   partID,
						InsertChannel: "",
						NumOfRows:     5000,
						State:         commonpb.SegmentState_Dropped,
						MaxRowNum:     65536,
						DroppedAt:     0,
					},
					segmentIndexes: map[UniqueID]*model.SegmentIndex{
						indexID: {
							SegmentID:     segID + 1,
							CollectionID:  collID,
							PartitionID:   partID,
							NumRows:       5000,
							IndexID:       indexID,
							BuildID:       buildID + 1,
							NodeID:        0,
							IndexVersion:  1,
							IndexState:    commonpb.IndexState_Finished,
							FailReason:    "",
							IsDeleted:     false,
							CreateTime:    0,
							IndexFileKeys: []string{"file3", "file4"},
							IndexSize:     1024,
							WriteHandoff:  false,
						},
					},
				},
				segID + 2: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID + 2,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "",
						NumOfRows:      10000,
						State:          commonpb.SegmentState_Dropped,
						MaxRowNum:      65536,
						DroppedAt:      10,
						CompactionFrom: []int64{segID, segID + 1},
					},
					segmentIndexes: map[UniqueID]*model.SegmentIndex{},
				},
				segID + 3: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID + 3,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "",
						NumOfRows:      2000,
						State:          commonpb.SegmentState_Dropped,
						MaxRowNum:      65536,
						DroppedAt:      10,
						CompactionFrom: nil,
					},
					segmentIndexes: map[UniqueID]*model.SegmentIndex{},
				},
				segID + 4: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID + 4,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "",
						NumOfRows:      12000,
						State:          commonpb.SegmentState_Flushed,
						MaxRowNum:      65536,
						DroppedAt:      10,
						CompactionFrom: []int64{segID + 2, segID + 3},
					},
					segmentIndexes: map[UniqueID]*model.SegmentIndex{},
				},
				// before channel cp,
				segID + 5: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID + 5,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "dmlChannel",
						NumOfRows:      2000,
						State:          commonpb.SegmentState_Dropped,
						MaxRowNum:      65535,
						DroppedAt:      0,
						CompactionFrom: nil,
						DmlPosition: &msgpb.MsgPosition{
							Timestamp: 1200,
						},
					},
				},
			},
		},
		buildID2SegmentIndex: map[UniqueID]*model.SegmentIndex{
			buildID: {
				SegmentID:     segID,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       5000,
				IndexID:       indexID,
				BuildID:       buildID,
				NodeID:        0,
				IndexVersion:  1,
				IndexState:    commonpb.IndexState_Finished,
				FailReason:    "",
				IsDeleted:     false,
				CreateTime:    0,
				IndexFileKeys: []string{"file1", "file2"},
				IndexSize:     1024,
				WriteHandoff:  false,
			},
			buildID + 1: {
				SegmentID:     segID + 1,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       5000,
				IndexID:       indexID,
				BuildID:       buildID + 1,
				NodeID:        0,
				IndexVersion:  1,
				IndexState:    commonpb.IndexState_Finished,
				FailReason:    "",
				IsDeleted:     false,
				CreateTime:    0,
				IndexFileKeys: []string{"file3", "file4"},
				IndexSize:     1024,
				WriteHandoff:  false,
			},
		},
		indexes: map[UniqueID]map[UniqueID]*model.Index{
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
			},
		},
		collections: map[UniqueID]*collectionInfo{
			collID: {
				ID: collID,
				Schema: &schemapb.CollectionSchema{
					Name:        "",
					Description: "",
					AutoID:      false,
					Fields: []*schemapb.FieldSchema{
						{
							FieldID:      fieldID,
							Name:         "",
							IsPrimaryKey: false,
							Description:  "",
							DataType:     schemapb.DataType_FloatVector,
							TypeParams:   nil,
							IndexParams:  nil,
							AutoID:       false,
							State:        0,
						},
					},
				},
				Partitions:     nil,
				StartPositions: nil,
				Properties:     nil,
			},
		},
	}
	cm := &mocks.ChunkManager{}
	cm.EXPECT().Remove(mock.Anything, mock.Anything).Return(nil)
	gc := &garbageCollector{
		option: GcOption{
			cli:           &mocks.ChunkManager{},
			dropTolerance: 1,
		},
		meta:    m,
		handler: newMockHandlerWithMeta(m),
	}
	gc.clearEtcd()

	segA := gc.meta.GetSegment(segID)
	assert.NotNil(t, segA)
	segB := gc.meta.GetSegment(segID + 1)
	assert.NotNil(t, segB)
	segC := gc.meta.GetSegment(segID + 2)
	assert.NotNil(t, segC)
	segD := gc.meta.GetSegment(segID + 3)
	assert.NotNil(t, segD)
	segE := gc.meta.GetSegment(segID + 4)
	assert.NotNil(t, segE)
	segF := gc.meta.GetSegment(segID + 5)
	assert.Nil(t, segF)

	err := gc.meta.AddSegmentIndex(&model.SegmentIndex{
		SegmentID:    segID + 4,
		CollectionID: collID,
		PartitionID:  partID,
		NumRows:      12000,
		IndexID:      indexID,
		BuildID:      buildID + 4,
	})
	assert.NoError(t, err)

	err = gc.meta.FinishTask(&indexpb.IndexTaskInfo{
		BuildID:        buildID + 4,
		State:          commonpb.IndexState_Finished,
		IndexFileKeys:  []string{"file1", "file2", "file3", "file4"},
		SerializedSize: 10240,
		FailReason:     "",
	})
	assert.NoError(t, err)

	gc.clearEtcd()
	//segA := gc.meta.GetSegmentUnsafe(segID)
	//assert.NotNil(t, segA)
	//segB := gc.meta.GetSegmentUnsafe(segID + 1)
	//assert.NotNil(t, segB)
	segC = gc.meta.GetSegment(segID + 2)
	assert.Nil(t, segC)
	segD = gc.meta.GetSegment(segID + 3)
	assert.Nil(t, segD)
	segE = gc.meta.GetSegment(segID + 4)
	assert.NotNil(t, segE)
	segF = gc.meta.GetSegment(segID + 5)
	assert.Nil(t, segF)

	gc.clearEtcd()
	segA = gc.meta.GetSegment(segID)
	assert.Nil(t, segA)
	segB = gc.meta.GetSegment(segID + 1)
	assert.Nil(t, segB)
	segF = gc.meta.GetSegment(segID + 5)
	assert.Nil(t, segF)

}
