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
	"fmt"
	"math/rand"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	broker2 "github.com/milvus-io/milvus/internal/datacoord/broker"
	kvmocks "github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/objectstorage"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/lock"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func Test_garbageCollector_basic(t *testing.T) {
	bucketName := `datacoord-ut` + strings.ToLower(funcutil.RandomString(8))
	rootPath := `gc` + funcutil.RandomString(8)
	// TODO change to Params
	cli, _, _, _, _, err := initUtOSSEnv(bucketName, rootPath, 0)
	require.NoError(t, err)

	meta, err := newMemoryMeta(t)
	assert.NoError(t, err)

	t.Run("normal gc", func(t *testing.T) {
		gc := newGarbageCollector(meta, newMockHandler(), GcOption{
			cli:              cli,
			enabled:          true,
			checkInterval:    time.Millisecond * 10,
			scanInterval:     time.Hour * 7 * 24,
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
			scanInterval:     time.Hour * 7 * 24,
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

func validateMinioPrefixElements(t *testing.T, manager *storage.RemoteChunkManager, bucketName string, prefix string, elements []string) {
	cli := manager.UnderlyingObjectStorage().(*storage.MinioObjectStorage).Client
	var current []string
	for info := range cli.ListObjects(context.TODO(), bucketName, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		current = append(current, info.Key)
	}
	assert.ElementsMatch(t, elements, current)
}

func Test_garbageCollector_scan(t *testing.T) {
	bucketName := `datacoord-ut` + strings.ToLower(funcutil.RandomString(8))
	rootPath := paramtable.Get().MinioCfg.RootPath.GetValue()
	// TODO change to Params
	cli, inserts, stats, delta, others, err := initUtOSSEnv(bucketName, rootPath, 4)
	require.NoError(t, err)

	meta, err := newMemoryMeta(t)
	assert.NoError(t, err)

	t.Run("key is reference", func(t *testing.T) {
		gc := newGarbageCollector(meta, newMockHandler(), GcOption{
			cli:              cli,
			enabled:          true,
			checkInterval:    time.Minute * 30,
			scanInterval:     time.Hour * 7 * 24,
			missingTolerance: time.Hour * 24,
			dropTolerance:    time.Hour * 24,
		})
		gc.recycleUnusedBinlogFiles(context.TODO())

		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, common.SegmentInsertLogPath), inserts)
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, common.SegmentStatslogPath), stats)
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, common.SegmentDeltaLogPath), delta)
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, `indexes`), others)
		gc.close()
	})

	t.Run("missing all but save tolerance", func(t *testing.T) {
		gc := newGarbageCollector(meta, newMockHandler(), GcOption{
			cli:              cli,
			enabled:          true,
			checkInterval:    time.Minute * 30,
			scanInterval:     time.Hour * 7 * 24,
			missingTolerance: time.Hour * 24,
			dropTolerance:    time.Hour * 24,
		})
		gc.recycleUnusedBinlogFiles(context.TODO())

		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, common.SegmentInsertLogPath), inserts)
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, common.SegmentStatslogPath), stats)
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, common.SegmentDeltaLogPath), delta)
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, `indexes`), others)

		gc.close()
	})
	t.Run("hit, no gc", func(t *testing.T) {
		segment := buildSegment(1, 10, 100, "ch")
		segment.State = commonpb.SegmentState_Flushed
		segment.Binlogs = []*datapb.FieldBinlog{getFieldBinlogPaths(0, inserts[0])}
		segment.Statslogs = []*datapb.FieldBinlog{getFieldBinlogPaths(0, stats[0])}
		segment.Deltalogs = []*datapb.FieldBinlog{getFieldBinlogPaths(0, delta[0])}
		err = meta.AddSegment(context.TODO(), segment)
		require.NoError(t, err)

		gc := newGarbageCollector(meta, newMockHandler(), GcOption{
			cli:              cli,
			enabled:          true,
			checkInterval:    time.Minute * 30,
			scanInterval:     time.Hour * 7 * 24,
			missingTolerance: time.Hour * 24,
			dropTolerance:    time.Hour * 24,
		})
		gc.start()
		gc.recycleUnusedBinlogFiles(context.TODO())
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, common.SegmentInsertLogPath), inserts)
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, common.SegmentStatslogPath), stats)
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, common.SegmentDeltaLogPath), delta)
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, `indexes`), others)

		gc.close()
	})

	t.Run("dropped gc one", func(t *testing.T) {
		segment := buildSegment(1, 10, 100, "ch")
		segment.State = commonpb.SegmentState_Dropped
		segment.DroppedAt = uint64(time.Now().Add(-time.Hour).UnixNano())
		segment.Binlogs = []*datapb.FieldBinlog{getFieldBinlogPaths(0, inserts[0])}
		segment.Statslogs = []*datapb.FieldBinlog{getFieldBinlogPaths(0, stats[0])}
		segment.Deltalogs = []*datapb.FieldBinlog{getFieldBinlogPaths(0, delta[0])}

		meta.DropSegment(context.TODO(), segment.ID)
		err = meta.AddSegment(context.TODO(), segment)
		require.NoError(t, err)

		gc := newGarbageCollector(meta, newMockHandler(), GcOption{
			cli:              cli,
			enabled:          true,
			checkInterval:    time.Minute * 30,
			scanInterval:     time.Hour * 7 * 24,
			missingTolerance: time.Hour * 24,
			dropTolerance:    0,
		})
		signal := make(chan gcCmd)
		gc.recycleDroppedSegments(context.TODO(), signal)
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, common.SegmentInsertLogPath), inserts[1:])
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, common.SegmentStatslogPath), stats[1:])
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, common.SegmentDeltaLogPath), delta[1:])
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, `indexes`), others)

		gc.close()
	})
	t.Run("missing gc all", func(t *testing.T) {
		gc := newGarbageCollector(meta, newMockHandler(), GcOption{
			cli:              cli,
			enabled:          true,
			checkInterval:    time.Minute * 30,
			scanInterval:     time.Hour * 7 * 24,
			missingTolerance: 0,
			dropTolerance:    0,
		})
		gc.start()
		gc.recycleUnusedBinlogFiles(context.TODO())
		signal := make(chan gcCmd)
		gc.recycleDroppedSegments(context.TODO(), signal)

		// bad path shall remains since datacoord cannot determine file is garbage or not if path is not valid
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, common.SegmentInsertLogPath), inserts[1:2])
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, common.SegmentStatslogPath), stats[1:2])
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, common.SegmentDeltaLogPath), delta[1:2])
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, `indexes`), others)

		gc.close()
	})

	t.Run("list object with error", func(t *testing.T) {
		gc := newGarbageCollector(meta, newMockHandler(), GcOption{
			cli:              cli,
			enabled:          true,
			checkInterval:    time.Minute * 30,
			scanInterval:     time.Hour * 7 * 24,
			missingTolerance: 0,
			dropTolerance:    0,
		})
		gc.start()
		gc.recycleUnusedBinlogFiles(context.TODO())

		// bad path shall remains since datacoord cannot determine file is garbage or not if path is not valid
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, common.SegmentInsertLogPath), inserts[1:2])
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, common.SegmentStatslogPath), stats[1:2])
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, common.SegmentDeltaLogPath), delta[1:2])
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, `indexes`), others)

		gc.close()
	})

	cleanupOSS(cli, bucketName, rootPath)
}

// initialize unit test sso env
func initUtOSSEnv(bucket, root string, n int) (mcm *storage.RemoteChunkManager, inserts []string, stats []string, delta []string, other []string, err error) {
	paramtable.Init()

	if Params.MinioCfg.UseSSL.GetAsBool() && len(Params.MinioCfg.SslCACert.GetValue()) > 0 {
		err := os.Setenv("SSL_CERT_FILE", Params.MinioCfg.SslCACert.GetValue())
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
	}

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
			token = path.Join(strconv.Itoa(i), strconv.Itoa(i), "error-seg-id", strconv.Itoa(i), fmt.Sprint(rand.Int63()))
		} else {
			token = path.Join(strconv.Itoa(1+i), strconv.Itoa(10+i), strconv.Itoa(100+i), strconv.Itoa(i), fmt.Sprint(rand.Int63()))
		}
		// insert
		filePath := path.Join(root, common.SegmentInsertLogPath, token)
		info, err := cli.PutObject(context.TODO(), bucket, filePath, reader, int64(len(content)), minio.PutObjectOptions{})
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
		inserts = append(inserts, info.Key)
		// stats
		filePath = path.Join(root, common.SegmentStatslogPath, token)
		info, err = cli.PutObject(context.TODO(), bucket, filePath, reader, int64(len(content)), minio.PutObjectOptions{})
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
		stats = append(stats, info.Key)

		// delta
		if i == 1 {
			token = path.Join(strconv.Itoa(i), strconv.Itoa(i), "error-seg-id", fmt.Sprint(rand.Int63()))
		} else {
			token = path.Join(strconv.Itoa(1+i), strconv.Itoa(10+i), strconv.Itoa(100+i), fmt.Sprint(rand.Int63()))
		}
		filePath = path.Join(root, common.SegmentDeltaLogPath, token)
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
	mcm = storage.NewRemoteChunkManagerForTesting(
		cli,
		bucket,
		root,
	)
	return mcm, inserts, stats, delta, other, nil
}

func cleanupOSS(chunkManager *storage.RemoteChunkManager, bucket, root string) {
	cli := chunkManager.UnderlyingObjectStorage().(*storage.MinioObjectStorage).Client
	ch := cli.ListObjects(context.TODO(), bucket, minio.ListObjectsOptions{Prefix: root, Recursive: true})
	cli.RemoveObjects(context.TODO(), bucket, ch, minio.RemoveObjectsOptions{})
	cli.RemoveBucket(context.TODO(), bucket)
}

func createMetaForRecycleUnusedIndexes(catalog metastore.DataCoordCatalog) *meta {
	var (
		ctx    = context.Background()
		collID = UniqueID(100)
		// partID = UniqueID(200)
		fieldID = UniqueID(300)
		indexID = UniqueID(400)
	)
	return &meta{
		ctx:          ctx,
		catalog:      catalog,
		collections:  nil,
		segments:     nil,
		channelCPs:   newChannelCps(),
		chunkManager: nil,
		indexMeta: &indexMeta{
			catalog: catalog,
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
			segmentBuildInfo: newSegmentIndexBuildInfo(),
		},
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
		gc := newGarbageCollector(createMetaForRecycleUnusedIndexes(catalog), nil, GcOption{})
		gc.recycleUnusedIndexes(context.TODO(), nil)
	})

	t.Run("fail", func(t *testing.T) {
		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.On("DropIndex",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(errors.New("fail"))
		gc := newGarbageCollector(createMetaForRecycleUnusedIndexes(catalog), nil, GcOption{})
		gc.recycleUnusedIndexes(context.TODO(), nil)
	})
}

func createMetaForRecycleUnusedSegIndexes(catalog metastore.DataCoordCatalog) *meta {
	var (
		ctx    = context.Background()
		collID = UniqueID(100)
		partID = UniqueID(200)
		// fieldID = UniqueID(300)
		indexID = UniqueID(400)
		segID   = UniqueID(500)
	)
	segments := map[int64]*SegmentInfo{
		segID: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:            segID,
				CollectionID:  collID,
				PartitionID:   partID,
				InsertChannel: "",
				NumOfRows:     1026,
				State:         commonpb.SegmentState_Flushed,
			},
		},
		segID + 1: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:            segID + 1,
				CollectionID:  collID,
				PartitionID:   partID,
				InsertChannel: "",
				NumOfRows:     1026,
				State:         commonpb.SegmentState_Dropped,
			},
		},
	}
	segIndexes := typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]]()
	segIdx0 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx0.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID,
		NodeID:              1,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      10,
		IndexFileKeys:       []string{"file1", "file2"},
		IndexSerializedSize: 0,
		WriteHandoff:        false,
	})
	segIdx1 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx1.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID + 1,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 1,
		NodeID:              1,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      10,
		IndexFileKeys:       []string{"file1", "file2"},
		IndexSerializedSize: 0,
		WriteHandoff:        false,
	})
	segIndexes.Insert(segID, segIdx0)
	segIndexes.Insert(segID+1, segIdx1)
	meta := &meta{
		ctx:         ctx,
		catalog:     catalog,
		collections: nil,
		segments:    NewSegmentsInfo(),
		indexMeta: &indexMeta{
			catalog:          catalog,
			segmentIndexes:   segIndexes,
			indexes:          map[UniqueID]map[UniqueID]*model.Index{},
			segmentBuildInfo: newSegmentIndexBuildInfo(),
			keyLock:          lock.NewKeyLock[UniqueID](),
		},
		channelCPs:   nil,
		chunkManager: nil,
	}

	meta.indexMeta.segmentBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID,
		NodeID:              1,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      10,
		IndexFileKeys:       []string{"file1", "file2"},
		IndexSerializedSize: 0,
	})

	meta.indexMeta.segmentBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID + 1,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 1,
		NodeID:              1,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      10,
		IndexFileKeys:       []string{"file1", "file2"},
		IndexSerializedSize: 0,
	})

	for id, segment := range segments {
		meta.segments.SetSegment(id, segment)
	}
	return meta
}

func TestGarbageCollector_recycleUnusedSegIndexes(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mockChunkManager := mocks.NewChunkManager(t)
		mockChunkManager.EXPECT().RootPath().Return("root")
		mockChunkManager.EXPECT().Remove(mock.Anything, mock.Anything).Return(nil)
		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.On("DropSegmentIndex",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)
		gc := newGarbageCollector(createMetaForRecycleUnusedSegIndexes(catalog), nil, GcOption{
			cli: mockChunkManager,
		})
		gc.recycleUnusedSegIndexes(context.TODO(), nil)
	})

	t.Run("fail", func(t *testing.T) {
		catalog := catalogmocks.NewDataCoordCatalog(t)
		mockChunkManager := mocks.NewChunkManager(t)
		mockChunkManager.EXPECT().RootPath().Return("root")
		mockChunkManager.EXPECT().Remove(mock.Anything, mock.Anything).Return(nil)
		catalog.On("DropSegmentIndex",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(errors.New("fail"))
		gc := newGarbageCollector(createMetaForRecycleUnusedSegIndexes(catalog), nil, GcOption{
			cli: mockChunkManager,
		})
		gc.recycleUnusedSegIndexes(context.TODO(), nil)
	})
}

func createMetaTableForRecycleUnusedIndexFiles(catalog *datacoord.Catalog) *meta {
	var (
		ctx    = context.Background()
		collID = UniqueID(100)
		partID = UniqueID(200)
		// fieldID = UniqueID(300)
		indexID = UniqueID(400)
		segID   = UniqueID(500)
		buildID = UniqueID(600)
	)
	segments := map[UniqueID]*SegmentInfo{
		segID: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:            segID,
				CollectionID:  collID,
				PartitionID:   partID,
				InsertChannel: "",
				NumOfRows:     1026,
				State:         commonpb.SegmentState_Flushed,
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
		},
	}
	segIndexes := typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]]()
	segIdx0 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx0.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID,
		NodeID:              1,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      10,
		IndexFileKeys:       []string{"file1", "file2"},
		IndexSerializedSize: 0,
		WriteHandoff:        false,
	})
	segIdx1 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx1.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID + 1,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 1,
		NodeID:              1,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_InProgress,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      10,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
		WriteHandoff:        false,
	})
	segIndexes.Insert(segID, segIdx0)
	segIndexes.Insert(segID+1, segIdx1)
	meta := &meta{
		ctx:         ctx,
		catalog:     catalog,
		collections: nil,
		segments:    NewSegmentsInfo(),
		indexMeta: &indexMeta{
			catalog:        catalog,
			segmentIndexes: segIndexes,
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
			segmentBuildInfo: newSegmentIndexBuildInfo(),
		},
	}
	meta.indexMeta.segmentBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID,
		NodeID:              1,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      10,
		IndexFileKeys:       []string{"file1", "file2"},
		IndexSerializedSize: 0,
		WriteHandoff:        false,
	})
	meta.indexMeta.segmentBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID + 1,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 1,
		NodeID:              1,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_InProgress,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      10,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
		WriteHandoff:        false,
	})
	for id, segment := range segments {
		meta.segments.SetSegment(id, segment)
	}

	return meta
}

func TestGarbageCollector_recycleUnusedIndexFiles(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		cm := &mocks.ChunkManager{}
		cm.EXPECT().RootPath().Return("root")
		cm.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, s string, b bool, cowf storage.ChunkObjectWalkFunc) error {
				for _, file := range []string{"a/b/c/", "a/b/600/", "a/b/601/", "a/b/602/"} {
					cowf(&storage.ChunkObjectInfo{FilePath: file})
				}
				return nil
			})

		cm.EXPECT().RemoveWithPrefix(mock.Anything, mock.Anything).Return(nil)
		cm.EXPECT().Remove(mock.Anything, mock.Anything).Return(nil)
		gc := newGarbageCollector(
			createMetaTableForRecycleUnusedIndexFiles(&datacoord.Catalog{MetaKv: kvmocks.NewMetaKv(t)}),
			nil,
			GcOption{
				cli: cm,
			})

		gc.recycleUnusedIndexFiles(context.TODO())
	})

	t.Run("list fail", func(t *testing.T) {
		cm := &mocks.ChunkManager{}
		cm.EXPECT().RootPath().Return("root")
		cm.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, s string, b bool, cowf storage.ChunkObjectWalkFunc) error {
				return errors.New("error")
			})
		gc := newGarbageCollector(
			createMetaTableForRecycleUnusedIndexFiles(&datacoord.Catalog{MetaKv: kvmocks.NewMetaKv(t)}),
			nil,
			GcOption{
				cli: cm,
			})
		gc.recycleUnusedIndexFiles(context.TODO())
	})

	t.Run("remove fail", func(t *testing.T) {
		cm := &mocks.ChunkManager{}
		cm.EXPECT().RootPath().Return("root")
		cm.EXPECT().Remove(mock.Anything, mock.Anything).Return(errors.New("error"))
		cm.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, s string, b bool, cowf storage.ChunkObjectWalkFunc) error {
				for _, file := range []string{"a/b/c/", "a/b/600/", "a/b/601/", "a/b/602/"} {
					cowf(&storage.ChunkObjectInfo{FilePath: file})
				}
				return nil
			})
		cm.EXPECT().RemoveWithPrefix(mock.Anything, mock.Anything).Return(nil)
		gc := newGarbageCollector(
			createMetaTableForRecycleUnusedIndexFiles(&datacoord.Catalog{MetaKv: kvmocks.NewMetaKv(t)}),
			nil,
			GcOption{
				cli: cm,
			})
		gc.recycleUnusedIndexFiles(context.TODO())
	})

	t.Run("remove with prefix fail", func(t *testing.T) {
		cm := &mocks.ChunkManager{}
		cm.EXPECT().RootPath().Return("root")
		cm.EXPECT().Remove(mock.Anything, mock.Anything).Return(errors.New("error"))
		cm.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, s string, b bool, cowf storage.ChunkObjectWalkFunc) error {
				for _, file := range []string{"a/b/c/", "a/b/600/", "a/b/601/", "a/b/602/"} {
					cowf(&storage.ChunkObjectInfo{FilePath: file})
				}
				return nil
			})
		cm.EXPECT().RemoveWithPrefix(mock.Anything, mock.Anything).Return(errors.New("error"))
		gc := newGarbageCollector(
			createMetaTableForRecycleUnusedIndexFiles(&datacoord.Catalog{MetaKv: kvmocks.NewMetaKv(t)}),
			nil,
			GcOption{
				cli: cm,
			})
		gc.recycleUnusedIndexFiles(context.TODO())
	})
}

func TestGarbageCollector_clearETCD(t *testing.T) {
	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.On("ChannelExists",
		mock.Anything,
		mock.Anything,
	).Return(true)
	catalog.On("DropChannelCheckpoint",
		mock.Anything,
		mock.Anything,
	).Return(nil).Maybe()
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
	catalog.On("DropSegmentIndex",
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
	).Return(nil).Maybe()

	channelCPs := newChannelCps()
	channelCPs.checkpoints["dmlChannel"] = &msgpb.MsgPosition{
		Timestamp: 1000,
	}

	segments := map[UniqueID]*SegmentInfo{
		segID: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:            segID,
				CollectionID:  collID,
				PartitionID:   partID,
				InsertChannel: "dmlChannel",
				NumOfRows:     5000,
				State:         commonpb.SegmentState_Dropped,
				MaxRowNum:     65536,
				DroppedAt:     0,
				DmlPosition: &msgpb.MsgPosition{
					Timestamp: 900,
				},
				Binlogs: []*datapb.FieldBinlog{
					{
						FieldID: 1,
						Binlogs: []*datapb.Binlog{
							{
								LogPath: "log1",
								LogSize: 1024,
							},
						},
					},
					{
						FieldID: 2,
						Binlogs: []*datapb.Binlog{
							{
								LogPath: "log2",
								LogSize: 1024,
							},
						},
					},
				},
				Deltalogs: []*datapb.FieldBinlog{
					{
						FieldID: 1,
						Binlogs: []*datapb.Binlog{
							{
								LogPath: "del_log1",
								LogSize: 1024,
							},
						},
					},
					{
						FieldID: 2,
						Binlogs: []*datapb.Binlog{
							{
								LogPath: "del_log2",
								LogSize: 1024,
							},
						},
					},
				},
				Statslogs: []*datapb.FieldBinlog{
					{
						FieldID: 1,
						Binlogs: []*datapb.Binlog{
							{
								LogPath: "stats_log1",
								LogSize: 1024,
							},
						},
					},
				},
			},
		},
		segID + 1: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:            segID + 1,
				CollectionID:  collID,
				PartitionID:   partID,
				InsertChannel: "dmlChannel",
				NumOfRows:     5000,
				State:         commonpb.SegmentState_Dropped,
				MaxRowNum:     65536,
				DroppedAt:     0,
				DmlPosition: &msgpb.MsgPosition{
					Timestamp: 900,
				},
			},
		},
		segID + 2: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:            segID + 2,
				CollectionID:  collID,
				PartitionID:   partID,
				InsertChannel: "dmlChannel",
				NumOfRows:     10000,
				State:         commonpb.SegmentState_Dropped,
				MaxRowNum:     65536,
				DroppedAt:     10,
				DmlPosition: &msgpb.MsgPosition{
					Timestamp: 900,
				},
				CompactionFrom: []int64{segID, segID + 1},
			},
		},
		segID + 3: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:            segID + 3,
				CollectionID:  collID,
				PartitionID:   partID,
				InsertChannel: "dmlChannel",
				NumOfRows:     2000,
				State:         commonpb.SegmentState_Dropped,
				MaxRowNum:     65536,
				DroppedAt:     10,
				DmlPosition: &msgpb.MsgPosition{
					Timestamp: 900,
				},
				CompactionFrom: nil,
			},
		},
		segID + 4: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:            segID + 4,
				CollectionID:  collID,
				PartitionID:   partID,
				InsertChannel: "dmlChannel",
				NumOfRows:     12000,
				State:         commonpb.SegmentState_Flushed,
				MaxRowNum:     65536,
				DroppedAt:     10,
				DmlPosition: &msgpb.MsgPosition{
					Timestamp: 900,
				},
				CompactionFrom: []int64{segID + 2, segID + 3},
			},
		},
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
		segID + 6: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:             segID + 6,
				CollectionID:   collID,
				PartitionID:    partID,
				InsertChannel:  "dmlChannel",
				NumOfRows:      2000,
				State:          commonpb.SegmentState_Dropped,
				MaxRowNum:      65535,
				DroppedAt:      uint64(time.Now().Add(time.Hour).UnixNano()),
				CompactionFrom: nil,
				DmlPosition: &msgpb.MsgPosition{
					Timestamp: 900,
				},
				Compacted: true,
			},
		},
		// compacted and child is GCed, dml pos is big than channel cp
		segID + 7: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:             segID + 7,
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
				Compacted: true,
			},
		},
	}

	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(collID, &collectionInfo{
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
	})

	segIndexes := typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]]()
	segIdx0 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx0.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             5000,
		IndexID:             indexID,
		BuildID:             buildID,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      0,
		IndexFileKeys:       []string{"file1", "file2"},
		IndexSerializedSize: 1024,
		WriteHandoff:        false,
	})
	segIdx1 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx1.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID + 1,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             5000,
		IndexID:             indexID,
		BuildID:             buildID + 1,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      0,
		IndexFileKeys:       []string{"file3", "file4"},
		IndexSerializedSize: 1024,
		WriteHandoff:        false,
	})
	segIndexes.Insert(segID, segIdx0)
	segIndexes.Insert(segID+1, segIdx1)
	m := &meta{
		catalog:    catalog,
		channelCPs: channelCPs,
		segments:   NewSegmentsInfo(),
		indexMeta: &indexMeta{
			keyLock:          lock.NewKeyLock[UniqueID](),
			catalog:          catalog,
			segmentIndexes:   segIndexes,
			segmentBuildInfo: newSegmentIndexBuildInfo(),
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
		},

		collections: collections,
	}

	m.indexMeta.segmentBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             5000,
		IndexID:             indexID,
		BuildID:             buildID,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      0,
		IndexFileKeys:       []string{"file1", "file2"},
		IndexSerializedSize: 1024,
		WriteHandoff:        false,
	})

	m.indexMeta.segmentBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID + 1,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             5000,
		IndexID:             indexID,
		BuildID:             buildID + 1,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      0,
		IndexFileKeys:       []string{"file3", "file4"},
		IndexSerializedSize: 1024,
		WriteHandoff:        false,
	})

	for id, segment := range segments {
		m.segments.SetSegment(id, segment)
	}

	for segID, segment := range map[UniqueID]*SegmentInfo{
		segID: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:            segID,
				CollectionID:  collID,
				PartitionID:   partID,
				InsertChannel: "dmlChannel",
				NumOfRows:     5000,
				State:         commonpb.SegmentState_Dropped,
				MaxRowNum:     65536,
				DroppedAt:     0,
				DmlPosition: &msgpb.MsgPosition{
					Timestamp: 900,
				},
				Binlogs: []*datapb.FieldBinlog{
					{
						FieldID: 1,
						Binlogs: []*datapb.Binlog{
							{
								LogPath: "log1",
								LogSize: 1024,
							},
						},
					},
					{
						FieldID: 2,
						Binlogs: []*datapb.Binlog{
							{
								LogPath: "log2",
								LogSize: 1024,
							},
						},
					},
				},
				Deltalogs: []*datapb.FieldBinlog{
					{
						FieldID: 1,
						Binlogs: []*datapb.Binlog{
							{
								LogPath: "del_log1",
								LogSize: 1024,
							},
						},
					},
					{
						FieldID: 2,
						Binlogs: []*datapb.Binlog{
							{
								LogPath: "del_log2",
								LogSize: 1024,
							},
						},
					},
				},
				Statslogs: []*datapb.FieldBinlog{
					{
						FieldID: 1,
						Binlogs: []*datapb.Binlog{
							{
								LogPath: "stats_log1",
								LogSize: 1024,
							},
						},
					},
				},
			},
		},
		segID + 1: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:            segID + 1,
				CollectionID:  collID,
				PartitionID:   partID,
				InsertChannel: "dmlChannel",
				NumOfRows:     5000,
				State:         commonpb.SegmentState_Dropped,
				MaxRowNum:     65536,
				DroppedAt:     0,
				DmlPosition: &msgpb.MsgPosition{
					Timestamp: 900,
				},
			},
		},
		segID + 2: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:            segID + 2,
				CollectionID:  collID,
				PartitionID:   partID,
				InsertChannel: "dmlChannel",
				NumOfRows:     10000,
				State:         commonpb.SegmentState_Dropped,
				MaxRowNum:     65536,
				DroppedAt:     10,
				DmlPosition: &msgpb.MsgPosition{
					Timestamp: 900,
				},
				CompactionFrom: []int64{segID, segID + 1},
			},
		},
		segID + 3: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:            segID + 3,
				CollectionID:  collID,
				PartitionID:   partID,
				InsertChannel: "dmlChannel",
				NumOfRows:     2000,
				State:         commonpb.SegmentState_Dropped,
				MaxRowNum:     65536,
				DroppedAt:     10,
				DmlPosition: &msgpb.MsgPosition{
					Timestamp: 900,
				},
				CompactionFrom: nil,
			},
		},
		segID + 4: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:            segID + 4,
				CollectionID:  collID,
				PartitionID:   partID,
				InsertChannel: "dmlChannel",
				NumOfRows:     12000,
				State:         commonpb.SegmentState_Flushed,
				MaxRowNum:     65536,
				DroppedAt:     10,
				DmlPosition: &msgpb.MsgPosition{
					Timestamp: 900,
				},
				CompactionFrom: []int64{segID + 2, segID + 3},
			},
		},
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
		// cannot dropped for not expired.
		segID + 6: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:             segID + 6,
				CollectionID:   collID,
				PartitionID:    partID,
				InsertChannel:  "dmlChannel",
				NumOfRows:      2000,
				State:          commonpb.SegmentState_Dropped,
				MaxRowNum:      65535,
				DroppedAt:      uint64(time.Now().Add(time.Hour).UnixNano()),
				CompactionFrom: nil,
				DmlPosition: &msgpb.MsgPosition{
					Timestamp: 900,
				},
				Compacted: true,
			},
		},
		// compacted and child is GCed, dml pos is big than channel cp
		segID + 7: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:             segID + 7,
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
				Compacted: true,
			},
		},
		// can be dropped for expired and compacted
		segID + 8: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:             segID + 8,
				CollectionID:   collID,
				PartitionID:    partID,
				InsertChannel:  "dmlChannel",
				NumOfRows:      2000,
				State:          commonpb.SegmentState_Dropped,
				MaxRowNum:      65535,
				DroppedAt:      uint64(time.Now().Add(-7 * 24 * time.Hour).UnixNano()),
				CompactionFrom: nil,
				DmlPosition: &msgpb.MsgPosition{
					Timestamp: 900,
				},
				Compacted: true,
			},
		},
	} {
		m.segments.SetSegment(segID, segment)
	}

	cm := &mocks.ChunkManager{}
	cm.EXPECT().Remove(mock.Anything, mock.Anything).Return(nil)
	cm.EXPECT().RootPath().Return("").Maybe()
	signal := make(chan gcCmd)
	gc := newGarbageCollector(
		m,
		newMockHandlerWithMeta(m),
		GcOption{
			cli:           cm,
			dropTolerance: 1,
		})
	gc.recycleDroppedSegments(context.TODO(), signal)

	/*
		A    B
		 \   /
		   C	D
		    \  /
		      E

		E: flushed, not indexed, should not be GCed
		D: dropped, not indexed, should not be GCed, since E is not GCed
		C: dropped, not indexed, should not be GCed, since E is not GCed
		A: dropped, indexed, should not be GCed, since C is not indexed
		B: dropped, indexed, should not be GCed, since C is not indexed

		F: dropped, compcated is false, should not be GCed, since dml position is larger than channel cp
		G: dropped, compacted is true, missing child info, should be GCed since dml pos is less than channel cp, FAST GC do not wait drop tolerance
		H: dropped, compacted is true, missing child info, should not be GCed since dml pos is larger than channel cp

		conclusion: only G is GCed.
	*/
	segA := gc.meta.GetSegment(context.TODO(), segID)
	assert.NotNil(t, segA)
	segB := gc.meta.GetSegment(context.TODO(), segID+1)
	assert.NotNil(t, segB)
	segC := gc.meta.GetSegment(context.TODO(), segID+2)
	assert.NotNil(t, segC)
	segD := gc.meta.GetSegment(context.TODO(), segID+3)
	assert.NotNil(t, segD)
	segE := gc.meta.GetSegment(context.TODO(), segID+4)
	assert.NotNil(t, segE)
	segF := gc.meta.GetSegment(context.TODO(), segID+5)
	assert.NotNil(t, segF)
	segG := gc.meta.GetSegment(context.TODO(), segID+6)
	assert.NotNil(t, segG)
	segH := gc.meta.GetSegment(context.TODO(), segID+7)
	assert.NotNil(t, segH)
	segG = gc.meta.GetSegment(context.TODO(), segID+8)
	assert.Nil(t, segG)
	err := gc.meta.indexMeta.AddSegmentIndex(context.TODO(), &model.SegmentIndex{
		SegmentID:    segID + 4,
		CollectionID: collID,
		PartitionID:  partID,
		NumRows:      12000,
		IndexID:      indexID,
		BuildID:      buildID + 4,
	})
	assert.NoError(t, err)

	err = gc.meta.indexMeta.FinishTask(&workerpb.IndexTaskInfo{
		BuildID:        buildID + 4,
		State:          commonpb.IndexState_Finished,
		IndexFileKeys:  []string{"file1", "file2", "file3", "file4"},
		SerializedSize: 10240,
		FailReason:     "",
	})
	assert.NoError(t, err)

	gc.recycleDroppedSegments(context.TODO(), signal)
	/*

		A: processed prior to C, C is not GCed yet and C is not indexed, A is not GCed in this turn
		B: processed prior to C, C is not GCed yet and C is not indexed, B is not GCed in this turn

		E: flushed, indexed, should not be GCed
		C: dropped, not indexed, should be GCed since E is indexed
		D: dropped, not indexed, should be GCed since E is indexed
	*/

	segC = gc.meta.GetSegment(context.TODO(), segID+2)
	assert.Nil(t, segC)
	segD = gc.meta.GetSegment(context.TODO(), segID+3)
	assert.Nil(t, segD)

	gc.recycleDroppedSegments(context.TODO(), signal)
	/*
		A: compacted became false due to C is GCed already, A should be GCed since dropTolernace is meet
		B: compacted became false due to C is GCed already, B should be GCed since dropTolerance is meet
	*/
	segA = gc.meta.GetSegment(context.TODO(), segID)
	assert.Nil(t, segA)
	segB = gc.meta.GetSegment(context.TODO(), segID+1)
	assert.Nil(t, segB)
}

func TestGarbageCollector_recycleChannelMeta(t *testing.T) {
	catalog := catalogmocks.NewDataCoordCatalog(t)

	m := &meta{
		catalog:    catalog,
		channelCPs: newChannelCps(),
	}

	m.channelCPs.checkpoints = map[string]*msgpb.MsgPosition{
		"cluster-id-rootcoord-dm_0_123v0": nil,
		"cluster-id-rootcoord-dm_1_123v0": nil,
		"cluster-id-rootcoord-dm_0_124v0": nil,
	}

	broker := broker2.NewMockBroker(t)
	broker.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(true, nil).Twice()

	gc := newGarbageCollector(m, newMockHandlerWithMeta(m), GcOption{broker: broker})

	t.Run("list channel cp fail", func(t *testing.T) {
		catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, errors.New("mock error")).Once()
		gc.recycleChannelCPMeta(context.TODO(), nil)
		assert.Equal(t, 3, len(m.channelCPs.checkpoints))
	})

	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Unset()
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(map[string]*msgpb.MsgPosition{
		"cluster-id-rootcoord-dm_0_123v0":                   nil,
		"cluster-id-rootcoord-dm_1_123v0":                   nil,
		"cluster-id-rootcoord-dm_0_invalidedCollectionIDv0": nil,
		"cluster-id-rootcoord-dm_0_124v0":                   nil,
	}, nil).Times(3)

	catalog.EXPECT().GcConfirm(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, collectionID int64, i2 int64) bool {
			return collectionID == 123
		}).Maybe()

	t.Run("skip drop channel due to collection is available", func(t *testing.T) {
		gc.recycleChannelCPMeta(context.TODO(), nil)
		assert.Equal(t, 3, len(m.channelCPs.checkpoints))
	})

	broker.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(false, nil).Times(4)
	t.Run("drop channel cp fail", func(t *testing.T) {
		catalog.EXPECT().DropChannelCheckpoint(mock.Anything, mock.Anything).Return(errors.New("mock error")).Twice()
		gc.recycleChannelCPMeta(context.TODO(), nil)
		assert.Equal(t, 3, len(m.channelCPs.checkpoints))
	})

	t.Run("channel cp gc ok", func(t *testing.T) {
		catalog.EXPECT().DropChannelCheckpoint(mock.Anything, mock.Anything).Return(nil).Twice()
		gc.recycleChannelCPMeta(context.TODO(), nil)
		assert.Equal(t, 1, len(m.channelCPs.checkpoints))
	})
}

func TestGarbageCollector_removeObjectPool(t *testing.T) {
	paramtable.Init()
	cm := mocks.NewChunkManager(t)
	gc := newGarbageCollector(
		nil,
		nil,
		GcOption{
			cli:           cm,
			dropTolerance: 1,
		})
	logs := make(map[string]struct{})
	for i := 0; i < 50; i++ {
		logs[fmt.Sprintf("log%d", i)] = struct{}{}
	}

	t.Run("success", func(t *testing.T) {
		call := cm.EXPECT().Remove(mock.Anything, mock.Anything).Return(nil)
		defer call.Unset()
		b := gc.removeObjectFiles(context.TODO(), logs)
		assert.NoError(t, b)
	})

	t.Run("oss not found error", func(t *testing.T) {
		call := cm.EXPECT().Remove(mock.Anything, mock.Anything).Return(merr.WrapErrIoKeyNotFound("not found"))
		defer call.Unset()
		b := gc.removeObjectFiles(context.TODO(), logs)
		assert.NoError(t, b)
	})

	t.Run("oss server error", func(t *testing.T) {
		call := cm.EXPECT().Remove(mock.Anything, mock.Anything).Return(merr.WrapErrIoFailed("server error", errors.New("err")))
		defer call.Unset()
		b := gc.removeObjectFiles(context.TODO(), logs)
		assert.Error(t, b)
	})

	t.Run("other type error", func(t *testing.T) {
		call := cm.EXPECT().Remove(mock.Anything, mock.Anything).Return(errors.New("other error"))
		defer call.Unset()
		b := gc.removeObjectFiles(context.TODO(), logs)
		assert.Error(t, b)
	})
}

type GarbageCollectorSuite struct {
	suite.Suite

	bucketName string
	rootPath   string

	cli     *storage.RemoteChunkManager
	inserts []string
	stats   []string
	delta   []string
	others  []string

	meta *meta
}

func (s *GarbageCollectorSuite) SetupTest() {
	s.bucketName = `datacoord-ut` + strings.ToLower(funcutil.RandomString(8))
	s.rootPath = `gc` + funcutil.RandomString(8)

	var err error
	s.cli, s.inserts, s.stats, s.delta, s.others, err = initUtOSSEnv(s.bucketName, s.rootPath, 4)
	s.Require().NoError(err)

	s.meta, err = newMemoryMeta(s.T())
	s.Require().NoError(err)
}

func (s *GarbageCollectorSuite) TearDownTest() {
	cleanupOSS(s.cli, s.bucketName, s.rootPath)
}

func (s *GarbageCollectorSuite) TestPauseResume() {
	s.Run("not_enabled", func() {
		gc := newGarbageCollector(s.meta, newMockHandler(), GcOption{
			cli:              s.cli,
			enabled:          false,
			checkInterval:    time.Millisecond * 10,
			scanInterval:     time.Hour * 24 * 7,
			missingTolerance: time.Hour * 24,
			dropTolerance:    time.Hour * 24,
		})

		gc.start()
		defer gc.close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := gc.Pause(ctx, -1, "", time.Second)
		s.NoError(err)

		err = gc.Resume(ctx, -1, "")
		s.Error(err)
	})

	s.Run("pause_then_resume", func() {
		gc := newGarbageCollector(s.meta, newMockHandler(), GcOption{
			cli:              s.cli,
			enabled:          true,
			checkInterval:    time.Millisecond * 10,
			scanInterval:     time.Hour * 7 * 24,
			missingTolerance: time.Hour * 24,
			dropTolerance:    time.Hour * 24,
		})

		gc.start()
		defer gc.close()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := gc.Pause(ctx, -1, "", time.Minute)
		s.NoError(err)

		s.NotZero(gc.pauseUntil.PauseUntil())

		err = gc.Resume(ctx, -1, "")
		s.NoError(err)

		s.Zero(gc.pauseUntil.PauseUntil())
	})

	s.Run("pause_before_until", func() {
		gc := newGarbageCollector(s.meta, newMockHandler(), GcOption{
			cli:              s.cli,
			enabled:          true,
			checkInterval:    time.Millisecond * 10,
			scanInterval:     time.Hour * 7 * 24,
			missingTolerance: time.Hour * 24,
			dropTolerance:    time.Hour * 24,
		})

		gc.start()
		defer gc.close()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := gc.Pause(ctx, -1, "", time.Minute)
		s.NoError(err)

		until := gc.pauseUntil.PauseUntil()
		s.NotZero(until)

		err = gc.Pause(ctx, -1, "", time.Second)
		s.NoError(err)

		second := gc.pauseUntil.PauseUntil()

		s.Equal(until, second)
	})

	s.Run("pause_resume_timeout", func() {
		gc := newGarbageCollector(s.meta, newMockHandler(), GcOption{
			cli:              s.cli,
			enabled:          true,
			checkInterval:    time.Millisecond * 10,
			scanInterval:     time.Hour * 7 * 24,
			missingTolerance: time.Hour * 24,
			dropTolerance:    time.Hour * 24,
		})

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		err := gc.Pause(ctx, -1, "", time.Minute)
		s.Error(err)

		s.Zero(gc.pauseUntil.PauseUntil())

		err = gc.Resume(ctx, -1, "")
		s.Error(err)

		s.Zero(gc.pauseUntil.PauseUntil())
	})

	s.Run("pause_collection", func() {
		gc := newGarbageCollector(s.meta, newMockHandler(), GcOption{
			cli:              s.cli,
			enabled:          true,
			checkInterval:    time.Millisecond * 10,
			scanInterval:     time.Hour * 7 * 24,
			missingTolerance: time.Hour * 24,
			dropTolerance:    time.Hour * 24,
		})

		gc.start()
		defer gc.close()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ticket := uuid.NewString()
		err := gc.Pause(ctx, 100, ticket, time.Minute)
		s.NoError(err)

		until, has := gc.pausedCollection.Get(100)
		firstPauseUntil := until.PauseUntil()
		s.True(has)
		s.NotZero(firstPauseUntil)

		ticket2 := uuid.NewString()
		err = gc.Pause(ctx, 100, ticket2, time.Second*30)
		s.NoError(err)

		second, has := gc.pausedCollection.Get(100)
		secondPauseUntil := second.PauseUntil()
		s.True(has)

		s.Equal(firstPauseUntil, secondPauseUntil)

		err = gc.Resume(ctx, 100, ticket2)
		s.NoError(err)

		afterResume, has := gc.pausedCollection.Get(100)
		s.True(has)
		afterUntil := afterResume.PauseUntil()
		s.Equal(firstPauseUntil, afterUntil)

		_ = gc.Resume(ctx, 100, ticket)

		_, has = gc.pausedCollection.Get(100)
		s.False(has)
	})
}

func (s *GarbageCollectorSuite) TestRunRecycleTaskWithPauser() {
	gc := newGarbageCollector(s.meta, newMockHandler(), GcOption{
		cli:              s.cli,
		enabled:          true,
		checkInterval:    time.Millisecond * 10,
		scanInterval:     time.Hour * 7 * 24,
		missingTolerance: time.Hour * 24,
		dropTolerance:    time.Hour * 24,
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*2500)
	defer cancel()

	cnt := 0
	gc.runRecycleTaskWithPauser(ctx, "test", time.Second, func(ctx context.Context, signal <-chan gcCmd) {
		cnt++
	})
	s.Equal(cnt, 2)
}

func (s *GarbageCollectorSuite) TestAvoidGCLoadedSegments() {
	handler := NewNMockHandler(s.T())
	handler.EXPECT().ListLoadedSegments(mock.Anything).Return([]int64{1}, nil).Once()
	gc := newGarbageCollector(s.meta, handler, GcOption{
		cli:              s.cli,
		enabled:          true,
		checkInterval:    time.Millisecond * 10,
		scanInterval:     time.Hour * 7 * 24,
		missingTolerance: time.Hour * 24,
		dropTolerance:    time.Hour * 24,
	})

	s.meta.AddSegment(context.TODO(), &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:        1,
			State:     commonpb.SegmentState_Dropped,
			DroppedAt: 0,
		},
	})

	signal := make(chan gcCmd)
	gc.recycleDroppedSegments(context.TODO(), signal)
	seg := s.meta.GetSegment(context.TODO(), 1)
	s.NotNil(seg)
}

func TestGarbageCollector(t *testing.T) {
	suite.Run(t, new(GarbageCollectorSuite))
}

func setupDroppedSegmentWithIndexForGC(t *testing.T) (*meta, *SegmentInfo, *model.SegmentIndex, string) {
	t.Helper()
	ctx := context.Background()
	const (
		collID  = int64(100)
		partID  = int64(10)
		segID   = int64(1001)
		fieldID = int64(20)
		indexID = int64(30)
		buildID = int64(40)
		logID   = int64(1)
	)
	// Build the binlog path the same way DecompressBinLog will rebuild it at GC
	// time so assertions stay paramtable-agnostic.
	binlogPath, err := binlog.BuildLogPath(storage.InsertBinlog, collID, partID, segID, fieldID, logID)
	require.NoError(t, err)

	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            segID,
			CollectionID:  collID,
			PartitionID:   partID,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Dropped,
			DroppedAt:     uint64(time.Now().Add(-time.Hour).UnixNano()),
			Binlogs:       []*datapb.FieldBinlog{getFieldBinlogPaths(fieldID, binlogPath)},
		},
	}
	require.NoError(t, m.AddSegment(ctx, segment))

	// Write through fieldIndexLock so this helper does not race with the GC
	// goroutine that reads indexMeta.indexes under the same lock (go test -race).
	m.indexMeta.fieldIndexLock.Lock()
	if m.indexMeta.indexes == nil {
		m.indexMeta.indexes = make(map[UniqueID]map[UniqueID]*model.Index)
	}
	m.indexMeta.indexes[collID] = map[UniqueID]*model.Index{
		indexID: {
			CollectionID: collID,
			FieldID:      fieldID,
			IndexID:      indexID,
			IndexName:    "deleted-index",
			IsDeleted:    true,
		},
	}
	m.indexMeta.fieldIndexLock.Unlock()
	segIdx := &model.SegmentIndex{
		SegmentID:     segID,
		CollectionID:  collID,
		PartitionID:   partID,
		IndexID:       indexID,
		BuildID:       buildID,
		IndexVersion:  1,
		IndexState:    commonpb.IndexState_Finished,
		IndexFileKeys: []string{"idx-file"},
	}
	require.NoError(t, m.indexMeta.AddSegmentIndex(ctx, segIdx))
	return m, segment, segIdx, binlogPath
}

func TestGarbageCollector_recycleDroppedSegments_RecyclesSegmentIndexMeta(t *testing.T) {
	ctx := context.Background()
	m, segment, segIdx, binlogPath := setupDroppedSegmentWithIndexForGC(t)

	expectedIndexFile := metautil.BuildSegmentIndexFilePath("root", segIdx.BuildID, segIdx.IndexVersion,
		segIdx.PartitionID, segIdx.SegmentID, "idx-file")

	cm := mocks.NewChunkManager(t)
	cm.EXPECT().RootPath().Return("root").Maybe()
	var mu sync.Mutex
	removed := make([]string, 0, 2)
	cm.EXPECT().Remove(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, filePath string) error {
			mu.Lock()
			defer mu.Unlock()
			removed = append(removed, filePath)
			return nil
		}).Maybe()

	gc := newGarbageCollector(m, newMockHandler(), GcOption{
		cli:           cm,
		dropTolerance: 0,
	})

	assert.Empty(t, m.indexMeta.GetSegmentIndexes(segment.CollectionID, segment.ID),
		"deleted field indexes are filtered by the existing query API")
	assert.Len(t, m.indexMeta.GetAllSegmentIndexes(segment.ID), 1)

	gc.recycleDroppedSegments(ctx, nil)

	assert.Nil(t, m.GetSegment(ctx, segment.ID))
	assert.Empty(t, m.indexMeta.GetAllSegmentIndexes(segment.ID))
	mu.Lock()
	assert.ElementsMatch(t, []string{binlogPath, expectedIndexFile}, removed)
	mu.Unlock()
}

func TestGarbageCollector_recycleDroppedSegments_FileDeleteFailureKeepsMeta(t *testing.T) {
	ctx := context.Background()
	m, segment, _, _ := setupDroppedSegmentWithIndexForGC(t)

	cli := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test"))
	gc := newGarbageCollector(m, newMockHandler(), GcOption{
		cli:           cli,
		dropTolerance: 0,
	})

	mockRemoveObjectFiles := mockey.Mock((*garbageCollector).removeObjectFiles).Return(errors.New("remove failed")).Build()
	defer mockRemoveObjectFiles.UnPatch()

	gc.recycleDroppedSegments(ctx, nil)

	assert.NotNil(t, m.GetSegment(ctx, segment.ID))
	assert.Len(t, m.indexMeta.GetAllSegmentIndexes(segment.ID), 1)
}

func TestGarbageCollector_recycleDroppedSegments_IndexMetaFailureKeepsSegmentMeta(t *testing.T) {
	ctx := context.Background()
	m, segment, _, _ := setupDroppedSegmentWithIndexForGC(t)

	cli := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test"))
	gc := newGarbageCollector(m, newMockHandler(), GcOption{
		cli:           cli,
		dropTolerance: 0,
	})

	mockRemoveObjectFiles := mockey.Mock((*garbageCollector).removeObjectFiles).Return(nil).Build()
	defer mockRemoveObjectFiles.UnPatch()
	mockRemoveSegmentIndex := mockey.Mock((*indexMeta).RemoveSegmentIndex).Return(errors.New("meta failed")).Build()
	defer mockRemoveSegmentIndex.UnPatch()

	gc.recycleDroppedSegments(ctx, nil)

	assert.NotNil(t, m.GetSegment(ctx, segment.ID))
	assert.Len(t, m.indexMeta.GetAllSegmentIndexes(segment.ID), 1)
}

func TestGarbageCollector_recycleDroppedSegment_DropSegmentFailure(t *testing.T) {
	ctx := context.Background()
	m, segment, _, _ := setupDroppedSegmentWithIndexForGC(t)
	cli := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test"))
	gc := newGarbageCollector(m, newMockHandler(), GcOption{
		cli:           cli,
		dropTolerance: 0,
	})

	mockRemoveObjectFiles := mockey.Mock((*garbageCollector).removeObjectFiles).Return(nil).Build()
	defer mockRemoveObjectFiles.UnPatch()
	mockDropSegment := mockey.Mock((*meta).DropSegment).Return(errors.New("drop segment failed")).Build()
	defer mockDropSegment.UnPatch()

	gc.recycleDroppedSegment(ctx, segment.ID, segment)

	assert.NotNil(t, m.GetSegment(ctx, segment.ID))
}

func TestGarbageCollector_DroppedSegmentIndexHelpers(t *testing.T) {
	ctx := context.Background()
	m, segment, segIdx, _ := setupDroppedSegmentWithIndexForGC(t)

	gc := newGarbageCollector(m, newMockHandler(), GcOption{
		cli: storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test")),
	})

	segIndexes, indexFiles := gc.getDroppedSegmentIndexFiles(segment.ID)
	require.Len(t, segIndexes, 1)
	assert.Equal(t, segIdx.BuildID, segIndexes[0].BuildID)
	expectedIndexFile := metautil.BuildSegmentIndexFilePath(gc.option.cli.RootPath(), segIdx.BuildID,
		segIdx.IndexVersion, segIdx.PartitionID, segIdx.SegmentID, "idx-file")
	assert.Contains(t, indexFiles, expectedIndexFile)

	assert.Nil(t, (&garbageCollector{}).getAllSegmentIndexesForDroppedSegment(segment.ID))
	assert.NoError(t, gc.removeDroppedSegmentIndexMeta(ctx, nil))
	require.NoError(t, gc.removeDroppedSegmentIndexMeta(ctx, segIndexes))
	assert.Empty(t, m.indexMeta.GetAllSegmentIndexes(segment.ID))
}

func TestGarbageCollector_recycleDroppedSegment_CancellationShortCircuit(t *testing.T) {
	t.Run("cancel before files step", func(t *testing.T) {
		m, segment, _, _ := setupDroppedSegmentWithIndexForGC(t)
		gc := newGarbageCollector(m, newMockHandler(), GcOption{
			cli:           storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test-gc-cancel-1")),
			dropTolerance: 0,
		})

		var removeCalled atomic.Bool
		mockRemove := mockey.Mock((*garbageCollector).removeObjectFiles).To(
			func(gc *garbageCollector, ctx context.Context, files map[string]struct{}) error {
				removeCalled.Store(true)
				return nil
			}).Build()
		defer mockRemove.UnPatch()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		gc.recycleDroppedSegment(ctx, segment.ID, segment)

		assert.False(t, removeCalled.Load(), "file removal must be skipped when ctx is canceled before the files step")
		assert.NotNil(t, m.GetSegment(context.Background(), segment.ID))
	})

	t.Run("cancel between files and index meta", func(t *testing.T) {
		m, segment, _, _ := setupDroppedSegmentWithIndexForGC(t)
		gc := newGarbageCollector(m, newMockHandler(), GcOption{
			cli:           storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test-gc-cancel-2")),
			dropTolerance: 0,
		})

		ctx, cancel := context.WithCancel(context.Background())
		mockRemove := mockey.Mock((*garbageCollector).removeObjectFiles).To(
			func(gc *garbageCollector, c context.Context, files map[string]struct{}) error {
				cancel()
				return nil
			}).Build()
		defer mockRemove.UnPatch()

		var indexMetaCalled atomic.Bool
		mockIdx := mockey.Mock((*garbageCollector).removeDroppedSegmentIndexMeta).To(
			func(gc *garbageCollector, c context.Context, idx []*model.SegmentIndex) error {
				indexMetaCalled.Store(true)
				return nil
			}).Build()
		defer mockIdx.UnPatch()

		gc.recycleDroppedSegment(ctx, segment.ID, segment)

		assert.False(t, indexMetaCalled.Load(), "index meta step must be skipped when ctx is canceled mid-segment")
		// segment meta should still exist since DropSegment never ran.
		assert.NotNil(t, m.GetSegment(context.Background(), segment.ID))
	})
}

func TestGarbageCollector_getDroppedSegmentIndexFiles_EdgeCases(t *testing.T) {
	t.Run("empty segIndexes returns nil maps", func(t *testing.T) {
		m, err := newMemoryMeta(t)
		require.NoError(t, err)
		gc := newGarbageCollector(m, newMockHandler(), GcOption{
			cli: storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test")),
		})
		segIndexes, indexFiles := gc.getDroppedSegmentIndexFiles(9999)
		assert.Nil(t, segIndexes)
		assert.Nil(t, indexFiles)
	})

	t.Run("segment with index returns full file set", func(t *testing.T) {
		m, segment, segIdx, _ := setupDroppedSegmentWithIndexForGC(t)
		gc := newGarbageCollector(m, newMockHandler(), GcOption{
			cli: storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test")),
		})
		segIndexes, indexFiles := gc.getDroppedSegmentIndexFiles(segment.ID)
		require.Len(t, segIndexes, 1)
		assert.Equal(t, segIdx.BuildID, segIndexes[0].BuildID)
		assert.NotEmpty(t, indexFiles)
	})
}

// TestGarbageCollector_recycleDroppedSegment_CtxCanceledBeforeDrop covers
// the ctx.Err() guard before DropSegment by canceling ctx inside
// removeDroppedSegmentIndexMeta so DropSegment must NOT run.
func TestGarbageCollector_recycleDroppedSegment_CtxCanceledBeforeDrop(t *testing.T) {
	m, segment, _, _ := setupDroppedSegmentWithIndexForGC(t)
	cli := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test-gc-cancel-drop"))
	gc := newGarbageCollector(m, newMockHandler(), GcOption{
		cli:           cli,
		dropTolerance: 0,
	})

	mockRemove := mockey.Mock((*garbageCollector).removeObjectFiles).Return(nil).Build()
	defer mockRemove.UnPatch()

	ctx, cancel := context.WithCancel(context.Background())
	var indexMetaCalls atomic.Int32
	mockIdx := mockey.Mock((*garbageCollector).removeDroppedSegmentIndexMeta).To(
		func(gc *garbageCollector, c context.Context, idx []*model.SegmentIndex) error {
			indexMetaCalls.Inc()
			cancel()
			return nil
		}).Build()
	defer mockIdx.UnPatch()

	var dropCalls atomic.Int32
	mockDrop := mockey.Mock((*meta).DropSegment).To(
		func(m *meta, ctx context.Context, sid int64) error {
			dropCalls.Inc()
			return nil
		}).Build()
	defer mockDrop.UnPatch()

	gc.recycleDroppedSegment(ctx, segment.ID, segment)

	assert.Equal(t, int32(1), indexMetaCalls.Load())
	assert.Equal(t, int32(0), dropCalls.Load(),
		"DropSegment must NOT run when ctx is canceled between index-meta and segment-meta steps")
	assert.NotNil(t, m.GetSegment(context.Background(), segment.ID))
}

// TestGarbageCollector_removeDroppedSegmentFiles_TextAndJSONLogs covers the
// for-range loops that merge getTextLogs and getJSONKeyLogs into the file
// deletion set in the V1/V2 path.
func TestGarbageCollector_removeDroppedSegmentFiles_TextAndJSONLogs(t *testing.T) {
	ctx := context.Background()
	cm := mocks.NewChunkManager(t)
	cm.EXPECT().RootPath().Return("root").Maybe()
	var mu sync.Mutex
	removed := make(map[string]struct{})
	cm.EXPECT().Remove(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, filePath string) error {
			mu.Lock()
			defer mu.Unlock()
			removed[filePath] = struct{}{}
			return nil
		}).Maybe()

	gc := newGarbageCollector(nil, nil, GcOption{cli: cm})
	const textFile = "text/log/file-1"
	const jsonFile = "json-file"
	const indexFile = "idx/extra/file-99"
	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:           7001,
			CollectionID: 100,
			PartitionID:  10,
			TextStatsLogs: map[int64]*datapb.TextIndexStats{
				101: {Files: []string{textFile}},
			},
			JsonKeyStats: map[int64]*datapb.JsonKeyStats{
				102: {
					FieldID:                102,
					BuildID:                11,
					Version:                1,
					Files:                  []string{jsonFile},
					JsonKeyStatsDataFormat: 1,
				},
			},
		},
	}

	indexFiles := map[string]struct{}{indexFile: {}}
	require.NoError(t, gc.removeDroppedSegmentFiles(ctx, segment, indexFiles))

	expectedJSON := path.Join("root", common.JSONIndexPath, "11", "1", "100", "10", "7001", "102", jsonFile)
	mu.Lock()
	defer mu.Unlock()
	assert.Contains(t, removed, textFile)
	assert.Contains(t, removed, expectedJSON)
	assert.Contains(t, removed, indexFile)
}

func TestGarbageCollector_removeDroppedSegmentFiles_JSONStatsV2(t *testing.T) {
	ctx := context.Background()
	cm := mocks.NewChunkManager(t)
	cm.EXPECT().RootPath().Return("root").Maybe()
	var mu sync.Mutex
	removed := make(map[string]struct{})
	cm.EXPECT().Remove(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, filePath string) error {
			mu.Lock()
			defer mu.Unlock()
			removed[filePath] = struct{}{}
			return nil
		}).Maybe()

	gc := newGarbageCollector(nil, nil, GcOption{cli: cm})
	const jsonFile = "shared_key_index/inverted_index_0"
	const indexFile = "idx/extra/file-99"
	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:           7001,
			CollectionID: 100,
			PartitionID:  10,
			JsonKeyStats: map[int64]*datapb.JsonKeyStats{
				102: {
					FieldID:                102,
					BuildID:                11,
					Version:                1,
					Files:                  []string{jsonFile},
					JsonKeyStatsDataFormat: 3,
				},
			},
		},
	}

	indexFiles := map[string]struct{}{indexFile: {}}
	require.NoError(t, gc.removeDroppedSegmentFiles(ctx, segment, indexFiles))

	expectedJSON := path.Join("root", common.JSONStatsPath, "3", "11", "1", "100", "10", "7001", "102", jsonFile)
	mu.Lock()
	defer mu.Unlock()
	assert.Contains(t, removed, expectedJSON)
	assert.Contains(t, removed, indexFile)
}

func TestGarbageCollector_recycleUnusedBinlogFiles_TextAndJSONStats(t *testing.T) {
	ctx := context.Background()

	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            1003,
			CollectionID:  100,
			PartitionID:   10,
			State:         commonpb.SegmentState_Flushed,
			InsertChannel: "ch1",
			TextStatsLogs: map[int64]*datapb.TextIndexStats{
				101: {
					FieldID: 101,
					Version: 1,
					BuildID: 501,
					Files:   []string{"text_file_keep"},
				},
			},
			JsonKeyStats: map[int64]*datapb.JsonKeyStats{
				102: {
					FieldID:                102,
					Version:                1,
					BuildID:                502,
					Files:                  []string{"json_file_keep"},
					JsonKeyStatsDataFormat: 1,
				},
				103: {
					FieldID:                103,
					Version:                1,
					BuildID:                503,
					Files:                  []string{"shared_key_index/json_file_keep"},
					JsonKeyStatsDataFormat: 3,
				},
			},
		},
	}

	meta := &meta{
		catalog:   &datacoord.Catalog{},
		indexMeta: &indexMeta{},
		segments: &SegmentsInfo{
			segments: map[int64]*SegmentInfo{1003: segment},
		},
		channelCPs: newChannelCps(),
	}

	cli := storage.NewLocalChunkManager(objectstorage.RootPath("gc"))
	gc := newGarbageCollector(meta, &ServerHandler{}, GcOption{
		cli:              cli,
		enabled:          true,
		checkInterval:    time.Millisecond * 10,
		scanInterval:     time.Hour * 7 * 24,
		missingTolerance: 0,
		dropTolerance:    time.Hour * 24,
	})

	removedFiles := []string{}
	mockRemove := mockey.Mock((*storage.LocalChunkManager).Remove).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, filePath string) error {
			removedFiles = append(removedFiles, filePath)
			return nil
		}).Build()
	defer mockRemove.UnPatch()

	segment.GetTextStatsLogs()[101].Files = []string{"gc/text_log/501/1/100/10/1003/101/text_file_keep"}

	mockWalk := mockey.Mock((*storage.LocalChunkManager).WalkWithPrefix).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, prefix string, recursive bool, fn storage.ChunkObjectWalkFunc) error {
			switch {
			case strings.Contains(prefix, common.TextIndexPath):
				fn(&storage.ChunkObjectInfo{FilePath: "gc/text_log/501/1/100/10/1003/101/text_file_keep", ModifyTime: time.Now().Add(-time.Hour)})
				fn(&storage.ChunkObjectInfo{FilePath: "gc/text_log/501/1/100/10/1003/101/text_file_gc", ModifyTime: time.Now().Add(-time.Hour)})
			case strings.Contains(prefix, common.JSONStatsPath):
				fn(&storage.ChunkObjectInfo{FilePath: "gc/json_stats/3/503/1/100/10/1003/103/shared_key_index/json_file_keep", ModifyTime: time.Now().Add(-time.Hour)})
				fn(&storage.ChunkObjectInfo{FilePath: "gc/json_stats/3/503/1/100/10/1003/103/shared_key_index/json_file_gc", ModifyTime: time.Now().Add(-time.Hour)})
			case strings.Contains(prefix, common.JSONIndexPath):
				fn(&storage.ChunkObjectInfo{FilePath: "gc/json_key_index_log/502/1/100/10/1003/102/json_file_keep", ModifyTime: time.Now().Add(-time.Hour)})
				fn(&storage.ChunkObjectInfo{FilePath: "gc/json_key_index_log/502/1/100/10/1003/102/json_file_gc", ModifyTime: time.Now().Add(-time.Hour)})
			}
			return nil
		}).Build()
	defer mockWalk.UnPatch()

	gc.recycleUnusedBinlogFiles(ctx)

	assert.ElementsMatch(t, []string{
		"gc/text_log/501/1/100/10/1003/101/text_file_gc",
		"gc/json_stats/3/503/1/100/10/1003/103/shared_key_index/json_file_gc",
		"gc/json_key_index_log/502/1/100/10/1003/102/json_file_gc",
	}, removedFiles)
}

func TestGarbageCollector_recycleUnusedBinlogFiles_TextAndJSONStats_SegmentNil(t *testing.T) {
	ctx := context.Background()

	meta := &meta{
		catalog:   &datacoord.Catalog{},
		indexMeta: &indexMeta{},
		segments: &SegmentsInfo{
			segments: map[int64]*SegmentInfo{},
		},
		channelCPs: newChannelCps(),
	}

	cli := storage.NewLocalChunkManager(objectstorage.RootPath("gc"))
	gc := newGarbageCollector(meta, &ServerHandler{}, GcOption{
		cli:              cli,
		enabled:          true,
		checkInterval:    time.Millisecond * 10,
		scanInterval:     time.Hour * 7 * 24,
		missingTolerance: 0,
		dropTolerance:    time.Hour * 24,
	})

	removedFiles := []string{}
	mockRemove := mockey.Mock((*storage.LocalChunkManager).Remove).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, filePath string) error {
			removedFiles = append(removedFiles, filePath)
			return nil
		}).Build()
	defer mockRemove.UnPatch()

	mockWalk := mockey.Mock((*storage.LocalChunkManager).WalkWithPrefix).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, prefix string, recursive bool, fn storage.ChunkObjectWalkFunc) error {
			switch {
			case strings.Contains(prefix, common.TextIndexPath):
				fn(&storage.ChunkObjectInfo{FilePath: "gc/text_log/501/1/100/10/1003/101/text_file_gc", ModifyTime: time.Now().Add(-time.Hour)})
			case strings.Contains(prefix, common.JSONStatsPath):
				fn(&storage.ChunkObjectInfo{FilePath: "gc/json_stats/3/503/1/100/10/1003/103/shared_key_index/json_file_gc", ModifyTime: time.Now().Add(-time.Hour)})
			case strings.Contains(prefix, common.JSONIndexPath):
				fn(&storage.ChunkObjectInfo{FilePath: "gc/json_key_index_log/502/1/100/10/1003/102/json_file_gc", ModifyTime: time.Now().Add(-time.Hour)})
			}
			return nil
		}).Build()
	defer mockWalk.UnPatch()

	gc.recycleUnusedBinlogFiles(ctx)

	assert.ElementsMatch(t, []string{
		"gc/text_log/501/1/100/10/1003/101/text_file_gc",
		"gc/json_stats/3/503/1/100/10/1003/103/shared_key_index/json_file_gc",
		"gc/json_key_index_log/502/1/100/10/1003/102/json_file_gc",
	}, removedFiles)
}
