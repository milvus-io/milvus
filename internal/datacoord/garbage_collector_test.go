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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	broker2 "github.com/milvus-io/milvus/internal/datacoord/broker"
	kvmocks "github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/objectstorage"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/lock"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
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
	meta.snapshotMeta = &snapshotMeta{}
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
		// Snapshot layer transparent: no buildID is blocked.
		mockIsBuildIDBlocked := mockey.Mock((*snapshotMeta).IsBuildIDGCBlocked).Return(false).Build()
		defer mockIsBuildIDBlocked.UnPatch()
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
		mockIsBuildIDBlocked := mockey.Mock((*snapshotMeta).IsBuildIDGCBlocked).Return(false).Build()
		defer mockIsBuildIDBlocked.UnPatch()
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
		catalog:      catalog,
		channelCPs:   channelCPs,
		segments:     NewSegmentsInfo(),
		snapshotMeta: &snapshotMeta{},
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
	signal := make(chan gcCmd)
	gc := newGarbageCollector(
		m,
		newMockHandlerWithMeta(m),
		GcOption{
			cli:           cm,
			dropTolerance: 1,
		})
	// Snapshot layer transparent: no segment is blocked.
	mockIsSegBlocked := mockey.Mock((*snapshotMeta).IsSegmentGCBlocked).Return(false).Build()
	defer mockIsSegBlocked.UnPatch()
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
			if collectionID == 123 {
				return true
			}
			return false
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

		err = gc.Resume(ctx, 100, ticket)

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

// TestGarbageCollector_recycleDroppedSegments_SnapshotReference tests that segments referenced by snapshots are not garbage collected
func TestGarbageCollector_recycleDroppedSegments_SnapshotReference(t *testing.T) {
	// Setup
	ctx := context.Background()

	// Create storage manager
	cli := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test"))

	// Create necessary components
	catalog := &datacoord.Catalog{}
	handler := &ServerHandler{}

	// Mock newSnapshotMeta
	smMeta := &snapshotMeta{}

	// Create meta
	meta := &meta{
		catalog:      catalog,
		snapshotMeta: smMeta,
		segments: &SegmentsInfo{
			segments: make(map[int64]*SegmentInfo),
		},
		channelCPs: newChannelCps(),
	}

	// Create garbage collector
	gc := newGarbageCollector(meta, handler, GcOption{
		cli:              cli,
		enabled:          true,
		checkInterval:    time.Millisecond * 10,
		scanInterval:     time.Hour * 7 * 24,
		missingTolerance: time.Hour * 24,
		dropTolerance:    0, // Set to 0 to immediately consider segments for GC
	})

	// Add dropped segments to meta
	droppedSegment1 := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            1001,
			CollectionID:  100,
			PartitionID:   10,
			State:         commonpb.SegmentState_Dropped,
			DroppedAt:     uint64(time.Now().Add(-time.Hour).UnixNano()),
			InsertChannel: "ch1",
		},
	}
	droppedSegment2 := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            1002,
			CollectionID:  100,
			PartitionID:   10,
			State:         commonpb.SegmentState_Dropped,
			DroppedAt:     uint64(time.Now().Add(-time.Hour).UnixNano()),
			InsertChannel: "ch1",
		},
	}

	meta.segments.segments[1001] = droppedSegment1
	meta.segments.segments[1002] = droppedSegment2

	// Setup mocks
	mock1 := mockey.Mock(meta.GetSnapshotMeta).Return(smMeta).Build()
	defer mock1.UnPatch()

	// New O(1) API: IsSegmentGCBlocked encodes both "loaded state" and "is referenced".
	// Block segment 1001; leave 1002 unblocked.
	mock2 := mockey.Mock((*snapshotMeta).IsSegmentGCBlocked).To(
		func(sm *snapshotMeta, collID, segID int64) bool {
			return segID == 1001
		}).Build()
	defer mock2.UnPatch()

	mock3 := mockey.Mock((*ServerHandler).ListLoadedSegments).To(func(h *ServerHandler, ctx context.Context) ([]int64, error) {
		return []int64{}, nil
	}).Build()
	defer mock3.UnPatch()

	mock4 := mockey.Mock((*datacoord.Catalog).ListChannelCheckpoint).To(func(c *datacoord.Catalog, ctx context.Context) (map[string]*msgpb.MsgPosition, error) {
		return map[string]*msgpb.MsgPosition{}, nil
	}).Build()
	defer mock4.UnPatch()

	mock5 := mockey.Mock((*datacoord.Catalog).ListIndexes).To(func(c *datacoord.Catalog, ctx context.Context) ([]*model.Index, error) {
		return []*model.Index{}, nil
	}).Build()
	defer mock5.UnPatch()

	mock6 := mockey.Mock((*datacoord.Catalog).ListSegmentIndexes).To(func(c *datacoord.Catalog, ctx context.Context, collectionID int64) ([]*model.SegmentIndex, error) {
		return []*model.SegmentIndex{}, nil
	}).Build()
	defer mock6.UnPatch()

	mock7 := mockey.Mock((*datacoord.Catalog).ChannelExists).To(func(c *datacoord.Catalog, ctx context.Context, channel string) bool {
		return true
	}).Build()
	defer mock7.UnPatch()

	dropSegmentCalled := false
	var droppedSegment *datapb.SegmentInfo
	mock8 := mockey.Mock((*datacoord.Catalog).DropSegment).To(func(c *datacoord.Catalog, ctx context.Context, segment *datapb.SegmentInfo) error {
		dropSegmentCalled = true
		droppedSegment = segment
		return nil
	}).Build()
	defer mock8.UnPatch()

	mock9 := mockey.Mock((*garbageCollector).removeObjectFiles).To(func(gc *garbageCollector, ctx context.Context, logs map[string]struct{}) error {
		return nil
	}).Build()
	defer mock9.UnPatch()

	// Execute
	gc.recycleDroppedSegments(ctx, nil)

	// Verify
	// Segment 1001 should still exist (not GC'd due to snapshot reference)
	assert.NotNil(t, meta.GetSegment(ctx, 1001))
	// Segment 1002 should be removed (GC'd)
	assert.Nil(t, meta.GetSegment(ctx, 1002))
	assert.True(t, dropSegmentCalled)
	if droppedSegment != nil {
		assert.Equal(t, int64(1002), droppedSegment.ID)
	}
}

// TestGarbageCollector_recycleUnusedSegIndexes_SnapshotReference tests that indexes referenced by snapshots are not garbage collected
func TestGarbageCollector_recycleUnusedSegIndexes_SnapshotReference(t *testing.T) {
	// Setup
	ctx := context.Background()

	// Create storage manager
	cli := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test"))

	// Create necessary components
	catalog := &datacoord.Catalog{}
	handler := &ServerHandler{}

	// Mock newSnapshotMeta
	smMeta := &snapshotMeta{}

	// Mock newIndexMeta
	idxMeta := &indexMeta{}

	// Create meta
	meta := &meta{
		catalog:      catalog,
		snapshotMeta: smMeta,
		indexMeta:    idxMeta,
		segments: &SegmentsInfo{
			segments: make(map[int64]*SegmentInfo),
		},
		channelCPs: newChannelCps(),
	}

	// Create garbage collector
	gc := newGarbageCollector(meta, handler, GcOption{
		cli:              cli,
		enabled:          true,
		checkInterval:    time.Millisecond * 10,
		scanInterval:     time.Hour * 7 * 24,
		missingTolerance: time.Hour * 24,
		dropTolerance:    0,
	})

	// Create segment indexes
	segIdx1 := &model.SegmentIndex{
		SegmentID:    2001,
		CollectionID: 200,
		PartitionID:  20,
		IndexID:      301,
		BuildID:      401,
		NodeID:       501,
	}
	segIdx2 := &model.SegmentIndex{
		SegmentID:    2002,
		CollectionID: 200,
		PartitionID:  20,
		IndexID:      302,
		BuildID:      402,
		NodeID:       502,
	}

	// Setup mocks
	// New O(1) API: IsBuildIDGCBlocked encodes both "loaded state" and "is referenced".
	// Block buildID 401; leave 402 unblocked.
	mock1 := mockey.Mock((*snapshotMeta).IsBuildIDGCBlocked).To(
		func(sm *snapshotMeta, collID, buildID int64) bool {
			return buildID == 401
		}).Build()
	defer mock1.UnPatch()

	mock2 := mockey.Mock((*datacoord.Catalog).ListSegmentIndexes).To(func(c *datacoord.Catalog, ctx context.Context, collectionID int64) ([]*model.SegmentIndex, error) {
		return []*model.SegmentIndex{segIdx1, segIdx2}, nil
	}).Build()
	defer mock2.UnPatch()

	mock3 := mockey.Mock(meta.GetSegment).To(func(ctx context.Context, segmentID int64) *SegmentInfo {
		return nil // All segments are not found
	}).Build()
	defer mock3.UnPatch()

	mock4 := mockey.Mock((*indexMeta).IsIndexExist).To(func(collID, indexID int64) bool {
		return false
	}).Build()
	defer mock4.UnPatch()

	mock5 := mockey.Mock((*indexMeta).GetAllSegIndexes).To(func() map[int64]*model.SegmentIndex {
		return map[int64]*model.SegmentIndex{
			segIdx1.BuildID: segIdx1,
			segIdx2.BuildID: segIdx2,
		}
	}).Build()
	defer mock5.UnPatch()

	mock6 := mockey.Mock((*garbageCollector).getAllIndexFilesOfIndex).To(func(gc *garbageCollector, segIdx *model.SegmentIndex) map[string]struct{} {
		return map[string]struct{}{
			fmt.Sprintf("index_%d", segIdx.BuildID): {},
		}
	}).Build()
	defer mock6.UnPatch()

	removeCallCount := 0
	mock7 := mockey.Mock((*garbageCollector).removeObjectFiles).To(func(gc *garbageCollector, ctx context.Context, logs map[string]struct{}) error {
		removeCallCount++
		return nil
	}).Build()
	defer mock7.UnPatch()

	removeSegmentIndexCalled := false
	removedBuildID := int64(0)
	mock8 := mockey.Mock((*indexMeta).RemoveSegmentIndex).To(func(ctx context.Context, buildID int64) error {
		removeSegmentIndexCalled = true
		removedBuildID = buildID
		return nil
	}).Build()
	defer mock8.UnPatch()

	// Execute
	gc.recycleUnusedSegIndexes(ctx, nil)

	// Verify
	// Only segIdx2 should have its files removed (segIdx1 is protected by snapshot)
	assert.Equal(t, 1, removeCallCount)
	assert.True(t, removeSegmentIndexCalled)
	assert.Equal(t, int64(402), removedBuildID)
}

// TestGarbageCollector_recycleUnusedBinlogFiles_SnapshotReference tests that binlog files of segments
// referenced by snapshots are not garbage collected
func TestGarbageCollector_recycleUnusedBinlogFiles_SnapshotReference(t *testing.T) {
	ctx := context.Background()

	// Create storage manager
	cli := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test"))

	// Create segment that exists in meta (will pass checker for insert logs)
	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            1001,
			CollectionID:  100,
			PartitionID:   10,
			State:         commonpb.SegmentState_Flushed,
			InsertChannel: "ch1",
		},
	}

	// Create meta with segment
	meta := &meta{
		catalog:      &datacoord.Catalog{},
		snapshotMeta: &snapshotMeta{},
		indexMeta:    &indexMeta{},
		segments: &SegmentsInfo{
			segments: map[int64]*SegmentInfo{
				1001: segment,
			},
		},
		channelCPs: newChannelCps(),
	}

	gc := newGarbageCollector(meta, &ServerHandler{}, GcOption{
		cli:              cli,
		enabled:          true,
		checkInterval:    time.Millisecond * 10,
		scanInterval:     time.Hour * 7 * 24,
		missingTolerance: 0,
		dropTolerance:    time.Hour * 24,
	})

	// Track if Remove is called
	removeCalledPaths := []string{}
	mockRemove := mockey.Mock((*storage.LocalChunkManager).Remove).To(func(cm *storage.LocalChunkManager, ctx context.Context, filePath string) error {
		removeCalledPaths = append(removeCalledPaths, filePath)
		return nil
	}).Build()
	defer mockRemove.UnPatch()

	// Mock WalkWithPrefix to return a file from this segment
	chunkInfo := &storage.ChunkObjectInfo{
		FilePath:   "gc/segments/insert_logs/1001/0/1",
		ModifyTime: time.Now().Add(-time.Hour),
	}

	mockWalk := mockey.Mock((*storage.LocalChunkManager).WalkWithPrefix).To(func(cm *storage.LocalChunkManager, ctx context.Context, prefix string, recursive bool, fn storage.ChunkObjectWalkFunc) error {
		fn(chunkInfo)
		return nil
	}).Build()
	defer mockWalk.UnPatch()

	// Mock ParseSegmentIDByBinlog
	mockParseSegID := mockey.Mock(storage.ParseSegmentIDByBinlog).Return(int64(1001), nil).Build()
	defer mockParseSegID.UnPatch()

	// New O(1) API: IsSegmentGCBlocked directly returns "protected".
	mockIsSegBlocked := mockey.Mock((*snapshotMeta).IsSegmentGCBlocked).Return(true).Build()
	defer mockIsSegBlocked.UnPatch()

	// Execute
	gc.recycleUnusedBinlogFiles(ctx)

	// Verify - Remove should NOT be called because segment is protected by snapshot reference
	assert.Empty(t, removeCalledPaths, "binlog files of snapshot-referenced segments should not be removed")
}

// TestGarbageCollector_recycleDroppedSegments_SnapshotMetaNil tests that GC handles nil snapshotMeta gracefully
func TestGarbageCollector_recycleDroppedSegments_SnapshotMetaNil(t *testing.T) {
	// Setup
	ctx := context.Background()

	// Create storage manager
	cli := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test"))

	// Create necessary components
	catalog := &datacoord.Catalog{}
	handler := &ServerHandler{}

	// Create meta with nil snapshotMeta
	meta := &meta{
		catalog:      catalog,
		snapshotMeta: nil, // nil snapshot meta
		segments: &SegmentsInfo{
			segments: make(map[int64]*SegmentInfo),
		},
		channelCPs: newChannelCps(),
	}

	// Create garbage collector
	gc := newGarbageCollector(meta, handler, GcOption{
		cli:              cli,
		enabled:          true,
		checkInterval:    time.Millisecond * 10,
		scanInterval:     time.Hour * 7 * 24,
		missingTolerance: time.Hour * 24,
		dropTolerance:    0, // Set to 0 to immediately consider segments for GC
	})

	// Add dropped segment to meta
	droppedSegment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            1003,
			CollectionID:  100,
			PartitionID:   10,
			State:         commonpb.SegmentState_Dropped,
			DroppedAt:     uint64(time.Now().Add(-time.Hour).UnixNano()),
			InsertChannel: "ch1",
		},
	}

	meta.segments.segments[1003] = droppedSegment

	// Setup mocks
	mockGetSnapshotMeta := mockey.Mock(meta.GetSnapshotMeta).Return(nil).Build()
	defer mockGetSnapshotMeta.UnPatch()

	mockListLoadedSegments := mockey.Mock((*ServerHandler).ListLoadedSegments).Return([]int64{}, nil).Build()
	defer mockListLoadedSegments.UnPatch()

	mockListChannelCP := mockey.Mock((*datacoord.Catalog).ListChannelCheckpoint).Return(map[string]*msgpb.MsgPosition{}, nil).Build()
	defer mockListChannelCP.UnPatch()

	mockListIndexes := mockey.Mock((*datacoord.Catalog).ListIndexes).Return([]*model.Index{}, nil).Build()
	defer mockListIndexes.UnPatch()

	mockListSegmentIndexes := mockey.Mock((*datacoord.Catalog).ListSegmentIndexes).Return([]*model.SegmentIndex{}, nil).Build()
	defer mockListSegmentIndexes.UnPatch()

	mockChannelExists := mockey.Mock((*datacoord.Catalog).ChannelExists).Return(true).Build()
	defer mockChannelExists.UnPatch()

	dropSegmentCalled := false
	mockDropSegment := mockey.Mock((*datacoord.Catalog).DropSegment).To(func(c *datacoord.Catalog, ctx context.Context, segment *datapb.SegmentInfo) error {
		dropSegmentCalled = true
		return nil
	}).Build()
	defer mockDropSegment.UnPatch()

	mockRemoveObjectFiles := mockey.Mock((*garbageCollector).removeObjectFiles).Return(nil).Build()
	defer mockRemoveObjectFiles.UnPatch()

	// Execute - should not panic with nil snapshotMeta
	assert.NotPanics(t, func() {
		gc.recycleDroppedSegments(ctx, nil)
	})

	// Verify - segment should be dropped (GC'd) since snapshotMeta is nil
	assert.True(t, dropSegmentCalled, "DropSegment should be called")
}

// TestGarbageCollector_recycleUnusedIndexFiles_SnapshotReference tests that index files referenced
// by snapshots are not garbage collected
func TestGarbageCollector_recycleUnusedIndexFiles_SnapshotReference(t *testing.T) {
	// Setup
	ctx := context.Background()

	// Create storage manager
	cli := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test"))

	// Create necessary components
	catalog := &datacoord.Catalog{}
	handler := &ServerHandler{}

	// Mock snapshotMeta
	smMeta := &snapshotMeta{}

	// Create meta
	meta := &meta{
		catalog:      catalog,
		snapshotMeta: smMeta,
		indexMeta:    &indexMeta{},
		segments: &SegmentsInfo{
			segments: make(map[int64]*SegmentInfo),
		},
		channelCPs: newChannelCps(),
	}

	// Create garbage collector
	gc := newGarbageCollector(meta, handler, GcOption{
		cli:              cli,
		enabled:          true,
		checkInterval:    time.Millisecond * 10,
		scanInterval:     time.Hour * 7 * 24,
		missingTolerance: time.Hour * 24,
		dropTolerance:    0,
	})

	buildID := int64(401)
	collectionID := int64(100)
	indexID := int64(301)

	// Setup mock for WalkWithPrefix to find index directory.
	// Note: the outer walk runs against common.SegmentIndexPath ("index_files"), not "indexes".
	// We match that prefix so the walker callback actually fires and exercises the
	// segIdx != nil + IsBuildIDGCBlocked branch below.
	mockWalk := mockey.Mock((*storage.LocalChunkManager).WalkWithPrefix).To(func(cm *storage.LocalChunkManager, ctx context.Context, prefix string, recursive bool, fn storage.ChunkObjectWalkFunc) error {
		if strings.Contains(prefix, common.SegmentIndexPath) {
			chunkInfo := &storage.ChunkObjectInfo{
				FilePath:   fmt.Sprintf("gc/index_files/%d/1/", buildID),
				ModifyTime: time.Now().Add(-time.Hour),
			}
			fn(chunkInfo)
		}
		return nil
	}).Build()
	defer mockWalk.UnPatch()

	// Mock CheckCleanSegmentIndex
	segIdx := &model.SegmentIndex{
		BuildID:      buildID,
		CollectionID: collectionID,
		IndexID:      indexID,
	}
	mockCheckClean := mockey.Mock((*indexMeta).CheckCleanSegmentIndex).Return(true, segIdx).Build()
	defer mockCheckClean.UnPatch()

	// Mock GetSnapshotMeta
	mockGetSnapMeta := mockey.Mock(meta.GetSnapshotMeta).Return(smMeta).Build()
	defer mockGetSnapMeta.UnPatch()

	// New O(1) API: IsBuildIDGCBlocked returns true (buildID protected by snapshot).
	mockIsBuildIDBlocked := mockey.Mock((*snapshotMeta).IsBuildIDGCBlocked).Return(true).Build()
	defer mockIsBuildIDBlocked.UnPatch()

	removeWithPrefixCalled := false
	mockRemoveWithPrefix := mockey.Mock((*storage.LocalChunkManager).RemoveWithPrefix).To(func(cm *storage.LocalChunkManager, ctx context.Context, prefix string) error {
		removeWithPrefixCalled = true
		return nil
	}).Build()
	defer mockRemoveWithPrefix.UnPatch()

	// Execute
	gc.recycleUnusedIndexFiles(ctx)

	// Verify - RemoveWithPrefix should NOT be called because index is referenced by snapshot
	assert.False(t, removeWithPrefixCalled, "RemoveWithPrefix should not be called for index files referenced by snapshot")
}

func TestGarbageCollector_recycleUnusedTextIndexFiles_SnapshotReference(t *testing.T) {
	ctx := context.Background()

	// Create segment with TextStatsLogs
	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            1001,
			CollectionID:  100,
			PartitionID:   10,
			State:         commonpb.SegmentState_Flushed,
			InsertChannel: "ch1",
			TextStatsLogs: map[int64]*datapb.TextIndexStats{
				101: {
					FieldID:    101,
					Version:    2, // Current version
					BuildID:    401,
					Files:      []string{"file1"},
					LogSize:    1024,
					MemorySize: 2048,
				},
			},
		},
	}

	// Create meta
	meta := &meta{
		catalog:      &datacoord.Catalog{},
		snapshotMeta: &snapshotMeta{},
		segments: &SegmentsInfo{
			segments: map[int64]*SegmentInfo{1001: segment},
		},
		channelCPs: newChannelCps(),
	}

	// Create storage manager
	cli := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test"))

	gc := newGarbageCollector(meta, &ServerHandler{}, GcOption{
		cli:              cli,
		enabled:          true,
		checkInterval:    time.Millisecond * 10,
		scanInterval:     time.Hour * 7 * 24,
		missingTolerance: 0,
		dropTolerance:    time.Hour * 24,
	})

	// Track Remove calls
	removedFiles := []string{}
	mockRemove := mockey.Mock((*storage.LocalChunkManager).Remove).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, filePath string) error {
			removedFiles = append(removedFiles, filePath)
			return nil
		}).Build()
	defer mockRemove.UnPatch()

	// Mock WalkWithPrefix to simulate text index files. Production builds the prefix via
	// metautil.BuildTextIndexPrefix which uses common.TextIndexPath ("text_log"), so the
	// substring match MUST reference that constant — a human-readable string like
	// "text_index" would silently never match and hide regressions.
	mockWalk := mockey.Mock((*storage.LocalChunkManager).WalkWithPrefix).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, prefix string,
			recursive bool, fn storage.ChunkObjectWalkFunc,
		) error {
			if strings.Contains(prefix, common.TextIndexPath) {
				chunkInfo := &storage.ChunkObjectInfo{
					FilePath:   "gc/text_log/401/1/100/10/1001/101/file1",
					ModifyTime: time.Now().Add(-time.Hour),
				}
				fn(chunkInfo)
			}
			return nil
		}).Build()
	defer mockWalk.UnPatch()

	// New O(1) API: IsSegmentGCBlocked returns true (segment protected by snapshot).
	mockIsSegBlocked := mockey.Mock((*snapshotMeta).IsSegmentGCBlocked).Return(true).Build()
	defer mockIsSegBlocked.UnPatch()

	// Execute
	gc.recycleUnusedTextIndexFiles(ctx, nil)

	// Verify - files should NOT be removed
	assert.Empty(t, removedFiles,
		"text index files should not be removed when segment is referenced by snapshot")
}

func TestGarbageCollector_recycleUnusedJSONIndexFiles_SnapshotReference(t *testing.T) {
	ctx := context.Background()

	// Create segment with JsonKeyStats
	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            1002,
			CollectionID:  100,
			PartitionID:   10,
			State:         commonpb.SegmentState_Flushed,
			InsertChannel: "ch1",
			JsonKeyStats: map[int64]*datapb.JsonKeyStats{
				102: {
					FieldID:    102,
					Version:    2,
					BuildID:    402,
					Files:      []string{"json_file1"},
					LogSize:    512,
					MemorySize: 1024,
				},
			},
		},
	}

	// Create meta
	meta := &meta{
		catalog:      &datacoord.Catalog{},
		snapshotMeta: &snapshotMeta{},
		segments: &SegmentsInfo{
			segments: map[int64]*SegmentInfo{1002: segment},
		},
		channelCPs: newChannelCps(),
	}

	// Create storage manager
	cli := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test"))

	gc := newGarbageCollector(meta, &ServerHandler{}, GcOption{
		cli:              cli,
		enabled:          true,
		checkInterval:    time.Millisecond * 10,
		scanInterval:     time.Hour * 7 * 24,
		missingTolerance: 0,
		dropTolerance:    time.Hour * 24,
	})

	// Track Remove calls
	removedFiles := []string{}
	mockRemove := mockey.Mock((*storage.LocalChunkManager).Remove).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, filePath string) error {
			removedFiles = append(removedFiles, filePath)
			return nil
		}).Build()
	defer mockRemove.UnPatch()

	// Mock WalkWithPrefix to simulate JSON index files. Production builds the prefix using
	// common.JSONIndexPath ("json_key_index_log"), so the substring match MUST reference
	// that constant — a shorthand like "json_index" would silently never match.
	mockWalk := mockey.Mock((*storage.LocalChunkManager).WalkWithPrefix).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, prefix string,
			recursive bool, fn storage.ChunkObjectWalkFunc,
		) error {
			if strings.Contains(prefix, common.JSONIndexPath) {
				chunkInfo := &storage.ChunkObjectInfo{
					FilePath:   "gc/json_key_index_log/402/1/100/10/1002/102/json_file1",
					ModifyTime: time.Now().Add(-time.Hour),
				}
				fn(chunkInfo)
			}
			return nil
		}).Build()
	defer mockWalk.UnPatch()

	// New O(1) API: IsSegmentGCBlocked returns true (segment protected by snapshot).
	mockIsSegBlocked := mockey.Mock((*snapshotMeta).IsSegmentGCBlocked).Return(true).Build()
	defer mockIsSegBlocked.UnPatch()

	// Execute
	gc.recycleUnusedJSONIndexFiles(ctx, nil)

	// Verify - files should NOT be removed
	assert.Empty(t, removedFiles,
		"JSON index files should not be removed when segment is referenced by snapshot")
}

// TestGarbageCollector_recycleUnusedBinlogFiles_SkipWhenRefIndexNotLoaded tests that binlog GC
// is skipped when the unified IsSegmentGCBlocked returns true due to an unloaded RefIndex
// (fail-closed coarse block during the startup window).
func TestGarbageCollector_recycleUnusedBinlogFiles_SkipWhenRefIndexNotLoaded(t *testing.T) {
	ctx := context.Background()

	cli := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test"))

	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            1001,
			CollectionID:  100,
			PartitionID:   10,
			State:         commonpb.SegmentState_Flushed,
			InsertChannel: "ch1",
		},
	}

	meta := &meta{
		catalog:      &datacoord.Catalog{},
		snapshotMeta: &snapshotMeta{},
		indexMeta:    &indexMeta{},
		segments: &SegmentsInfo{
			segments: map[int64]*SegmentInfo{1001: segment},
		},
		channelCPs: newChannelCps(),
	}

	gc := newGarbageCollector(meta, &ServerHandler{}, GcOption{
		cli:              cli,
		enabled:          true,
		checkInterval:    time.Millisecond * 10,
		scanInterval:     time.Hour * 7 * 24,
		missingTolerance: 0,
		dropTolerance:    time.Hour * 24,
	})

	removedFiles := []string{}
	mockRemove := mockey.Mock((*storage.LocalChunkManager).Remove).To(func(cm *storage.LocalChunkManager, ctx context.Context, filePath string) error {
		removedFiles = append(removedFiles, filePath)
		return nil
	}).Build()
	defer mockRemove.UnPatch()

	chunkInfo := &storage.ChunkObjectInfo{
		FilePath:   "gc/segments/insert_logs/1001/0/1",
		ModifyTime: time.Now().Add(-time.Hour),
	}
	mockWalk := mockey.Mock((*storage.LocalChunkManager).WalkWithPrefix).To(func(cm *storage.LocalChunkManager, ctx context.Context, prefix string, recursive bool, fn storage.ChunkObjectWalkFunc) error {
		fn(chunkInfo)
		return nil
	}).Build()
	defer mockWalk.UnPatch()

	mockParseSegID := mockey.Mock(storage.ParseSegmentIDByBinlog).Return(int64(1001), nil).Build()
	defer mockParseSegID.UnPatch()

	// New O(1) API: IsSegmentGCBlocked returns true because the collection's RefIndex
	// is unloaded (fail-closed coarse block during startup window).
	mockIsSegBlocked := mockey.Mock((*snapshotMeta).IsSegmentGCBlocked).Return(true).Build()
	defer mockIsSegBlocked.UnPatch()

	gc.recycleUnusedBinlogFiles(ctx)

	assert.Empty(t, removedFiles, "binlog files should not be removed when RefIndex is not loaded")
}

// TestGarbageCollector_recycleUnusedTextIndexFiles_SkipWhenRefIndexNotLoaded tests that text index GC
// is skipped when the unified IsSegmentGCBlocked returns true due to an unloaded RefIndex.
func TestGarbageCollector_recycleUnusedTextIndexFiles_SkipWhenRefIndexNotLoaded(t *testing.T) {
	ctx := context.Background()

	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            1001,
			CollectionID:  100,
			PartitionID:   10,
			State:         commonpb.SegmentState_Flushed,
			InsertChannel: "ch1",
			TextStatsLogs: map[int64]*datapb.TextIndexStats{
				101: {
					FieldID:    101,
					Version:    2,
					BuildID:    401,
					Files:      []string{"file1"},
					LogSize:    1024,
					MemorySize: 2048,
				},
			},
		},
	}

	meta := &meta{
		catalog:      &datacoord.Catalog{},
		snapshotMeta: &snapshotMeta{},
		segments: &SegmentsInfo{
			segments: map[int64]*SegmentInfo{1001: segment},
		},
		channelCPs: newChannelCps(),
	}

	cli := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test"))

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

	// Substring match references common.TextIndexPath ("text_log") to stay in sync with
	// production metautil.BuildTextIndexPrefix — a shorthand would silently never match.
	mockWalk := mockey.Mock((*storage.LocalChunkManager).WalkWithPrefix).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, prefix string,
			recursive bool, fn storage.ChunkObjectWalkFunc,
		) error {
			if strings.Contains(prefix, common.TextIndexPath) {
				chunkInfo := &storage.ChunkObjectInfo{
					FilePath:   "gc/text_log/401/1/100/10/1001/101/file1",
					ModifyTime: time.Now().Add(-time.Hour),
				}
				fn(chunkInfo)
			}
			return nil
		}).Build()
	defer mockWalk.UnPatch()

	// New O(1) API: IsSegmentGCBlocked returns true due to unloaded RefIndex (fail-closed).
	mockIsSegBlocked := mockey.Mock((*snapshotMeta).IsSegmentGCBlocked).Return(true).Build()
	defer mockIsSegBlocked.UnPatch()

	gc.recycleUnusedTextIndexFiles(ctx, nil)

	assert.Empty(t, removedFiles, "text index files should not be removed when RefIndex is not loaded")
}

// TestGarbageCollector_recycleUnusedJSONIndexFiles_SkipWhenRefIndexNotLoaded tests that JSON index GC
// is skipped when the unified IsSegmentGCBlocked returns true due to an unloaded RefIndex.
func TestGarbageCollector_recycleUnusedJSONIndexFiles_SkipWhenRefIndexNotLoaded(t *testing.T) {
	ctx := context.Background()

	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            1002,
			CollectionID:  100,
			PartitionID:   10,
			State:         commonpb.SegmentState_Flushed,
			InsertChannel: "ch1",
			JsonKeyStats: map[int64]*datapb.JsonKeyStats{
				102: {
					FieldID:    102,
					Version:    2,
					BuildID:    402,
					Files:      []string{"json_file1"},
					LogSize:    512,
					MemorySize: 1024,
				},
			},
		},
	}

	meta := &meta{
		catalog:      &datacoord.Catalog{},
		snapshotMeta: &snapshotMeta{},
		segments: &SegmentsInfo{
			segments: map[int64]*SegmentInfo{1002: segment},
		},
		channelCPs: newChannelCps(),
	}

	cli := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test"))

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

	// Substring match references common.JSONIndexPath ("json_key_index_log") to stay in
	// sync with production — a shorthand like "json_index" would silently never match.
	mockWalk := mockey.Mock((*storage.LocalChunkManager).WalkWithPrefix).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, prefix string,
			recursive bool, fn storage.ChunkObjectWalkFunc,
		) error {
			if strings.Contains(prefix, common.JSONIndexPath) {
				chunkInfo := &storage.ChunkObjectInfo{
					FilePath:   "gc/json_key_index_log/402/1/100/10/1002/102/json_file1",
					ModifyTime: time.Now().Add(-time.Hour),
				}
				fn(chunkInfo)
			}
			return nil
		}).Build()
	defer mockWalk.UnPatch()

	// New O(1) API: IsSegmentGCBlocked returns true due to unloaded RefIndex (fail-closed).
	mockIsSegBlocked := mockey.Mock((*snapshotMeta).IsSegmentGCBlocked).Return(true).Build()
	defer mockIsSegBlocked.UnPatch()

	gc.recycleUnusedJSONIndexFiles(ctx, nil)

	assert.Empty(t, removedFiles, "JSON index files should not be removed when RefIndex is not loaded")
}

// TestGarbageCollector_recycleUnusedIndexFiles_SegIdxNil_SnapshotProtection tests that when segIdx is nil
// (buildID no longer in meta), index files are NOT deleted if a snapshot references the buildID.
func TestGarbageCollector_recycleUnusedIndexFiles_SegIdxNil_SnapshotProtection(t *testing.T) {
	ctx := context.Background()

	cli := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test"))

	smMeta := &snapshotMeta{}
	meta := &meta{
		catalog:      &datacoord.Catalog{},
		snapshotMeta: smMeta,
		indexMeta:    &indexMeta{},
		segments: &SegmentsInfo{
			segments: make(map[int64]*SegmentInfo),
		},
		channelCPs: newChannelCps(),
	}

	gc := newGarbageCollector(meta, &ServerHandler{}, GcOption{
		cli:              cli,
		enabled:          true,
		checkInterval:    time.Millisecond * 10,
		scanInterval:     time.Hour * 7 * 24,
		missingTolerance: time.Hour * 24,
		dropTolerance:    0,
	})

	buildID := int64(501)

	mockWalk := mockey.Mock((*storage.LocalChunkManager).WalkWithPrefix).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, prefix string, recursive bool, fn storage.ChunkObjectWalkFunc) error {
			// Outer walk listing buildID directories
			chunkInfo := &storage.ChunkObjectInfo{
				FilePath:   fmt.Sprintf("gc/index_files/%d/", buildID),
				ModifyTime: time.Now().Add(-time.Hour),
			}
			fn(chunkInfo)
			return nil
		}).Build()
	defer mockWalk.UnPatch()

	// Mock CheckCleanSegmentIndex to return (true, nil) — buildID not in meta
	mockCheckClean := mockey.Mock((*indexMeta).CheckCleanSegmentIndex).Return(true, nil).Build()
	defer mockCheckClean.UnPatch()

	// Mock parseBuildIDFromFilePath
	mockParseBuildID := mockey.Mock(parseBuildIDFromFilePath).Return(buildID, nil).Build()
	defer mockParseBuildID.UnPatch()

	// Mock GetSnapshotMeta to return smMeta
	mockGetSnapMeta := mockey.Mock(meta.GetSnapshotMeta).Return(smMeta).Build()
	defer mockGetSnapMeta.UnPatch()

	// New O(1) API: mock IsBuildIDGCBlocked to return true for our buildID (the orphan
	// branch passes collectionID = -1 since it has no segIdx context).
	mockIsBuildIDBlocked := mockey.Mock((*snapshotMeta).IsBuildIDGCBlocked).To(
		func(sm *snapshotMeta, collID, bid int64) bool {
			return bid == buildID
		}).Build()
	defer mockIsBuildIDBlocked.UnPatch()

	removeWithPrefixCalled := false
	mockRemoveWithPrefix := mockey.Mock((*storage.LocalChunkManager).RemoveWithPrefix).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, prefix string) error {
			removeWithPrefixCalled = true
			return nil
		}).Build()
	defer mockRemoveWithPrefix.UnPatch()

	gc.recycleUnusedIndexFiles(ctx)

	assert.False(t, removeWithPrefixCalled,
		"RemoveWithPrefix should not be called for index files when segIdx is nil but buildID is snapshot-referenced")
}

// TestGarbageCollector_recycleUnusedBinlogFiles_SegmentNil_SnapshotProtection tests that binlog files
// are NOT deleted when segment meta is nil but the segment is referenced by a snapshot.
// This verifies the fix: collectionID=-1 scans all collections when segment is nil.
func TestGarbageCollector_recycleUnusedBinlogFiles_SegmentNil_SnapshotProtection(t *testing.T) {
	ctx := context.Background()

	cli := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test"))

	// Create meta with NO segments (segment is nil)
	meta := &meta{
		catalog:      &datacoord.Catalog{},
		snapshotMeta: &snapshotMeta{},
		indexMeta:    &indexMeta{},
		segments: &SegmentsInfo{
			segments: make(map[int64]*SegmentInfo),
		},
		channelCPs: newChannelCps(),
	}

	gc := newGarbageCollector(meta, &ServerHandler{}, GcOption{
		cli:              cli,
		enabled:          true,
		checkInterval:    time.Millisecond * 10,
		scanInterval:     time.Hour * 7 * 24,
		missingTolerance: 0,
		dropTolerance:    time.Hour * 24,
	})

	// Track if Remove is called
	removeCalledPaths := []string{}
	mockRemove := mockey.Mock((*storage.LocalChunkManager).Remove).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, filePath string) error {
			removeCalledPaths = append(removeCalledPaths, filePath)
			return nil
		}).Build()
	defer mockRemove.UnPatch()

	// Mock WalkWithPrefix to return a file
	chunkInfo := &storage.ChunkObjectInfo{
		FilePath:   "gc/insert_log/100/10/1001/101/10000001",
		ModifyTime: time.Now().Add(-time.Hour),
	}
	mockWalk := mockey.Mock((*storage.LocalChunkManager).WalkWithPrefix).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, prefix string, recursive bool, fn storage.ChunkObjectWalkFunc) error {
			fn(chunkInfo)
			return nil
		}).Build()
	defer mockWalk.UnPatch()

	// Mock ParseSegmentIDByBinlog to return segmentID 1001
	mockParseSegID := mockey.Mock(storage.ParseSegmentIDByBinlog).Return(int64(1001), nil).Build()
	defer mockParseSegID.UnPatch()

	// New O(1) API: IsSegmentGCBlocked returns true so the file is kept.
	// The caller passes collectionID = -1 (segment is nil in this test), which exercises
	// the "global fail-closed" branch of the new API — but here we short-circuit to true
	// directly to simulate a snapshot referencing segment 1001.
	mockIsSegBlocked := mockey.Mock((*snapshotMeta).IsSegmentGCBlocked).Return(true).Build()
	defer mockIsSegBlocked.UnPatch()

	gc.recycleUnusedBinlogFiles(ctx)

	assert.Empty(t, removeCalledPaths,
		"binlog files should not be removed when segment is nil but snapshot references the segment")
}

// TestGarbageCollector_recycleUnusedJSONStatsFiles_SnapshotReference tests that JSON stats files
// are NOT deleted when the segment is referenced by a snapshot.
func TestGarbageCollector_recycleUnusedJSONStatsFiles_SnapshotReference(t *testing.T) {
	ctx := context.Background()

	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            1002,
			CollectionID:  100,
			PartitionID:   10,
			State:         commonpb.SegmentState_Flushed,
			InsertChannel: "ch1",
			JsonKeyStats: map[int64]*datapb.JsonKeyStats{
				102: {
					FieldID:    102,
					Version:    2,
					BuildID:    402,
					Files:      []string{"json_stats_file1"},
					LogSize:    512,
					MemorySize: 1024,
				},
			},
		},
	}

	meta := &meta{
		catalog:      &datacoord.Catalog{},
		snapshotMeta: &snapshotMeta{},
		segments: &SegmentsInfo{
			segments: map[int64]*SegmentInfo{1002: segment},
		},
		channelCPs: newChannelCps(),
	}

	cli := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test"))

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
		func(cm *storage.LocalChunkManager, ctx context.Context, prefix string,
			recursive bool, fn storage.ChunkObjectWalkFunc,
		) error {
			if strings.Contains(prefix, "json_stats") {
				chunkInfo := &storage.ChunkObjectInfo{
					FilePath:   "gc/json_stats/1/402/1/100/10/1002/102/json_stats_file1",
					ModifyTime: time.Now().Add(-time.Hour),
				}
				fn(chunkInfo)
			}
			return nil
		}).Build()
	defer mockWalk.UnPatch()

	// New O(1) API: IsSegmentGCBlocked returns true for the referenced segment.
	mockIsSegBlocked := mockey.Mock((*snapshotMeta).IsSegmentGCBlocked).Return(true).Build()
	defer mockIsSegBlocked.UnPatch()

	gc.recycleUnusedJSONStatsFiles(ctx, nil)

	assert.Empty(t, removedFiles,
		"JSON stats files should not be removed when segment is referenced by snapshot")
}

func Test_parseV3SegmentID(t *testing.T) {
	rootPath := "files"

	t.Run("valid V3 data path", func(t *testing.T) {
		segID, err := parseV3SegmentID(rootPath, "files/insert_log/1/10/100/_data/0_abc.parquet")
		assert.NoError(t, err)
		assert.Equal(t, int64(100), segID)
	})

	t.Run("valid V3 stats path", func(t *testing.T) {
		segID, err := parseV3SegmentID(rootPath, "files/insert_log/1/10/200/_stats/bloom_filter.101/42")
		assert.NoError(t, err)
		assert.Equal(t, int64(200), segID)
	})

	t.Run("valid V3 delta path", func(t *testing.T) {
		segID, err := parseV3SegmentID(rootPath, "files/insert_log/1/10/300/_delta/501")
		assert.NoError(t, err)
		assert.Equal(t, int64(300), segID)
	})

	t.Run("valid V3 metadata path", func(t *testing.T) {
		segID, err := parseV3SegmentID(rootPath, "files/insert_log/1/10/400/_metadata/manifest-5.avro")
		assert.NoError(t, err)
		assert.Equal(t, int64(400), segID)
	})

	t.Run("not insert_log prefix", func(t *testing.T) {
		_, err := parseV3SegmentID(rootPath, "files/delta_log/1/10/100/501")
		assert.Error(t, err)
	})

	t.Run("too few parts", func(t *testing.T) {
		_, err := parseV3SegmentID(rootPath, "files/insert_log/1/10")
		assert.Error(t, err)
	})

	t.Run("wrong rootPath", func(t *testing.T) {
		_, err := parseV3SegmentID("other_root", "files/insert_log/1/10/100/_data/file")
		assert.Error(t, err)
	})

	t.Run("non-numeric segmentID", func(t *testing.T) {
		_, err := parseV3SegmentID(rootPath, "files/insert_log/1/10/not-a-number/_data/file")
		assert.Error(t, err)
	})
}

func TestGarbageCollector_recycleDroppedSegments_V3(t *testing.T) {
	ctx := context.Background()

	cli := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test-gc-v3"))

	catalog := &datacoord.Catalog{}
	smMeta := &snapshotMeta{}

	m := &meta{
		catalog:      catalog,
		snapshotMeta: smMeta,
		segments: &SegmentsInfo{
			segments: make(map[int64]*SegmentInfo),
		},
		channelCPs: newChannelCps(),
	}

	basePath := "/tmp/test-gc-v3/insert_log/100/10/2001"
	manifestPath := packed.MarshalManifestPath(basePath, 1)

	// V3 dropped segment with ManifestPath and StorageVersion=3
	v3Segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             2001,
			CollectionID:   100,
			PartitionID:    10,
			State:          commonpb.SegmentState_Dropped,
			DroppedAt:      uint64(time.Now().Add(-time.Hour).UnixNano()),
			InsertChannel:  "ch1",
			StorageVersion: storage.StorageV3,
			ManifestPath:   manifestPath,
		},
	}

	// V1 dropped segment (no ManifestPath, StorageVersion=0)
	v1Segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            2002,
			CollectionID:  100,
			PartitionID:   10,
			State:         commonpb.SegmentState_Dropped,
			DroppedAt:     uint64(time.Now().Add(-time.Hour).UnixNano()),
			InsertChannel: "ch1",
			Binlogs: []*datapb.FieldBinlog{
				{
					FieldID: 1,
					Binlogs: []*datapb.Binlog{{LogPath: "log1", LogSize: 100}},
				},
			},
		},
	}

	m.segments.segments[2001] = v3Segment
	m.segments.segments[2002] = v1Segment

	gc := newGarbageCollector(m, &ServerHandler{}, GcOption{
		cli:              cli,
		enabled:          true,
		checkInterval:    time.Millisecond * 10,
		scanInterval:     time.Hour * 7 * 24,
		missingTolerance: time.Hour * 24,
		dropTolerance:    0,
	})

	// Track calls
	removeWithPrefixCalled := false
	var removeWithPrefixArg string
	removeObjectFilesCalled := false
	droppedSegmentIDs := []int64{}

	// Snapshot layer transparent: no segment is blocked.
	mockIsSegBlocked := mockey.Mock((*snapshotMeta).IsSegmentGCBlocked).Return(false).Build()
	defer mockIsSegBlocked.UnPatch()
	mockListLoaded := mockey.Mock((*ServerHandler).ListLoadedSegments).Return([]int64{}, nil).Build()
	defer mockListLoaded.UnPatch()
	mockChannelExists := mockey.Mock((*datacoord.Catalog).ChannelExists).Return(true).Build()
	defer mockChannelExists.UnPatch()
	mockDropSegment := mockey.Mock((*datacoord.Catalog).DropSegment).To(func(c *datacoord.Catalog, ctx context.Context, segment *datapb.SegmentInfo) error {
		droppedSegmentIDs = append(droppedSegmentIDs, segment.ID)
		return nil
	}).Build()
	defer mockDropSegment.UnPatch()

	// Mock RemoveWithPrefix for V3 segment
	mockRemoveWithPrefix := mockey.Mock((*storage.LocalChunkManager).RemoveWithPrefix).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, prefix string) error {
			removeWithPrefixCalled = true
			removeWithPrefixArg = prefix
			return nil
		}).Build()
	defer mockRemoveWithPrefix.UnPatch()

	// Mock removeObjectFiles for V1 segment
	mockRemoveObjectFiles := mockey.Mock((*garbageCollector).removeObjectFiles).To(
		func(gc *garbageCollector, ctx context.Context, logs map[string]struct{}) error {
			removeObjectFilesCalled = true
			return nil
		}).Build()
	defer mockRemoveObjectFiles.UnPatch()

	gc.recycleDroppedSegments(ctx, nil)

	// V3 segment should use RemoveWithPrefix with basePath
	assert.True(t, removeWithPrefixCalled, "V3 segment should use RemoveWithPrefix")
	assert.Equal(t, basePath, removeWithPrefixArg, "RemoveWithPrefix should be called with basePath")

	// V1 segment should use removeObjectFiles
	assert.True(t, removeObjectFilesCalled, "V1 segment should use removeObjectFiles")

	// Both segments should be dropped from meta
	assert.Contains(t, droppedSegmentIDs, int64(2001))
	assert.Contains(t, droppedSegmentIDs, int64(2002))
	assert.Nil(t, m.GetSegment(ctx, 2001))
	assert.Nil(t, m.GetSegment(ctx, 2002))
}

func TestGarbageCollector_recycleUnusedBinlogFiles_SkipV3(t *testing.T) {
	ctx := context.Background()

	rootPath := "gc"
	cli := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test-gc-v3-orphan"))

	m := &meta{
		segments: &SegmentsInfo{
			segments: make(map[int64]*SegmentInfo),
		},
		channelCPs:   newChannelCps(),
		snapshotMeta: &snapshotMeta{},
	}

	// Add a V3 segment to meta
	v3Segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             500,
			CollectionID:   1,
			PartitionID:    10,
			State:          commonpb.SegmentState_Flushed,
			StorageVersion: storage.StorageV3,
			ManifestPath:   packed.MarshalManifestPath(rootPath+"/insert_log/1/10/500", 1),
		},
	}
	m.segments.segments[500] = v3Segment

	gc := newGarbageCollector(m, &ServerHandler{}, GcOption{
		cli:              cli,
		enabled:          true,
		checkInterval:    time.Millisecond * 10,
		scanInterval:     time.Hour * 7 * 24,
		missingTolerance: 0,
		dropTolerance:    0,
	})

	removedFiles := []string{}

	// Snapshot layer transparent: no segment is blocked.
	mockIsSegBlocked := mockey.Mock((*snapshotMeta).IsSegmentGCBlocked).Return(false).Build()
	defer mockIsSegBlocked.UnPatch()

	// Mock WalkWithPrefix to return V3 files under insert_log
	mockWalk := mockey.Mock((*storage.LocalChunkManager).WalkWithPrefix).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, prefix string,
			recursive bool, fn storage.ChunkObjectWalkFunc,
		) error {
			if strings.Contains(prefix, common.SegmentInsertLogPath) {
				// V3 file that matches 6-part format (ParseSegmentIDByBinlog will succeed)
				fn(&storage.ChunkObjectInfo{
					FilePath:   rootPath + "/insert_log/1/10/500/0/data.parquet",
					ModifyTime: time.Now().Add(-time.Hour),
				})
				// V3 file under _data/ (ParseSegmentIDByBinlog will fail, fallback to parseV3SegmentID)
				fn(&storage.ChunkObjectInfo{
					FilePath:   rootPath + "/insert_log/1/10/500/_data/0_abc.parquet",
					ModifyTime: time.Now().Add(-time.Hour),
				})
				// V3 file under _stats/ (ParseSegmentIDByBinlog will fail)
				fn(&storage.ChunkObjectInfo{
					FilePath:   rootPath + "/insert_log/1/10/500/_stats/bloom_filter.101/42",
					ModifyTime: time.Now().Add(-time.Hour),
				})
				// V3 file under _delta/ (ParseSegmentIDByBinlog will fail)
				fn(&storage.ChunkObjectInfo{
					FilePath:   rootPath + "/insert_log/1/10/500/_delta/501",
					ModifyTime: time.Now().Add(-time.Hour),
				})
				// V3 file under _metadata/ (ParseSegmentIDByBinlog will fail)
				fn(&storage.ChunkObjectInfo{
					FilePath:   rootPath + "/insert_log/1/10/500/_metadata/manifest-1.avro",
					ModifyTime: time.Now().Add(-time.Hour),
				})
			}
			return nil
		}).Build()
	defer mockWalk.UnPatch()

	mockRemove := mockey.Mock((*storage.LocalChunkManager).Remove).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, filePath string) error {
			removedFiles = append(removedFiles, filePath)
			return nil
		}).Build()
	defer mockRemove.UnPatch()

	gc.recycleUnusedBinlogFiles(ctx)

	// None of the V3 files should be removed — they are managed by loon
	assert.Empty(t, removedFiles, "V3 segment files should not be removed by orphan scan")
}

func TestGarbageCollector_recycleSnapshots_OrphanCleanup(t *testing.T) {
	ctx := context.Background()

	setupGCTest := func(t *testing.T, sm *snapshotMeta, broker *broker2.MockBroker) *garbageCollector {
		m := &meta{
			snapshotMeta: sm,
			segments:     &SegmentsInfo{segments: make(map[int64]*SegmentInfo)},
			channelCPs:   newChannelCps(),
		}
		gc := newGarbageCollector(m, newMockHandlerWithMeta(m), GcOption{broker: broker})
		return gc
	}

	t.Run("orphan_collection_deleted", func(t *testing.T) {
		// Snapshot belongs to a dropped collection → should be cleaned up
		sm := createTestSnapshotMeta(t)
		insertTestSnapshot(sm, &datapb.SnapshotInfo{
			Id:           1,
			Name:         "orphan_snap",
			CollectionId: 100,
			State:        datapb.SnapshotState_SnapshotStateCommitted,
		}, nil, nil)

		broker := broker2.NewMockBroker(t)
		broker.EXPECT().HasCollection(mock.Anything, int64(100)).Return(false, nil).Once()
		gc := setupGCTest(t, sm, broker)

		// Mock catalog calls to skip pending/deleting cleanup, reach orphan cleanup
		mockGetPending := mockey.Mock((*snapshotMeta).GetPendingSnapshots).Return(nil, nil).Build()
		defer mockGetPending.UnPatch()
		mockGetDeleting := mockey.Mock((*snapshotMeta).GetDeletingSnapshots).Return(nil, nil).Build()
		defer mockGetDeleting.UnPatch()

		// Mock DropSnapshot internals for DropSnapshotsByCollection
		mockSave := mockey.Mock((*datacoord.Catalog).SaveSnapshot).Return(nil).Build()
		defer mockSave.UnPatch()
		mockDropCatalog := mockey.Mock((*datacoord.Catalog).DropSnapshot).Return(nil).Build()
		defer mockDropCatalog.UnPatch()
		mockWriter := mockey.Mock((*SnapshotWriter).Drop).Return(nil).Build()
		defer mockWriter.UnPatch()

		gc.recycleSnapshots(ctx, nil)

		// Verify orphan snapshot was cleaned up
		_, exists := sm.snapshotID2Info.Get(int64(1))
		assert.False(t, exists, "orphan snapshot should be deleted")
	})

	t.Run("collection_still_exists", func(t *testing.T) {
		// Snapshot belongs to an active collection → should NOT be cleaned up
		sm := createTestSnapshotMeta(t)
		insertTestSnapshot(sm, &datapb.SnapshotInfo{
			Id:           2,
			Name:         "active_snap",
			CollectionId: 200,
			State:        datapb.SnapshotState_SnapshotStateCommitted,
		}, nil, nil)

		broker := broker2.NewMockBroker(t)
		broker.EXPECT().HasCollection(mock.Anything, int64(200)).Return(true, nil).Once()
		gc := setupGCTest(t, sm, broker)

		mockGetPending := mockey.Mock((*snapshotMeta).GetPendingSnapshots).Return(nil, nil).Build()
		defer mockGetPending.UnPatch()
		mockGetDeleting := mockey.Mock((*snapshotMeta).GetDeletingSnapshots).Return(nil, nil).Build()
		defer mockGetDeleting.UnPatch()

		gc.recycleSnapshots(ctx, nil)

		// Verify snapshot was NOT deleted
		_, exists := sm.snapshotID2Info.Get(int64(2))
		assert.True(t, exists, "snapshot of active collection should not be deleted")
	})

	t.Run("live_collection_reaps_expired_pins_and_keeps_snapshot", func(t *testing.T) {
		// Regression: the GC hoist moved cleanExpiredPinsForCollection in front
		// of the HasCollection check so active collections also get their
		// expired pins reaped — previously they accumulated forever on live
		// collections. Assert that both effects happen: expired pin removed,
		// snapshot still present.
		sm := createTestSnapshotMeta(t)
		now := time.Now().UnixMilli()
		insertTestSnapshot(sm, &datapb.SnapshotInfo{
			Id:            4,
			Name:          "live_with_expired",
			CollectionId:  400,
			State:         datapb.SnapshotState_SnapshotStateCommitted,
			PinIds:        []int64{9001, 9002},
			PinExpireAtMs: map[int64]int64{9001: now - 1000, 9002: now + 3600000},
		}, nil, nil)

		broker := broker2.NewMockBroker(t)
		broker.EXPECT().HasCollection(mock.Anything, int64(400)).Return(true, nil).Once()
		gc := setupGCTest(t, sm, broker)

		mockGetPending := mockey.Mock((*snapshotMeta).GetPendingSnapshots).Return(nil, nil).Build()
		defer mockGetPending.UnPatch()
		mockGetDeleting := mockey.Mock((*snapshotMeta).GetDeletingSnapshots).Return(nil, nil).Build()
		defer mockGetDeleting.UnPatch()
		// cleanExpiredPinsForCollection persists the reap via SaveSnapshot.
		mockSave := mockey.Mock((*datacoord.Catalog).SaveSnapshot).Return(nil).Build()
		defer mockSave.UnPatch()

		gc.recycleSnapshots(ctx, nil)

		// Snapshot must still exist (live collection, not orphaned).
		updated, exists := sm.snapshotID2Info.Get(int64(4))
		assert.True(t, exists, "snapshot of live collection must not be deleted")
		// Expired pin 9001 reaped; active pin 9002 preserved.
		assert.ElementsMatch(t, []int64{9002}, updated.GetPinIds(),
			"expired pin must be removed, active pin preserved")
		_, hasExpired := updated.GetPinExpireAtMs()[9001]
		assert.False(t, hasExpired, "expired pin's expiry entry must be removed")
	})

	t.Run("has_collection_error_skips", func(t *testing.T) {
		// HasCollection returns error → should skip, not delete
		sm := createTestSnapshotMeta(t)
		insertTestSnapshot(sm, &datapb.SnapshotInfo{
			Id:           3,
			Name:         "err_snap",
			CollectionId: 300,
			State:        datapb.SnapshotState_SnapshotStateCommitted,
		}, nil, nil)

		broker := broker2.NewMockBroker(t)
		broker.EXPECT().HasCollection(mock.Anything, int64(300)).Return(false, fmt.Errorf("rpc unavailable")).Once()
		gc := setupGCTest(t, sm, broker)

		mockGetPending := mockey.Mock((*snapshotMeta).GetPendingSnapshots).Return(nil, nil).Build()
		defer mockGetPending.UnPatch()
		mockGetDeleting := mockey.Mock((*snapshotMeta).GetDeletingSnapshots).Return(nil, nil).Build()
		defer mockGetDeleting.UnPatch()

		gc.recycleSnapshots(ctx, nil)

		// Verify snapshot was NOT deleted (error → skip)
		_, exists := sm.snapshotID2Info.Get(int64(3))
		assert.True(t, exists, "snapshot should not be deleted when HasCollection fails")
	})
}
