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
	"errors"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	kvmocks "github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type GarbageCollectorSuite struct {
	suite.Suite

	mockChunkManager *mocks.ChunkManager
	gc               *garbageCollector
}

func (s *GarbageCollectorSuite) SetupTest() {
	meta, err := newMemoryMeta()
	s.Require().NoError(err)
	s.mockChunkManager = &mocks.ChunkManager{}

	mockKV := &kvmocks.TxnKV{}
	mockKV.EXPECT().LoadWithPrefix(mock.Anything).Return([]string{}, []string{}, nil)
	segRefer, err := NewSegmentReferenceManager(mockKV, nil)

	s.Require().NoError(err)
	s.Require().NotNil(segRefer)

	s.gc = newGarbageCollector(
		meta, newMockHandler(), segRefer, &mocks.MockIndexCoord{}, GcOption{
			cli:              s.mockChunkManager,
			enabled:          true,
			checkInterval:    time.Millisecond * 10,
			missingTolerance: time.Hour * 24,
			dropTolerance:    time.Hour * 24,
		},
	)

}

func (s *GarbageCollectorSuite) TearDownTest() {
	s.mockChunkManager = nil
	s.gc.close()
	s.gc = nil
}

func (s *GarbageCollectorSuite) TestBasicOperation() {
	s.Run("normal_gc", func() {
		gc := s.gc
		s.mockChunkManager.EXPECT().RootPath().Return("files")
		s.mockChunkManager.EXPECT().ListWithPrefix(mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("bool")).
			Return([]string{}, []time.Time{}, nil)
		gc.start()
		// make ticker run at least once
		time.Sleep(time.Millisecond * 20)

		s.NotPanics(func() {
			gc.close()
		})
	})

	s.Run("nil_client", func() {
		// initial a new garbageCollector here
		mockKV := &kvmocks.TxnKV{}
		mockKV.EXPECT().LoadWithPrefix(mock.Anything).Return([]string{}, []string{}, nil)
		segRefer, err := NewSegmentReferenceManager(mockKV, nil)
		s.Require().NoError(err)

		gc := newGarbageCollector(nil, newMockHandler(), segRefer, &mocks.MockIndexCoord{}, GcOption{
			cli:     nil,
			enabled: true,
		})

		s.NotPanics(func() {
			gc.start()
		})

		s.NotPanics(func() {
			gc.close()
		})
	})
}

func (s *GarbageCollectorSuite) TestScan() {
	s.Run("listCollectionPrefix_fails", func() {
		s.mockChunkManager.ExpectedCalls = nil
		s.mockChunkManager.EXPECT().RootPath().Return("files")
		s.mockChunkManager.EXPECT().ListWithPrefix(mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("bool")).
			Return(nil, nil, errors.New("mocked"))

		s.gc.scan()
		s.mockChunkManager.AssertNotCalled(s.T(), "Remove", mock.Anything, mock.Anything)
	})

	s.Run("collectionPrefix_invalid", func() {
		s.mockChunkManager.ExpectedCalls = nil
		s.mockChunkManager.EXPECT().RootPath().Return("files")
		/*
			s.mockChunkManager.EXPECT().ListWithPrefix(mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("bool")).
				Return([]string{"files/insert_log/1/", "files/bad_prefix", "files/insert_log/string/"}, lo.RepeatBy(3, func(_ int) time.Time {
					return time.Now().Add(-time.Hour)
				}), nil)*/

		logTypes := []string{"files/insert_log/", "files/stats_log/", "files/delta_log/"}
		for _, logType := range logTypes {
			validSubPath := "1/2/3/100/2000"
			if logType == "files/delta_log/" {
				validSubPath = "1/2/3/2000"
			}
			s.mockChunkManager.EXPECT().ListWithPrefix(mock.Anything, logType, false).
				Return([]string{path.Join(logType, "1") + "/", path.Join(logType, "2") + "/", path.Join(logType, "string") + "/", "files/badprefix/"}, lo.RepeatBy(4, func(_ int) time.Time { return time.Now() }), nil)
			s.mockChunkManager.EXPECT().ListWithPrefix(mock.Anything, path.Join(logType, "1")+"/", true).
				Return([]string{path.Join(logType, validSubPath)}, []time.Time{time.Now().Add(time.Hour * -48)}, nil)
			s.mockChunkManager.EXPECT().Remove(mock.Anything, path.Join(logType, validSubPath)).Return(nil)
		}

		s.gc.option.collValidator = func(collID int64) bool {
			return collID == 1
		}

		s.gc.scan()
		//s.mockChunkManager.AssertNotCalled(s.T(), "Remove", mock.Anything, mock.Anything)
		s.mockChunkManager.AssertExpectations(s.T())
	})

	s.Run("fileScan_fails", func() {
		s.mockChunkManager.ExpectedCalls = nil
		s.mockChunkManager.Calls = nil
		s.mockChunkManager.EXPECT().RootPath().Return("files")
		isCollPrefix := func(prefix string) bool {
			return lo.Contains([]string{"files/insert_log/", "files/stats_log/", "files/delta_log/"}, prefix)
		}
		s.mockChunkManager.EXPECT().ListWithPrefix(mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("bool")).Call.Return(
			func(_ context.Context, prefix string, recursive bool) []string {
				if isCollPrefix(prefix) {
					return []string{path.Join(prefix, "1")}
				}
				return nil
			},
			func(_ context.Context, prefix string, recursive bool) []time.Time {
				if isCollPrefix(prefix) {
					return []time.Time{time.Now()}
				}
				return nil
			},
			func(_ context.Context, prefix string, recursive bool) error {
				if isCollPrefix(prefix) {
					return nil
				}
				return errors.New("mocked")
			},
		)
		s.gc.option.collValidator = func(collID int64) bool {
			return true
		}

		s.gc.scan()
		s.mockChunkManager.AssertNotCalled(s.T(), "Remove", mock.Anything, mock.Anything)
	})
}

func TestGarbageCollectorSuite(t *testing.T) {
	suite.Run(t, new(GarbageCollectorSuite))
}

/*
func Test_garbageCollector_basic(t *testing.T) {
	bucketName := `datacoord-ut` + strings.ToLower(funcutil.RandomString(8))
	rootPath := `gc` + funcutil.RandomString(8)
	//TODO change to Params
	cli, _, _, _, _, err := initUtOSSEnv(bucketName, rootPath, 0)
	require.NoError(t, err)

	meta, err := newMemoryMeta()
	assert.Nil(t, err)

	etcdCli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd,
		Params.EtcdCfg.EtcdUseSSL,
		Params.EtcdCfg.Endpoints,
		Params.EtcdCfg.EtcdTLSCert,
		Params.EtcdCfg.EtcdTLSKey,
		Params.EtcdCfg.EtcdTLSCACert,
		Params.EtcdCfg.EtcdTLSMinVersion)
	assert.Nil(t, err)
	etcdKV := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)
	segRefer, err := NewSegmentReferenceManager(etcdKV, nil)
	assert.NoError(t, err)
	assert.NotNil(t, segRefer)

	indexCoord := mocks.NewMockIndexCoord(t)

	t.Run("normal gc", func(t *testing.T) {
		gc := newGarbageCollector(meta, newMockHandler(), segRefer, indexCoord, GcOption{
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
		gc := newGarbageCollector(meta, newMockHandler(), segRefer, indexCoord, GcOption{
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

}*/

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
	assert.Nil(t, err)

	etcdCli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd,
		Params.EtcdCfg.EtcdUseSSL,
		Params.EtcdCfg.Endpoints,
		Params.EtcdCfg.EtcdTLSCert,
		Params.EtcdCfg.EtcdTLSKey,
		Params.EtcdCfg.EtcdTLSCACert,
		Params.EtcdCfg.EtcdTLSMinVersion)
	assert.Nil(t, err)
	etcdKV := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)
	segRefer, err := NewSegmentReferenceManager(etcdKV, nil)
	assert.NoError(t, err)
	assert.NotNil(t, segRefer)

	t.Run("key is reference", func(t *testing.T) {
		segReferManager := &SegmentReferenceManager{
			etcdKV: etcdKV,
			segmentsLock: map[UniqueID]map[UniqueID]*datapb.SegmentReferenceLock{
				1: {
					1: {
						TaskID:     1,
						NodeID:     1,
						SegmentIDs: []UniqueID{2},
					},
				},
			},
			segmentReferCnt: map[UniqueID]int{
				2: 1,
			},
		}

		indexCoord := mocks.NewMockIndexCoord(t)
		gc := newGarbageCollector(meta, newMockHandler(), segRefer, indexCoord, GcOption{
			cli:              cli,
			enabled:          true,
			checkInterval:    time.Minute * 30,
			missingTolerance: time.Hour * 24,
			dropTolerance:    time.Hour * 24,
		})
		gc.segRefer = segReferManager
		gc.scan()

		validateMinioPrefixElements(t, cli.Client, bucketName, path.Join(rootPath, insertLogPrefix), inserts)
		validateMinioPrefixElements(t, cli.Client, bucketName, path.Join(rootPath, statsLogPrefix), stats)
		validateMinioPrefixElements(t, cli.Client, bucketName, path.Join(rootPath, deltaLogPrefix), delta)
		validateMinioPrefixElements(t, cli.Client, bucketName, path.Join(rootPath, `indexes`), others)

		err = gc.segRefer.ReleaseSegmentsLock(1, 1)
		assert.NoError(t, err)
		gc.close()
	})

	t.Run("missing all but save tolerance", func(t *testing.T) {
		indexCoord := mocks.NewMockIndexCoord(t)
		gc := newGarbageCollector(meta, newMockHandler(), segRefer, indexCoord, GcOption{
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

		indexCoord := mocks.NewMockIndexCoord(t)
		gc := newGarbageCollector(meta, newMockHandler(), segRefer, indexCoord, GcOption{
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

		indexCoord := mocks.NewMockIndexCoord(t)
		gc := newGarbageCollector(meta, newMockHandler(), segRefer, indexCoord, GcOption{
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
		indexCoord := mocks.NewMockIndexCoord(t)
		gc := newGarbageCollector(meta, newMockHandler(), segRefer, indexCoord, GcOption{
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
		indexCoord := mocks.NewMockIndexCoord(t)
		gc := newGarbageCollector(meta, newMockHandler(), segRefer, indexCoord, GcOption{
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
	cli, err := minio.New(Params.MinioCfg.Address, &minio.Options{
		Creds:  credentials.NewStaticV4(Params.MinioCfg.AccessKeyID, Params.MinioCfg.SecretAccessKey, ""),
		Secure: Params.MinioCfg.UseSSL,
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
