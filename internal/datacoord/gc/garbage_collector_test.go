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

package gc

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"path"
	"path/filepath"
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
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/snapshot"
	kvmocks "github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestGarbageCollectorManifestReadContextPerSweep(t *testing.T) {
	checkpointCatalog := MockCheckpointCatalog(t, 1000)

	for _, dropped := range []bool{false, true} {
		for _, cancelRead := range []bool{false, true} {
			t.Run(fmt.Sprintf("dropped=%t/cancel=%t", dropped, cancelRead), func(t *testing.T) {
				m, dm, err := newMemoryMetaWithSnapshot(t)
				require.NoError(t, err)
				root := t.TempDir()
				for id := int64(1); id <= 2; id++ {
					state := commonpb.SegmentState_Flushed
					if dropped {
						state = commonpb.SegmentState_Dropped
					}
					require.NoError(t, m.AddSegment(context.Background(), meta.NewSegmentInfo(&datapb.SegmentInfo{
						ID: id, CollectionID: 100, PartitionID: 10, State: state,
						DroppedAt:      uint64(time.Now().Add(-time.Hour).UnixNano()),
						StorageVersion: storage.StorageV3, ManifestHasIndex: true,
						ManifestPath: packed.MarshalManifestPath(path.Join(root, "insert_log/100/10", strconv.FormatInt(id, 10)), 1),
					})))
					require.NoError(t, dm.IndexMeta.AddSegmentIndex(context.Background(), &model.SegmentIndex{
						CollectionID: 100, PartitionID: 10, SegmentID: id, IndexID: 100 + id,
						BuildID: 200 + id, IndexState: commonpb.IndexState_Finished,
					}))
				}
				dm.SnapshotMeta = nil
				m.CompactionProtection = nil
				gc := NewGarbageCollector(m, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{
					StreamingNodeCatalog: checkpointCatalog,
					Cli:                  storage.NewLocalChunkManager(objectstorage.RootPath(root)),
				}))
				// A read failure cannot authorize deletion of a manifest still present.
				defer mockey.Mock((*GarbageCollector).manifestExists).Return(true).Build().UnPatch()
				var seen []*packed.ManifestIOContext
				var cancel context.CancelFunc
				defer mockey.Mock(packed.GetManifestIndexInfosAsync).To(func(_ context.Context, io *packed.ManifestIOContext, _ string, _ *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
					seen = append(seen, io)
					if cancelRead {
						cancel()
						return nil, context.Canceled
					}
					return nil, merr.ErrServiceUnavailable
				}).Build().UnPatch()
				var previous *packed.ManifestIOContext
				for range 2 {
					var ctx context.Context
					ctx, cancel = context.WithCancel(context.Background())
					seen = nil
					if dropped {
						gc.RecycleDroppedSegments(ctx, nil)
					} else {
						gc.RecycleUnusedSegIndexes(ctx, nil)
					}
					cancel()
					wantReads := 2
					if cancelRead {
						wantReads = 1
					}
					require.Len(t, seen, wantReads)
					for _, io := range seen {
						require.Same(t, seen[0], io, "reuse the context across this sweep")
					}
					if previous != nil {
						require.NotSame(t, previous, seen[0], "each sweep owns its context")
					}
					previous = seen[0]
					err := packed.SubmitManifestIndexInfos(context.Background(), previous, packed.MarshalManifestPath(root, 0), nil,
						func([]packed.ManifestIndexInfo, error) { t.Error("GC left its context open") })
					require.ErrorIs(t, err, merr.ErrServiceUnavailable)
					for id := int64(1); id <= 2; id++ {
						require.NotNil(t, m.GetSegment(context.Background(), id))
						require.Len(t, dm.IndexMeta.GetAllSegmentIndexes(id), 1, "failed reads must retain metadata")
					}
				}
			})
		}
	}
}

func Test_garbageCollector_basic(t *testing.T) {
	bucketName := `datacoord-ut` + strings.ToLower(funcutil.RandomString(8))
	rootPath := `gc` + funcutil.RandomString(8)
	// TODO change to Params
	cli, _, _, _, _, err := initUtOSSEnv(bucketName, rootPath, 0)
	require.NoError(t, err)

	mt, dm, err := newMemoryMetaWithSnapshot(t)
	assert.NoError(t, err)

	t.Run("normal gc", func(t *testing.T) {
		gc := NewGarbageCollector(mt, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{
			Cli:              cli,
			Enabled:          true,
			CheckInterval:    time.Millisecond * 10,
			ScanInterval:     time.Hour * 7 * 24,
			MissingTolerance: time.Hour * 24,
			DropTolerance:    time.Hour * 24,
		}))
		gc.Start()

		time.Sleep(time.Millisecond * 20)
		assert.NotPanics(t, func() {
			gc.Close()
		})
	})

	t.Run("with nil cli", func(t *testing.T) {
		gc := NewGarbageCollector(mt, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{
			Cli:              nil,
			Enabled:          true,
			CheckInterval:    time.Millisecond * 10,
			ScanInterval:     time.Hour * 7 * 24,
			MissingTolerance: time.Hour * 24,
			DropTolerance:    time.Hour * 24,
		}))
		assert.NotPanics(t, func() {
			gc.Start()
		})

		assert.NotPanics(t, func() {
			gc.Close()
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
	checkpointCatalog := MockCheckpointCatalog(t, 1000)

	bucketName := `datacoord-ut` + strings.ToLower(funcutil.RandomString(8))
	rootPath := paramtable.Get().MinioCfg.RootPath.GetValue()
	// TODO change to Params
	cli, inserts, stats, delta, others, err := initUtOSSEnv(bucketName, rootPath, 4)
	require.NoError(t, err)

	mt, dm, err := newMemoryMetaWithSnapshot(t)
	assert.NoError(t, err)

	t.Run("key is reference", func(t *testing.T) {
		gc := NewGarbageCollector(mt, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{
			StreamingNodeCatalog: checkpointCatalog,
			Cli:                  cli,
			Enabled:              true,
			CheckInterval:        time.Minute * 30,
			ScanInterval:         time.Hour * 7 * 24,
			MissingTolerance:     time.Hour * 24,
			DropTolerance:        time.Hour * 24,
		}))
		gc.RecycleUnusedBinlogFiles(context.TODO())

		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, common.SegmentInsertLogPath), inserts)
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, common.SegmentStatslogPath), stats)
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, common.SegmentDeltaLogPath), delta)
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, `indexes`), others)
		gc.Close()
	})

	t.Run("missing all but save tolerance", func(t *testing.T) {
		gc := NewGarbageCollector(mt, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{
			StreamingNodeCatalog: checkpointCatalog,
			Cli:                  cli,
			Enabled:              true,
			CheckInterval:        time.Minute * 30,
			ScanInterval:         time.Hour * 7 * 24,
			MissingTolerance:     time.Hour * 24,
			DropTolerance:        time.Hour * 24,
		}))
		gc.RecycleUnusedBinlogFiles(context.TODO())

		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, common.SegmentInsertLogPath), inserts)
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, common.SegmentStatslogPath), stats)
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, common.SegmentDeltaLogPath), delta)
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, `indexes`), others)

		gc.Close()
	})
	t.Run("hit, no gc", func(t *testing.T) {
		segment := meta.BuildSegment(1, 10, 100, "ch")
		segment.State = commonpb.SegmentState_Flushed
		segment.Binlogs = []*datapb.FieldBinlog{getFieldBinlogPaths(0, inserts[0])}
		segment.Statslogs = []*datapb.FieldBinlog{getFieldBinlogPaths(0, stats[0])}
		segment.Deltalogs = []*datapb.FieldBinlog{getFieldBinlogPaths(0, delta[0])}
		err = mt.AddSegment(context.TODO(), segment)
		require.NoError(t, err)

		gc := NewGarbageCollector(mt, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{
			StreamingNodeCatalog: checkpointCatalog,
			Cli:                  cli,
			Enabled:              true,
			CheckInterval:        time.Minute * 30,
			ScanInterval:         time.Hour * 7 * 24,
			MissingTolerance:     time.Hour * 24,
			DropTolerance:        time.Hour * 24,
		}))
		gc.Start()
		gc.RecycleUnusedBinlogFiles(context.TODO())
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, common.SegmentInsertLogPath), inserts)
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, common.SegmentStatslogPath), stats)
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, common.SegmentDeltaLogPath), delta)
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, `indexes`), others)

		gc.Close()
	})

	t.Run("dropped gc one", func(t *testing.T) {
		segment := meta.BuildSegment(1, 10, 100, "ch")
		segment.State = commonpb.SegmentState_Dropped
		segment.DroppedAt = uint64(time.Now().Add(-time.Hour).UnixNano())
		segment.Binlogs = []*datapb.FieldBinlog{getFieldBinlogPaths(0, inserts[0])}
		segment.Statslogs = []*datapb.FieldBinlog{getFieldBinlogPaths(0, stats[0])}
		segment.Deltalogs = []*datapb.FieldBinlog{getFieldBinlogPaths(0, delta[0])}

		mt.DropSegment(context.TODO(), segment.ID)
		err = mt.AddSegment(context.TODO(), segment)
		require.NoError(t, err)

		gc := NewGarbageCollector(mt, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{
			StreamingNodeCatalog: checkpointCatalog,
			Cli:                  cli,
			Enabled:              true,
			CheckInterval:        time.Minute * 30,
			ScanInterval:         time.Hour * 7 * 24,
			MissingTolerance:     time.Hour * 24,
			DropTolerance:        0,
		}))
		signal := make(chan gcCmd)
		gc.RecycleDroppedSegments(context.TODO(), signal)
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, common.SegmentInsertLogPath), inserts[1:])
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, common.SegmentStatslogPath), stats[1:])
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, common.SegmentDeltaLogPath), delta[1:])
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, `indexes`), others)

		gc.Close()
	})
	t.Run("missing gc all", func(t *testing.T) {
		gc := NewGarbageCollector(mt, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{
			StreamingNodeCatalog: checkpointCatalog,
			Cli:                  cli,
			Enabled:              true,
			CheckInterval:        time.Minute * 30,
			ScanInterval:         time.Hour * 7 * 24,
			MissingTolerance:     0,
			DropTolerance:        0,
		}))
		gc.Start()
		gc.RecycleUnusedBinlogFiles(context.TODO())
		signal := make(chan gcCmd)
		gc.RecycleDroppedSegments(context.TODO(), signal)

		// bad path shall remains since datacoord cannot determine file is garbage or not if path is not valid
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, common.SegmentInsertLogPath), inserts[1:2])
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, common.SegmentStatslogPath), stats[1:2])
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, common.SegmentDeltaLogPath), delta[1:2])
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, `indexes`), others)

		gc.Close()
	})

	t.Run("list object with error", func(t *testing.T) {
		gc := NewGarbageCollector(mt, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{
			StreamingNodeCatalog: checkpointCatalog,
			Cli:                  cli,
			Enabled:              true,
			CheckInterval:        time.Minute * 30,
			ScanInterval:         time.Hour * 7 * 24,
			MissingTolerance:     0,
			DropTolerance:        0,
		}))
		gc.Start()
		gc.RecycleUnusedBinlogFiles(context.TODO())

		// bad path shall remains since datacoord cannot determine file is garbage or not if path is not valid
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, common.SegmentInsertLogPath), inserts[1:2])
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, common.SegmentStatslogPath), stats[1:2])
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, common.SegmentDeltaLogPath), delta[1:2])
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, `indexes`), others)

		gc.Close()
	})

	cleanupOSS(cli, bucketName, rootPath)
}

// Test_garbageCollector_importingSegmentStatsKept pins that a stats file under
// an Importing segment is retained even though its log ID is not registered in
// Statslogs yet. Import V3 pre-registers the segment at task creation but
// persists Statslogs only at acceptance, while the PK stats file is already
// written under stats_log/ and the task may retry indefinitely.
func Test_garbageCollector_importingSegmentStatsKept(t *testing.T) {
	rootPath := paramtable.Get().MinioCfg.RootPath.GetValue()
	newGC := func(mt *meta.Meta, dm *snapshot.SnapshotTestMetas, cli *storage.RemoteChunkManager) *GarbageCollector {
		return NewGarbageCollector(mt, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{
			Cli:              cli,
			Enabled:          true,
			CheckInterval:    time.Minute * 30,
			ScanInterval:     time.Hour * 7 * 24,
			MissingTolerance: 0,
			DropTolerance:    time.Hour * 24,
		}))
	}

	t.Run("importing segment keeps unregistered stats", func(t *testing.T) {
		bucketName := `datacoord-ut` + strings.ToLower(funcutil.RandomString(8))
		cli, _, stats, _, _, err := initUtOSSEnv(bucketName, rootPath, 1)
		require.NoError(t, err)

		mt, dm, err := newMemoryMetaWithSnapshot(t)
		require.NoError(t, err)
		segment := meta.BuildSegment(1, 10, 100, "ch")
		segment.State = commonpb.SegmentState_Importing
		segment.IsImporting = true
		require.NoError(t, mt.AddSegment(context.TODO(), segment))

		gc := newGC(mt, dm, cli)
		gc.RecycleUnusedBinlogFiles(context.TODO())
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, common.SegmentStatslogPath), stats)
		gc.Close()
		cleanupOSS(cli, bucketName, rootPath)
	})

	t.Run("flushed segment without stats registration still collected", func(t *testing.T) {
		bucketName := `datacoord-ut` + strings.ToLower(funcutil.RandomString(8))
		cli, _, stats, _, _, err := initUtOSSEnv(bucketName, rootPath, 1)
		require.NoError(t, err)
		require.Len(t, stats, 1)

		mt, dm, err := newMemoryMetaWithSnapshot(t)
		require.NoError(t, err)
		segment := meta.BuildSegment(1, 10, 100, "ch")
		segment.State = commonpb.SegmentState_Flushed
		require.NoError(t, mt.AddSegment(context.TODO(), segment))

		gc := newGC(mt, dm, cli)
		gc.RecycleUnusedBinlogFiles(context.TODO())
		validateMinioPrefixElements(t, cli, bucketName, path.Join(rootPath, common.SegmentStatslogPath), []string{})
		gc.Close()
		cleanupOSS(cli, bucketName, rootPath)
	})
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

func createMetaForRecycleUnusedIndexes(catalog metastore.DataCoordCatalog) (*meta.Meta, *index.IndexMeta) {
	var (
		ctx    = context.Background()
		collID = UniqueID(100)
		// partID = UniqueID(200)
		fieldID = UniqueID(300)
		indexID = UniqueID(400)
	)
	im := &index.IndexMeta{
		Catalog: catalog,
		Indexes: map[UniqueID]map[UniqueID]*model.Index{
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
		SegmentBuildInfo: index.NewSegmentIndexBuildInfo(),
	}
	return &meta.Meta{
		Ctx:          ctx,
		MetaStore:    metacache.NewMetaStore(catalog),
		Segments:     nil,
		ChunkManager: nil,
	}, im
}

func TestGarbageCollector_recycleUnusedIndexes(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.On("DropIndexes", mock.Anything, mock.Anything).Return(nil)
		m, im := createMetaForRecycleUnusedIndexes(catalog)
		gc := NewGarbageCollector(m, nil, GcOption{IndexMeta: im})
		gc.recycleUnusedIndexes(context.TODO(), nil)
	})

	t.Run("fail", func(t *testing.T) {
		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.On("DropIndexes", mock.Anything, mock.Anything).Return(errors.New("fail"))
		m, im := createMetaForRecycleUnusedIndexes(catalog)
		gc := NewGarbageCollector(m, nil, GcOption{IndexMeta: im})
		gc.recycleUnusedIndexes(context.TODO(), nil)
	})
}

func createMetaForRecycleUnusedSegIndexes(catalog metastore.DataCoordCatalog) (*meta.Meta, *snapshot.SnapshotTestMetas) {
	var (
		ctx    = context.Background()
		collID = UniqueID(100)
		partID = UniqueID(200)
		// fieldID = UniqueID(300)
		indexID = UniqueID(400)
		segID   = UniqueID(500)
	)
	segments := map[int64]*meta.SegmentInfo{
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
		BuildID:             meta.BuildID,
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
		BuildID:             meta.BuildID + 1,
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
	store := metacache.NewMetaStore(catalog)
	im := &index.IndexMeta{
		Catalog:          catalog,
		SegmentIndexes:   segIndexes,
		Indexes:          map[UniqueID]map[UniqueID]*model.Index{},
		SegmentBuildInfo: index.NewSegmentIndexBuildInfo(),
		KeyLock:          lock.NewKeyLock[UniqueID](),
	}
	mt := &meta.Meta{
		Ctx:          ctx,
		MetaStore:    store,
		Segments:     meta.NewSegmentsInfo(store),
		ChunkManager: nil,
	}

	im.SegmentBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             meta.BuildID,
		NodeID:              1,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      10,
		IndexFileKeys:       []string{"file1", "file2"},
		IndexSerializedSize: 0,
	})

	im.SegmentBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID + 1,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             meta.BuildID + 1,
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
		mt.Segments.SetSegment(id, segment)
	}
	sm := &snapshot.SnapshotMeta{}
	mt.CompactionProtection = sm
	return mt, &snapshot.SnapshotTestMetas{IndexMeta: im, SnapshotMeta: sm}
}

func TestGarbageCollector_recycleUnusedSegIndexes(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mockChunkManager := mocks.NewChunkManager(t)
		mockChunkManager.EXPECT().RootPath().Return("root")
		mockChunkManager.EXPECT().Remove(mock.Anything, mock.Anything).Return(nil)
		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.On("DropSegmentIndexes", mock.Anything, mock.Anything).Return(nil)
		m, dm := createMetaForRecycleUnusedSegIndexes(catalog)
		gc := NewGarbageCollector(m, nil, GcTestDomainOption(dm, GcOption{
			Cli: mockChunkManager,
		}))
		// Snapshot layer transparent: no buildID is blocked.
		mockIsBuildIDBlocked := mockey.Mock((*snapshot.SnapshotMeta).IsBuildIDGCBlocked).Return(false).Build()
		defer mockIsBuildIDBlocked.UnPatch()
		gc.RecycleUnusedSegIndexes(context.TODO(), nil)
	})

	t.Run("fail", func(t *testing.T) {
		catalog := catalogmocks.NewDataCoordCatalog(t)
		mockChunkManager := mocks.NewChunkManager(t)
		mockChunkManager.EXPECT().RootPath().Return("root")
		mockChunkManager.EXPECT().Remove(mock.Anything, mock.Anything).Return(nil)
		catalog.On("DropSegmentIndexes", mock.Anything, mock.Anything).Return(errors.New("fail"))
		m, dm := createMetaForRecycleUnusedSegIndexes(catalog)
		gc := NewGarbageCollector(m, nil, GcTestDomainOption(dm, GcOption{
			Cli: mockChunkManager,
		}))
		mockIsBuildIDBlocked := mockey.Mock((*snapshot.SnapshotMeta).IsBuildIDGCBlocked).Return(false).Build()
		defer mockIsBuildIDBlocked.UnPatch()
		gc.RecycleUnusedSegIndexes(context.TODO(), nil)
	})

	t.Run("uses latest segment index when candidate is stale", func(t *testing.T) {
		const (
			collID  = UniqueID(100)
			partID  = UniqueID(200)
			segID   = UniqueID(300)
			indexID = UniqueID(400)
			buildID = UniqueID(2000)
		)

		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.EXPECT().DropSegmentIndexes(mock.Anything, mock.Anything).Return(nil)

		store := metacache.NewMetaStore(nil)
		im := &index.IndexMeta{
			Catalog:          catalog,
			SegmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
			Indexes:          map[UniqueID]map[UniqueID]*model.Index{},
			SegmentBuildInfo: index.NewSegmentIndexBuildInfo(),
			KeyLock:          lock.NewKeyLock[UniqueID](),
		}
		mt := &meta.Meta{
			MetaStore: store,
			Segments:  meta.NewSegmentsInfo(store),
		}
		sm := &snapshot.SnapshotMeta{}
		mt.CompactionProtection = sm
		latest := &model.SegmentIndex{
			SegmentID:             segID,
			CollectionID:          collID,
			PartitionID:           partID,
			IndexID:               indexID,
			BuildID:               buildID,
			NodeID:                1,
			IndexVersion:          1,
			IndexStorePathVersion: 1,
			IndexState:            commonpb.IndexState_Finished,
			IndexFileKeys:         []string{"file1", "file2"},
		}
		im.SegmentBuildInfo.Add(latest)
		segIdxes := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
		segIdxes.Insert(indexID, latest)
		im.SegmentIndexes.Insert(segID, segIdxes)

		stale := model.CloneSegmentIndex(latest)
		stale.IndexState = commonpb.IndexState_InProgress
		stale.IndexFileKeys = nil
		// Let the real range observe the old entry, then publish the new
		// task during candidate discovery, before GC refreshes by build ID.
		im.SegmentBuildInfo.Add(stale)
		refreshed := false
		refresh := mockey.Mock((*meta.Meta).GetSegment).To(func(_ *meta.Meta, _ context.Context, _ int64) *meta.SegmentInfo {
			if !refreshed {
				im.SegmentBuildInfo.Add(latest)
				refreshed = true
			}
			return nil
		}).Build()
		defer refresh.UnPatch()

		mockIsBuildIDBlocked := mockey.Mock((*snapshot.SnapshotMeta).IsBuildIDGCBlocked).Return(false).Build()
		defer mockIsBuildIDBlocked.UnPatch()

		cm := mocks.NewChunkManager(t)
		cm.EXPECT().RootPath().Return("root")
		cm.EXPECT().Remove(mock.Anything, "root/index_v1/100/200/300/2000/1/file1").Return(nil)
		cm.EXPECT().Remove(mock.Anything, "root/index_v1/100/200/300/2000/1/file2").Return(nil)

		gc := NewGarbageCollector(mt, nil, GcTestDomainOption(&snapshot.SnapshotTestMetas{IndexMeta: im, SnapshotMeta: sm}, GcOption{Cli: cm}))
		gc.RecycleUnusedSegIndexes(context.TODO(), nil)

		_, ok := im.SegmentBuildInfo.Get(buildID)
		assert.False(t, ok)
	})

	t.Run("skip when latest segment index is still in progress", func(t *testing.T) {
		const (
			collID  = UniqueID(100)
			partID  = UniqueID(200)
			segID   = UniqueID(300)
			indexID = UniqueID(400)
			buildID = UniqueID(2000)
		)

		catalog := catalogmocks.NewDataCoordCatalog(t)
		store := metacache.NewMetaStore(nil)
		im := &index.IndexMeta{
			Catalog:          catalog,
			SegmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
			Indexes:          map[UniqueID]map[UniqueID]*model.Index{},
			SegmentBuildInfo: index.NewSegmentIndexBuildInfo(),
			KeyLock:          lock.NewKeyLock[UniqueID](),
		}
		mt := &meta.Meta{
			MetaStore: store,
			Segments:  meta.NewSegmentsInfo(store),
		}
		sm := &snapshot.SnapshotMeta{}
		mt.CompactionProtection = sm
		latest := &model.SegmentIndex{
			SegmentID:             segID,
			CollectionID:          collID,
			PartitionID:           partID,
			IndexID:               indexID,
			BuildID:               buildID,
			NodeID:                1,
			IndexVersion:          1,
			IndexStorePathVersion: 1,
			IndexState:            commonpb.IndexState_InProgress,
			IndexFileKeys:         []string{"file1"},
		}
		im.SegmentBuildInfo.Add(latest)

		stale := model.CloneSegmentIndex(latest)
		stale.IndexState = commonpb.IndexState_Finished
		// Let the real range observe the old entry, then publish the new
		// task during candidate discovery, before GC refreshes by build ID.
		im.SegmentBuildInfo.Add(stale)
		refreshed := false
		refresh := mockey.Mock((*meta.Meta).GetSegment).To(func(_ *meta.Meta, _ context.Context, _ int64) *meta.SegmentInfo {
			if !refreshed {
				im.SegmentBuildInfo.Add(latest)
				refreshed = true
			}
			return nil
		}).Build()
		defer refresh.UnPatch()

		cm := mocks.NewChunkManager(t)
		gc := NewGarbageCollector(mt, nil, GcTestDomainOption(&snapshot.SnapshotTestMetas{IndexMeta: im, SnapshotMeta: sm}, GcOption{Cli: cm}))
		gc.RecycleUnusedSegIndexes(context.TODO(), nil)

		_, ok := im.SegmentBuildInfo.Get(buildID)
		assert.True(t, ok)
		catalog.AssertNotCalled(t, "DropSegmentIndexes", mock.Anything, mock.Anything)
		cm.AssertNotCalled(t, "Remove", mock.Anything, mock.Anything)
	})

	t.Run("remove finished segment index meta without recorded files", func(t *testing.T) {
		const (
			collID  = UniqueID(100)
			partID  = UniqueID(200)
			segID   = UniqueID(300)
			indexID = UniqueID(400)
			buildID = UniqueID(2000)
		)

		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.EXPECT().DropSegmentIndexes(mock.Anything, mock.Anything).Return(nil)
		store := metacache.NewMetaStore(nil)
		im := &index.IndexMeta{
			Catalog:          catalog,
			SegmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
			Indexes:          map[UniqueID]map[UniqueID]*model.Index{},
			SegmentBuildInfo: index.NewSegmentIndexBuildInfo(),
			KeyLock:          lock.NewKeyLock[UniqueID](),
		}
		mt := &meta.Meta{
			MetaStore: store,
			Segments:  meta.NewSegmentsInfo(store),
		}
		sm := &snapshot.SnapshotMeta{}
		mt.CompactionProtection = sm
		im.SegmentBuildInfo.Add(&model.SegmentIndex{
			SegmentID:             segID,
			CollectionID:          collID,
			PartitionID:           partID,
			IndexID:               indexID,
			BuildID:               buildID,
			NodeID:                1,
			IndexVersion:          1,
			IndexStorePathVersion: 1,
			IndexState:            commonpb.IndexState_Finished,
			IndexFileKeys:         nil,
		})

		cm := mocks.NewChunkManager(t)
		cm.EXPECT().RootPath().Return("root")

		gc := NewGarbageCollector(mt, nil, GcTestDomainOption(&snapshot.SnapshotTestMetas{IndexMeta: im, SnapshotMeta: sm}, GcOption{Cli: cm}))
		gc.RecycleUnusedSegIndexes(context.TODO(), nil)

		_, ok := im.SegmentBuildInfo.Get(buildID)
		assert.False(t, ok)
	})
}

func createMetaTableForRecycleUnusedIndexFiles(catalog *datacoord.Catalog) (*meta.Meta, *snapshot.SnapshotTestMetas) {
	var (
		ctx    = context.Background()
		collID = UniqueID(100)
		partID = UniqueID(200)
		// fieldID = UniqueID(300)
		indexID = UniqueID(400)
		segID   = UniqueID(500)
		buildID = UniqueID(600)
	)
	segments := map[UniqueID]*meta.SegmentInfo{
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
	store := metacache.NewMetaStore(catalog)
	im := &index.IndexMeta{
		Catalog:        catalog,
		SegmentIndexes: segIndexes,
		Indexes: map[UniqueID]map[UniqueID]*model.Index{
			collID: {
				indexID: {
					TenantID:        "",
					CollectionID:    collID,
					FieldID:         meta.FieldID,
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
		SegmentBuildInfo: index.NewSegmentIndexBuildInfo(),
	}
	mt := &meta.Meta{
		Ctx:       ctx,
		MetaStore: store,
		Segments:  meta.NewSegmentsInfo(store),
	}
	im.SegmentBuildInfo.Add(&model.SegmentIndex{
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
	im.SegmentBuildInfo.Add(&model.SegmentIndex{
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
		mt.Segments.SetSegment(id, segment)
	}

	return mt, &snapshot.SnapshotTestMetas{IndexMeta: im}
}

func TestGarbageCollector_recycleUnusedIndexFilesV0(t *testing.T) {
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
		m, dm := createMetaTableForRecycleUnusedIndexFiles(&datacoord.Catalog{MetaKv: kvmocks.NewMetaKv(t)})
		gc := NewGarbageCollector(
			m,
			nil,
			GcTestDomainOption(dm, GcOption{
				Cli: cm,
			}))

		gc.RecycleUnusedIndexFilesV0(context.TODO())
	})

	t.Run("list fail", func(t *testing.T) {
		cm := &mocks.ChunkManager{}
		cm.EXPECT().RootPath().Return("root")
		cm.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, s string, b bool, cowf storage.ChunkObjectWalkFunc) error {
				return errors.New("error")
			})
		m, dm := createMetaTableForRecycleUnusedIndexFiles(&datacoord.Catalog{MetaKv: kvmocks.NewMetaKv(t)})
		gc := NewGarbageCollector(
			m,
			nil,
			GcTestDomainOption(dm, GcOption{
				Cli: cm,
			}))
		gc.RecycleUnusedIndexFilesV0(context.TODO())
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
		m, dm := createMetaTableForRecycleUnusedIndexFiles(&datacoord.Catalog{MetaKv: kvmocks.NewMetaKv(t)})
		gc := NewGarbageCollector(
			m,
			nil,
			GcTestDomainOption(dm, GcOption{
				Cli: cm,
			}))
		gc.RecycleUnusedIndexFilesV0(context.TODO())
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
		m, dm := createMetaTableForRecycleUnusedIndexFiles(&datacoord.Catalog{MetaKv: kvmocks.NewMetaKv(t)})
		gc := NewGarbageCollector(
			m,
			nil,
			GcTestDomainOption(dm, GcOption{
				Cli: cm,
			}))
		gc.RecycleUnusedIndexFilesV0(context.TODO())
	})
}

func TestGetAllIndexFilesOfIndex_PathVersions(t *testing.T) {
	cm := mocks.NewChunkManager(t)
	cm.EXPECT().RootPath().Return("root")

	gc := &GarbageCollector{
		Option: GcOption{Cli: cm},
	}

	t.Run("v0 legacy path", func(t *testing.T) {
		segIdx := &model.SegmentIndex{
			CollectionID:          100,
			PartitionID:           200,
			SegmentID:             300,
			BuildID:               1000,
			IndexVersion:          1,
			IndexStorePathVersion: 0,
			IndexFileKeys:         []string{"file1", "file2"},
		}
		files := gc.GetAllIndexFilesOfIndex(segIdx)
		assert.Contains(t, files, "root/index_files/1000/1/200/300/file1")
		assert.Contains(t, files, "root/index_files/1000/1/200/300/file2")
		assert.Len(t, files, 2)
	})

	t.Run("v1 collection-partitioned path", func(t *testing.T) {
		segIdx := &model.SegmentIndex{
			CollectionID:          100,
			PartitionID:           200,
			SegmentID:             300,
			BuildID:               1000,
			IndexVersion:          1,
			IndexStorePathVersion: 1,
			IndexFileKeys:         []string{"file1", "file2"},
		}
		files := gc.GetAllIndexFilesOfIndex(segIdx)
		assert.Contains(t, files, "root/index_v1/100/200/300/1000/1/file1")
		assert.Contains(t, files, "root/index_v1/100/200/300/1000/1/file2")
		assert.Len(t, files, 2)
	})
}

func TestGarbageCollector_recycleUnusedIndexFilesV0_TreatsMatchingV1CollectionIDAsBuildID(t *testing.T) {
	// V1 indexes use a separate root, so a first-level dir under index_files/
	// is always a legacy buildID candidate even if it numerically equals a v1 collectionID.
	catalog := catalogmocks.NewDataCoordCatalog(t)
	segIndexes := typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]]()

	store := metacache.NewMetaStore(nil)
	im := &index.IndexMeta{
		Catalog:          catalog,
		SegmentIndexes:   segIndexes,
		Indexes:          map[UniqueID]map[UniqueID]*model.Index{},
		SegmentBuildInfo: index.NewSegmentIndexBuildInfo(),
		KeyLock:          lock.NewKeyLock[UniqueID](),
	}
	mt := &meta.Meta{
		MetaStore: store,
		Segments:  meta.NewSegmentsInfo(store),
	}
	sm := &snapshot.SnapshotMeta{}
	mt.CompactionProtection = sm

	// Add a v1 index for collection 100
	im.SegmentBuildInfo.Add(&model.SegmentIndex{
		BuildID:               2000,
		CollectionID:          100,
		PartitionID:           200,
		SegmentID:             300,
		IndexVersion:          1,
		IndexStorePathVersion: 1,
		IndexState:            commonpb.IndexState_Finished,
		IndexFileKeys:         []string{"file1"},
	})

	removedPrefixes := make([]string, 0)
	cm := mocks.NewChunkManager(t)
	cm.EXPECT().RootPath().Return("root")
	// Walk returns "100/" as a first-level entry (this is the collectionID dir)
	cm.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, s string, b bool, cowf storage.ChunkObjectWalkFunc) error {
			if !b {
				// Non-recursive walk (depth=1) — return first-level dirs
				cowf(&storage.ChunkObjectInfo{FilePath: "root/index_files/100/"})
			}
			return nil
		})
	cm.EXPECT().RemoveWithPrefix(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, s string) error {
			removedPrefixes = append(removedPrefixes, s)
			return nil
		}).Maybe()

	gc := NewGarbageCollector(mt, nil, GcTestDomainOption(&snapshot.SnapshotTestMetas{IndexMeta: im, SnapshotMeta: sm}, GcOption{Cli: cm}))
	gc.RecycleUnusedIndexFilesV0(context.TODO())

	assert.Equal(t, []string{"root/index_files/100/"}, removedPrefixes)
}

func TestGarbageCollector_recycleUnusedIndexFilesV0_OnlyWalksLegacyPrefix(t *testing.T) {
	catalog := catalogmocks.NewDataCoordCatalog(t)
	segIndexes := typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]]()

	store := metacache.NewMetaStore(nil)
	im := &index.IndexMeta{
		Catalog:          catalog,
		SegmentIndexes:   segIndexes,
		Indexes:          map[UniqueID]map[UniqueID]*model.Index{},
		SegmentBuildInfo: index.NewSegmentIndexBuildInfo(),
		KeyLock:          lock.NewKeyLock[UniqueID](),
	}
	mt := &meta.Meta{
		MetaStore: store,
		Segments:  meta.NewSegmentsInfo(store),
	}
	sm := &snapshot.SnapshotMeta{}
	mt.CompactionProtection = sm

	im.SegmentBuildInfo.Add(&model.SegmentIndex{
		BuildID:               2000,
		CollectionID:          100,
		PartitionID:           200,
		SegmentID:             300,
		IndexVersion:          1,
		IndexStorePathVersion: 0,
		IndexState:            commonpb.IndexState_Finished,
		IndexFileKeys:         []string{"v0-file"},
	})
	im.SegmentBuildInfo.Add(&model.SegmentIndex{
		BuildID:               3000,
		CollectionID:          100,
		PartitionID:           200,
		SegmentID:             301,
		IndexVersion:          1,
		IndexStorePathVersion: 1,
		IndexState:            commonpb.IndexState_Finished,
		IndexFileKeys:         []string{"v1-file"},
	})

	removedPrefixes := make([]string, 0)
	cm := mocks.NewChunkManager(t)
	cm.EXPECT().RootPath().Return("root")
	cm.EXPECT().WalkWithPrefix(mock.Anything, "root/index_files/", false, mock.Anything).RunAndReturn(
		func(ctx context.Context, s string, b bool, cowf storage.ChunkObjectWalkFunc) error {
			cowf(&storage.ChunkObjectInfo{FilePath: "root/index_files/100/"})
			cowf(&storage.ChunkObjectInfo{FilePath: "root/index_files/999/"})
			return nil
		})
	cm.EXPECT().RemoveWithPrefix(mock.Anything, "root/index_files/100/").RunAndReturn(
		func(ctx context.Context, s string) error {
			removedPrefixes = append(removedPrefixes, s)
			return nil
		})
	cm.EXPECT().RemoveWithPrefix(mock.Anything, "root/index_files/999/").RunAndReturn(
		func(ctx context.Context, s string) error {
			removedPrefixes = append(removedPrefixes, s)
			return nil
		})

	gc := NewGarbageCollector(mt, nil, GcTestDomainOption(&snapshot.SnapshotTestMetas{IndexMeta: im, SnapshotMeta: sm}, GcOption{Cli: cm}))
	gc.RecycleUnusedIndexFilesV0(context.TODO())

	assert.ElementsMatch(t, []string{"root/index_files/100/", "root/index_files/999/"}, removedPrefixes)
}

func TestGarbageCollector_recycleUnusedIndexFilesV0_IgnoresV1MetadataUnderSeparatePrefix(t *testing.T) {
	catalog := catalogmocks.NewDataCoordCatalog(t)
	segIndexes := typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]]()

	store := metacache.NewMetaStore(nil)
	im := &index.IndexMeta{
		Catalog:          catalog,
		SegmentIndexes:   segIndexes,
		Indexes:          map[UniqueID]map[UniqueID]*model.Index{},
		SegmentBuildInfo: index.NewSegmentIndexBuildInfo(),
		KeyLock:          lock.NewKeyLock[UniqueID](),
	}
	mt := &meta.Meta{
		MetaStore: store,
		Segments:  meta.NewSegmentsInfo(store),
	}
	sm := &snapshot.SnapshotMeta{}
	mt.CompactionProtection = sm

	removedPrefixes := make([]string, 0)
	cm := mocks.NewChunkManager(t)
	cm.EXPECT().RootPath().Return("root")
	cm.EXPECT().WalkWithPrefix(mock.Anything, "root/index_files/", false, mock.Anything).RunAndReturn(
		func(ctx context.Context, s string, b bool, cowf storage.ChunkObjectWalkFunc) error {
			im.SegmentBuildInfo.Add(&model.SegmentIndex{
				BuildID:               3000,
				CollectionID:          100,
				PartitionID:           200,
				SegmentID:             300,
				IndexVersion:          1,
				IndexStorePathVersion: 1,
				IndexState:            commonpb.IndexState_Finished,
				IndexFileKeys:         []string{"file1"},
			})
			cowf(&storage.ChunkObjectInfo{FilePath: "root/index_files/100/"})
			return nil
		})
	cm.EXPECT().RemoveWithPrefix(mock.Anything, "root/index_files/100/").RunAndReturn(
		func(ctx context.Context, s string) error {
			removedPrefixes = append(removedPrefixes, s)
			return nil
		})

	gc := NewGarbageCollector(mt, nil, GcTestDomainOption(&snapshot.SnapshotTestMetas{IndexMeta: im, SnapshotMeta: sm}, GcOption{Cli: cm}))
	gc.RecycleUnusedIndexFilesV0(context.TODO())

	assert.Equal(t, []string{"root/index_files/100/"}, removedPrefixes)
}

func TestGarbageCollector_recycleUnusedIndexFilesV1(t *testing.T) {
	for _, version := range []indexpb.IndexStorePathVersion{1, 2} {
		t.Run(version.String(), func(t *testing.T) { testRecycleUnusedIndexFiles(t, version) })
	}
}

func testRecycleUnusedIndexFiles(t *testing.T, version indexpb.IndexStorePathVersion) {
	recycle := func(gc *GarbageCollector) {
		if metautil.IsSegmentRooted(version) {
			gc.recycleUnusedBinLogWithChecker(context.TODO(), "root/insert_log", metrics.InsertFileLabel,
				storage.ParseSegmentIDByBinlog, func(_ *storage.ChunkObjectInfo, segment *meta.SegmentInfo) bool { return segment != nil })
		} else {
			gc.recycleUnusedIndexFilesV1(context.TODO())
		}
	}
	toPath := func(file string) string {
		if version == indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED {
			file = strings.Replace(file, "root/index_v1/", "root/insert_log/", 1)
			parts := strings.Split(file, "/")
			if len(parts) >= 8 && parts[1] == "insert_log" {
				file = path.Join(path.Join(parts[:5]...), "_index", path.Join(parts[5:]...))
			}
		}
		return file
	}
	fromPath := func(file string) string {
		if version == indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED {
			file = strings.Replace(file, "root/insert_log/", "root/index_v1/", 1)
			file = strings.Replace(file, "/_index/", "/", 1)
		}
		return file
	}
	const (
		collID  = UniqueID(100)
		partID  = UniqueID(200)
		segID   = UniqueID(300)
		indexID = UniqueID(400)
	)
	newMeta := func(t *testing.T) (*meta.Meta, *snapshot.SnapshotTestMetas) {
		store := metacache.NewMetaStore(nil)
		im := &index.IndexMeta{
			Catalog:          catalogmocks.NewDataCoordCatalog(t),
			SegmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
			Indexes:          map[UniqueID]map[UniqueID]*model.Index{},
			SegmentBuildInfo: index.NewSegmentIndexBuildInfo(),
			KeyLock:          lock.NewKeyLock[UniqueID](),
		}
		m := &meta.Meta{
			MetaStore: store,
			Segments:  meta.NewSegmentsInfo(store),
		}
		m.Segments.SetSegment(segID, meta.NewSegmentInfo(&datapb.SegmentInfo{
			ID: segID, CollectionID: collID, PartitionID: partID,
			State: commonpb.SegmentState_Flushed, StorageVersion: storage.StorageV3,
		}))
		sm := &snapshot.SnapshotMeta{}
		m.CompactionProtection = sm
		add := func(buildID UniqueID, state commonpb.IndexState, keys []string) {
			im.SegmentBuildInfo.Add(&model.SegmentIndex{
				BuildID: buildID, CollectionID: collID, PartitionID: partID, SegmentID: segID, IndexID: indexID,
				IndexVersion: 2, IndexStorePathVersion: version,
				IndexState: state, IndexFileKeys: keys,
			})
		}
		add(2000, commonpb.IndexState_Finished, []string{"file1"})
		add(2001, commonpb.IndexState_InProgress, nil)
		add(2002, commonpb.IndexState_Failed, nil) // aborted; meta GC removes the record first
		return m, &snapshot.SnapshotTestMetas{IndexMeta: im, SnapshotMeta: sm}
	}
	old := time.Now().Add(-2 * time.Hour)
	walk := func(files map[string]time.Time) func(context.Context, string, bool, storage.ChunkObjectWalkFunc) error {
		return func(_ context.Context, prefix string, recursive bool, fn storage.ChunkObjectWalkFunc) error {
			expected := "root/index_v1/"
			if version == indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED {
				expected = "root/insert_log"
			}
			assert.Equal(t, expected, prefix)
			assert.True(t, recursive)
			for file, mt := range files {
				if !fn(&storage.ChunkObjectInfo{FilePath: toPath(file), ModifyTime: mt}) {
					break
				}
			}
			return nil
		}
	}
	collectRemoved := func(cm *mocks.ChunkManager) *[]string {
		var mu sync.Mutex
		removed := make([]string, 0)
		cm.EXPECT().Remove(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, file string) error {
			mu.Lock()
			defer mu.Unlock()
			removed = append(removed, fromPath(file))
			return nil
		}).Maybe()
		return &removed
	}

	if metautil.IsSegmentRooted(version) {
		t.Run("single binlog traversal reclaims indexes with switch off", func(t *testing.T) {
			key := paramtable.Get().DataCoordCfg.WriteSegmentIndexToManifest.Key
			previous := paramtable.Get().DataCoordCfg.WriteSegmentIndexToManifest.GetValue()
			require.NoError(t, paramtable.Get().Save(key, "false"))
			t.Cleanup(func() { require.NoError(t, paramtable.Get().Save(key, previous)) })
			unblocked := mockey.Mock((*snapshot.SnapshotMeta).IsBuildIDGCBlocked).Return(false).Build()
			defer unblocked.UnPatch()
			cm := mocks.NewChunkManager(t)
			cm.EXPECT().RootPath().Return("root")
			walks := map[string]int{}
			cm.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, true, mock.Anything).RunAndReturn(
				func(_ context.Context, prefix string, _ bool, fn storage.ChunkObjectWalkFunc) error {
					walks[prefix]++
					if prefix == "root/insert_log" {
						for _, file := range []string{
							toPath("root/index_v1/100/200/300/2003/2/orphan"),
							"root/insert_log/100/200/300/_data/live",
							"root/insert_log/100/200/300/_manifest/1.manifest",
						} {
							fn(&storage.ChunkObjectInfo{FilePath: file, ModifyTime: old})
						}
					}
					return nil
				})
			removed := collectRemoved(cm)
			m, dm := newMeta(t)
			gc := NewGarbageCollector(m, nil, GcTestDomainOption(dm, GcOption{Cli: cm, MissingTolerance: time.Hour}))
			gc.RecycleUnusedBinlogFiles(context.TODO())
			require.Equal(t, 1, walks["root/insert_log"])
			for prefix, count := range walks {
				require.Equal(t, 1, count, prefix)
			}
			require.ElementsMatch(t, []string{"root/index_v1/100/200/300/2003/2/orphan"}, *removed)
		})
	}

	t.Run("removes files meta does not account for", func(t *testing.T) {
		unblocked := mockey.Mock((*snapshot.SnapshotMeta).IsBuildIDGCBlocked).Return(false).Build()
		defer unblocked.UnPatch()
		cm := mocks.NewChunkManager(t)
		cm.EXPECT().RootPath().Return("root")
		cm.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(walk(map[string]time.Time{
			"root/index_v1/100/200/300/2000/2/file1": old,        // recorded key: keep
			"root/index_v1/100/200/300/2000/2/stale": old,        // finished build, unrecorded file: remove
			"root/index_v1/100/200/300/2000/1/file1": old,        // earlier attempt of a finished build: remove
			"root/index_v1/100/200/300/2001/2/part":  old,        // build still running: keep
			"root/index_v1/100/200/300/2002/2/part":  old,        // aborted build whose record still exists: keep for now
			"root/index_v1/100/200/300/2003/2/part":  old,        // no record at all: remove
			"root/index_v1/100/200/300/2003/2/young": time.Now(), // may still be uploading: keep
			"root/insert_log/100/200/300/_data/file": old,        // ordinary V3 data must survive the index scan
			"root/insert_log/100/200/300/_index/bad": old,        // malformed v2 index: keep
			"root/index_v1/not/a/valid/path":         old,        // unparsable: skip
		}))
		removed := collectRemoved(cm)

		m, dm := newMeta(t)
		gc := NewGarbageCollector(m, nil, GcTestDomainOption(dm, GcOption{Cli: cm, MissingTolerance: time.Hour}))
		recycle(gc)

		assert.ElementsMatch(t, []string{
			"root/index_v1/100/200/300/2000/2/stale",
			"root/index_v1/100/200/300/2000/1/file1",
			"root/index_v1/100/200/300/2003/2/part",
		}, *removed)
	})

	t.Run("build of a segment still being restored is kept", func(t *testing.T) {
		unblocked := mockey.Mock((*snapshot.SnapshotMeta).IsBuildIDGCBlocked).Return(false).Build()
		defer unblocked.UnPatch()
		m, dm := newMeta(t)
		// A snapshot restore pre-registers its target segment as Importing and
		// only writes the SegmentIndex once every file has been copied.
		m.Segments.SetSegment(301, meta.NewSegmentInfo(&datapb.SegmentInfo{
			ID: 301, CollectionID: collID, PartitionID: partID,
			State: commonpb.SegmentState_Importing, IsImporting: true,
		}))
		m.Segments.SetSegment(302, meta.NewSegmentInfo(&datapb.SegmentInfo{
			ID: 302, CollectionID: collID, PartitionID: partID, State: commonpb.SegmentState_Flushed,
		}))
		cm := mocks.NewChunkManager(t)
		cm.EXPECT().RootPath().Return("root")
		cm.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(walk(map[string]time.Time{
			"root/index_v1/100/200/301/2004/1/part": old, // restore in progress, no record yet: keep
			"root/index_v1/100/200/302/2005/1/part": old, // live segment, no record: remove
		}))
		removed := collectRemoved(cm)

		gc := NewGarbageCollector(m, nil, GcTestDomainOption(dm, GcOption{Cli: cm, MissingTolerance: time.Hour}))
		recycle(gc)
		assert.ElementsMatch(t, []string{"root/index_v1/100/200/302/2005/1/part"}, *removed)
	})

	t.Run("interleaved builds are each decided correctly", func(t *testing.T) {
		unblocked := mockey.Mock((*snapshot.SnapshotMeta).IsBuildIDGCBlocked).Return(false).Build()
		defer unblocked.UnPatch()
		// Listing order is not guaranteed to keep a build's keys together; a
		// build seen again after another one must get the same decision.
		files := []string{
			"root/index_v1/100/200/300/2000/2/stale",
			"root/index_v1/100/200/300/2003/2/part",
			"root/index_v1/100/200/300/2000/2/file1",
			"root/index_v1/100/200/300/2003/2/part2",
			"root/index_v1/100/200/300/2000/1/file1",
		}
		cm := mocks.NewChunkManager(t)
		cm.EXPECT().RootPath().Return("root")
		cm.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, _ string, _ bool, fn storage.ChunkObjectWalkFunc) error {
				for _, file := range files {
					fn(&storage.ChunkObjectInfo{FilePath: toPath(file), ModifyTime: old})
				}
				return nil
			})
		removed := collectRemoved(cm)

		m, dm := newMeta(t)
		gc := NewGarbageCollector(m, nil, GcTestDomainOption(dm, GcOption{Cli: cm, MissingTolerance: time.Hour}))
		recycle(gc)
		assert.ElementsMatch(t, []string{
			"root/index_v1/100/200/300/2000/2/stale",
			"root/index_v1/100/200/300/2003/2/part",
			"root/index_v1/100/200/300/2003/2/part2",
			"root/index_v1/100/200/300/2000/1/file1",
		}, *removed)
	})

	t.Run("files of a dropped segment are left to dropped-segment GC", func(t *testing.T) {
		unblocked := mockey.Mock((*snapshot.SnapshotMeta).IsBuildIDGCBlocked).Return(false).Build()
		defer unblocked.UnPatch()
		m, dm := newMeta(t)
		// While the segment row survives, its files belong to
		// recycleDroppedSegment and to the cleanup plan of a rejected copy
		// task; the orphan scan only takes over once the row is gone.
		m.Segments.SetSegment(304, meta.NewSegmentInfo(&datapb.SegmentInfo{
			ID: 304, CollectionID: collID, PartitionID: partID, State: commonpb.SegmentState_Dropped,
		}))
		cm := mocks.NewChunkManager(t)
		cm.EXPECT().RootPath().Return("root")
		cm.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(walk(map[string]time.Time{
			"root/index_v1/100/200/304/2008/1/part": old, // dropped segment still in meta: keep
			"root/index_v1/100/200/305/2009/1/part": old, // segment row already gone: remove
		}))
		removed := collectRemoved(cm)

		gc := NewGarbageCollector(m, nil, GcTestDomainOption(dm, GcOption{Cli: cm, MissingTolerance: time.Hour}))
		recycle(gc)
		assert.ElementsMatch(t, []string{"root/index_v1/100/200/305/2009/1/part"}, *removed)
	})

	t.Run("snapshot protected build is kept", func(t *testing.T) {
		blocked := mockey.Mock((*snapshot.SnapshotMeta).IsBuildIDGCBlocked).To(func(_ *snapshot.SnapshotMeta, collectionID, buildID int64) bool {
			return collectionID == collID && buildID == 2003
		}).Build()
		defer blocked.UnPatch()
		cm := mocks.NewChunkManager(t)
		cm.EXPECT().RootPath().Return("root")
		cm.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(walk(map[string]time.Time{
			"root/index_v1/100/200/300/2003/2/part": old,
		}))
		removed := collectRemoved(cm)

		m, dm := newMeta(t)
		gc := NewGarbageCollector(m, nil, GcTestDomainOption(dm, GcOption{Cli: cm, MissingTolerance: time.Hour}))
		recycle(gc)
		assert.Empty(t, *removed)
	})

	t.Run("remove and walk failures are tolerated", func(t *testing.T) {
		unblocked := mockey.Mock((*snapshot.SnapshotMeta).IsBuildIDGCBlocked).Return(false).Build()
		defer unblocked.UnPatch()
		cm := mocks.NewChunkManager(t)
		cm.EXPECT().RootPath().Return("root")
		cm.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, _ string, _ bool, fn storage.ChunkObjectWalkFunc) error {
				fn(&storage.ChunkObjectInfo{FilePath: toPath("root/index_v1/100/200/300/2003/2/part"), ModifyTime: old})
				return errors.New("list error")
			})
		cm.EXPECT().Remove(mock.Anything, mock.Anything).Return(errors.New("remove error"))

		m, dm := newMeta(t)
		gc := NewGarbageCollector(m, nil, GcTestDomainOption(dm, GcOption{Cli: cm, MissingTolerance: time.Hour}))
		recycle(gc)
	})
}

func TestGarbageCollector_clearETCD(t *testing.T) {
	checkpointCatalog := MockCheckpointCatalog(t, 1000)

	catalog := catalogmocks.NewDataCoordCatalog(t)

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
	catalog.On("DropSegments",
		mock.Anything,
		mock.Anything,
	).Return(nil)
	catalog.On("DropSegmentIndexes",
		mock.Anything,
		mock.Anything,
	).Return(nil).Maybe()

	segments := map[UniqueID]*meta.SegmentInfo{
		meta.SegID: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:            meta.SegID,
				CollectionID:  meta.CollID,
				PartitionID:   meta.PartID,
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
		meta.SegID + 1: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:            meta.SegID + 1,
				CollectionID:  meta.CollID,
				PartitionID:   meta.PartID,
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
		meta.SegID + 2: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:            meta.SegID + 2,
				CollectionID:  meta.CollID,
				PartitionID:   meta.PartID,
				InsertChannel: "dmlChannel",
				NumOfRows:     10000,
				State:         commonpb.SegmentState_Dropped,
				MaxRowNum:     65536,
				DroppedAt:     10,
				DmlPosition: &msgpb.MsgPosition{
					Timestamp: 900,
				},
				CompactionFrom: []int64{meta.SegID, meta.SegID + 1},
			},
		},
		meta.SegID + 3: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:            meta.SegID + 3,
				CollectionID:  meta.CollID,
				PartitionID:   meta.PartID,
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
		meta.SegID + 4: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:            meta.SegID + 4,
				CollectionID:  meta.CollID,
				PartitionID:   meta.PartID,
				InsertChannel: "dmlChannel",
				NumOfRows:     12000,
				State:         commonpb.SegmentState_Flushed,
				MaxRowNum:     65536,
				DroppedAt:     10,
				DmlPosition: &msgpb.MsgPosition{
					Timestamp: 900,
				},
				CompactionFrom: []int64{meta.SegID + 2, meta.SegID + 3},
			},
		},
		meta.SegID + 5: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:             meta.SegID + 5,
				CollectionID:   meta.CollID,
				PartitionID:    meta.PartID,
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
		meta.SegID + 6: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:             meta.SegID + 6,
				CollectionID:   meta.CollID,
				PartitionID:    meta.PartID,
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
		meta.SegID + 7: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:             meta.SegID + 7,
				CollectionID:   meta.CollID,
				PartitionID:    meta.PartID,
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

	store := metacache.NewMetaStore(catalog)
	store.PutCollection(&meta.CollectionInfo{
		ID: meta.CollID,
		Schema: &schemapb.CollectionSchema{
			Name:        "",
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      meta.FieldID,
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
	segIdx0.Insert(meta.IndexID, &model.SegmentIndex{
		SegmentID:           meta.SegID,
		CollectionID:        meta.CollID,
		PartitionID:         meta.PartID,
		NumRows:             5000,
		IndexID:             meta.IndexID,
		BuildID:             meta.BuildID,
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
	segIdx1.Insert(meta.IndexID, &model.SegmentIndex{
		SegmentID:           meta.SegID + 1,
		CollectionID:        meta.CollID,
		PartitionID:         meta.PartID,
		NumRows:             5000,
		IndexID:             meta.IndexID,
		BuildID:             meta.BuildID + 1,
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
	segIndexes.Insert(meta.SegID, segIdx0)
	segIndexes.Insert(meta.SegID+1, segIdx1)
	im := &index.IndexMeta{
		KeyLock:          lock.NewKeyLock[UniqueID](),
		Catalog:          catalog,
		SegmentIndexes:   segIndexes,
		SegmentBuildInfo: index.NewSegmentIndexBuildInfo(),
		Indexes: map[UniqueID]map[UniqueID]*model.Index{
			meta.CollID: {
				meta.IndexID: {
					TenantID:        "",
					CollectionID:    meta.CollID,
					FieldID:         meta.FieldID,
					IndexID:         meta.IndexID,
					IndexName:       meta.IndexName,
					IsDeleted:       false,
					CreateTime:      0,
					TypeParams:      nil,
					IndexParams:     nil,
					IsAutoIndex:     false,
					UserIndexParams: nil,
				},
			},
		},
	}
	sm := &snapshot.SnapshotMeta{}
	m := &meta.Meta{
		Catalog:   catalog,
		MetaStore: store,
		Segments:  meta.NewSegmentsInfo(store),
	}
	m.CompactionProtection = sm
	m.MetaStore.LoadChannelCheckpoints(map[string]*msgpb.MsgPosition{
		"dmlChannel": {Timestamp: 1000},
	})

	im.SegmentBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           meta.SegID,
		CollectionID:        meta.CollID,
		PartitionID:         meta.PartID,
		NumRows:             5000,
		IndexID:             meta.IndexID,
		BuildID:             meta.BuildID,
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

	im.SegmentBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           meta.SegID + 1,
		CollectionID:        meta.CollID,
		PartitionID:         meta.PartID,
		NumRows:             5000,
		IndexID:             meta.IndexID,
		BuildID:             meta.BuildID + 1,
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
		m.Segments.SetSegment(id, segment)
	}

	for segID, segment := range map[UniqueID]*meta.SegmentInfo{
		meta.SegID: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:            meta.SegID,
				CollectionID:  meta.CollID,
				PartitionID:   meta.PartID,
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
		meta.SegID + 1: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:            meta.SegID + 1,
				CollectionID:  meta.CollID,
				PartitionID:   meta.PartID,
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
		meta.SegID + 2: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:            meta.SegID + 2,
				CollectionID:  meta.CollID,
				PartitionID:   meta.PartID,
				InsertChannel: "dmlChannel",
				NumOfRows:     10000,
				State:         commonpb.SegmentState_Dropped,
				MaxRowNum:     65536,
				DroppedAt:     10,
				DmlPosition: &msgpb.MsgPosition{
					Timestamp: 900,
				},
				CompactionFrom: []int64{meta.SegID, meta.SegID + 1},
			},
		},
		meta.SegID + 3: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:            meta.SegID + 3,
				CollectionID:  meta.CollID,
				PartitionID:   meta.PartID,
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
		meta.SegID + 4: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:            meta.SegID + 4,
				CollectionID:  meta.CollID,
				PartitionID:   meta.PartID,
				InsertChannel: "dmlChannel",
				NumOfRows:     12000,
				State:         commonpb.SegmentState_Flushed,
				MaxRowNum:     65536,
				DroppedAt:     10,
				DmlPosition: &msgpb.MsgPosition{
					Timestamp: 900,
				},
				CompactionFrom: []int64{meta.SegID + 2, meta.SegID + 3},
			},
		},
		meta.SegID + 5: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:             meta.SegID + 5,
				CollectionID:   meta.CollID,
				PartitionID:    meta.PartID,
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
		meta.SegID + 6: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:             meta.SegID + 6,
				CollectionID:   meta.CollID,
				PartitionID:    meta.PartID,
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
		meta.SegID + 7: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:             meta.SegID + 7,
				CollectionID:   meta.CollID,
				PartitionID:    meta.PartID,
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
		meta.SegID + 8: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:             meta.SegID + 8,
				CollectionID:   meta.CollID,
				PartitionID:    meta.PartID,
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
		m.Segments.SetSegment(segID, segment)
	}

	cm := &mocks.ChunkManager{}
	cm.EXPECT().Remove(mock.Anything, mock.Anything).Return(nil)
	cm.EXPECT().RootPath().Return("").Maybe()
	signal := make(chan gcCmd)
	gc := NewGarbageCollector(
		m,
		meta.NewMockHandlerWithMeta(m),
		GcTestDomainOption(&snapshot.SnapshotTestMetas{IndexMeta: im, SnapshotMeta: sm}, GcOption{
			StreamingNodeCatalog: checkpointCatalog,
			Cli:                  cm,
			DropTolerance:        1,
		}))
	// Snapshot layer transparent: no segment is blocked.
	mockIsSegBlocked := mockey.Mock((*snapshot.SnapshotMeta).IsSegmentGCBlocked).Return(false).Build()
	defer mockIsSegBlocked.UnPatch()
	gc.RecycleDroppedSegments(context.TODO(), signal)

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
	segA := gc.Meta.GetSegment(context.TODO(), meta.SegID)
	assert.NotNil(t, segA)
	segB := gc.Meta.GetSegment(context.TODO(), meta.SegID+1)
	assert.NotNil(t, segB)
	segC := gc.Meta.GetSegment(context.TODO(), meta.SegID+2)
	assert.NotNil(t, segC)
	segD := gc.Meta.GetSegment(context.TODO(), meta.SegID+3)
	assert.NotNil(t, segD)
	segE := gc.Meta.GetSegment(context.TODO(), meta.SegID+4)
	assert.NotNil(t, segE)
	segF := gc.Meta.GetSegment(context.TODO(), meta.SegID+5)
	assert.NotNil(t, segF)
	segG := gc.Meta.GetSegment(context.TODO(), meta.SegID+6)
	assert.NotNil(t, segG)
	segH := gc.Meta.GetSegment(context.TODO(), meta.SegID+7)
	assert.NotNil(t, segH)
	segG = gc.Meta.GetSegment(context.TODO(), meta.SegID+8)
	assert.Nil(t, segG)
	err := im.AddSegmentIndex(context.TODO(), &model.SegmentIndex{
		SegmentID:    meta.SegID + 4,
		CollectionID: meta.CollID,
		PartitionID:  meta.PartID,
		NumRows:      12000,
		IndexID:      meta.IndexID,
		BuildID:      meta.BuildID + 4,
	})
	assert.NoError(t, err)

	err = im.FinishTask(&workerpb.IndexTaskInfo{
		BuildID:        meta.BuildID + 4,
		State:          commonpb.IndexState_Finished,
		IndexFileKeys:  []string{"file1", "file2", "file3", "file4"},
		SerializedSize: 10240,
		FailReason:     "",
	})
	assert.NoError(t, err)

	gc.RecycleDroppedSegments(context.TODO(), signal)
	/*

		A: processed prior to C, C is not GCed yet and C is not indexed, A is not GCed in this turn
		B: processed prior to C, C is not GCed yet and C is not indexed, B is not GCed in this turn

		E: flushed, indexed, should not be GCed
		C: dropped, not indexed, should be GCed since E is indexed
		D: dropped, not indexed, should be GCed since E is indexed
	*/

	segC = gc.Meta.GetSegment(context.TODO(), meta.SegID+2)
	assert.Nil(t, segC)
	segD = gc.Meta.GetSegment(context.TODO(), meta.SegID+3)
	assert.Nil(t, segD)

	gc.RecycleDroppedSegments(context.TODO(), signal)
	/*
		A: compacted became false due to C is GCed already, A should be GCed since dropTolernace is meet
		B: compacted became false due to C is GCed already, B should be GCed since dropTolerance is meet
	*/
	segA = gc.Meta.GetSegment(context.TODO(), meta.SegID)
	assert.Nil(t, segA)
	segB = gc.Meta.GetSegment(context.TODO(), meta.SegID+1)
	assert.Nil(t, segB)
}

func TestGarbageCollector_recycleChannelMeta(t *testing.T) {
	catalog := catalogmocks.NewDataCoordCatalog(t)
	store := metacache.NewMetaStore(catalog)

	m := &meta.Meta{
		Ctx:         context.Background(),
		Catalog:     catalog,
		MetaStore:   store,
		ChannelSync: meta.NewChannelSync(),
	}

	store.LoadChannelCheckpoints(map[string]*msgpb.MsgPosition{
		"cluster-id-rootcoord-dm_0_123v0": nil,
		"cluster-id-rootcoord-dm_1_123v0": nil,
		"cluster-id-rootcoord-dm_0_124v0": nil,
		// A vchannel whose name does not parse to a collection ID. The GC must
		// skip it rather than GC it -- see the "it will lead meta leak in this
		// case" guard in recycleChannelCPMeta. Without this entry that branch
		// has no coverage anywhere.
		"cluster-id-rootcoord-dm_0_invalidedCollectionIDv0": nil,
	})
	store.PutCollection(&metacache.CollectionInfo{ID: 123})
	store.PutCollection(&metacache.CollectionInfo{ID: 124})

	gc := NewGarbageCollector(m, meta.NewMockHandlerWithMeta(m), GcOption{})

	t.Run("list channel cp fail", func(t *testing.T) {
		catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, errors.New("mock error")).Once()
		gc.RecycleChannelCPMeta(context.TODO(), nil)
		assert.Equal(t, 4, len(m.MetaStore.GetChannelCheckpoints()))
	})

	// The persisted checkpoints mirror the store, so drops are visible to
	// the next GC round.
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).
		RunAndReturn(func(ctx context.Context) (map[string]*msgpb.MsgPosition, error) {
			return store.GetChannelCheckpoints(), nil
		}).Times(4)

	catalog.EXPECT().GcConfirm(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, collectionID int64, i2 int64) bool {
			return collectionID == 123
		}).Maybe()

	t.Run("skip drop channel due to collection is available", func(t *testing.T) {
		gc.RecycleChannelCPMeta(context.TODO(), nil)
		assert.Equal(t, 4, len(m.MetaStore.GetChannelCheckpoints()))
	})

	store.RemoveCollection(123)
	store.RemoveCollection(124)
	t.Run("drop collection meta fail keeps channel cp", func(t *testing.T) {
		catalog.EXPECT().DropSegmentChangeGroups(mock.Anything, int64(123)).Return(errors.New("mock error")).Once()
		gc.RecycleChannelCPMeta(context.TODO(), nil)
		assert.Equal(t, 4, len(m.MetaStore.GetChannelCheckpoints()))
	})

	t.Run("drop channel cp fail", func(t *testing.T) {
		catalog.EXPECT().DropSegmentChangeGroups(mock.Anything, int64(123)).Return(nil).Once()
		catalog.EXPECT().DropChannelCheckpoint(mock.Anything, mock.Anything).Return(errors.New("mock error")).Twice()
		gc.RecycleChannelCPMeta(context.TODO(), nil)
		assert.Equal(t, 4, len(m.MetaStore.GetChannelCheckpoints()))
	})

	t.Run("channel cp gc ok", func(t *testing.T) {
		catalog.EXPECT().DropSegmentChangeGroups(mock.Anything, int64(123)).Return(nil).Once()
		catalog.EXPECT().DropChannelCheckpoint(mock.Anything, mock.Anything).Return(nil).Twice()
		gc.RecycleChannelCPMeta(context.TODO(), nil)
		// 123's checkpoints are GC'd (its segments are all recycled); 124's
		// survive (GcConfirm is false) and so does the unparseable one, which
		// must never be GC'd.
		assert.Equal(t, 2, len(m.MetaStore.GetChannelCheckpoints()))
		_, stillThere := m.MetaStore.GetChannelCheckpoints()["cluster-id-rootcoord-dm_0_invalidedCollectionIDv0"]
		assert.True(t, stillThere, "an unparseable vchannel must be skipped, not GC'd")
	})
}

func TestGarbageCollector_recycleDroppedChannelFlags(t *testing.T) {
	catalog := catalogmocks.NewDataCoordCatalog(t)
	m := &meta.Meta{Catalog: catalog, MetaStore: metacache.NewMetaStore(catalog)}
	gc := NewGarbageCollector(m, meta.NewMockHandlerWithMeta(m), GcOption{CheckInterval: time.Millisecond})

	// A failed sweep is retried, and the first success ends the loop.
	catalog.EXPECT().RemoveDroppedChannelFlags(mock.Anything).Return(0, errors.New("mock error")).Once()
	catalog.EXPECT().RemoveDroppedChannelFlags(mock.Anything).Return(2, nil).Once()
	gc.recycleDroppedChannelFlags(context.TODO())
}

func TestGarbageCollector_removeObjectPool(t *testing.T) {
	paramtable.Init()
	cm := mocks.NewChunkManager(t)
	gc := NewGarbageCollector(
		nil,
		nil,
		GcOption{
			Cli:           cm,
			DropTolerance: 1,
		})
	logs := make(map[string]struct{})
	for i := 0; i < 50; i++ {
		logs[fmt.Sprintf("log%d", i)] = struct{}{}
	}

	t.Run("success", func(t *testing.T) {
		call := cm.EXPECT().Remove(mock.Anything, mock.Anything).Return(nil)
		defer call.Unset()
		b := gc.RemoveObjectFiles(context.TODO(), logs)
		assert.NoError(t, b)
	})

	t.Run("oss not found error", func(t *testing.T) {
		call := cm.EXPECT().Remove(mock.Anything, mock.Anything).Return(merr.WrapErrIoKeyNotFound("not found"))
		defer call.Unset()
		b := gc.RemoveObjectFiles(context.TODO(), logs)
		assert.NoError(t, b)
	})

	t.Run("oss server error", func(t *testing.T) {
		call := cm.EXPECT().Remove(mock.Anything, mock.Anything).Return(merr.WrapErrIoFailed("server error", errors.New("err")))
		defer call.Unset()
		b := gc.RemoveObjectFiles(context.TODO(), logs)
		assert.Error(t, b)
	})

	t.Run("other type error", func(t *testing.T) {
		call := cm.EXPECT().Remove(mock.Anything, mock.Anything).Return(errors.New("other error"))
		defer call.Unset()
		b := gc.RemoveObjectFiles(context.TODO(), logs)
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

	meta *meta.Meta
	dm   *snapshot.SnapshotTestMetas
}

func (s *GarbageCollectorSuite) SetupTest() {
	s.bucketName = `datacoord-ut` + strings.ToLower(funcutil.RandomString(8))
	s.rootPath = `gc` + funcutil.RandomString(8)

	var err error
	s.cli, s.inserts, s.stats, s.delta, s.others, err = initUtOSSEnv(s.bucketName, s.rootPath, 4)
	s.Require().NoError(err)

	s.meta, s.dm, err = newMemoryMetaWithSnapshot(s.T())
	s.Require().NoError(err)
}

func (s *GarbageCollectorSuite) TearDownTest() {
	cleanupOSS(s.cli, s.bucketName, s.rootPath)
}

func (s *GarbageCollectorSuite) TestPauseResume() {
	s.Run("not_enabled", func() {
		gc := NewGarbageCollector(s.meta, meta.NewMockHandler(), GcTestDomainOption(s.dm, GcOption{
			Cli:              s.cli,
			Enabled:          false,
			CheckInterval:    time.Millisecond * 10,
			ScanInterval:     time.Hour * 24 * 7,
			MissingTolerance: time.Hour * 24,
			DropTolerance:    time.Hour * 24,
		}))

		gc.Start()
		defer gc.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := gc.Pause(ctx, -1, "", time.Second)
		s.NoError(err)

		err = gc.Resume(ctx, -1, "")
		s.Error(err)
	})

	s.Run("pause_then_resume", func() {
		gc := NewGarbageCollector(s.meta, meta.NewMockHandler(), GcTestDomainOption(s.dm, GcOption{
			Cli:              s.cli,
			Enabled:          true,
			CheckInterval:    time.Millisecond * 10,
			ScanInterval:     time.Hour * 7 * 24,
			MissingTolerance: time.Hour * 24,
			DropTolerance:    time.Hour * 24,
		}))

		gc.Start()
		defer gc.Close()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := gc.Pause(ctx, -1, "", time.Minute)
		s.NoError(err)

		s.NotZero(gc.pauseUntil.PauseUntil())

		err = gc.Resume(ctx, -1, "")
		s.NoError(err)

		s.Zero(gc.pauseUntil.PauseUntil())
	})

	s.Run("resume_without_ticket_releases_every_pause", func() {
		gc := NewGarbageCollector(s.meta, meta.NewMockHandler(), GcTestDomainOption(s.dm, GcOption{
			Cli:              s.cli,
			Enabled:          true,
			CheckInterval:    time.Millisecond * 10,
			ScanInterval:     time.Hour * 7 * 24,
			MissingTolerance: time.Hour * 24,
			DropTolerance:    time.Hour * 24,
		}))

		gc.Start()
		defer gc.Close()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		s.NoError(gc.Pause(ctx, -1, "ticket-1", time.Minute))
		s.NoError(gc.Pause(ctx, -1, "ticket-2", time.Minute))
		s.NoError(gc.Pause(ctx, 100, "ticket-3", time.Minute))
		s.True(gc.GetStatus().IsPaused)
		s.True(gc.collectionGCPaused(100))

		// a caller predating collection level GC control sends no ticket, it means
		// "GC must not be paused anymore"
		s.NoError(gc.Resume(ctx, -1, ""))

		s.Zero(gc.pauseUntil.PauseUntil())
		s.False(gc.GetStatus().IsPaused)
		s.False(gc.collectionGCPaused(100))
	})

	s.Run("resume_with_ticket_keeps_other_pauses", func() {
		gc := NewGarbageCollector(s.meta, meta.NewMockHandler(), GcTestDomainOption(s.dm, GcOption{
			Cli:              s.cli,
			Enabled:          true,
			CheckInterval:    time.Millisecond * 10,
			ScanInterval:     time.Hour * 7 * 24,
			MissingTolerance: time.Hour * 24,
			DropTolerance:    time.Hour * 24,
		}))

		gc.Start()
		defer gc.Close()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		s.NoError(gc.Pause(ctx, -1, "ticket-1", time.Minute))
		s.NoError(gc.Pause(ctx, -1, "ticket-2", time.Minute))

		s.NoError(gc.Resume(ctx, -1, "ticket-1"))
		s.True(gc.GetStatus().IsPaused)

		s.NoError(gc.Resume(ctx, -1, "ticket-2"))
		s.False(gc.GetStatus().IsPaused)
	})

	s.Run("resume_with_ticket_keeps_other_collection_pauses", func() {
		gc := NewGarbageCollector(s.meta, meta.NewMockHandler(), GcTestDomainOption(s.dm, GcOption{
			Cli:              s.cli,
			Enabled:          true,
			CheckInterval:    time.Millisecond * 10,
			ScanInterval:     time.Hour * 7 * 24,
			MissingTolerance: time.Hour * 24,
			DropTolerance:    time.Hour * 24,
		}))

		gc.Start()
		defer gc.Close()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		s.NoError(gc.Pause(ctx, 100, "ticket-1", time.Minute))
		s.NoError(gc.Pause(ctx, 200, "ticket-2", time.Minute))

		s.NoError(gc.Resume(ctx, 100, "ticket-1"))

		s.False(gc.collectionGCPaused(100))
		s.True(gc.collectionGCPaused(200))
	})

	s.Run("pause_before_until", func() {
		gc := NewGarbageCollector(s.meta, meta.NewMockHandler(), GcTestDomainOption(s.dm, GcOption{
			Cli:              s.cli,
			Enabled:          true,
			CheckInterval:    time.Millisecond * 10,
			ScanInterval:     time.Hour * 7 * 24,
			MissingTolerance: time.Hour * 24,
			DropTolerance:    time.Hour * 24,
		}))

		gc.Start()
		defer gc.Close()
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
		gc := NewGarbageCollector(s.meta, meta.NewMockHandler(), GcTestDomainOption(s.dm, GcOption{
			Cli:              s.cli,
			Enabled:          true,
			CheckInterval:    time.Millisecond * 10,
			ScanInterval:     time.Hour * 7 * 24,
			MissingTolerance: time.Hour * 24,
			DropTolerance:    time.Hour * 24,
		}))

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		err := gc.Pause(ctx, -1, "", time.Minute)
		s.Error(err)

		s.Zero(gc.pauseUntil.PauseUntil())

		err = gc.Resume(ctx, -1, "")
		s.Error(err)

		s.Zero(gc.pauseUntil.PauseUntil())
	})

	s.Run("pause_timeout_after_signal_received", func() {
		gc := NewGarbageCollector(s.meta, meta.NewMockHandler(), GcTestDomainOption(s.dm, GcOption{
			Cli:              s.cli,
			Enabled:          true,
			CheckInterval:    time.Hour,
			ScanInterval:     time.Hour * 7 * 24,
			MissingTolerance: time.Hour * 24,
			DropTolerance:    time.Hour * 24,
		}))

		controlDone := make(chan struct{})
		go func() {
			defer close(controlDone)
			gc.startControlLoop(context.Background())
		}()
		defer func() {
			gc.cancel()
			<-controlDone
			gc.Option.RemoveObjectPool.Release()
		}()

		signalCh := make(chan gcCmd, 1)
		go func() {
			signalCh <- <-gc.controlChannels["meta"]
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		pauseDone := make(chan error, 1)
		go func() {
			pauseDone <- gc.Pause(ctx, -1, "timeout-ticket", time.Minute)
		}()

		// Receiving here means the meta worker already took the signal, so pause()
		// is past the send and parked in the ack wait.
		select {
		case <-signalCh:
		case <-time.After(time.Second * 5):
			s.T().Fatal("pause signal was not delivered to meta worker")
		}

		// Deliberately never close signal.done: canceling only now makes the inner
		// select take its timeout arm, with no dependency on wall-clock ordering.
		cancel()

		err := <-pauseDone
		s.ErrorIs(err, context.Canceled)
		s.Zero(gc.pauseUntil.PauseUntil())
	})

	// Tickets are not unique: the REST route in restful_mgr_routes.go issues every
	// pause with an empty ticket. A failed pause must therefore roll back only its
	// own record -- a ticket-scoped delete would also wipe a concurrent caller's
	// still-valid pause and silently resume GC while that caller believes it is
	// paused.
	s.Run("pause_rollback_preserves_other_empty_ticket_record", func() {
		gc := NewGarbageCollector(s.meta, meta.NewMockHandler(), GcTestDomainOption(s.dm, GcOption{
			Cli:              s.cli,
			Enabled:          true,
			CheckInterval:    time.Hour,
			ScanInterval:     time.Hour * 7 * 24,
			MissingTolerance: time.Hour * 24,
			DropTolerance:    time.Hour * 24,
		}))

		controlDone := make(chan struct{})
		go func() {
			defer close(controlDone)
			gc.startControlLoop(context.Background())
		}()
		defer func() {
			gc.cancel()
			<-controlDone
			gc.Option.RemoveObjectPool.Release()
		}()

		metaCh := gc.controlChannels["meta"]
		secondSignal := make(chan struct{})
		go func() {
			// Ack the first pause so it completes.
			cmd := <-metaCh
			close(cmd.done)
			// Take the second pause's signal but never ack it, parking pause() in
			// the inner ack wait so its rollback path is the one exercised.
			<-metaCh
			close(secondSignal)
		}()

		// First caller: a successful global pause with an empty ticket.
		s.NoError(gc.Pause(context.Background(), -1, "", time.Minute))
		firstUntil := gc.pauseUntil.PauseUntil()
		s.NotZero(firstUntil)

		// Second caller: same empty ticket, canceled while waiting for the ack.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		pauseDone := make(chan error, 1)
		go func() {
			pauseDone <- gc.Pause(ctx, -1, "", time.Minute)
		}()

		select {
		case <-secondSignal:
		case <-time.After(time.Second * 5):
			s.T().Fatal("second pause signal was not delivered to meta worker")
		}
		cancel()
		s.ErrorIs(<-pauseDone, context.Canceled)

		// The first caller's pause must survive the second caller's rollback.
		s.Equal(firstUntil, gc.pauseUntil.PauseUntil())
	})

	s.Run("pause_collection", func() {
		gc := NewGarbageCollector(s.meta, meta.NewMockHandler(), GcTestDomainOption(s.dm, GcOption{
			Cli:              s.cli,
			Enabled:          true,
			CheckInterval:    time.Millisecond * 10,
			ScanInterval:     time.Hour * 7 * 24,
			MissingTolerance: time.Hour * 24,
			DropTolerance:    time.Hour * 24,
		}))

		gc.Start()
		defer gc.Close()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ticket := uuid.NewString()
		err := gc.Pause(ctx, 100, ticket, time.Minute)
		s.NoError(err)

		until, has := gc.PausedCollection.Get(100)
		firstPauseUntil := until.PauseUntil()
		s.True(has)
		s.NotZero(firstPauseUntil)

		ticket2 := uuid.NewString()
		err = gc.Pause(ctx, 100, ticket2, time.Second*30)
		s.NoError(err)

		second, has := gc.PausedCollection.Get(100)
		secondPauseUntil := second.PauseUntil()
		s.True(has)

		s.Equal(firstPauseUntil, secondPauseUntil)

		err = gc.Resume(ctx, 100, ticket2)
		s.NoError(err)

		afterResume, has := gc.PausedCollection.Get(100)
		s.True(has)
		afterUntil := afterResume.PauseUntil()
		s.Equal(firstPauseUntil, afterUntil)

		_ = gc.Resume(ctx, 100, ticket)

		_, has = gc.PausedCollection.Get(100)
		s.False(has)
	})
}

func (s *GarbageCollectorSuite) TestRunRecycleTaskWithPauser() {
	gc := NewGarbageCollector(s.meta, meta.NewMockHandler(), GcTestDomainOption(s.dm, GcOption{
		Cli:              s.cli,
		Enabled:          true,
		CheckInterval:    time.Millisecond * 10,
		ScanInterval:     time.Hour * 7 * 24,
		MissingTolerance: time.Hour * 24,
		DropTolerance:    time.Hour * 24,
	}))

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*2500)
	defer cancel()

	cnt := 0
	gc.runRecycleTaskWithPauser(ctx, "test", time.Second, func(ctx context.Context, signal <-chan gcCmd) {
		cnt++
	})
	s.Equal(cnt, 2)
}

func (s *GarbageCollectorSuite) TestAvoidGCLoadedSegments() {
	checkpointCatalog := MockCheckpointCatalog(s.T(), 1000)

	handler := meta.NewNMockHandler(s.T())
	handler.EXPECT().ListLoadedSegments(mock.Anything).Return([]int64{1}, nil).Once()
	gc := NewGarbageCollector(s.meta, handler, GcTestDomainOption(s.dm, GcOption{
		StreamingNodeCatalog: checkpointCatalog,
		Cli:                  s.cli,
		Enabled:              true,
		CheckInterval:        time.Millisecond * 10,
		ScanInterval:         time.Hour * 7 * 24,
		MissingTolerance:     time.Hour * 24,
		DropTolerance:        time.Hour * 24,
	}))

	s.meta.AddSegment(context.TODO(), &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:        1,
			State:     commonpb.SegmentState_Dropped,
			DroppedAt: 0,
		},
	})

	signal := make(chan gcCmd)
	gc.RecycleDroppedSegments(context.TODO(), signal)
	seg := s.meta.GetSegment(context.TODO(), 1)
	s.NotNil(seg)
}

func TestGarbageCollector(t *testing.T) {
	suite.Run(t, new(GarbageCollectorSuite))
}

func TestGarbageCollector_recycleDroppedSegments_NoIndexCollection(t *testing.T) {
	checkpointCatalog := MockCheckpointCatalog(t, 1000)

	const (
		collectionID    = int64(100)
		indexID         = int64(200)
		parentSegmentID = int64(1000)
		childSegmentID  = int64(1001)
		channelName     = "no-index-channel"
	)

	tests := []struct {
		name    string
		indexes map[UniqueID]map[UniqueID]*model.Index
	}{
		{
			name:    "without index definitions",
			indexes: make(map[UniqueID]map[UniqueID]*model.Index),
		},
		{
			name: "with deleted index definition",
			indexes: map[UniqueID]map[UniqueID]*model.Index{
				collectionID: {
					indexID: {
						CollectionID: collectionID,
						IndexID:      indexID,
						IsDeleted:    true,
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			catalog := catalogmocks.NewDataCoordCatalog(t)

			store := metacache.NewMetaStore(catalog)
			im := &index.IndexMeta{
				Indexes: test.indexes,
			}
			mt := &meta.Meta{
				Catalog:   catalog,
				MetaStore: store,
				Segments:  meta.NewSegmentsInfo(store),
			}
			mt.Segments.SetSegment(parentSegmentID, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID:            parentSegmentID,
				CollectionID:  collectionID,
				InsertChannel: channelName,
				State:         commonpb.SegmentState_Dropped,
				DroppedAt:     uint64(time.Now().Add(-time.Hour).UnixNano()),
			}})
			mt.Segments.SetSegment(childSegmentID, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID:             childSegmentID,
				CollectionID:   collectionID,
				InsertChannel:  channelName,
				State:          commonpb.SegmentState_Flushed,
				CompactionFrom: []int64{parentSegmentID},
			}})

			handler := meta.NewNMockHandler(t)
			handler.EXPECT().ListLoadedSegments(mock.Anything).Return(nil, nil).Once()

			recycled := make([]int64, 0, 1)
			mockRecycle := mockey.Mock((*GarbageCollector).prepareDroppedSegmentGCBatchCandidate).
				To(func(_ *GarbageCollector, _ context.Context, _ *packed.ManifestIOContext, segmentID int64, _ *meta.SegmentInfo) (*droppedSegmentGCBatchCandidate, error) {
					recycled = append(recycled, segmentID)
					return nil, nil
				}).Build()
			defer mockRecycle.UnPatch()

			gc := NewGarbageCollector(mt, handler, GcTestDomainOption(&snapshot.SnapshotTestMetas{IndexMeta: im}, GcOption{StreamingNodeCatalog: checkpointCatalog, DropTolerance: 0}))
			gc.RecycleDroppedSegments(context.Background(), nil)

			require.Equal(t, []int64{parentSegmentID}, recycled)
			handler.AssertNotCalled(t, "GetCollection", mock.Anything, mock.Anything)
		})
	}
}

// TestGarbageCollector_recycleDroppedSegments_LoadedSegments pins the GC side
// of the contract QueryCoord targets rely on: a segment ListLoadedSegments
// reports is never recycled, and if the call fails no segment is recycled.
// TestListLoadedSegmentsCoversCurrentTarget pins the QueryCoord side.
func TestGarbageCollector_recycleDroppedSegments_LoadedSegments(t *testing.T) {
	const (
		collectionID = int64(100)
		loadedID     = int64(1000)
		unloadedID   = int64(1001)
		channelName  = "loaded-segments-channel"
	)

	tests := []struct {
		name     string
		loaded   []int64
		err      error
		recycled []int64
	}{
		{"loaded segment kept", []int64{loadedID}, nil, []int64{unloadedID}},
		{"nothing loaded", nil, nil, []int64{loadedID, unloadedID}},
		{"list fails", nil, errors.New("mock"), nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			catalog := catalogmocks.NewDataCoordCatalog(t)
			checkpointCatalog := MockCheckpointCatalog(t, 1000)

			store := metacache.NewMetaStore(catalog)
			im := &index.IndexMeta{Indexes: make(map[UniqueID]map[UniqueID]*model.Index)}
			mt := &meta.Meta{
				Catalog:   catalog,
				MetaStore: store,
				Segments:  meta.NewSegmentsInfo(store),
			}
			for _, id := range []int64{loadedID, unloadedID} {
				mt.Segments.SetSegment(id, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
					ID:            id,
					CollectionID:  collectionID,
					InsertChannel: channelName,
					State:         commonpb.SegmentState_Dropped,
					DroppedAt:     uint64(time.Now().Add(-time.Hour).UnixNano()),
				}})
			}

			handler := meta.NewNMockHandler(t)
			handler.EXPECT().ListLoadedSegments(mock.Anything).Return(test.loaded, test.err).Once()

			var recycled []int64
			if len(test.recycled) > 0 {
				catalog.EXPECT().DropSegments(mock.Anything, mock.Anything).RunAndReturn(
					func(_ context.Context, segments []*datapb.SegmentInfo) error {
						for _, segment := range segments {
							recycled = append(recycled, segment.ID)
						}
						return nil
					}).Once()
			}

			gc := NewGarbageCollector(mt, handler, GcTestDomainOption(&snapshot.SnapshotTestMetas{IndexMeta: im}, GcOption{StreamingNodeCatalog: checkpointCatalog, DropTolerance: 0}))
			gc.RecycleDroppedSegments(context.Background(), nil)

			assert.ElementsMatch(t, test.recycled, recycled)
		})
	}
}

func setupDroppedSegmentWithIndexForGC(t *testing.T) (*meta.Meta, *snapshot.SnapshotTestMetas, *meta.SegmentInfo, *model.SegmentIndex, string) {
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

	m, dm, err := newMemoryMetaWithSnapshot(t)
	require.NoError(t, err)
	segment := &meta.SegmentInfo{
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

	if dm.IndexMeta.Indexes == nil {
		dm.IndexMeta.Indexes = make(map[UniqueID]map[UniqueID]*model.Index)
	}
	dm.IndexMeta.Indexes[collID] = map[UniqueID]*model.Index{
		indexID: {
			CollectionID: collID,
			FieldID:      fieldID,
			IndexID:      indexID,
			IndexName:    "deleted-index",
			IsDeleted:    true,
		},
	}
	segIdx := &model.SegmentIndex{
		SegmentID:             segID,
		CollectionID:          collID,
		PartitionID:           partID,
		IndexID:               indexID,
		BuildID:               buildID,
		IndexVersion:          1,
		IndexStorePathVersion: 0,
		IndexState:            commonpb.IndexState_Finished,
		IndexFileKeys:         []string{"idx-file"},
	}
	require.NoError(t, dm.IndexMeta.AddSegmentIndex(ctx, segIdx))
	return m, dm, segment, segIdx, binlogPath
}

func TestGarbageCollector_recycleDroppedSegments_RecyclesSegmentIndexMeta(t *testing.T) {
	checkpointCatalog := MockCheckpointCatalog(t, 1000)

	ctx := context.Background()
	m, dm, segment, segIdx, binlogPath := setupDroppedSegmentWithIndexForGC(t)

	expectedIndexFile := path.Join("root", common.SegmentIndexV0Path, strconv.FormatInt(segIdx.BuildID, 10),
		strconv.FormatInt(segIdx.IndexVersion, 10), strconv.FormatInt(segIdx.PartitionID, 10),
		strconv.FormatInt(segIdx.SegmentID, 10), "idx-file")

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

	gc := NewGarbageCollector(m, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{
		StreamingNodeCatalog: checkpointCatalog,
		Cli:                  cm,
		DropTolerance:        0,
	}))

	assert.Empty(t, dm.IndexMeta.GetSegmentIndexes(segment.CollectionID, segment.ID),
		"deleted field indexes are filtered by the existing query API")
	assert.Len(t, dm.IndexMeta.GetAllSegmentIndexes(segment.ID), 1)

	gc.RecycleDroppedSegments(ctx, nil)

	assert.Nil(t, m.GetSegment(ctx, segment.ID))
	assert.Empty(t, dm.IndexMeta.GetAllSegmentIndexes(segment.ID))
	mu.Lock()
	assert.ElementsMatch(t, []string{binlogPath, expectedIndexFile}, removed)
	mu.Unlock()
}

// TestGarbageCollector_recycleDroppedSegments_StorageV3WithoutManifest pins that
// a StorageV3 segment dropped before any manifest was written (import retry,
// failure, or cleanup) is removed from meta instead of erroring forever on the
// empty manifest path.
func TestGarbageCollector_recycleDroppedSegments_StorageV3WithoutManifest(t *testing.T) {
	checkpointCatalog := MockCheckpointCatalog(t, 1000)

	ctx := context.Background()
	m, dm, err := newMemoryMetaWithSnapshot(t)
	require.NoError(t, err)
	const segID = int64(1001)
	segment := &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             segID,
			CollectionID:   100,
			PartitionID:    10,
			InsertChannel:  "ch1",
			State:          commonpb.SegmentState_Dropped,
			DroppedAt:      uint64(time.Now().Add(-time.Hour).UnixNano()),
			StorageVersion: storage.StorageV3,
		},
	}
	require.NoError(t, m.AddSegment(ctx, segment))

	cm := mocks.NewChunkManager(t)
	cm.EXPECT().RootPath().Return("root").Maybe()
	gc := NewGarbageCollector(m, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{
		StreamingNodeCatalog: checkpointCatalog,
		Cli:                  cm,
		DropTolerance:        0,
	}))

	gc.RecycleDroppedSegments(ctx, nil)

	assert.Nil(t, m.GetSegment(ctx, segID))
}

func TestGarbageCollector_recycleDroppedSegments_FileDeleteFailureKeepsMeta(t *testing.T) {
	checkpointCatalog := MockCheckpointCatalog(t, 1000)

	ctx := context.Background()
	m, dm, segment, _, _ := setupDroppedSegmentWithIndexForGC(t)

	cli := mocks.NewChunkManager(t)
	cli.EXPECT().RootPath().Return("root").Maybe()
	cli.EXPECT().Remove(mock.Anything, mock.Anything).Return(errors.New("remove failed")).Maybe()
	gc := NewGarbageCollector(m, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{
		StreamingNodeCatalog: checkpointCatalog,
		Cli:                  cli,
		DropTolerance:        0,
	}))
	defer gc.Close()

	gc.RecycleDroppedSegments(ctx, nil)

	assert.NotNil(t, m.GetSegment(ctx, segment.ID))
	assert.Len(t, dm.IndexMeta.GetAllSegmentIndexes(segment.ID), 1)
}

func TestGarbageCollector_recycleDroppedSegments_IndexMetaFailureKeepsSegmentMeta(t *testing.T) {
	checkpointCatalog := MockCheckpointCatalog(t, 1000)

	ctx := context.Background()
	m, dm, segment, _, _ := setupDroppedSegmentWithIndexForGC(t)
	dm.IndexMeta.Catalog = &batchIndexDeleteCatalog{
		DataCoordCatalog: dm.IndexMeta.Catalog,
		dropSegmentIndexes: func(context.Context, []*model.SegmentIndex) error {
			return errors.New("meta failed")
		},
	}

	cli := mocks.NewChunkManager(t)
	cli.EXPECT().RootPath().Return("root").Maybe()
	cli.EXPECT().Remove(mock.Anything, mock.Anything).Return(nil).Maybe()
	gc := NewGarbageCollector(m, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{
		StreamingNodeCatalog: checkpointCatalog,
		Cli:                  cli,
		DropTolerance:        0,
	}))
	defer gc.Close()

	gc.RecycleDroppedSegments(ctx, nil)

	assert.NotNil(t, m.GetSegment(ctx, segment.ID))
	assert.Len(t, dm.IndexMeta.GetAllSegmentIndexes(segment.ID), 1)
}

func TestGarbageCollector_recycleDroppedSegments_IndexSnapshotReference(t *testing.T) {
	checkpointCatalog := MockCheckpointCatalog(t, 1000)

	ctx := context.Background()
	m, dm, segment, segIdx, _ := setupDroppedSegmentWithIndexForGC(t)
	dm.SnapshotMeta = &snapshot.SnapshotMeta{}
	m.CompactionProtection = dm.SnapshotMeta

	cli := mocks.NewChunkManager(t)
	cli.EXPECT().RootPath().Return("root").Maybe()
	cli.EXPECT().Remove(mock.Anything, mock.Anything).Return(nil).Maybe()
	gc := NewGarbageCollector(m, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{
		StreamingNodeCatalog: checkpointCatalog,
		Cli:                  cli,
		DropTolerance:        0,
	}))
	defer gc.Close()

	mockIsSegmentBlocked := mockey.Mock((*snapshot.SnapshotMeta).IsSegmentGCBlocked).Return(false).Build()
	defer mockIsSegmentBlocked.UnPatch()
	mockIsBuildIDBlocked := mockey.Mock((*snapshot.SnapshotMeta).IsBuildIDGCBlocked).To(
		func(sm *snapshot.SnapshotMeta, collID, buildID int64) bool {
			return buildID == segIdx.BuildID
		}).Build()
	defer mockIsBuildIDBlocked.UnPatch()

	gc.RecycleDroppedSegments(ctx, nil)

	assert.NotNil(t, m.GetSegment(ctx, segment.ID))
	assert.Len(t, dm.IndexMeta.GetAllSegmentIndexes(segment.ID), 1)
	cli.AssertNotCalled(t, "Remove", mock.Anything, mock.Anything)
}

func TestGarbageCollector_recycleDroppedSegment_DropSegmentFailure(t *testing.T) {
	ctx := context.Background()
	m, dm, segment, _, _ := setupDroppedSegmentWithIndexForGC(t)
	cli := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test"))
	gc := NewGarbageCollector(m, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{
		Cli:           cli,
		DropTolerance: 0,
	}))

	mockRemoveObjectFiles := mockey.Mock((*GarbageCollector).RemoveObjectFiles).Return(nil).Build()
	defer mockRemoveObjectFiles.UnPatch()
	mockDropSegment := mockey.Mock((*meta.Meta).DropSegments).Return(0, errors.New("drop segment failed")).Build()
	defer mockDropSegment.UnPatch()

	gc.RecycleDroppedSegment(ctx, NewGCManifestReadIO(t), segment.ID)

	assert.NotNil(t, m.GetSegment(ctx, segment.ID))
}

func TestGarbageCollector_DroppedSegmentIndexHelpers(t *testing.T) {
	ctx := context.Background()
	m, dm, segment, segIdx, _ := setupDroppedSegmentWithIndexForGC(t)

	gc := NewGarbageCollector(m, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{
		Cli: storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test")),
	}))

	segIndexes, indexFiles, blocked := gc.GetDroppedSegmentIndexFiles(context.TODO(), NewGCManifestReadIO(t), segment.ID)
	require.Equal(t, GcNotBlocked, blocked)
	require.Len(t, segIndexes, 1)
	assert.Equal(t, segIdx.BuildID, segIndexes[0].BuildID)
	expectedIndexFile := path.Join(gc.Option.Cli.RootPath(), common.SegmentIndexV0Path,
		strconv.FormatInt(segIdx.BuildID, 10), strconv.FormatInt(segIdx.IndexVersion, 10),
		strconv.FormatInt(segIdx.PartitionID, 10), strconv.FormatInt(segIdx.SegmentID, 10), "idx-file")
	assert.Contains(t, indexFiles, expectedIndexFile)

	assert.Nil(t, (&GarbageCollector{}).getAllSegmentIndexesForDroppedSegment(segment.ID))
	removed, err := dm.IndexMeta.RemoveSegmentIndexes(ctx, nil)
	require.NoError(t, err)
	assert.Zero(t, removed)
	removed, err = dm.IndexMeta.RemoveSegmentIndexes(ctx, segIndexes)
	require.NoError(t, err)
	assert.Equal(t, 1, removed)
	assert.Empty(t, dm.IndexMeta.GetAllSegmentIndexes(segment.ID))
}

func TestGarbageCollector_getDroppedSegmentIndexFiles_BlockedReturnsNilFiles(t *testing.T) {
	m, dm, segment, segIdx, _ := setupDroppedSegmentWithIndexForGC(t)
	dm.SnapshotMeta = &snapshot.SnapshotMeta{}
	m.CompactionProtection = dm.SnapshotMeta
	gc := NewGarbageCollector(m, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{
		Cli: storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test")),
	}))

	mockIsBlocked := mockey.Mock((*snapshot.SnapshotMeta).IsBuildIDGCBlocked).To(
		func(sm *snapshot.SnapshotMeta, collID, buildID int64) bool {
			return buildID == segIdx.BuildID
		}).Build()
	defer mockIsBlocked.UnPatch()

	segIndexes, indexFiles, blocked := gc.GetDroppedSegmentIndexFiles(context.TODO(), NewGCManifestReadIO(t), segment.ID)
	assert.Equal(t, gcBlockedBySnapshot, blocked)
	assert.Nil(t, indexFiles)
	assert.Len(t, segIndexes, 1)
}

func TestGarbageCollector_recycleDroppedSegment_CancellationShortCircuit(t *testing.T) {
	t.Run("cancel before files step", func(t *testing.T) {
		m, dm, segment, _, _ := setupDroppedSegmentWithIndexForGC(t)
		gc := NewGarbageCollector(m, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{
			Cli:           storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test-gc-cancel-1")),
			DropTolerance: 0,
		}))

		var removeCalled atomic.Bool
		mockRemove := mockey.Mock((*GarbageCollector).removeObjectFilesWithResult).To(
			func(gc *GarbageCollector, ctx context.Context, files []string) []storage.RemoveResult {
				removeCalled.Store(true)
				return successfulRemoveResults(files)
			}).Build()
		defer mockRemove.UnPatch()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		gc.RecycleDroppedSegment(ctx, NewGCManifestReadIO(t), segment.ID)

		assert.False(t, removeCalled.Load(), "file removal must be skipped when ctx is canceled before the files step")
		assert.NotNil(t, m.GetSegment(context.Background(), segment.ID))
	})

	t.Run("cancel between files and index meta", func(t *testing.T) {
		m, dm, segment, _, _ := setupDroppedSegmentWithIndexForGC(t)
		gc := NewGarbageCollector(m, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{
			Cli:           storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test-gc-cancel-2")),
			DropTolerance: 0,
		}))

		ctx, cancel := context.WithCancel(context.Background())
		mockRemove := mockey.Mock((*GarbageCollector).removeObjectFilesWithResult).To(
			func(gc *GarbageCollector, c context.Context, files []string) []storage.RemoveResult {
				cancel()
				return successfulRemoveResults(files)
			}).Build()
		defer mockRemove.UnPatch()

		var indexMetaCalled atomic.Bool
		mockIdx := mockey.Mock((*GarbageCollector).removeSegmentIndexesInBatches).To(
			func(gc *GarbageCollector, c context.Context, idx []*model.SegmentIndex) {
				indexMetaCalled.Store(true)
			}).Build()
		defer mockIdx.UnPatch()

		gc.RecycleDroppedSegment(ctx, NewGCManifestReadIO(t), segment.ID)

		assert.False(t, indexMetaCalled.Load(), "index meta step must be skipped when ctx is canceled mid-segment")
		// segment meta should still exist since DropSegment never ran.
		assert.NotNil(t, m.GetSegment(context.Background(), segment.ID))
	})
}

func TestGarbageCollector_getDroppedSegmentIndexFiles_EdgeCases(t *testing.T) {
	t.Run("empty segIndexes returns nil maps", func(t *testing.T) {
		m, dm, err := newMemoryMetaWithSnapshot(t)
		require.NoError(t, err)
		gc := NewGarbageCollector(m, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{
			Cli: storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test")),
		}))
		segIndexes, indexFiles, blocked := gc.GetDroppedSegmentIndexFiles(context.TODO(), NewGCManifestReadIO(t), 9999)
		assert.Equal(t, GcNotBlocked, blocked)
		assert.Nil(t, segIndexes)
		assert.Nil(t, indexFiles)
	})

	t.Run("nil snapshotMeta skips block check", func(t *testing.T) {
		m, dm, segment, segIdx, _ := setupDroppedSegmentWithIndexForGC(t)
		// newMemoryMeta always wires a snapshotMeta; clear it to exercise the
		// nil-guard path in getDroppedSegmentIndexFiles.
		dm.SnapshotMeta = nil
		m.CompactionProtection = nil
		gc := NewGarbageCollector(m, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{
			Cli: storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test")),
		}))
		segIndexes, indexFiles, blocked := gc.GetDroppedSegmentIndexFiles(context.TODO(), NewGCManifestReadIO(t), segment.ID)
		assert.Equal(t, GcNotBlocked, blocked)
		require.Len(t, segIndexes, 1)
		assert.Equal(t, segIdx.BuildID, segIndexes[0].BuildID)
		assert.NotEmpty(t, indexFiles)
	})

	t.Run("non-blocked snapshotMeta returns full file set", func(t *testing.T) {
		m, dm, segment, _, _ := setupDroppedSegmentWithIndexForGC(t)
		dm.SnapshotMeta = &snapshot.SnapshotMeta{}
		m.CompactionProtection = dm.SnapshotMeta
		gc := NewGarbageCollector(m, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{
			Cli: storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test")),
		}))
		mockIsBlocked := mockey.Mock((*snapshot.SnapshotMeta).IsBuildIDGCBlocked).Return(false).Build()
		defer mockIsBlocked.UnPatch()

		segIndexes, indexFiles, blocked := gc.GetDroppedSegmentIndexFiles(context.TODO(), NewGCManifestReadIO(t), segment.ID)
		assert.Equal(t, GcNotBlocked, blocked)
		require.Len(t, segIndexes, 1)
		assert.NotEmpty(t, indexFiles)
	})
}

// A StorageV3 segment whose index metadata is only in its manifest must not be
// recycled while that manifest is unreadable: nothing else records the
// artifacts, which live outside the segment directory. Once the manifest is
// gone there is nothing left to protect and the segment must be recyclable,
// otherwise its metadata is stranded forever.
func TestGarbageCollector_getDroppedSegmentIndexFiles_ManifestUnreadable(t *testing.T) {
	meta.WithSegmentIndexManifestWrites(t, true)
	newV3GC := func(t *testing.T) (*GarbageCollector, int64) {
		t.Helper()
		m, dm, err := newMemoryMetaWithSnapshot(t)
		require.NoError(t, err)
		basePath := "/tmp/test-gc-manifest-unreadable/insert_log/100/10/3001"
		require.NoError(t, m.AddSegment(context.TODO(), meta.NewSegmentInfo(&datapb.SegmentInfo{
			ID:               3001,
			CollectionID:     100,
			PartitionID:      10,
			State:            commonpb.SegmentState_Dropped,
			StorageVersion:   storage.StorageV3,
			ManifestPath:     packed.MarshalManifestPath(basePath, 1),
			ManifestHasIndex: true,
		})))
		return NewGarbageCollector(m, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{
			Cli: storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test")),
		})), 3001
	}

	readErr := mockey.Mock(packed.GetManifestIndexInfosAsync).
		Return(nil, merr.WrapErrIoFailedReason("throttled")).Build()
	defer readErr.UnPatch()

	t.Run("blocks while the manifest is still present", func(t *testing.T) {
		gc, segmentID := newV3GC(t)
		exists := mockey.Mock((*storage.LocalChunkManager).Exist).Return(true, nil).Build()
		defer exists.UnPatch()

		_, indexFiles, blocked := gc.GetDroppedSegmentIndexFiles(context.TODO(), NewGCManifestReadIO(t), segmentID)
		assert.Equal(t, gcBlockedByManifest, blocked)
		assert.Nil(t, indexFiles)
	})

	t.Run("proceeds once the manifest is gone", func(t *testing.T) {
		gc, segmentID := newV3GC(t)
		exists := mockey.Mock((*storage.LocalChunkManager).Exist).Return(false, nil).Build()
		defer exists.UnPatch()

		_, indexFiles, blocked := gc.GetDroppedSegmentIndexFiles(context.TODO(), NewGCManifestReadIO(t), segmentID)
		assert.Equal(t, GcNotBlocked, blocked)
		assert.Nil(t, indexFiles)
	})

	t.Run("blocks when existence cannot be determined", func(t *testing.T) {
		gc, segmentID := newV3GC(t)
		exists := mockey.Mock((*storage.LocalChunkManager).Exist).
			Return(false, merr.WrapErrIoFailedReason("list failed")).Build()
		defer exists.UnPatch()

		_, _, blocked := gc.GetDroppedSegmentIndexFiles(context.TODO(), NewGCManifestReadIO(t), segmentID)
		assert.Equal(t, gcBlockedByManifest, blocked)
	})
}

func TestGarbageCollector_removeDroppedSegmentFilesV3(t *testing.T) {
	ctx := context.Background()
	basePath := "root/insert_log/100/10/2001"
	segment := &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             2001,
			CollectionID:   100,
			PartitionID:    10,
			State:          commonpb.SegmentState_Dropped,
			StorageVersion: storage.StorageV3,
			ManifestPath:   packed.MarshalManifestPath(basePath, 1),
		},
	}

	t.Run("success with index files", func(t *testing.T) {
		cm := mocks.NewChunkManager(t)
		cm.EXPECT().RootPath().Return("root").Maybe()
		cm.EXPECT().RemoveWithPrefix(mock.Anything, basePath+"/").Return(nil).Once()
		cm.EXPECT().Remove(mock.Anything, "root/index_files/40/1/10/2001/idx-file").Return(nil).Once()
		gc := NewGarbageCollector(nil, nil, GcOption{Cli: cm})

		err := gc.RemoveDroppedSegmentFiles(ctx, segment, map[string]struct{}{
			"root/index_files/40/1/10/2001/idx-file": {},
		})
		assert.NoError(t, err)
	})

	t.Run("invalid manifest", func(t *testing.T) {
		cm := mocks.NewChunkManager(t)
		gc := NewGarbageCollector(nil, nil, GcOption{Cli: cm})
		invalid := segment.Clone()
		invalid.ManifestPath = "invalid"

		assert.Error(t, gc.RemoveDroppedSegmentFiles(ctx, invalid, nil))
	})

	t.Run("remove base path failed", func(t *testing.T) {
		cm := mocks.NewChunkManager(t)
		cm.EXPECT().RootPath().Return("root").Maybe()
		cm.EXPECT().RemoveWithPrefix(mock.Anything, basePath+"/").Return(errors.New("remove failed")).Once()
		gc := NewGarbageCollector(nil, nil, GcOption{Cli: cm})

		assert.Error(t, gc.RemoveDroppedSegmentFiles(ctx, segment, nil))
	})

	t.Run("remote not found semantics unchanged", func(t *testing.T) {
		cm := mocks.NewChunkManager(t)
		cm.EXPECT().RootPath().Return("root").Maybe()
		cm.EXPECT().RemoveWithPrefix(mock.Anything, basePath+"/").Return(os.ErrNotExist).Once()
		gc := NewGarbageCollector(nil, nil, GcOption{Cli: cm})

		assert.ErrorIs(t, gc.RemoveDroppedSegmentFiles(ctx, segment, nil), os.ErrNotExist)
	})

	t.Run("remove index file failed", func(t *testing.T) {
		cm := mocks.NewChunkManager(t)
		cm.EXPECT().RootPath().Return("root").Maybe()
		// No RemoveWithPrefix expectation: a failed index-file delete must
		// leave basePath - and the manifest inside it, the only thing naming
		// the surviving index files - untouched for the next cycle.
		cm.EXPECT().Remove(mock.Anything, "root/index_files/40/1/10/2001/idx-file").Return(errors.New("remove failed")).Once()
		gc := NewGarbageCollector(nil, nil, GcOption{Cli: cm})

		err := gc.RemoveDroppedSegmentFiles(ctx, segment, map[string]struct{}{
			"root/index_files/40/1/10/2001/idx-file": {},
		})
		assert.Error(t, err)
	})

	// Index artifacts live outside basePath while the manifest naming them
	// lives inside it, so basePath must go last: removing it first would
	// strip a partially-failed cycle of the only record of the survivors.
	t.Run("index files removed before basePath", func(t *testing.T) {
		var mu sync.Mutex
		var calls []string
		cm := mocks.NewChunkManager(t)
		cm.EXPECT().RootPath().Return("root").Maybe()
		cm.EXPECT().Remove(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, file string) error {
			mu.Lock()
			defer mu.Unlock()
			calls = append(calls, file)
			return nil
		})
		cm.EXPECT().RemoveWithPrefix(mock.Anything, basePath+"/").RunAndReturn(func(_ context.Context, prefix string) error {
			mu.Lock()
			defer mu.Unlock()
			calls = append(calls, prefix)
			return nil
		}).Once()
		gc := NewGarbageCollector(nil, nil, GcOption{Cli: cm})

		require.NoError(t, gc.RemoveDroppedSegmentFiles(ctx, segment, map[string]struct{}{
			"root/index_files/40/1/10/2001/idx-file":  {},
			"root/index_files/40/1/10/2001/idx-file2": {},
		}))
		require.Len(t, calls, 3)
		assert.Equal(t, basePath+"/", calls[len(calls)-1])
	})
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

func TestGarbageCollectorRemoveDroppedSegmentFilesRejectsUnsafeLocalManifestRoot(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	cli := storage.NewLocalChunkManager(objectstorage.RootPath(root))
	gc := &GarbageCollector{Option: GcOption{Cli: cli}}
	sentinelPath := path.Join(root, "sentinel")
	require.NoError(t, cli.Write(ctx, sentinelPath, []byte("keep")))

	unsafeBasePaths := []string{
		"",
		".",
		"/",
		"../outside",
		root,
		"s3://bucket/files/insert_log/100/10/2001",
		"insert_log",
		"insert_log/100/10/20010",
	}
	for _, basePath := range unsafeBasePaths {
		t.Run(fmt.Sprintf("base_%q", basePath), func(t *testing.T) {
			segment := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID:             2001,
				StorageVersion: storage.StorageV3,
				ManifestPath:   packed.MarshalManifestPath(basePath, 1),
			}}
			err := gc.RemoveDroppedSegmentFiles(ctx, segment, nil)
			require.Error(t, err)
			content, readErr := cli.Read(ctx, sentinelPath)
			require.NoError(t, readErr)
			assert.Equal(t, []byte("keep"), content)
		})
	}
}

func TestGarbageCollectorRemoveDroppedSegmentFilesUsesSegmentBoundary(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	cli := storage.NewLocalChunkManager(objectstorage.RootPath(root))
	gc := &GarbageCollector{Option: GcOption{Cli: cli}}
	segmentFile := path.Join(root, "files/insert_log/100/10/2001/data")
	siblingFile := path.Join(root, "files/insert_log/100/10/20010/data")
	require.NoError(t, cli.Write(ctx, segmentFile, []byte("remove")))
	require.NoError(t, cli.Write(ctx, siblingFile, []byte("keep")))

	segment := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:             2001,
		CollectionID:   100,
		PartitionID:    10,
		StorageVersion: storage.StorageV3,
		ManifestPath:   packed.MarshalManifestPath(path.Join(root, "files/insert_log/100/10/2001"), 1),
	}}
	require.NoError(t, gc.RemoveDroppedSegmentFiles(ctx, segment, nil))

	segmentExists, err := cli.Exist(ctx, segmentFile)
	require.NoError(t, err)
	assert.False(t, segmentExists)
	siblingContent, err := cli.Read(ctx, siblingFile)
	require.NoError(t, err)
	assert.Equal(t, []byte("keep"), siblingContent)
}

func TestGarbageCollectorRemoveDroppedSegmentFilesRejectsForeignLocalRoot(t *testing.T) {
	ctx := context.Background()
	testRoot := t.TempDir()
	root := path.Join(testRoot, "owned")
	cli := storage.NewLocalChunkManager(objectstorage.RootPath(root))
	gc := &GarbageCollector{Option: GcOption{Cli: cli}}
	for _, otherRoot := range []string{path.Join(testRoot, "other"), root + "-other"} {
		t.Run(path.Base(otherRoot), func(t *testing.T) {
			basePath := path.Join(otherRoot, "insert_log/100/10/2001")
			sentinelPath := path.Join(basePath, "data")
			require.NoError(t, cli.Write(ctx, sentinelPath, []byte("keep foreign data")))
			segment := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID: 2001, CollectionID: 100, PartitionID: 10,
				StorageVersion: storage.StorageV3,
				ManifestPath:   packed.MarshalManifestPath(basePath, 1),
			}}
			require.ErrorIs(t, gc.RemoveDroppedSegmentFiles(ctx, segment, nil), merr.ErrDataIntegrity)
			content, err := cli.Read(ctx, sentinelPath)
			require.NoError(t, err)
			assert.Equal(t, "keep foreign data", string(content))
		})
	}
}

func TestGarbageCollectorRemoveDroppedSegmentFilesLocalFilesystemRoot(t *testing.T) {
	ctx := context.Background()
	// Even with a filesystem-wide configured root, only test-owned files are
	// created and deleted; the manifest's segment suffix remains mandatory.
	testRoot, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	basePath := path.Join(testRoot, "insert_log/100/10/2001")
	cli := storage.NewLocalChunkManager(objectstorage.RootPath("/"))
	gc := &GarbageCollector{Option: GcOption{Cli: cli}}
	filePath := path.Join(basePath, "data")
	require.NoError(t, cli.Write(ctx, filePath, []byte("remove")))
	segment := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 2001, CollectionID: 100, PartitionID: 10,
		StorageVersion: storage.StorageV3,
		ManifestPath:   packed.MarshalManifestPath(basePath, 1),
	}}
	require.NoError(t, gc.RemoveDroppedSegmentFiles(ctx, segment, nil))
	assert.NoFileExists(t, filePath)
}

func TestGarbageCollectorRemoveDroppedSegmentFilesAllowsMissingLocalSuffix(t *testing.T) {
	for _, existingSuffix := range []string{"", "insert_log/100/10"} {
		t.Run(fmt.Sprintf("existing_%q", existingSuffix), func(t *testing.T) {
			ctx := context.Background()
			root := t.TempDir()
			cli := storage.NewLocalChunkManager(objectstorage.RootPath(root))
			gc := NewGarbageCollector(nil, nil, GcOption{Cli: cli})
			require.NoError(t, os.MkdirAll(path.Join(root, existingSuffix), 0o755))
			basePath := path.Join(root, "insert_log/100/10/2001")
			segment := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID: 2001, CollectionID: 100, PartitionID: 10,
				StorageVersion: storage.StorageV3,
				ManifestPath:   packed.MarshalManifestPath(basePath, 1),
			}}
			indexFile := path.Join(root, "index_files/40/1/10/2001/idx-file")
			require.NoError(t, cli.Write(ctx, indexFile, []byte("remove index")))
			segment.ID = 20010
			require.ErrorIs(t, gc.RemoveDroppedSegmentFiles(ctx, segment, map[string]struct{}{indexFile: {}}), merr.ErrDataIntegrity)
			require.FileExists(t, indexFile, "a missing base must not bypass segment identity validation")
			segment.ID = 2001
			for range 2 {
				// Exercise the real local prefix walker: a missing base returns
				// ENOENT unless GC recognizes that data removal is already done.
				require.NoError(t, gc.RemoveDroppedSegmentFiles(ctx, segment, map[string]struct{}{indexFile: {}}))
				assert.NoFileExists(t, indexFile)
				assert.NoDirExists(t, basePath)
			}
		})
	}
}

func TestGarbageCollector_recycleDroppedSegment_MissingLocalV3DataStillRemovesMeta(t *testing.T) {
	ctx := context.Background()
	m, dm, segment, _, _ := setupDroppedSegmentWithIndexForGC(t)
	root := t.TempDir()
	cli := storage.NewLocalChunkManager(objectstorage.RootPath(root))
	basePath := path.Join(root, "files/insert_log/100/10/1001")
	segment.StorageVersion = storage.StorageV3
	segment.ManifestPath = packed.MarshalManifestPath(basePath, 1)
	gc := NewGarbageCollector(m, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{Cli: cli}))
	_, indexFiles, blocked := gc.GetDroppedSegmentIndexFiles(ctx, NewGCManifestReadIO(t), segment.ID)
	require.Equal(t, GcNotBlocked, blocked)
	require.NotEmpty(t, indexFiles)
	for file := range indexFiles {
		if strings.HasPrefix(file, basePath+"/") {
			continue
		}
		require.NoError(t, cli.Write(ctx, file, []byte("remaining index")))
	}
	sibling := path.Join(root, "files/insert_log/100/10/10010/data")
	require.NoError(t, cli.Write(ctx, sibling, []byte("keep sibling")))
	require.NoDirExists(t, basePath)

	gc.RecycleDroppedSegment(ctx, NewGCManifestReadIO(t), segment.ID)

	assert.Nil(t, m.GetSegment(ctx, segment.ID))
	assert.Empty(t, dm.IndexMeta.GetAllSegmentIndexes(segment.ID))
	for file := range indexFiles {
		assert.NoFileExists(t, file)
	}
	contents, err := cli.Read(ctx, sibling)
	require.NoError(t, err)
	assert.Equal(t, "keep sibling", string(contents))
}

func TestGarbageCollectorRemoveDroppedSegmentFilesRejectsInvalidConfiguredRoot(t *testing.T) {
	segment := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 2001, CollectionID: 100, PartitionID: 10,
		StorageVersion: storage.StorageV3,
	}}
	t.Run("relative local root", func(t *testing.T) {
		cli := storage.NewLocalChunkManager(objectstorage.RootPath("data"))
		gc := &GarbageCollector{Option: GcOption{Cli: cli}}
		segment.ManifestPath = packed.MarshalManifestPath(path.Join(t.TempDir(), "insert_log/100/10/2001"), 1)
		require.ErrorIs(t, gc.RemoveDroppedSegmentFiles(context.Background(), segment, nil), merr.ErrDataIntegrity)
	})
	for _, root := range []string{"/", "/files", "s3://bucket/files"} {
		t.Run(fmt.Sprintf("remote_root_%q", root), func(t *testing.T) {
			cli := &gcRootedChunkManager{root: root}
			gc := &GarbageCollector{Option: GcOption{Cli: cli}}
			segment.ManifestPath = packed.MarshalManifestPath("files/insert_log/100/10/2001", 1)
			require.ErrorIs(t, gc.RemoveDroppedSegmentFiles(context.Background(), segment, nil), merr.ErrDataIntegrity)
			assert.False(t, cli.removeCalled)
		})
	}
}

type gcRootedChunkManager struct {
	storage.ChunkManager
	root          string
	removeCalled  bool
	removedPrefix string
}

func (cm *gcRootedChunkManager) RootPath() string {
	return cm.root
}

func (cm *gcRootedChunkManager) RemoveWithPrefix(_ context.Context, prefix string) error {
	cm.removeCalled = true
	cm.removedPrefix = prefix
	return nil
}

// A manifest must identify both the configured namespace and this segment;
// matching IDs alone do not authorize deletion in another instance's prefix.
func TestGarbageCollectorRemoveDroppedSegmentFilesRejectsMalformedRemoteManifestBase(t *testing.T) {
	unsafeBasePaths := []string{
		"files/insert_log",
		"files/insert_log/100/10/20010",
		"/files/insert_log/100/10/2001",
		"s3://bucket/files/insert_log/100/10/2001",
		"../insert_log/100/10/2001",
		"other/insert_log/100/10/2001",
		"files-other/insert_log/100/10/2001",
		"insert_log/100/10/2001",
	}
	for _, basePath := range unsafeBasePaths {
		t.Run(fmt.Sprintf("base_%q", basePath), func(t *testing.T) {
			cli := &gcRootedChunkManager{root: "files"}
			gc := &GarbageCollector{Option: GcOption{Cli: cli}}
			segment := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID:             2001,
				CollectionID:   100,
				PartitionID:    10,
				StorageVersion: storage.StorageV3,
				ManifestPath:   packed.MarshalManifestPath(basePath, 1),
			}}
			err := gc.RemoveDroppedSegmentFiles(context.Background(), segment, nil)
			require.ErrorIs(t, err, merr.ErrDataIntegrity)
			assert.False(t, cli.removeCalled)
		})
	}
}

func TestGarbageCollectorRemoveDroppedSegmentFilesUsesCompleteRemoteKey(t *testing.T) {
	for _, tc := range []struct {
		name string
		root string
		base string
	}{
		{"configured prefix", "files", "files/insert_log/100/10/2001"},
		{"nested prefix", "tenant/files", "tenant/files/insert_log/100/10/2001"},
		{"layout prefix within root", "files", "files/legacy/insert_log/100/10/2001"},
		{"bucket root complete key", "", "files/insert_log/100/10/2001"},
		{"dot bucket root complete key", ".", "files/insert_log/100/10/2001"},
		{"parent component is part of object key", "files", "files/tmp/../insert_log/100/10/2001"},
		{"dot component is part of object key", "files", "files/./insert_log/100/10/2001"},
		{"repeated separator is part of object key", "files", "files//insert_log/100/10/2001"},
		{"trailing directory separator", "files", "files/insert_log/100/10/2001/"},
		{"parent component at bucket root", "", "../insert_log/100/10/2001"},
		{"parent component within bucket", ".", "tmp/../insert_log/100/10/2001"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cli := &gcRootedChunkManager{root: tc.root}
			gc := &GarbageCollector{Option: GcOption{Cli: cli}}
			segment := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID:             2001,
				CollectionID:   100,
				PartitionID:    10,
				StorageVersion: storage.StorageV3,
				ManifestPath:   packed.MarshalManifestPath(tc.base, 1),
			}}

			require.NoError(t, gc.RemoveDroppedSegmentFiles(context.Background(), segment, nil))
			assert.True(t, cli.removeCalled)
			assert.Equal(t, strings.TrimRight(tc.base, "/")+"/", cli.removedPrefix)
		})
	}
}

func TestGarbageCollectorRemoveDroppedSegmentFilesRemoteBucketRoot(t *testing.T) {
	for _, root := range []string{"", "."} {
		t.Run(fmt.Sprintf("root_%q", root), func(t *testing.T) {
			cli := &gcRootedChunkManager{root: root}
			gc := &GarbageCollector{Option: GcOption{Cli: cli}}
			segment := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID:             2001,
				CollectionID:   100,
				PartitionID:    10,
				StorageVersion: storage.StorageV3,
				ManifestPath:   packed.MarshalManifestPath(path.Join(root, "insert_log/100/10/2001"), 1),
			}}
			require.NoError(t, gc.RemoveDroppedSegmentFiles(context.Background(), segment, nil))
			assert.Equal(t, "insert_log/100/10/2001/", cli.removedPrefix)

			for _, unsafeBase := range []string{"", ".", "..", "insert_log/100/10/20010"} {
				cli.removeCalled = false
				segment.ManifestPath = packed.MarshalManifestPath(unsafeBase, 1)
				require.Error(t, gc.RemoveDroppedSegmentFiles(context.Background(), segment, nil))
				assert.False(t, cli.removeCalled)
			}
		})
	}
}

func TestGarbageCollector_recycleSnapshots_OrphanCleanup(t *testing.T) {
	ctx := context.Background()

	setupGCTest := func(t *testing.T, sm *snapshot.SnapshotMeta, colls ...*meta.CollectionInfo) *GarbageCollector {
		m := &meta.Meta{
			Segments:  meta.NewSegmentsInfoWithSegments(make(map[int64]*meta.SegmentInfo)),
			MetaStore: metacache.NewMetaStore(nil),
		}
		m.CompactionProtection = sm
		for _, coll := range colls {
			m.MetaStore.PutCollection(coll)
		}
		gc := NewGarbageCollector(m, meta.NewMockHandlerWithMeta(m), GcTestDomainOption(&snapshot.SnapshotTestMetas{SnapshotMeta: sm}, GcOption{}))
		return gc
	}

	t.Run("orphan_collection_deleted", func(t *testing.T) {
		// Snapshot belongs to a dropped collection → should be cleaned up
		sm := snapshot.CreateTestSnapshotMeta(t)
		snapshot.InsertTestSnapshot(sm, &datapb.SnapshotInfo{
			Id:           1,
			Name:         "orphan_snap",
			CollectionId: 100,
			State:        datapb.SnapshotState_SnapshotStateCommitted,
		}, nil, nil)

		gc := setupGCTest(t, sm)

		// Mock catalog calls to skip pending/deleting cleanup, reach orphan cleanup
		mockGetPending := mockey.Mock((*snapshot.SnapshotMeta).GetPendingSnapshots).Return(nil, nil).Build()
		defer mockGetPending.UnPatch()
		mockGetDeleting := mockey.Mock((*snapshot.SnapshotMeta).GetDeletingSnapshots).Return(nil, nil).Build()
		defer mockGetDeleting.UnPatch()

		// Mock DropSnapshot internals for DropSnapshotsByCollection
		mockSave := mockey.Mock((*datacoord.Catalog).SaveSnapshot).Return(nil).Build()
		defer mockSave.UnPatch()
		mockDropCatalog := mockey.Mock((*datacoord.Catalog).DropSnapshot).Return(nil).Build()
		defer mockDropCatalog.UnPatch()
		mockWriter := mockey.Mock((*snapshotstorage.SnapshotWriter).Drop).Return(nil).Build()
		defer mockWriter.UnPatch()

		gc.recycleSnapshots(ctx, nil)

		// Verify orphan snapshot was cleaned up
		_, exists := sm.SnapshotID2Info.Get(int64(1))
		assert.False(t, exists, "orphan snapshot should be deleted")
	})

	t.Run("collection_still_exists", func(t *testing.T) {
		// Snapshot belongs to an active collection → should NOT be cleaned up
		sm := snapshot.CreateTestSnapshotMeta(t)
		snapshot.InsertTestSnapshot(sm, &datapb.SnapshotInfo{
			Id:           2,
			Name:         "active_snap",
			CollectionId: 200,
			State:        datapb.SnapshotState_SnapshotStateCommitted,
		}, nil, nil)

		gc := setupGCTest(t, sm, &meta.CollectionInfo{ID: 200})

		mockGetPending := mockey.Mock((*snapshot.SnapshotMeta).GetPendingSnapshots).Return(nil, nil).Build()
		defer mockGetPending.UnPatch()
		mockGetDeleting := mockey.Mock((*snapshot.SnapshotMeta).GetDeletingSnapshots).Return(nil, nil).Build()
		defer mockGetDeleting.UnPatch()

		gc.recycleSnapshots(ctx, nil)

		// Verify snapshot was NOT deleted
		_, exists := sm.SnapshotID2Info.Get(int64(2))
		assert.True(t, exists, "snapshot of active collection should not be deleted")
	})

	t.Run("live_collection_reaps_expired_pins_and_keeps_snapshot", func(t *testing.T) {
		// Regression: the GC hoist moved cleanExpiredPinsForCollection in front
		// of the collection check so active collections also get their
		// expired pins reaped — previously they accumulated forever on live
		// collections. Assert that both effects happen: expired pin removed,
		// snapshot still present.
		sm := snapshot.CreateTestSnapshotMeta(t)
		now := time.Now().UnixMilli()
		snapshot.InsertTestSnapshot(sm, &datapb.SnapshotInfo{
			Id:            4,
			Name:          "live_with_expired",
			CollectionId:  400,
			State:         datapb.SnapshotState_SnapshotStateCommitted,
			PinIds:        []int64{9001, 9002},
			PinExpireAtMs: map[int64]int64{9001: now - 1000, 9002: now + 3600000},
		}, nil, nil)

		gc := setupGCTest(t, sm, &meta.CollectionInfo{ID: 400})

		mockGetPending := mockey.Mock((*snapshot.SnapshotMeta).GetPendingSnapshots).Return(nil, nil).Build()
		defer mockGetPending.UnPatch()
		mockGetDeleting := mockey.Mock((*snapshot.SnapshotMeta).GetDeletingSnapshots).Return(nil, nil).Build()
		defer mockGetDeleting.UnPatch()
		// cleanExpiredPinsForCollection persists the reap via SaveSnapshot.
		mockSave := mockey.Mock((*datacoord.Catalog).SaveSnapshot).Return(nil).Build()
		defer mockSave.UnPatch()

		gc.recycleSnapshots(ctx, nil)

		// Snapshot must still exist (live collection, not orphaned).
		updated, exists := sm.SnapshotID2Info.Get(int64(4))
		assert.True(t, exists, "snapshot of live collection must not be deleted")
		// Expired pin 9001 reaped; active pin 9002 preserved.
		assert.ElementsMatch(t, []int64{9002}, updated.GetPinIds(),
			"expired pin must be removed, active pin preserved")
		_, hasExpired := updated.GetPinExpireAtMs()[9001]
		assert.False(t, hasExpired, "expired pin's expiry entry must be removed")
	})

	t.Run("dropping_collection_deleted", func(t *testing.T) {
		// A Dropping collection counts as dropped, as it did when this
		// check was a public describe through the broker.
		sm := snapshot.CreateTestSnapshotMeta(t)
		snapshot.InsertTestSnapshot(sm, &datapb.SnapshotInfo{
			Id:           3,
			Name:         "dropping_snap",
			CollectionId: 300,
			State:        datapb.SnapshotState_SnapshotStateCommitted,
		}, nil, nil)

		gc := setupGCTest(t, sm, &meta.CollectionInfo{ID: 300, State: etcdpb.CollectionState_CollectionDropping})

		mockGetPending := mockey.Mock((*snapshot.SnapshotMeta).GetPendingSnapshots).Return(nil, nil).Build()
		defer mockGetPending.UnPatch()
		mockGetDeleting := mockey.Mock((*snapshot.SnapshotMeta).GetDeletingSnapshots).Return(nil, nil).Build()
		defer mockGetDeleting.UnPatch()
		mockSave := mockey.Mock((*datacoord.Catalog).SaveSnapshot).Return(nil).Build()
		defer mockSave.UnPatch()
		mockDropCatalog := mockey.Mock((*datacoord.Catalog).DropSnapshot).Return(nil).Build()
		defer mockDropCatalog.UnPatch()
		mockWriter := mockey.Mock((*snapshotstorage.SnapshotWriter).Drop).Return(nil).Build()
		defer mockWriter.UnPatch()

		gc.recycleSnapshots(ctx, nil)

		_, exists := sm.SnapshotID2Info.Get(int64(3))
		assert.False(t, exists, "snapshot of a Dropping collection should be deleted")
	})
}

func TestCheckDroppedSegmentGC_CommitTimestamp(t *testing.T) {
	checkpointCatalog := MockCheckpointCatalog(t, 1000)

	t.Run("import segment not GCed when commit_timestamp > cpTimestamp", func(t *testing.T) {
		catalog := catalogmocks.NewDataCoordCatalog(t)

		m := &meta.Meta{
			Catalog:   catalog,
			MetaStore: metacache.NewMetaStore(catalog),
		}

		gc := NewGarbageCollector(m, meta.NewMockHandler(), GcOption{StreamingNodeCatalog: checkpointCatalog})

		segment := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:              200,
			InsertChannel:   "ch-import",
			State:           commonpb.SegmentState_Dropped,
			DroppedAt:       0,
			DmlPosition:     &msgpb.MsgPosition{ChannelName: "ch-import", Timestamp: 1000},
			CommitTimestamp: 5000,
		}}

		// cpTimestamp=3000: effective dml ts is 5000 > 3000 → should NOT GC
		result := gc.checkDroppedSegmentGC(segment, nil, typeutil.NewUniqueSet(), 3000)
		assert.False(t, result, "import segment with commit_ts=5000 should NOT be GCed when cpTimestamp=3000")
	})

	t.Run("import segment GCed when commit_timestamp <= cpTimestamp", func(t *testing.T) {
		catalog := catalogmocks.NewDataCoordCatalog(t)

		m := &meta.Meta{
			Catalog:   catalog,
			MetaStore: metacache.NewMetaStore(catalog),
		}

		gc := NewGarbageCollector(m, meta.NewMockHandler(), GcOption{
			StreamingNodeCatalog: checkpointCatalog,
			DropTolerance:        0,
		})

		segment := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:              201,
			InsertChannel:   "ch-import",
			State:           commonpb.SegmentState_Dropped,
			DroppedAt:       0,
			DmlPosition:     &msgpb.MsgPosition{ChannelName: "ch-import", Timestamp: 1000},
			CommitTimestamp: 5000,
		}}

		// cpTimestamp=6000: effective dml ts is 5000 <= 6000 → should GC (drop tolerance met since DroppedAt=0)
		result := gc.checkDroppedSegmentGC(segment, nil, typeutil.NewUniqueSet(), 6000)
		assert.True(t, result, "import segment with commit_ts=5000 should be GCed when cpTimestamp=6000")
	})
}

// TestGarbageCollector_recycleDroppedSegment_CtxCanceledBeforeDrop covers
// cancellation between index metadata deletion and segment metadata deletion.
// The segment marker must remain available for retry.
func TestGarbageCollector_recycleDroppedSegment_CtxCanceledBeforeDrop(t *testing.T) {
	m, dm, segment, _, _ := setupDroppedSegmentWithIndexForGC(t)
	cli := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test-gc-cancel-drop"))
	gc := NewGarbageCollector(m, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{
		Cli:           cli,
		DropTolerance: 0,
	}))

	mockRemove := mockey.Mock((*GarbageCollector).RemoveObjectFiles).Return(nil).Build()
	defer mockRemove.UnPatch()

	ctx, cancel := context.WithCancel(context.Background())
	var indexMetaCalls atomic.Int32
	mockIdx := mockey.Mock((*GarbageCollector).removeSegmentIndexesInBatches).To(
		func(gc *GarbageCollector, c context.Context, idx []*model.SegmentIndex) {
			indexMetaCalls.Inc()
			cancel()
		}).Build()
	defer mockIdx.UnPatch()

	var dropCalls atomic.Int32
	mockDrop := mockey.Mock((*meta.Meta).DropSegments).To(
		func(m *meta.Meta, ctx context.Context, segments []*meta.SegmentInfo) (int, error) {
			dropCalls.Inc()
			return 0, nil
		}).Build()
	defer mockDrop.UnPatch()

	gc.RecycleDroppedSegment(ctx, NewGCManifestReadIO(t), segment.ID)

	assert.Equal(t, int32(1), indexMetaCalls.Load())
	assert.Equal(t, int32(0), dropCalls.Load(),
		"DropSegment must NOT run when ctx is canceled between index-meta and segment-meta steps")
	assert.NotNil(t, m.GetSegment(context.Background(), segment.ID))
}

// TestGarbageCollector_removeDroppedSegmentFiles_LegacyJSONLogs covers the V1/V2
// legacy JSON key index path reconstruction in getJSONKeyLogs.
func TestGarbageCollector_removeDroppedSegmentFiles_LegacyJSONLogs(t *testing.T) {
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

	gc := NewGarbageCollector(nil, nil, GcOption{Cli: cm})
	const textFile = "text/log/file-1"
	const jsonFile = "json-file"
	const indexFile = "idx/extra/file-99"
	segment := &meta.SegmentInfo{
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
	require.NoError(t, gc.RemoveDroppedSegmentFiles(ctx, segment, indexFiles))

	expectedJSON := path.Join("root", common.JSONIndexPath, "11", "1", "100", "10", "7001", "102", jsonFile)
	mu.Lock()
	defer mu.Unlock()
	assert.Contains(t, removed, textFile)
	assert.Contains(t, removed, expectedJSON)
	assert.Contains(t, removed, indexFile)
}

// TestGarbageCollector_removeDroppedSegmentFiles_JSONStatsV2 covers the V1/V2
// new-format JSON stats path reconstruction under json_stats/{dataFormat}/....
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

	gc := NewGarbageCollector(nil, nil, GcOption{Cli: cm})
	const jsonFile = "shared_key_index/inverted_index_0"
	const indexFile = "idx/extra/file-99"
	segment := &meta.SegmentInfo{
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
	require.NoError(t, gc.RemoveDroppedSegmentFiles(ctx, segment, indexFiles))

	expectedJSON := path.Join("root", common.JSONStatsPath, "3", "11", "1", "100", "10", "7001", "102", jsonFile)
	mu.Lock()
	defer mu.Unlock()
	assert.Contains(t, removed, expectedJSON)
	assert.Contains(t, removed, indexFile)
}

// setupV3SegIndexGC builds a healthy StorageV3 segment carrying one finished
// index whose field index no longer exists, which is what makes
// recycleUnusedSegIndexes act on it.
func setupV3SegIndexGC(t *testing.T) (*meta.Meta, *snapshot.SnapshotTestMetas, string, string) {
	t.Helper()
	meta.WithSegmentIndexManifestWrites(t, true)
	const (
		collID  = UniqueID(100)
		partID  = UniqueID(10)
		segID   = UniqueID(4001)
		indexID = UniqueID(400)
		buildID = UniqueID(4100)
	)
	basePath := "/tmp/test-gc-v3-retract/insert_log/100/10/4001"

	m, dm, err := newMemoryMetaWithSnapshot(t)
	require.NoError(t, err)
	require.NoError(t, m.AddSegment(context.TODO(), meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID:               segID,
		CollectionID:     collID,
		PartitionID:      partID,
		State:            commonpb.SegmentState_Flushed,
		StorageVersion:   storage.StorageV3,
		ManifestPath:     packed.MarshalManifestPath(basePath, 3),
		ManifestHasIndex: true,
	})))
	require.NoError(t, dm.IndexMeta.AddSegmentIndex(context.TODO(), &model.SegmentIndex{
		CollectionID:          collID,
		PartitionID:           partID,
		SegmentID:             segID,
		IndexID:               indexID,
		BuildID:               buildID,
		IndexVersion:          1,
		IndexState:            commonpb.IndexState_Finished,
		IndexFileKeys:         []string{"etcd-file"},
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	}))
	dm.SnapshotMeta = &snapshot.SnapshotMeta{}
	m.CompactionProtection = dm.SnapshotMeta

	return m, dm, packed.MarshalManifestPath(basePath, 4), "root/index_files/4100/1/10/4001/manifest-file"
}

func mockV3ManifestIndexEntry(t *testing.T, newManifest string) {
	t.Helper()
	infos := mockey.Mock(packed.GetManifestIndexInfosAsync).Return([]packed.ManifestIndexInfo{{
		IndexID:               400,
		BuildID:               4100,
		FieldID:               101,
		IndexName:             "idx",
		IndexType:             "HNSW",
		IndexVersion:          1,
		NumRows:               100,
		SerializedSize:        10,
		MemSize:               20,
		Path:                  "root/index_files/4100/1/10/4001",
		IndexFileKeys:         []string{"manifest-file"},
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED,
	}}, nil).Build()
	t.Cleanup(func() { infos.UnPatch() })

	commit := mockey.Mock(packed.CommitManifestUpdatesWithResultAsync).Return(meta.MockManifestUpdateResult(newManifest, nil), nil).Build()
	t.Cleanup(func() { commit.UnPatch() })

	blocked := mockey.Mock((*snapshot.SnapshotMeta).IsBuildIDGCBlocked).Return(false).Build()
	t.Cleanup(func() { blocked.UnPatch() })
}

func v3GCManifestIndex(indexID, buildID int64) packed.ManifestIndexInfo {
	return packed.ManifestIndexInfo{
		IndexID:               indexID,
		BuildID:               buildID,
		FieldID:               101,
		IndexName:             "idx",
		IndexType:             "HNSW",
		IndexVersion:          1,
		NumRows:               100,
		SerializedSize:        10,
		MemSize:               20,
		Path:                  fmt.Sprintf("root/index_files/%d/1/10/4001", buildID),
		IndexFileKeys:         []string{"manifest-file"},
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED,
	}
}

func addV3GCFinishedIndex(t *testing.T, im *index.IndexMeta, indexID, buildID int64) {
	t.Helper()
	require.NoError(t, im.AddSegmentIndex(context.TODO(), &model.SegmentIndex{
		CollectionID:          100,
		PartitionID:           10,
		SegmentID:             4001,
		IndexID:               indexID,
		BuildID:               buildID,
		IndexVersion:          1,
		IndexState:            commonpb.IndexState_Finished,
		IndexFileKeys:         []string{"etcd-file"},
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	}))
}

// A pause queued after candidate selection must protect manifest entries as
// well as record-only siblings, even when the record-only batch is empty.
func TestGarbageCollector_recycleUnusedSegIndexes_ManifestPauseBarrier(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		for _, pausedCollection := range []int64{100, -1, 200} {
			t.Run(fmt.Sprintf("mixed=%t/pausedCollection=%d", mixed, pausedCollection), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				m, dm, newManifest, _ := setupV3SegIndexGC(t)
				if mixed {
					addV3GCFinishedIndex(t, dm.IndexMeta, 401, 4200)
				}
				originalManifest := m.GetSegment(ctx, 4001).GetManifestPath()
				var removed atomic.Int32
				cm := mocks.NewChunkManager(t)
				cm.EXPECT().RootPath().Return("root").Maybe()
				cm.EXPECT().Remove(mock.Anything, mock.Anything).RunAndReturn(func(context.Context, string) error {
					removed.Inc()
					return nil
				}).Maybe()
				gc := NewGarbageCollector(m, nil, GcTestDomainOption(dm, GcOption{Cli: cm, Enabled: true}))
				controlDone := make(chan struct{})
				go func() {
					defer close(controlDone)
					gc.startControlLoop(ctx)
				}()
				defer func() {
					cancel()
					gc.Close()
					<-controlDone
				}()

				// Relay the real pause command into the worker's signal channel.
				// Receiving it proves pause() has recorded the ticket; buffering it
				// makes the next acknowledgement deterministic without a sleep.
				signal := make(chan gcCmd, 1)
				pauseDone := make(chan error, 1)
				var pauseOnce sync.Once
				defer mockey.Mock(packed.GetManifestIndexInfosAsync).To(
					func(context.Context, *packed.ManifestIOContext, string, *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
						var pauseErr error
						pauseOnce.Do(func() {
							go func() {
								pauseDone <- gc.Pause(ctx, pausedCollection, "manifest-gc", time.Minute)
							}()
							select {
							case cmd := <-gc.controlChannels["meta"]:
								signal <- cmd
							case <-ctx.Done():
								pauseErr = ctx.Err()
							}
						})
						return []packed.ManifestIndexInfo{v3GCManifestIndex(400, 4100)}, pauseErr
					}).Build().UnPatch()
				defer mockey.Mock(packed.CommitManifestUpdatesWithResultAsync).
					Return(meta.MockManifestUpdateResult(newManifest, nil), nil).Build().UnPatch()
				defer mockey.Mock((*snapshot.SnapshotMeta).IsBuildIDGCBlocked).Return(false).Build().UnPatch()

				gc.RecycleUnusedSegIndexes(ctx, signal)
				select {
				case err := <-pauseDone:
					require.NoError(t, err)
				case <-ctx.Done():
					t.Fatal("GC did not acknowledge the pause")
				}
				if pausedCollection != 200 {
					assert.Zero(t, removed.Load(), "no files may be deleted after acknowledging this collection's pause")
					assert.Equal(t, originalManifest, m.GetSegment(ctx, 4001).GetManifestPath())
					_, exists := dm.IndexMeta.SegmentBuildInfo.Get(4100)
					assert.True(t, exists, "the manifest index record must remain for retry")
					if mixed {
						_, exists = dm.IndexMeta.SegmentBuildInfo.Get(4200)
						assert.True(t, exists, "the record-only sibling must remain for retry")
					}
				} else {
					assert.Positive(t, removed.Load(), "pausing another collection must not prevent this collection's GC")
				}

				require.NoError(t, gc.Resume(ctx, pausedCollection, "manifest-gc"))
				gc.RecycleUnusedSegIndexes(ctx, nil)
				assert.Positive(t, removed.Load(), "resuming must allow deferred files to be deleted")
				assert.Equal(t, newManifest, m.GetSegment(ctx, 4001).GetManifestPath())
				_, exists := dm.IndexMeta.SegmentBuildInfo.Get(4100)
				assert.False(t, exists)
				if mixed {
					_, exists = dm.IndexMeta.SegmentBuildInfo.Get(4200)
					assert.False(t, exists)
				}
			})
		}
	}
}

func TestGarbageCollector_recycleUnusedSegIndexes_ManifestPauseDrainsDeletion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	m, dm, newManifest, _ := setupV3SegIndexGC(t)
	mockV3ManifestIndexEntry(t, newManifest)

	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce, releaseOnce sync.Once
	cm := mocks.NewChunkManager(t)
	cm.EXPECT().RootPath().Return("root").Maybe()
	cm.EXPECT().Remove(mock.Anything, mock.Anything).RunAndReturn(func(context.Context, string) error {
		startedOnce.Do(func() { close(started) })
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	gc := NewGarbageCollector(m, nil, GcTestDomainOption(dm, GcOption{Cli: cm, Enabled: true}))
	controlDone := make(chan struct{})
	go func() {
		defer close(controlDone)
		gc.startControlLoop(ctx)
	}()
	signal := make(chan gcCmd, 1)
	gcDone := make(chan struct{})
	go func() {
		defer close(gcDone)
		gc.RecycleUnusedSegIndexes(ctx, signal)
	}()
	defer func() {
		cancel()
		releaseOnce.Do(func() { close(release) })
		<-gcDone
		gc.Close()
		<-controlDone
	}()

	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("manifest file deletion did not start")
	}
	pauseDone := make(chan error, 1)
	go func() {
		pauseDone <- gc.Pause(ctx, 100, "manifest-gc", time.Minute)
	}()
	var cmd gcCmd
	select {
	case cmd = <-gc.controlChannels["meta"]:
		signal <- cmd
	case <-ctx.Done():
		t.Fatal("pause was not delivered")
	}
	select {
	case <-cmd.done:
		t.Fatal("pause was acknowledged while manifest deletion was in flight")
	default:
	}

	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-pauseDone:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("pause was not acknowledged after manifest deletion")
	}
	assert.Equal(t, newManifest, m.GetSegment(ctx, 4001).GetManifestPath(),
		"the in-flight manifest transaction must complete before acknowledging pause")
	_, exists := dm.IndexMeta.SegmentBuildInfo.Get(4100)
	assert.False(t, exists)
	<-gcDone
}

func TestGarbageCollector_recycleUnusedSegIndexes_ManifestCancellationBeforeDeletion(t *testing.T) {
	m, dm, _, _ := setupV3SegIndexGC(t)
	originalManifest := m.GetSegment(context.Background(), 4001).GetManifestPath()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer mockey.Mock((*meta.Meta).ReadManifestIndexesWithIO).To(
		func(*meta.Meta, context.Context, *packed.ManifestIOContext, string, *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
			cancel()
			return []packed.ManifestIndexInfo{v3GCManifestIndex(400, 4100)}, nil
		}).Build().UnPatch()
	defer mockey.Mock((*snapshot.SnapshotMeta).IsBuildIDGCBlocked).Return(false).Build().UnPatch()

	var removed atomic.Int32
	cm := mocks.NewChunkManager(t)
	cm.EXPECT().RootPath().Return("root").Maybe()
	cm.EXPECT().Remove(mock.Anything, mock.Anything).RunAndReturn(func(context.Context, string) error {
		removed.Inc()
		return nil
	}).Maybe()
	gc := NewGarbageCollector(m, nil, GcTestDomainOption(dm, GcOption{Cli: cm}))
	defer gc.Close()
	gc.RecycleUnusedSegIndexes(ctx, nil)

	assert.Zero(t, removed.Load(), "canceled selection must not start deleting manifest files")
	assert.Equal(t, originalManifest, m.GetSegment(context.Background(), 4001).GetManifestPath())
	_, exists := dm.IndexMeta.SegmentBuildInfo.Get(4100)
	assert.True(t, exists)
}

func TestGarbageCollector_recycleUnusedSegIndexes_BatchesSameSegmentManifestRetractions(t *testing.T) {
	m, dm, newManifest, _ := setupV3SegIndexGC(t)
	addV3GCFinishedIndex(t, dm.IndexMeta, 401, 4200)
	store := meta.NewFakeManifestStore(t)
	originalManifest := m.GetSegment(context.TODO(), 4001).GetManifestPath()
	store.Revisions[originalManifest] = []packed.ManifestIndexInfo{
		v3GCManifestIndex(400, 4100),
		v3GCManifestIndex(401, 4200),
		v3GCManifestIndex(402, 4300),
	}
	defer mockey.Mock((*snapshot.SnapshotMeta).IsBuildIDGCBlocked).Return(false).Build().UnPatch()

	cm := mocks.NewChunkManager(t)
	cm.EXPECT().RootPath().Return("root")
	cm.EXPECT().Remove(mock.Anything, mock.Anything).Return(nil)
	gc := NewGarbageCollector(m, nil, GcTestDomainOption(dm, GcOption{Cli: cm}))
	gc.RecycleUnusedSegIndexes(context.TODO(), nil)

	assert.Equal(t, 1, store.ReadCount, "read ownership once; commit returns its marker without a read")
	assert.True(t, m.GetSegment(context.TODO(), 4001).GetManifestHasIndex())
	assert.Equal(t, 1, store.CommitCount)
	_, firstExists := dm.IndexMeta.SegmentBuildInfo.Get(4100)
	_, secondExists := dm.IndexMeta.SegmentBuildInfo.Get(4200)
	assert.False(t, firstExists)
	assert.False(t, secondExists)
	assert.Equal(t, newManifest, m.GetSegment(context.TODO(), 4001).GetManifestPath())
	require.Len(t, store.Revisions[newManifest], 1)
	assert.Equal(t, int64(402), store.Revisions[newManifest][0].IndexID,
		"the same revision must preserve manifest entries not selected by this GC batch")
}

func TestGarbageCollector_recycleUnusedSegIndexes_BatchCommitsOnlyDeletedFiles(t *testing.T) {
	m, dm, newManifest, _ := setupV3SegIndexGC(t)
	addV3GCFinishedIndex(t, dm.IndexMeta, 401, 4200)

	defer mockey.Mock(packed.GetManifestIndexInfosAsync).Return([]packed.ManifestIndexInfo{
		v3GCManifestIndex(400, 4100),
		v3GCManifestIndex(401, 4200),
	}, nil).Build().UnPatch()
	defer mockey.Mock((*snapshot.SnapshotMeta).IsBuildIDGCBlocked).Return(false).Build().UnPatch()
	defer mockey.Mock(packed.CommitManifestUpdatesWithResultAsync).To(
		func(_ context.Context, _ *packed.ManifestIOContext, _ string, _ *indexpb.StorageConfig, updates *packed.ManifestUpdates) (packed.ManifestUpdateResult, error) {
			require.Equal(t, []packed.DropIndexEntry{{IndexID: 400, ExpectedBuildID: 4100}}, updates.DropIndexes)
			return meta.MockManifestUpdateResult(newManifest, updates), nil
		}).Build().UnPatch()

	cm := mocks.NewChunkManager(t)
	cm.EXPECT().RootPath().Return("root")
	cm.EXPECT().Remove(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, file string) error {
		if strings.Contains(file, "/4200/") {
			return errors.New("throttled")
		}
		return nil
	})
	gc := NewGarbageCollector(m, nil, GcTestDomainOption(dm, GcOption{Cli: cm}))
	gc.RecycleUnusedSegIndexes(context.TODO(), nil)

	_, firstExists := dm.IndexMeta.SegmentBuildInfo.Get(4100)
	_, secondExists := dm.IndexMeta.SegmentBuildInfo.Get(4200)
	assert.False(t, firstExists)
	assert.True(t, secondExists, "a failed file deletion must leave its manifest entry and etcd record for retry")
	assert.Equal(t, newManifest, m.GetSegment(context.TODO(), 4001).GetManifestPath())
}

func TestGarbageCollector_recycleUnusedSegIndexes_MixedManifestAndLegacyOwnership(t *testing.T) {
	m, dm, newManifest, _ := setupV3SegIndexGC(t)
	addV3GCFinishedIndex(t, dm.IndexMeta, 401, 4200)
	store := meta.NewFakeManifestStore(t)
	originalManifest := m.GetSegment(context.TODO(), 4001).GetManifestPath()
	store.Revisions[originalManifest] = []packed.ManifestIndexInfo{v3GCManifestIndex(400, 4100)}
	defer mockey.Mock((*snapshot.SnapshotMeta).IsBuildIDGCBlocked).Return(false).Build().UnPatch()

	cm := mocks.NewChunkManager(t)
	cm.EXPECT().RootPath().Return("root")
	cm.EXPECT().Remove(mock.Anything, mock.Anything).Return(nil)
	gc := NewGarbageCollector(m, nil, GcTestDomainOption(dm, GcOption{Cli: cm}))
	gc.RecycleUnusedSegIndexes(context.TODO(), nil)

	assert.Equal(t, 1, store.ReadCount, "read ownership once; commit returns its marker without a read")
	assert.False(t, m.GetSegment(context.TODO(), 4001).GetManifestHasIndex())
	assert.Equal(t, 1, store.CommitCount)
	assert.Empty(t, store.Revisions[newManifest])
	_, manifestOwnedExists := dm.IndexMeta.SegmentBuildInfo.Get(4100)
	_, recordOwnedExists := dm.IndexMeta.SegmentBuildInfo.Get(4200)
	assert.False(t, manifestOwnedExists)
	assert.False(t, recordOwnedExists)
}

func TestGarbageCollector_recycleUnusedSegIndexes_BatchKeepsSnapshotPinnedIndex(t *testing.T) {
	m, dm, newManifest, _ := setupV3SegIndexGC(t)
	addV3GCFinishedIndex(t, dm.IndexMeta, 401, 4200)
	store := meta.NewFakeManifestStore(t)
	originalManifest := m.GetSegment(context.TODO(), 4001).GetManifestPath()
	store.Revisions[originalManifest] = []packed.ManifestIndexInfo{
		v3GCManifestIndex(400, 4100),
		v3GCManifestIndex(401, 4200),
	}
	defer mockey.Mock((*snapshot.SnapshotMeta).IsBuildIDGCBlocked).To(
		func(_ *snapshot.SnapshotMeta, _, buildID int64) bool { return buildID == 4200 }).Build().UnPatch()

	cm := mocks.NewChunkManager(t)
	cm.EXPECT().RootPath().Return("root")
	cm.EXPECT().Remove(mock.Anything, mock.Anything).Return(nil)
	gc := NewGarbageCollector(m, nil, GcTestDomainOption(dm, GcOption{Cli: cm}))
	gc.RecycleUnusedSegIndexes(context.TODO(), nil)

	assert.Equal(t, 1, store.ReadCount, "read ownership once; commit returns its marker without a read")
	assert.True(t, m.GetSegment(context.TODO(), 4001).GetManifestHasIndex())
	assert.Equal(t, 1, store.CommitCount)
	require.Len(t, store.Revisions[newManifest], 1)
	assert.Equal(t, int64(401), store.Revisions[newManifest][0].IndexID)
	_, unpinnedExists := dm.IndexMeta.SegmentBuildInfo.Get(4100)
	_, pinnedExists := dm.IndexMeta.SegmentBuildInfo.Get(4200)
	assert.False(t, unpinnedExists)
	assert.True(t, pinnedExists)
}

func TestGarbageCollector_recycleUnusedSegIndexes_BatchCatalogFailureRetriesAtomically(t *testing.T) {
	m, dm, newManifest, _ := setupV3SegIndexGC(t)
	addV3GCFinishedIndex(t, dm.IndexMeta, 401, 4200)
	store := meta.NewFakeManifestStore(t)
	originalManifest := m.GetSegment(context.TODO(), 4001).GetManifestPath()
	store.Revisions[originalManifest] = []packed.ManifestIndexInfo{
		v3GCManifestIndex(400, 4100),
		v3GCManifestIndex(401, 4200),
	}
	defer mockey.Mock((*snapshot.SnapshotMeta).IsBuildIDGCBlocked).Return(false).Build().UnPatch()

	cm := mocks.NewChunkManager(t)
	cm.EXPECT().RootPath().Return("root")
	cm.EXPECT().Remove(mock.Anything, mock.Anything).Return(nil)
	gc := NewGarbageCollector(m, nil, GcTestDomainOption(dm, GcOption{Cli: cm}))
	failCatalog := mockey.Mock((*datacoord.Catalog).Update).Return(errors.New("catalog unavailable")).Build()
	gc.RecycleUnusedSegIndexes(context.TODO(), nil)

	assert.Equal(t, originalManifest, m.GetSegment(context.TODO(), 4001).GetManifestPath())
	_, firstExists := dm.IndexMeta.SegmentBuildInfo.Get(4100)
	_, secondExists := dm.IndexMeta.SegmentBuildInfo.Get(4200)
	assert.True(t, firstExists)
	assert.True(t, secondExists)
	assert.Equal(t, 1, store.CommitCount, "the failed catalog write may leave only an unreachable manifest revision")

	failCatalog.UnPatch()
	gc.RecycleUnusedSegIndexes(context.TODO(), nil)
	assert.Equal(t, newManifest, m.GetSegment(context.TODO(), 4001).GetManifestPath())
	_, firstExists = dm.IndexMeta.SegmentBuildInfo.Get(4100)
	_, secondExists = dm.IndexMeta.SegmentBuildInfo.Get(4200)
	assert.False(t, firstExists)
	assert.False(t, secondExists)
}

func TestGarbageCollector_recycleUnusedSegIndexes_SplitsAtCatalogTransactionLimit(t *testing.T) {
	originalLimit := Params.MetaStoreCfg.MaxEtcdTxnNum.SwapTempValue("2")
	defer Params.MetaStoreCfg.MaxEtcdTxnNum.SwapTempValue(originalLimit)
	m, dm, _, _ := setupV3SegIndexGC(t)
	addV3GCFinishedIndex(t, dm.IndexMeta, 401, 4200)

	store := meta.NewFakeManifestStore(t)
	store.Revisions[m.GetSegment(context.TODO(), 4001).GetManifestPath()] = []packed.ManifestIndexInfo{
		v3GCManifestIndex(400, 4100), v3GCManifestIndex(401, 4200),
	}
	defer mockey.Mock((*snapshot.SnapshotMeta).IsBuildIDGCBlocked).Return(false).Build().UnPatch()

	cm := mocks.NewChunkManager(t)
	cm.EXPECT().RootPath().Return("root")
	cm.EXPECT().Remove(mock.Anything, mock.Anything).Return(nil)
	gc := NewGarbageCollector(m, nil, GcTestDomainOption(dm, GcOption{Cli: cm}))
	gc.RecycleUnusedSegIndexes(context.TODO(), nil)

	assert.Equal(t, 1, store.ReadCount, "one ownership read; neither commit rereads its marker")
	assert.Equal(t, 2, store.CommitCount)
	assert.False(t, m.GetSegment(context.TODO(), 4001).GetManifestHasIndex())
	_, firstExists := dm.IndexMeta.SegmentBuildInfo.Get(4100)
	_, secondExists := dm.IndexMeta.SegmentBuildInfo.Get(4200)
	assert.False(t, firstExists)
	assert.False(t, secondExists)
	assert.Equal(t, packed.MarshalManifestPath("/tmp/test-gc-v3-retract/insert_log/100/10/4001", 5),
		m.GetSegment(context.TODO(), 4001).GetManifestPath())
}

func TestGarbageCollector_recycleUnusedSegIndexes_UnmarkedUsesLegacyPathWithoutManifestRead(t *testing.T) {
	m, dm, _, _ := setupV3SegIndexGC(t)
	m.GetSegment(context.TODO(), 4001).ManifestHasIndex = false
	manifestReads := 0
	defer mockey.Mock(packed.GetManifestIndexInfosAsync).To(
		func(context.Context, *packed.ManifestIOContext, string, *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
			manifestReads++
			return nil, errors.New("unexpected manifest read")
		}).Build().UnPatch()
	defer mockey.Mock((*snapshot.SnapshotMeta).IsBuildIDGCBlocked).Return(false).Build().UnPatch()

	cm := mocks.NewChunkManager(t)
	cm.EXPECT().RootPath().Return("root")
	cm.EXPECT().Remove(mock.Anything, mock.Anything).Return(nil)
	gc := NewGarbageCollector(m, nil, GcTestDomainOption(dm, GcOption{Cli: cm}))
	gc.RecycleUnusedSegIndexes(context.TODO(), nil)

	assert.Zero(t, manifestReads)
	_, exists := dm.IndexMeta.SegmentBuildInfo.Get(4100)
	assert.False(t, exists)
	assert.Equal(t, packed.MarshalManifestPath("/tmp/test-gc-v3-retract/insert_log/100/10/4001", 3),
		m.GetSegment(context.TODO(), 4001).GetManifestPath())
}

// Artifact bytes are deleted BEFORE the metadata naming them, on the manifest
// path as much as the legacy one. This path only runs for an index whose
// definition or segment is already gone, so a window where metadata still names
// deleted files harms no reader - whereas removing the record first would strip
// GC of the only thing that can re-drive a failed deletion.
func TestGarbageCollector_recycleUnusedSegIndexes_V3DeletesFilesBeforeMeta(t *testing.T) {
	m, dm, newManifest, manifestFile := setupV3SegIndexGC(t)
	meta.WithSegmentIndexManifestWrites(t, false)
	mockV3ManifestIndexEntry(t, newManifest)

	// removeObjectFiles fans the deletions out over a conc.Pool, so this
	// callback runs on several goroutines at once and every piece of state it
	// touches has to be guarded - including the "is this the first remove?"
	// read, which is what decides when the metadata snapshot is taken.
	var mu sync.Mutex
	var metaPresentAtFirstRemove bool
	var removed []string
	cm := mocks.NewChunkManager(t)
	cm.EXPECT().RootPath().Return("root")
	cm.EXPECT().Remove(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, file string) error {
		mu.Lock()
		defer mu.Unlock()
		if len(removed) == 0 {
			_, metaPresentAtFirstRemove = dm.IndexMeta.SegmentBuildInfo.Get(4100)
		}
		removed = append(removed, file)
		return nil
	})

	gc := NewGarbageCollector(m, nil, GcTestDomainOption(dm, GcOption{Cli: cm}))
	gc.RecycleUnusedSegIndexes(context.TODO(), nil)

	assert.True(t, metaPresentAtFirstRemove, "artifact deletion must start while the metadata naming it still exists")
	// The retraction still lands, and still atomically: manifest pointer
	// advanced and the record gone in the same catalog transaction.
	_, ok := dm.IndexMeta.SegmentBuildInfo.Get(4100)
	assert.False(t, ok)
	assert.Equal(t, newManifest, m.GetSegment(context.TODO(), 4001).GetManifestPath())
	// Both the record's own file keys and the manifest entry's paths are deleted.
	assert.Contains(t, removed, manifestFile)
	assert.Contains(t, removed, "root/index_v1/100/10/4001/4100/1/etcd-file")
}

// A deletion failure must leave BOTH the record and the manifest entry intact,
// so the next cycle re-drives the whole step. This is the property the ordering
// exists for: a COLLECTION_ROOTED artifact whose record was already removed is
// unreclaimable, because recycleUnusedIndexFilesV1 is meta-driven.
func TestGarbageCollector_recycleUnusedSegIndexes_V3FileDeletionFailureRetriesFromMeta(t *testing.T) {
	m, dm, newManifest, _ := setupV3SegIndexGC(t)
	mockV3ManifestIndexEntry(t, newManifest)
	originalManifest := m.GetSegment(context.TODO(), 4001).GetManifestPath()

	cm := mocks.NewChunkManager(t)
	cm.EXPECT().RootPath().Return("root")
	cm.EXPECT().Remove(mock.Anything, mock.Anything).Return(errors.New("throttled"))

	gc := NewGarbageCollector(m, nil, GcTestDomainOption(dm, GcOption{Cli: cm}))
	gc.RecycleUnusedSegIndexes(context.TODO(), nil)

	_, ok := dm.IndexMeta.SegmentBuildInfo.Get(4100)
	assert.True(t, ok, "a failed delete must leave the record to re-drive the retry from")
	assert.Equal(t, originalManifest, m.GetSegment(context.TODO(), 4001).GetManifestPath(),
		"the manifest entry must not be retracted while its bytes are still there")
	assert.NotEqual(t, newManifest, originalManifest)
}

// A manifest that does not carry the entry takes the same ordering through the
// plain RemoveSegmentIndex path.
func TestGarbageCollector_recycleUnusedSegIndexes_NonManifestKeepsLegacyOrder(t *testing.T) {
	m, dm, _, _ := setupV3SegIndexGC(t)
	infos := mockey.Mock(packed.GetManifestIndexInfosAsync).Return(nil, nil).Build()
	defer infos.UnPatch()
	blocked := mockey.Mock((*snapshot.SnapshotMeta).IsBuildIDGCBlocked).Return(false).Build()
	defer blocked.UnPatch()

	cm := mocks.NewChunkManager(t)
	cm.EXPECT().RootPath().Return("root")
	cm.EXPECT().Remove(mock.Anything, mock.Anything).Return(errors.New("throttled"))

	gc := NewGarbageCollector(m, nil, GcTestDomainOption(dm, GcOption{Cli: cm}))
	gc.RecycleUnusedSegIndexes(context.TODO(), nil)

	_, ok := dm.IndexMeta.SegmentBuildInfo.Get(4100)
	assert.True(t, ok, "files-first ordering must leave the record for the next cycle")
}

// Recovery must restore a retained dropped segment's index records. Also exercise
// GC's manifest fallback independently of recovery by removing the in-memory
// projection after checking it was restored: the manifest remains authoritative
// when a caller has an incomplete record view.
func TestGarbageCollector_DroppedSegmentIndexFilesComeFromManifestAfterReload(t *testing.T) {
	meta.MockManifestIndexSubmissions(t)
	meta.WithSegmentIndexManifestWrites(t, true)

	const segmentID = int64(3101)
	basePath := "/tmp/test-gc-dropped-reload/insert_log/100/10/3101"
	m, dm, err := newMemoryMetaWithSnapshot(t)
	require.NoError(t, err)
	m.ChunkManager = storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test"))
	require.NoError(t, m.AddSegment(context.TODO(), meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID:               segmentID,
		CollectionID:     100,
		PartitionID:      10,
		State:            commonpb.SegmentState_Dropped,
		NumOfRows:        100,
		StorageVersion:   storage.StorageV3,
		ManifestPath:     packed.MarshalManifestPath(basePath, 1),
		ManifestHasIndex: true,
	})))

	infos := mockey.Mock(packed.GetManifestIndexInfosAsync).Return([]packed.ManifestIndexInfo{{
		IndexID:               500,
		BuildID:               5300,
		FieldID:               101,
		IndexName:             "idx",
		IndexType:             "HNSW",
		IndexVersion:          1,
		NumRows:               100,
		SerializedSize:        2000,
		MemSize:               3000,
		Path:                  "/tmp/test/index_files/5300/1/10/3101",
		IndexFileKeys:         []string{"f0"},
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED,
	}}, nil).Build()
	defer infos.UnPatch()

	require.NoError(t, m.ReloadSegmentIndexesFromManifests(context.TODO()))
	recovered := dm.IndexMeta.GetAllSegmentIndexes(segmentID)
	require.Len(t, recovered, 1, "retained dropped segments need their index records after restart")
	require.EqualValues(t, 5300, recovered[0].BuildID)

	// Model a missing projection separately from the restart contract.
	dm.IndexMeta.SegmentBuildInfo.Remove(5300)
	dm.IndexMeta.SegmentIndexes.Remove(segmentID)

	gc := NewGarbageCollector(m, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{
		Cli: storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test")),
	}))
	segIndexes, indexFiles, blocked := gc.GetDroppedSegmentIndexFiles(context.TODO(), NewGCManifestReadIO(t), segmentID)
	assert.Equal(t, GcNotBlocked, blocked)
	assert.Empty(t, segIndexes)
	require.NotEmpty(t, indexFiles,
		"with no records, the manifest is the only thing naming the artifacts; without the fallback they leak")
}

// A dropped StorageV3 segment can have manifest artifacts and record-only
// results completed after the drop. Exercise the union with an incomplete
// in-memory projection so neither source of file ownership can hide the other.
func TestGarbageCollector_getDroppedSegmentIndexFiles_UnionsRecordsAndManifest(t *testing.T) {
	meta.WithSegmentIndexManifestWrites(t, true)
	const (
		collID    = int64(100)
		partID    = int64(10)
		segmentID = int64(3201)
	)
	basePath := "/tmp/test-gc-dropped-union/insert_log/100/10/3201"
	m, dm, err := newMemoryMetaWithSnapshot(t)
	require.NoError(t, err)
	require.NoError(t, m.AddSegment(context.TODO(), meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID:               segmentID,
		CollectionID:     collID,
		PartitionID:      partID,
		State:            commonpb.SegmentState_Dropped,
		NumOfRows:        100,
		StorageVersion:   storage.StorageV3,
		ManifestPath:     packed.MarshalManifestPath(basePath, 1),
		ManifestHasIndex: true,
	})))
	require.NoError(t, dm.IndexMeta.AddSegmentIndex(context.TODO(), &model.SegmentIndex{
		CollectionID:          collID,
		PartitionID:           partID,
		SegmentID:             segmentID,
		IndexID:               600,
		BuildID:               6100,
		IndexVersion:          1,
		IndexState:            commonpb.IndexState_Finished,
		IndexFileKeys:         []string{"etcd-file"},
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	}))

	infos := mockey.Mock(packed.GetManifestIndexInfosAsync).Return([]packed.ManifestIndexInfo{{
		IndexID:               601,
		BuildID:               6200,
		FieldID:               101,
		IndexName:             "idx",
		IndexType:             "HNSW",
		IndexVersion:          1,
		NumRows:               100,
		SerializedSize:        2000,
		MemSize:               3000,
		Path:                  "/tmp/test/index_files/6200/1/10/3201",
		IndexFileKeys:         []string{"manifest-file"},
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED,
	}}, nil).Build()
	defer infos.UnPatch()

	gc := NewGarbageCollector(m, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{
		Cli: storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test")),
	}))
	segIndexes, indexFiles, blocked := gc.GetDroppedSegmentIndexFiles(context.TODO(), NewGCManifestReadIO(t), segmentID)
	assert.Equal(t, GcNotBlocked, blocked)
	require.Len(t, segIndexes, 1)
	assert.Contains(t, indexFiles, "/tmp/test/index_files/6200/1/10/3201/manifest-file",
		"the manifest-only entry's files must be in the delete list even though records exist")
	for file := range gc.GetAllIndexFilesOfIndex(segIndexes[0]) {
		assert.Contains(t, indexFiles, file,
			"the record-derived files must survive the union with the manifest")
	}
}

// A manifest entry that fails validation blocks recycling deterministically -
// recycling anyway would leak whatever the entry actually names - and must
// surface as a manifest block, not as snapshot protection: only
// gcBlockedBySnapshot takes the "protected by snapshot" message path in
// recycleDroppedSegment.
func TestGarbageCollector_getDroppedSegmentIndexFiles_InvalidManifestEntryBlocks(t *testing.T) {
	meta.WithSegmentIndexManifestWrites(t, true)
	const segmentID = int64(3301)
	basePath := "/tmp/test-gc-dropped-invalid/insert_log/100/10/3301"
	m, dm, err := newMemoryMetaWithSnapshot(t)
	require.NoError(t, err)
	require.NoError(t, m.AddSegment(context.TODO(), meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID:               segmentID,
		CollectionID:     100,
		PartitionID:      10,
		State:            commonpb.SegmentState_Dropped,
		NumOfRows:        100,
		StorageVersion:   storage.StorageV3,
		ManifestPath:     packed.MarshalManifestPath(basePath, 1),
		ManifestHasIndex: true,
	})))

	// A path-escaping file key fails manifestIndexFilePathInfo validation.
	infos := mockey.Mock(packed.GetManifestIndexInfosAsync).Return([]packed.ManifestIndexInfo{{
		IndexID:               700,
		BuildID:               7100,
		FieldID:               101,
		IndexName:             "idx",
		IndexType:             "HNSW",
		IndexVersion:          1,
		NumRows:               100,
		SerializedSize:        2000,
		MemSize:               3000,
		Path:                  "root/index/100/10/3301/7100/1",
		IndexFileKeys:         []string{"../escape"},
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED,
	}}, nil).Build()
	defer infos.UnPatch()

	gc := NewGarbageCollector(m, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{
		Cli: storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test")),
	}))
	counter := metrics.GarbageCollectorInvalidManifestCount.WithLabelValues(paramtable.GetStringNodeID())
	before := testutil.ToFloat64(counter)
	_, indexFiles, blocked := gc.GetDroppedSegmentIndexFiles(context.TODO(), NewGCManifestReadIO(t), segmentID)
	assert.Equal(t, gcBlockedByManifest, blocked)
	assert.Nil(t, indexFiles)
	assert.Equal(t, before+1, testutil.ToFloat64(counter))

	// The block holds through recycleDroppedSegment: nothing is deleted and
	// the segment meta survives for the cycle after the manifest is repaired.
	gc.RecycleDroppedSegment(context.TODO(), NewGCManifestReadIO(t), segmentID)
	assert.NotNil(t, m.GetSegment(context.TODO(), segmentID))
}

// An unmarked segment has never carried a manifest index entry. Recovery and
// GC skip it even though they are independent of the current write mode.
func TestGarbageCollector_getDroppedSegmentIndexFiles_SkipsUnmarkedManifestWhenDisabled(t *testing.T) {
	meta.WithSegmentIndexManifestWrites(t, false)
	const segmentID = int64(3401)
	basePath := "/tmp/test-gc-dropped-unmarked/insert_log/100/10/3401"
	m, dm, err := newMemoryMetaWithSnapshot(t)
	require.NoError(t, err)
	require.NoError(t, m.AddSegment(context.TODO(), meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID:             segmentID,
		CollectionID:   100,
		PartitionID:    10,
		State:          commonpb.SegmentState_Dropped,
		NumOfRows:      100,
		StorageVersion: storage.StorageV3,
		ManifestPath:   packed.MarshalManifestPath(basePath, 1),
	})))

	// Every read fails; not blocking proves no read was attempted.
	readErr := mockey.Mock(packed.GetManifestIndexInfosAsync).
		Return(nil, merr.WrapErrIoFailedReason("throttled")).Build()
	defer readErr.UnPatch()

	gc := NewGarbageCollector(m, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{
		Cli: storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test")),
	}))
	segIndexes, indexFiles, blocked := gc.GetDroppedSegmentIndexFiles(context.TODO(), NewGCManifestReadIO(t), segmentID)
	assert.Equal(t, GcNotBlocked, blocked)
	assert.Empty(t, segIndexes)
	assert.Empty(t, indexFiles)
}

func TestParseIndexV1FilePath(t *testing.T) {
	collID, segID, buildID, err := parseIndexV1FilePath("root", "root/index_v1/100/200/300/2000/2/file1")
	require.NoError(t, err)
	assert.Equal(t, int64(100), collID)
	assert.Equal(t, int64(300), segID)
	assert.Equal(t, int64(2000), buildID)

	// Nested keys below the version directory are still attributed to the build.
	collID, segID, buildID, err = parseIndexV1FilePath("root", "root/index_v1/100/200/300/2000/2/dir/file1")
	require.NoError(t, err)
	assert.Equal(t, int64(100), collID)
	assert.Equal(t, int64(300), segID)
	assert.Equal(t, int64(2000), buildID)

	for name, file := range map[string]string{
		"not under the v1 prefix": "root/index_files/2000/2/200/300/file1",
		"too few components":      "root/index_v1/100/200/300/2000/file1",
		"non-numeric collection":  "root/index_v1/abc/200/300/2000/2/file1",
		"non-numeric segment":     "root/index_v1/100/200/abc/2000/2/file1",
		"non-numeric build":       "root/index_v1/100/200/300/abc/2/file1",
	} {
		t.Run(name, func(t *testing.T) {
			_, _, _, err := parseIndexV1FilePath("root", file)
			assert.Error(t, err)
		})
	}
}

// fakeDataViewGC records the collections and retainLatest values it was asked
// to garbage collect, implementing DataViewGarbageCollector.
type fakeDataViewGC struct {
	mu        sync.Mutex
	collected []int64
	retains   []int
	err       error
}

func (f *fakeDataViewGC) GarbageCollect(_ context.Context, collectionID int64, retainLatest int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.collected = append(f.collected, collectionID)
	f.retains = append(f.retains, retainLatest)
	return f.err
}

func (f *fakeDataViewGC) snap() ([]int64, []int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int64(nil), f.collected...), append([]int(nil), f.retains...)
}

// TestGarbageCollector_recycleDataViews verifies the DataView GC sweep: every
// non-paused collection is garbage collected with retainLatest=1, paused
// collections are skipped, GC errors are tolerated (logged, not fatal), a
// canceled context stops the sweep, and a nil collector is a no-op.
func TestGarbageCollector_recycleDataViews(t *testing.T) {
	mt, dm, err := newMemoryMetaWithSnapshot(t)
	require.NoError(t, err)
	for _, id := range []int64{100, 101, 102} {
		mt.AddCollection(&meta.CollectionInfo{ID: id})
	}

	t.Run("nil dataViewGC is a no-op", func(t *testing.T) {
		gc := NewGarbageCollector(mt, nil, GcTestDomainOption(dm, GcOption{}))
		require.NotPanics(t, func() {
			gc.recycleDataViews(context.Background(), nil)
		})
	})

	t.Run("collects every non-paused collection with retainLatest=1", func(t *testing.T) {
		fake := &fakeDataViewGC{}
		gc := NewGarbageCollector(mt, nil, GcTestDomainOption(dm, GcOption{DataViewGC: fake}))
		gc.recycleDataViews(context.Background(), nil)
		collected, retains := fake.snap()
		assert.ElementsMatch(t, []int64{100, 101, 102}, collected)
		for _, r := range retains {
			assert.Equal(t, 1, r)
		}
	})

	t.Run("skips paused collections", func(t *testing.T) {
		fake := &fakeDataViewGC{}
		gc := NewGarbageCollector(mt, nil, GcTestDomainOption(dm, GcOption{DataViewGC: fake}))
		// Pause collection 101 far into the future so collectionGCPaused is true.
		records := NewGCPauseRecords()
		_, err := records.Insert("ticket", time.Now().Add(time.Hour))
		require.NoError(t, err)
		gc.PausedCollection.Insert(101, records)

		gc.recycleDataViews(context.Background(), nil)
		collected, _ := fake.snap()
		assert.ElementsMatch(t, []int64{100, 102}, collected)
	})

	t.Run("tolerates GC errors", func(t *testing.T) {
		fake := &fakeDataViewGC{err: errors.New("injected data view gc failure")}
		gc := NewGarbageCollector(mt, nil, GcTestDomainOption(dm, GcOption{DataViewGC: fake}))
		require.NotPanics(t, func() {
			gc.recycleDataViews(context.Background(), nil)
		})
		collected, _ := fake.snap()
		assert.ElementsMatch(t, []int64{100, 101, 102}, collected)
	})

	t.Run("canceled context stops the sweep", func(t *testing.T) {
		fake := &fakeDataViewGC{}
		gc := NewGarbageCollector(mt, nil, GcTestDomainOption(dm, GcOption{DataViewGC: fake}))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		gc.recycleDataViews(ctx, nil)
		collected, _ := fake.snap()
		assert.Empty(t, collected)
	})
}

func TestGarbageCollector_recycleUnusedImportV3Prefixes(t *testing.T) {
	ctx := context.Background()
	cli := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	gc := NewGarbageCollector(nil, meta.NewMockHandler(), GcOption{
		Cli:              cli,
		MissingTolerance: 0, // freshly written objects are already past tolerance
		ImportJobAlive: func(_ context.Context, jobID int64) bool {
			return jobID == 43 // job 43 is still alive; job 42 is an orphan
		},
	})

	orphanPath := path.Join(cli.RootPath(), metautil.BuildImportV3JobPath(42), "plans", "reshard", "10", "plan.pb")
	livePath := path.Join(cli.RootPath(), metautil.BuildImportV3JobPath(43), "plans", "reshard", "10", "plan.pb")
	require.NoError(t, cli.Write(ctx, orphanPath, []byte("orphan")))
	require.NoError(t, cli.Write(ctx, livePath, []byte("live")))

	// The orphan prefix is removed while the live job's prefix is kept.
	gc.recycleUnusedImportV3Prefixes(ctx)
	exist, err := cli.Exist(ctx, orphanPath)
	require.NoError(t, err)
	require.False(t, exist)
	exist, err = cli.Exist(ctx, livePath)
	require.NoError(t, err)
	require.True(t, exist)

	// A nil importJobAlive disables the scan entirely.
	gc.Option.ImportJobAlive = nil
	gc.recycleUnusedImportV3Prefixes(ctx)
	exist, err = cli.Exist(ctx, livePath)
	require.NoError(t, err)
	require.True(t, exist)
}

func TestGarbageCollectorIsPaused(t *testing.T) {
	gc := &GarbageCollector{
		pauseUntil:       NewGCPauseRecords(),
		PausedCollection: typeutil.NewConcurrentMap[int64, *gcPauseRecords](),
	}
	assert.False(t, gc.IsPaused(1))

	coll := NewGCPauseRecords()
	_, err := coll.Insert("t", time.Now().Add(time.Hour))
	require.NoError(t, err)
	gc.PausedCollection.Insert(1, coll)
	assert.True(t, gc.IsPaused(1))
	assert.False(t, gc.IsPaused(2))

	_, err = gc.pauseUntil.Insert("t", time.Now().Add(time.Hour))
	require.NoError(t, err)
	assert.True(t, gc.IsPaused(2), "a global pause covers every collection")
}

type batchIndexDeleteCatalog struct {
	metastore.DataCoordCatalog
	dropIndexes        func(context.Context, []*model.Index) error
	dropSegmentIndexes func(context.Context, []*model.SegmentIndex) error
}

type failingDropSegmentCatalog struct {
	metastore.DataCoordCatalog
	err error
}

func (c *failingDropSegmentCatalog) DropSegments(context.Context, []*datapb.SegmentInfo) error {
	return c.err
}

func (c *batchIndexDeleteCatalog) DropIndexes(ctx context.Context, indexes []*model.Index) error {
	if c.dropIndexes == nil {
		return nil
	}
	return c.dropIndexes(ctx, indexes)
}

func (c *batchIndexDeleteCatalog) DropSegmentIndexes(ctx context.Context, indexes []*model.SegmentIndex) error {
	if c.dropSegmentIndexes == nil {
		return nil
	}
	return c.dropSegmentIndexes(ctx, indexes)
}

func TestGarbageCollector_recycleUnusedIndexesInBatches(t *testing.T) {
	t.Run("deletes field indexes in one catalog batch", func(t *testing.T) {
		baseCatalog := catalogmocks.NewDataCoordCatalog(t)
		var calls [][]*model.Index
		catalog := &batchIndexDeleteCatalog{
			DataCoordCatalog: baseCatalog,
			dropIndexes: func(_ context.Context, indexes []*model.Index) error {
				calls = append(calls, append([]*model.Index(nil), indexes...))
				return nil
			},
		}
		m, im := createMetaForRecycleUnusedIndexes(catalog)
		gc := NewGarbageCollector(m, nil, GcOption{IndexMeta: im})

		gc.recycleUnusedIndexes(context.Background(), nil)

		require.Len(t, calls, 1)
		assert.Len(t, calls[0], 2)
		assert.Empty(t, im.GetDeletedIndexes())
		baseCatalog.AssertNotCalled(t, "DropIndex", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("catalog failure keeps resident field indexes", func(t *testing.T) {
		baseCatalog := catalogmocks.NewDataCoordCatalog(t)
		catalog := &batchIndexDeleteCatalog{
			DataCoordCatalog: baseCatalog,
			dropIndexes: func(_ context.Context, _ []*model.Index) error {
				return errors.New("batch metadata delete failed")
			},
		}
		m, im := createMetaForRecycleUnusedIndexes(catalog)
		gc := NewGarbageCollector(m, nil, GcOption{IndexMeta: im})

		gc.recycleUnusedIndexes(context.Background(), nil)

		assert.Len(t, im.GetDeletedIndexes(), 2)
		baseCatalog.AssertNotCalled(t, "DropIndex", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("stale field-index candidate is skipped", func(t *testing.T) {
		baseCatalog := catalogmocks.NewDataCoordCatalog(t)
		var removed []*model.Index
		catalog := &batchIndexDeleteCatalog{
			DataCoordCatalog: baseCatalog,
			dropIndexes: func(_ context.Context, indexes []*model.Index) error {
				removed = append(removed, indexes...)
				return nil
			},
		}
		_, im := createMetaForRecycleUnusedIndexes(catalog)
		candidates := im.GetDeletedIndexes()
		require.Len(t, candidates, 2)
		stale := candidates[0]
		replacement := model.CloneIndex(stale)
		im.Indexes[stale.CollectionID][stale.IndexID] = replacement

		count, err := im.RemoveIndexes(context.Background(), candidates)

		require.NoError(t, err)
		assert.Equal(t, 1, count)
		require.Len(t, removed, 1)
		assert.NotEqual(t, stale.IndexID, removed[0].IndexID)
		assert.Same(t, replacement, im.Indexes[stale.CollectionID][stale.IndexID])
	})
}

type batchRemoveChunkManager struct {
	storage.ChunkManager
	rootPath     string
	remove       func(context.Context, []string) []storage.RemoveResult
	removePrefix func(context.Context, []string) []storage.RemovePrefixResult

	mu          sync.Mutex
	calls       [][]string
	prefixCalls [][]string
}

func (cm *batchRemoveChunkManager) RootPath() string {
	return cm.rootPath
}

func (cm *batchRemoveChunkManager) MultiRemoveWithResult(ctx context.Context, filePaths []string) []storage.RemoveResult {
	paths := append([]string(nil), filePaths...)
	cm.mu.Lock()
	cm.calls = append(cm.calls, paths)
	cm.mu.Unlock()
	if cm.remove != nil {
		return cm.remove(ctx, paths)
	}

	results := make([]storage.RemoveResult, 0, len(paths))
	for _, filePath := range paths {
		results = append(results, storage.RemoveResult{Path: filePath})
	}
	return results
}

func (cm *batchRemoveChunkManager) MultiRemoveWithPrefix(ctx context.Context, prefixes []string) []storage.RemovePrefixResult {
	paths := append([]string(nil), prefixes...)
	cm.mu.Lock()
	cm.prefixCalls = append(cm.prefixCalls, paths)
	cm.mu.Unlock()
	if cm.removePrefix != nil {
		return cm.removePrefix(ctx, paths)
	}

	results := make([]storage.RemovePrefixResult, len(paths))
	for i, prefix := range paths {
		results[i].Prefix = prefix
	}
	return results
}

func setSegmentIndexFileBatchSize(t *testing.T, batchSize int) {
	t.Helper()
	params := paramtable.Get()
	oldBatchSize := params.DataCoordCfg.GCIndexFileBatchSize.GetValue()
	require.NoError(t, params.Save(params.DataCoordCfg.GCIndexFileBatchSize.Key, strconv.Itoa(batchSize)))
	t.Cleanup(func() {
		require.NoError(t, params.Save(params.DataCoordCfg.GCIndexFileBatchSize.Key, oldBatchSize))
	})
}

func TestGarbageCollector_recycleUnusedSegIndexesInBatches(t *testing.T) {
	t.Run("deletes segment metadata in one catalog batch", func(t *testing.T) {
		setSegmentIndexFileBatchSize(t, 100)

		baseCatalog := catalogmocks.NewDataCoordCatalog(t)
		var calls [][]*model.SegmentIndex
		catalog := &batchIndexDeleteCatalog{
			DataCoordCatalog: baseCatalog,
			dropSegmentIndexes: func(_ context.Context, indexes []*model.SegmentIndex) error {
				calls = append(calls, append([]*model.SegmentIndex(nil), indexes...))
				return nil
			},
		}
		meta, dm := createMetaForRecycleUnusedSegIndexes(catalog)
		cm := &batchRemoveChunkManager{rootPath: "root"}
		mockIsBuildIDBlocked := mockey.Mock((*snapshot.SnapshotMeta).IsBuildIDGCBlocked).Return(false).Build()
		defer mockIsBuildIDBlocked.UnPatch()

		gc := NewGarbageCollector(meta, nil, GcTestDomainOption(dm, GcOption{Cli: cm}))
		gc.RecycleUnusedSegIndexes(context.Background(), nil)

		require.Len(t, calls, 1)
		require.Len(t, calls[0], 2)
		assert.ElementsMatch(t, []int64{600, 601}, []int64{calls[0][0].BuildID, calls[0][1].BuildID})
		assert.Empty(t, dm.IndexMeta.SegmentBuildInfo.List())
		baseCatalog.AssertNotCalled(t, "DropSegmentIndex", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("catalog failure keeps all resident segment metadata", func(t *testing.T) {
		setSegmentIndexFileBatchSize(t, 100)

		baseCatalog := catalogmocks.NewDataCoordCatalog(t)
		catalog := &batchIndexDeleteCatalog{
			DataCoordCatalog: baseCatalog,
			dropSegmentIndexes: func(_ context.Context, _ []*model.SegmentIndex) error {
				return errors.New("batch metadata delete failed")
			},
		}
		meta, dm := createMetaForRecycleUnusedSegIndexes(catalog)
		cm := &batchRemoveChunkManager{rootPath: "root"}
		mockIsBuildIDBlocked := mockey.Mock((*snapshot.SnapshotMeta).IsBuildIDGCBlocked).Return(false).Build()
		defer mockIsBuildIDBlocked.UnPatch()

		gc := NewGarbageCollector(meta, nil, GcTestDomainOption(dm, GcOption{Cli: cm}))
		gc.RecycleUnusedSegIndexes(context.Background(), nil)

		assert.Len(t, dm.IndexMeta.SegmentBuildInfo.List(), 2)
		baseCatalog.AssertNotCalled(t, "DropSegmentIndex", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("candidate changed after file deletion is skipped", func(t *testing.T) {
		setSegmentIndexFileBatchSize(t, 100)

		baseCatalog := catalogmocks.NewDataCoordCatalog(t)
		var removed []*model.SegmentIndex
		catalog := &batchIndexDeleteCatalog{
			DataCoordCatalog: baseCatalog,
			dropSegmentIndexes: func(_ context.Context, indexes []*model.SegmentIndex) error {
				removed = append(removed, indexes...)
				return nil
			},
		}
		meta, dm := createMetaForRecycleUnusedSegIndexes(catalog)
		cm := &batchRemoveChunkManager{
			rootPath: "root",
			remove: func(_ context.Context, paths []string) []storage.RemoveResult {
				current, ok := dm.IndexMeta.SegmentBuildInfo.Get(600)
				require.True(t, ok)
				updated := model.CloneSegmentIndex(current)
				updated.IndexFileKeys = append(updated.IndexFileKeys, "new-file")
				dm.IndexMeta.SegmentBuildInfo.Add(updated)

				results := make([]storage.RemoveResult, len(paths))
				for i, filePath := range paths {
					results[i].Path = filePath
				}
				return results
			},
		}
		mockIsBuildIDBlocked := mockey.Mock((*snapshot.SnapshotMeta).IsBuildIDGCBlocked).Return(false).Build()
		defer mockIsBuildIDBlocked.UnPatch()

		gc := NewGarbageCollector(meta, nil, GcTestDomainOption(dm, GcOption{Cli: cm}))
		gc.RecycleUnusedSegIndexes(context.Background(), nil)

		require.Len(t, removed, 1)
		assert.Equal(t, int64(601), removed[0].BuildID)
		current, ok := dm.IndexMeta.SegmentBuildInfo.Get(600)
		require.True(t, ok)
		assert.Contains(t, current.IndexFileKeys, "new-file")
		_, ok = dm.IndexMeta.SegmentBuildInfo.Get(601)
		assert.False(t, ok)
	})

	t.Run("newer resident build is not removed with old build metadata", func(t *testing.T) {
		baseCatalog := catalogmocks.NewDataCoordCatalog(t)
		catalog := &batchIndexDeleteCatalog{DataCoordCatalog: baseCatalog}
		_, dm := createMetaForRecycleUnusedSegIndexes(catalog)
		candidate, ok := dm.IndexMeta.SegmentBuildInfo.Get(600)
		require.True(t, ok)
		newer := model.CloneSegmentIndex(candidate)
		newer.BuildID = 700
		resident, ok := dm.IndexMeta.SegmentIndexes.Get(candidate.SegmentID)
		require.True(t, ok)
		resident.Insert(candidate.IndexID, newer)

		count, err := dm.IndexMeta.RemoveSegmentIndexes(context.Background(), []*model.SegmentIndex{candidate})

		require.NoError(t, err)
		assert.Equal(t, 1, count)
		current, ok := resident.Get(candidate.IndexID)
		require.True(t, ok)
		assert.Equal(t, int64(700), current.BuildID)
	})

	t.Run("deletes files and metadata across segment indexes in batches", func(t *testing.T) {
		setSegmentIndexFileBatchSize(t, 100)

		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.EXPECT().DropSegmentIndexes(mock.Anything, mock.Anything).Return(nil).Once()
		meta, dm := createMetaForRecycleUnusedSegIndexes(catalog)
		cm := &batchRemoveChunkManager{rootPath: "root"}
		mockIsBuildIDBlocked := mockey.Mock((*snapshot.SnapshotMeta).IsBuildIDGCBlocked).Return(false).Build()
		defer mockIsBuildIDBlocked.UnPatch()

		gc := NewGarbageCollector(meta, nil, GcTestDomainOption(dm, GcOption{Cli: cm}))
		gc.RecycleUnusedSegIndexes(context.Background(), nil)

		require.Len(t, cm.calls, 1)
		assert.Len(t, cm.calls[0], 4)
		_, ok := dm.IndexMeta.SegmentBuildInfo.Get(600)
		assert.False(t, ok)
		_, ok = dm.IndexMeta.SegmentBuildInfo.Get(601)
		assert.False(t, ok)
	})

	t.Run("limits batches by estimated file references", func(t *testing.T) {
		setSegmentIndexFileBatchSize(t, 3)

		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.EXPECT().DropSegmentIndexes(mock.Anything, mock.Anything).Return(nil).Times(2)
		meta, dm := createMetaForRecycleUnusedSegIndexes(catalog)
		cm := &batchRemoveChunkManager{rootPath: "root"}
		mockIsBuildIDBlocked := mockey.Mock((*snapshot.SnapshotMeta).IsBuildIDGCBlocked).Return(false).Build()
		defer mockIsBuildIDBlocked.UnPatch()

		gc := NewGarbageCollector(meta, nil, GcTestDomainOption(dm, GcOption{Cli: cm}))
		gc.RecycleUnusedSegIndexes(context.Background(), nil)

		require.Len(t, cm.calls, 2)
		assert.Len(t, cm.calls[0], 2)
		assert.Len(t, cm.calls[1], 2)
	})

	t.Run("partial file failure retains only the affected metadata", func(t *testing.T) {
		setSegmentIndexFileBatchSize(t, 100)

		catalog := catalogmocks.NewDataCoordCatalog(t)
		var removed []*model.SegmentIndex
		catalog.EXPECT().DropSegmentIndexes(mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, indexes []*model.SegmentIndex) error {
				removed = append(removed, indexes...)
				return nil
			}).Once()
		meta, dm := createMetaForRecycleUnusedSegIndexes(catalog)
		cm := &batchRemoveChunkManager{
			rootPath: "root",
			remove: func(_ context.Context, paths []string) []storage.RemoveResult {
				results := make([]storage.RemoveResult, 0, len(paths))
				for _, filePath := range paths {
					result := storage.RemoveResult{Path: filePath}
					switch {
					case strings.Contains(filePath, "/600/"):
						result.Err = merr.WrapErrIoTooManyRequests(filePath, errors.New("throttled"))
					case strings.HasSuffix(filePath, "/file1"):
						result.Err = merr.WrapErrIoKeyNotFound(filePath)
					}
					results = append(results, result)
				}
				return results
			},
		}
		mockIsBuildIDBlocked := mockey.Mock((*snapshot.SnapshotMeta).IsBuildIDGCBlocked).Return(false).Build()
		defer mockIsBuildIDBlocked.UnPatch()

		gc := NewGarbageCollector(meta, nil, GcTestDomainOption(dm, GcOption{Cli: cm}))
		gc.RecycleUnusedSegIndexes(context.Background(), nil)

		require.Len(t, cm.calls, 1)
		_, ok := dm.IndexMeta.SegmentBuildInfo.Get(600)
		assert.True(t, ok, "metadata must remain when any file delete is throttled")
		_, ok = dm.IndexMeta.SegmentBuildInfo.Get(601)
		assert.False(t, ok, "not-found files are idempotent successes")
		require.Len(t, removed, 1)
		assert.Equal(t, int64(601), removed[0].BuildID)
	})

	t.Run("missing per-file result fails closed", func(t *testing.T) {
		setSegmentIndexFileBatchSize(t, 100)

		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.EXPECT().DropSegmentIndexes(mock.Anything, mock.Anything).Return(nil).Once()
		meta, dm := createMetaForRecycleUnusedSegIndexes(catalog)
		cm := &batchRemoveChunkManager{
			rootPath: "root",
			remove: func(_ context.Context, paths []string) []storage.RemoveResult {
				results := make([]storage.RemoveResult, 0, len(paths)-1)
				for _, filePath := range paths {
					if strings.Contains(filePath, "/600/") && strings.HasSuffix(filePath, "/file2") {
						continue
					}
					results = append(results, storage.RemoveResult{Path: filePath})
				}
				return results
			},
		}
		mockIsBuildIDBlocked := mockey.Mock((*snapshot.SnapshotMeta).IsBuildIDGCBlocked).Return(false).Build()
		defer mockIsBuildIDBlocked.UnPatch()

		gc := NewGarbageCollector(meta, nil, GcTestDomainOption(dm, GcOption{Cli: cm}))
		gc.RecycleUnusedSegIndexes(context.Background(), nil)

		_, ok := dm.IndexMeta.SegmentBuildInfo.Get(600)
		assert.True(t, ok, "metadata must remain when the backend omits one file result")
		_, ok = dm.IndexMeta.SegmentBuildInfo.Get(601)
		assert.False(t, ok)
	})

	t.Run("empty file list removes metadata without storage request", func(t *testing.T) {
		setSegmentIndexFileBatchSize(t, 100)

		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.EXPECT().DropSegmentIndexes(mock.Anything, mock.Anything).Return(nil).Once()
		meta, dm := createMetaForRecycleUnusedSegIndexes(catalog)
		empty, ok := dm.IndexMeta.SegmentBuildInfo.Get(600)
		require.True(t, ok)
		empty = model.CloneSegmentIndex(empty)
		empty.IndexFileKeys = nil
		dm.IndexMeta.SegmentBuildInfo.Add(empty)
		inProgress, ok := dm.IndexMeta.SegmentBuildInfo.Get(601)
		require.True(t, ok)
		inProgress = model.CloneSegmentIndex(inProgress)
		inProgress.IndexState = commonpb.IndexState_InProgress
		dm.IndexMeta.SegmentBuildInfo.Add(inProgress)

		cm := &batchRemoveChunkManager{rootPath: "root"}
		mockIsBuildIDBlocked := mockey.Mock((*snapshot.SnapshotMeta).IsBuildIDGCBlocked).Return(false).Build()
		defer mockIsBuildIDBlocked.UnPatch()

		gc := NewGarbageCollector(meta, nil, GcTestDomainOption(dm, GcOption{Cli: cm}))
		gc.RecycleUnusedSegIndexes(context.Background(), nil)

		assert.Empty(t, cm.calls)
		_, ok = dm.IndexMeta.SegmentBuildInfo.Get(600)
		assert.False(t, ok)
		_, ok = dm.IndexMeta.SegmentBuildInfo.Get(601)
		assert.True(t, ok)
	})

	t.Run("nonterminal and snapshot-protected candidates are not submitted", func(t *testing.T) {
		setSegmentIndexFileBatchSize(t, 100)

		catalog := catalogmocks.NewDataCoordCatalog(t)
		meta, dm := createMetaForRecycleUnusedSegIndexes(catalog)
		inProgress, ok := dm.IndexMeta.SegmentBuildInfo.Get(600)
		require.True(t, ok)
		inProgress = model.CloneSegmentIndex(inProgress)
		inProgress.IndexState = commonpb.IndexState_InProgress
		dm.IndexMeta.SegmentBuildInfo.Add(inProgress)

		cm := &batchRemoveChunkManager{rootPath: "root"}
		mockIsBuildIDBlocked := mockey.Mock((*snapshot.SnapshotMeta).IsBuildIDGCBlocked).To(
			func(_ *snapshot.SnapshotMeta, _ int64, buildID int64) bool {
				return buildID == 601
			}).Build()
		defer mockIsBuildIDBlocked.UnPatch()

		gc := NewGarbageCollector(meta, nil, GcTestDomainOption(dm, GcOption{Cli: cm}))
		gc.RecycleUnusedSegIndexes(context.Background(), nil)

		assert.Empty(t, cm.calls)
		_, ok = dm.IndexMeta.SegmentBuildInfo.Get(600)
		assert.True(t, ok)
		_, ok = dm.IndexMeta.SegmentBuildInfo.Get(601)
		assert.True(t, ok)
		catalog.AssertNotCalled(t, "DropSegmentIndexes", mock.Anything, mock.Anything)
	})

	t.Run("paused collection is not submitted", func(t *testing.T) {
		setSegmentIndexFileBatchSize(t, 100)

		catalog := catalogmocks.NewDataCoordCatalog(t)
		meta, dm := createMetaForRecycleUnusedSegIndexes(catalog)
		cm := &batchRemoveChunkManager{rootPath: "root"}
		gc := NewGarbageCollector(meta, nil, GcTestDomainOption(dm, GcOption{Cli: cm}))
		pauseRecords := NewGCPauseRecords()
		_, err := pauseRecords.Insert("test", time.Now().Add(time.Minute))
		require.NoError(t, err)
		gc.PausedCollection.Insert(100, pauseRecords)

		gc.RecycleUnusedSegIndexes(context.Background(), nil)

		assert.Empty(t, cm.calls)
		_, ok := dm.IndexMeta.SegmentBuildInfo.Get(600)
		assert.True(t, ok)
		_, ok = dm.IndexMeta.SegmentBuildInfo.Get(601)
		assert.True(t, ok)
		catalog.AssertNotCalled(t, "DropSegmentIndexes", mock.Anything, mock.Anything)
	})
}

func setDroppedSegmentBatchSize(t *testing.T, batchSize int) {
	t.Helper()
	oldBatchSize := Params.DataCoordCfg.GCDroppedSegmentBatchSize.GetValue()
	require.NoError(t, Params.Save(Params.DataCoordCfg.GCDroppedSegmentBatchSize.Key, strconv.Itoa(batchSize)))
	t.Cleanup(func() {
		require.NoError(t, Params.Save(Params.DataCoordCfg.GCDroppedSegmentBatchSize.Key, oldBatchSize))
	})
}

func TestGarbageCollector_recycleDroppedSegmentsBatchFallbackWithIndexes(t *testing.T) {
	checkpointCatalog := MockCheckpointCatalog(t, 1000)
	ctx := context.Background()
	m, dm, err := newMemoryMetaWithSnapshot(t)
	require.NoError(t, err)
	m.DataViewManager = nil
	dm.SnapshotMeta = nil
	m.CompactionProtection = nil

	for segmentID := int64(1); segmentID <= 4; segmentID++ {
		require.NoError(t, m.AddSegment(ctx, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:            segmentID,
			CollectionID:  1,
			PartitionID:   1,
			InsertChannel: "channel-1",
			State:         commonpb.SegmentState_Dropped,
			DroppedAt:     uint64(time.Now().Add(-time.Hour).UnixNano()),
		}}))
		require.NoError(t, dm.IndexMeta.AddSegmentIndex(ctx, &model.SegmentIndex{
			SegmentID:             segmentID,
			CollectionID:          1,
			PartitionID:           1,
			IndexID:               segmentID,
			BuildID:               segmentID,
			IndexVersion:          1,
			IndexStorePathVersion: 1,
			IndexState:            commonpb.IndexState_Finished,
			IndexFileKeys:         []string{"index-file"},
		}))
	}

	chunkManager := mocks.NewChunkManager(t)
	chunkManager.EXPECT().RootPath().Return("root").Maybe()
	chunkManager.EXPECT().Remove(mock.Anything, mock.Anything).Return(nil).Times(4)
	gc := NewGarbageCollector(m, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{
		StreamingNodeCatalog: checkpointCatalog,
		Cli:                  chunkManager,
		DropTolerance:        0,
	}))
	defer gc.Close()

	gc.RecycleDroppedSegments(ctx, nil)

	for segmentID := int64(1); segmentID <= 4; segmentID++ {
		assert.Nil(t, m.GetSegment(ctx, segmentID))
		assert.Empty(t, dm.IndexMeta.GetAllSegmentIndexes(segmentID))
	}
}

func setupDroppedSegmentBatchFixture(t *testing.T, count int) (*meta.Meta, *snapshot.SnapshotTestMetas) {
	t.Helper()
	ctx := context.Background()
	m, dm, err := newMemoryMetaWithSnapshot(t)
	require.NoError(t, err)
	m.DataViewManager = nil
	dm.SnapshotMeta = nil
	m.CompactionProtection = nil
	for i := 1; i <= count; i++ {
		segmentID := int64(i)
		require.NoError(t, m.AddSegment(ctx, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:            segmentID,
			CollectionID:  1,
			PartitionID:   1,
			InsertChannel: "channel-1",
			State:         commonpb.SegmentState_Dropped,
			DroppedAt:     uint64(time.Now().Add(-time.Hour).UnixNano()),
		}}))
		require.NoError(t, dm.IndexMeta.AddSegmentIndex(ctx, &model.SegmentIndex{
			SegmentID:             segmentID,
			CollectionID:          1,
			PartitionID:           1,
			IndexID:               1,
			BuildID:               segmentID,
			IndexVersion:          1,
			IndexStorePathVersion: 1,
			IndexState:            commonpb.IndexState_Finished,
			IndexFileKeys:         []string{fmt.Sprintf("index-file-%d", segmentID)},
		}))
	}
	return m, dm
}

func TestDroppedSegmentBatchRechecksManifestBeforeDeletion(t *testing.T) {
	ctx := t.Context()
	m, dm := setupDroppedSegmentBatchFixture(t, 2)
	cm := &batchRemoveChunkManager{rootPath: "root"}
	gc := NewGarbageCollector(m, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{Cli: cm}))
	t.Cleanup(gc.Close)
	io := NewGCManifestReadIO(t)
	var batch []*droppedSegmentGCBatchCandidate
	for _, id := range []int64{1, 2} {
		candidate, err := gc.prepareDroppedSegmentGCBatchCandidate(ctx, io, id, m.GetSegment(ctx, id))
		require.NoError(t, err)
		require.NotNil(t, candidate)
		batch = append(batch, candidate)
	}

	// A rollback published a new revision after the GC scan planned both deletions.
	locks := m.GetSegmentManifestLocks()
	locks.Lock(1)
	unlockWriter := m.LockSegments(1)
	updated := m.GetSegment(ctx, 1).Clone()
	updated.ManifestPath = "root/insert_log/1/1/1/_metadata/manifest_2"
	m.Segments.SetSegment(1, updated)
	unlockWriter()
	locks.Unlock(1)

	cm.remove = func(_ context.Context, paths []string) []storage.RemoveResult {
		require.Len(t, paths, 1)
		require.True(t, strings.HasSuffix(paths[0], "index-file-2"))
		if locks.TryLock(2) {
			locks.Unlock(2)
			t.Error("manifest publication could run during deletion")
		}
		return []storage.RemoveResult{{Path: paths[0]}}
	}
	gc.recycleDroppedSegmentBatch(ctx, batch)
	require.Len(t, cm.calls, 1)
	require.Equal(t, updated.GetManifestPath(), m.GetSegment(ctx, 1).GetManifestPath())
	require.Len(t, dm.IndexMeta.GetAllSegmentIndexes(1), 1)
	require.Nil(t, m.GetSegment(ctx, 2))
}

func TestGarbageCollector_recycleDroppedSegmentsInBatches(t *testing.T) {
	checkpointCatalog := MockCheckpointCatalog(t, 1000)
	t.Run("batches files and both metadata levels", func(t *testing.T) {
		setDroppedSegmentBatchSize(t, 100)
		ctx := context.Background()
		m, dm := setupDroppedSegmentBatchFixture(t, 3)
		cm := &batchRemoveChunkManager{rootPath: "root"}
		gc := NewGarbageCollector(m, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{Cli: cm, DropTolerance: 0, StreamingNodeCatalog: checkpointCatalog}))
		defer gc.Close()

		gc.RecycleDroppedSegments(ctx, nil)

		cm.mu.Lock()
		require.Len(t, cm.calls, 1)
		assert.Len(t, cm.calls[0], 3)
		cm.mu.Unlock()
		for segmentID := int64(1); segmentID <= 3; segmentID++ {
			assert.Nil(t, m.GetSegment(ctx, segmentID))
			assert.Empty(t, dm.IndexMeta.GetAllSegmentIndexes(segmentID))
		}
	})

	t.Run("file failure only retains the owning segment", func(t *testing.T) {
		setDroppedSegmentBatchSize(t, 100)
		ctx := context.Background()
		m, dm := setupDroppedSegmentBatchFixture(t, 2)
		cm := &batchRemoveChunkManager{
			rootPath: "root",
			remove: func(_ context.Context, paths []string) []storage.RemoveResult {
				results := make([]storage.RemoveResult, len(paths))
				for i, filePath := range paths {
					results[i].Path = filePath
					if strings.HasSuffix(filePath, "index-file-2") {
						results[i].Err = merr.WrapErrIoTooManyRequests(filePath, errors.New("throttled"))
					}
				}
				return results
			},
		}
		gc := NewGarbageCollector(m, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{Cli: cm, DropTolerance: 0, StreamingNodeCatalog: checkpointCatalog}))
		defer gc.Close()

		gc.RecycleDroppedSegments(ctx, nil)

		assert.Nil(t, m.GetSegment(ctx, 1))
		assert.Empty(t, dm.IndexMeta.GetAllSegmentIndexes(1))
		assert.NotNil(t, m.GetSegment(ctx, 2))
		assert.Len(t, dm.IndexMeta.GetAllSegmentIndexes(2), 1)
	})

	t.Run("missing file result only retains the owning segment", func(t *testing.T) {
		setDroppedSegmentBatchSize(t, 100)
		ctx := context.Background()
		m, dm := setupDroppedSegmentBatchFixture(t, 2)
		cm := &batchRemoveChunkManager{
			rootPath: "root",
			remove: func(_ context.Context, paths []string) []storage.RemoveResult {
				results := make([]storage.RemoveResult, 0, 1)
				for _, filePath := range paths {
					if strings.HasSuffix(filePath, "index-file-1") {
						results = append(results, storage.RemoveResult{Path: filePath})
					}
				}
				return results
			},
		}
		gc := NewGarbageCollector(m, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{Cli: cm, DropTolerance: 0, StreamingNodeCatalog: checkpointCatalog}))
		defer gc.Close()

		gc.RecycleDroppedSegments(ctx, nil)

		assert.Nil(t, m.GetSegment(ctx, 1))
		assert.Empty(t, dm.IndexMeta.GetAllSegmentIndexes(1))
		assert.NotNil(t, m.GetSegment(ctx, 2))
		assert.Len(t, dm.IndexMeta.GetAllSegmentIndexes(2), 1)
	})

	t.Run("V3 prefix failure only retains the owning segment", func(t *testing.T) {
		setDroppedSegmentBatchSize(t, 100)
		ctx := context.Background()
		m, dm, err := newMemoryMetaWithSnapshot(t)
		require.NoError(t, err)
		m.DataViewManager = nil
		dm.SnapshotMeta = nil
		m.CompactionProtection = nil
		for segmentID := int64(1); segmentID <= 2; segmentID++ {
			require.NoError(t, m.AddSegment(ctx, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID:             segmentID,
				CollectionID:   1,
				PartitionID:    1,
				InsertChannel:  "channel-1",
				State:          commonpb.SegmentState_Dropped,
				DroppedAt:      uint64(time.Now().Add(-time.Hour).UnixNano()),
				StorageVersion: storage.StorageV3,
				ManifestPath:   packed.MarshalManifestPath(fmt.Sprintf("root/insert_log/1/1/%d", segmentID), 1),
			}}))
		}
		cm := &batchRemoveChunkManager{
			rootPath: "root",
			removePrefix: func(_ context.Context, prefixes []string) []storage.RemovePrefixResult {
				results := make([]storage.RemovePrefixResult, len(prefixes))
				for i, prefix := range prefixes {
					results[i].Prefix = prefix
					if prefix == "root/insert_log/1/1/2/" {
						results[i].Err = merr.WrapErrIoTooManyRequests(prefix, errors.New("throttled"))
					}
				}
				return results
			},
		}
		gc := NewGarbageCollector(m, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{Cli: cm, DropTolerance: 0, StreamingNodeCatalog: checkpointCatalog}))
		defer gc.Close()

		gc.RecycleDroppedSegments(ctx, nil)

		assert.Nil(t, m.GetSegment(ctx, 1))
		assert.NotNil(t, m.GetSegment(ctx, 2))
		cm.mu.Lock()
		require.Len(t, cm.prefixCalls, 1)
		assert.ElementsMatch(t, []string{"root/insert_log/1/1/1/", "root/insert_log/1/1/2/"}, cm.prefixCalls[0])
		cm.mu.Unlock()
	})

	t.Run("V3 prefix results are matched by prefix", func(t *testing.T) {
		setDroppedSegmentBatchSize(t, 100)
		ctx := context.Background()
		m, dm, err := newMemoryMetaWithSnapshot(t)
		require.NoError(t, err)
		m.DataViewManager = nil
		dm.SnapshotMeta = nil
		m.CompactionProtection = nil
		for segmentID := int64(1); segmentID <= 2; segmentID++ {
			require.NoError(t, m.AddSegment(ctx, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID:             segmentID,
				CollectionID:   1,
				PartitionID:    1,
				InsertChannel:  "channel-1",
				State:          commonpb.SegmentState_Dropped,
				DroppedAt:      uint64(time.Now().Add(-time.Hour).UnixNano()),
				StorageVersion: storage.StorageV3,
				ManifestPath:   packed.MarshalManifestPath(fmt.Sprintf("root/insert_log/1/1/%d", segmentID), 1),
			}}))
		}
		cm := &batchRemoveChunkManager{
			rootPath: "root",
			removePrefix: func(_ context.Context, _ []string) []storage.RemovePrefixResult {
				return []storage.RemovePrefixResult{
					{Prefix: "root/insert_log/1/1/2/"},
					{Prefix: "root/insert_log/1/1/1/", Err: merr.WrapErrIoTooManyRequests("root/insert_log/1/1/1/", errors.New("throttled"))},
				}
			},
		}
		gc := NewGarbageCollector(m, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{Cli: cm, DropTolerance: 0, StreamingNodeCatalog: checkpointCatalog}))
		defer gc.Close()

		gc.RecycleDroppedSegments(ctx, nil)

		assert.NotNil(t, m.GetSegment(ctx, 1))
		assert.Nil(t, m.GetSegment(ctx, 2))
	})

	t.Run("SegmentIndex batch failure keeps segment metadata", func(t *testing.T) {
		setDroppedSegmentBatchSize(t, 100)
		ctx := context.Background()
		m, dm := setupDroppedSegmentBatchFixture(t, 1)
		dm.IndexMeta.Catalog = &batchIndexDeleteCatalog{
			DataCoordCatalog: dm.IndexMeta.Catalog,
			dropSegmentIndexes: func(context.Context, []*model.SegmentIndex) error {
				return errors.New("segment index batch failed")
			},
		}
		cm := &batchRemoveChunkManager{rootPath: "root"}
		gc := NewGarbageCollector(m, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{Cli: cm, DropTolerance: 0, StreamingNodeCatalog: checkpointCatalog}))
		defer gc.Close()

		gc.RecycleDroppedSegments(ctx, nil)

		assert.NotNil(t, m.GetSegment(ctx, 1))
		assert.Len(t, dm.IndexMeta.GetAllSegmentIndexes(1), 1)
	})

	t.Run("segment metadata batch failure keeps final marker", func(t *testing.T) {
		setDroppedSegmentBatchSize(t, 100)
		ctx := context.Background()
		m, dm := setupDroppedSegmentBatchFixture(t, 1)
		m.Catalog = &failingDropSegmentCatalog{
			DataCoordCatalog: m.Catalog,
			err:              errors.New("segment batch failed"),
		}
		cm := &batchRemoveChunkManager{rootPath: "root"}
		gc := NewGarbageCollector(m, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{Cli: cm, DropTolerance: 0, StreamingNodeCatalog: checkpointCatalog}))
		defer gc.Close()

		gc.RecycleDroppedSegments(ctx, nil)

		assert.NotNil(t, m.GetSegment(ctx, 1))
		assert.Empty(t, dm.IndexMeta.GetAllSegmentIndexes(1))
	})
}

func TestGarbageCollector_recycleDroppedSegmentsBatchFallbackPauseBarrier(t *testing.T) {
	checkpointCatalog := MockCheckpointCatalog(t, 1000)
	ctx := context.Background()
	m, dm, err := newMemoryMetaWithSnapshot(t)
	require.NoError(t, err)
	m.DataViewManager = nil
	dm.SnapshotMeta = nil
	m.CompactionProtection = nil

	for segmentID := int64(1); segmentID <= 2; segmentID++ {
		require.NoError(t, m.AddSegment(ctx, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:            segmentID,
			CollectionID:  1,
			PartitionID:   1,
			InsertChannel: "channel-1",
			State:         commonpb.SegmentState_Dropped,
			DroppedAt:     uint64(time.Now().Add(-time.Hour).UnixNano()),
			Binlogs: []*datapb.FieldBinlog{{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{{LogID: segmentID}},
			}},
		}}))
	}

	started := make(chan struct{}, 2)
	release := make(chan struct{})
	chunkManager := mocks.NewChunkManager(t)
	chunkManager.EXPECT().RootPath().Return("root").Maybe()
	chunkManager.EXPECT().Remove(mock.Anything, mock.Anything).RunAndReturn(
		func(context.Context, string) error {
			started <- struct{}{}
			<-release
			return nil
		},
	).Times(2)

	gc := NewGarbageCollector(m, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{
		StreamingNodeCatalog: checkpointCatalog,
		Cli:                  chunkManager,
		DropTolerance:        0,
	}))
	defer gc.Close()
	signal := make(chan gcCmd)
	gcDone := make(chan struct{})
	go func() {
		defer close(gcDone)
		gc.RecycleDroppedSegments(ctx, signal)
	}()

	<-started
	<-started
	pauseDone := make(chan error)
	go func() {
		signal <- gcCmd{done: pauseDone}
	}()
	select {
	case <-pauseDone:
		t.Fatal("pause was acknowledged while dropped-segment deletion was still in flight")
	case <-time.After(20 * time.Millisecond):
	}

	close(release)
	select {
	case <-pauseDone:
	case <-time.After(time.Second):
		t.Fatal("pause was not acknowledged after the concurrent batch completed")
	}
	select {
	case <-gcDone:
	case <-time.After(time.Second):
		t.Fatal("dropped-segment GC did not finish")
	}
}

func TestGarbageCollector_recycleDroppedSegmentsBatchPauseBarrier(t *testing.T) {
	checkpointCatalog := MockCheckpointCatalog(t, 1000)
	setDroppedSegmentBatchSize(t, 100)
	ctx := context.Background()
	m, dm, err := newMemoryMetaWithSnapshot(t)
	require.NoError(t, err)
	m.DataViewManager = nil
	dm.SnapshotMeta = nil
	m.CompactionProtection = nil

	for segmentID := int64(1); segmentID <= 2; segmentID++ {
		require.NoError(t, m.AddSegment(ctx, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:            segmentID,
			CollectionID:  1,
			PartitionID:   1,
			InsertChannel: "channel-1",
			State:         commonpb.SegmentState_Dropped,
			DroppedAt:     uint64(time.Now().Add(-time.Hour).UnixNano()),
			Binlogs: []*datapb.FieldBinlog{{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{{LogID: segmentID}},
			}},
		}}))
	}

	started := make(chan struct{})
	release := make(chan struct{})
	cm := &batchRemoveChunkManager{
		rootPath: "root",
		remove: func(_ context.Context, paths []string) []storage.RemoveResult {
			close(started)
			<-release
			results := make([]storage.RemoveResult, len(paths))
			for i, filePath := range paths {
				results[i].Path = filePath
			}
			return results
		},
	}
	gc := NewGarbageCollector(m, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{Cli: cm, DropTolerance: 0, StreamingNodeCatalog: checkpointCatalog}))
	defer gc.Close()
	signal := make(chan gcCmd)
	gcDone := make(chan struct{})
	go func() {
		defer close(gcDone)
		gc.RecycleDroppedSegments(ctx, signal)
	}()

	<-started
	pauseDone := make(chan error)
	go func() {
		signal <- gcCmd{done: pauseDone}
	}()
	select {
	case <-pauseDone:
		t.Fatal("pause was acknowledged while a dropped-segment storage batch was in flight")
	case <-time.After(20 * time.Millisecond):
	}

	close(release)
	select {
	case <-pauseDone:
	case <-time.After(time.Second):
		t.Fatal("pause was not acknowledged after the dropped-segment storage batch completed")
	}
	select {
	case <-gcDone:
	case <-time.After(time.Second):
		t.Fatal("dropped-segment batch GC did not finish")
	}
}

func TestGarbageCollector_recycleDroppedSegmentsBatch_CancellationShortCircuit(t *testing.T) {
	checkpointCatalog := MockCheckpointCatalog(t, 1000)
	t.Run("cancel before files step", func(t *testing.T) {
		m, dm, segment, _, _ := setupDroppedSegmentWithIndexForGC(t)
		cli := mocks.NewChunkManager(t)
		cli.EXPECT().RootPath().Return("root").Maybe()
		cli.EXPECT().Remove(mock.Anything, mock.Anything).Return(nil).Maybe()
		gc := NewGarbageCollector(m, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{
			StreamingNodeCatalog: checkpointCatalog,
			Cli:                  cli,
			DropTolerance:        0,
		}))
		defer gc.Close()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		gc.RecycleDroppedSegments(ctx, nil)

		cli.AssertNotCalled(t, "Remove", mock.Anything, mock.Anything)
		assert.NotNil(t, m.GetSegment(context.Background(), segment.ID))
	})

	t.Run("cancel between files and index meta", func(t *testing.T) {
		m, dm, segment, _, _ := setupDroppedSegmentWithIndexForGC(t)
		ctx, cancel := context.WithCancel(context.Background())
		cli := &batchRemoveChunkManager{
			rootPath: "root",
			remove: func(_ context.Context, filePaths []string) []storage.RemoveResult {
				cancel()
				results := make([]storage.RemoveResult, len(filePaths))
				for i, filePath := range filePaths {
					results[i].Path = filePath
				}
				return results
			},
		}
		gc := NewGarbageCollector(m, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{Cli: cli, DropTolerance: 0, StreamingNodeCatalog: checkpointCatalog}))
		defer gc.Close()

		gc.RecycleDroppedSegments(ctx, nil)

		assert.Len(t, dm.IndexMeta.GetAllSegmentIndexes(segment.ID), 1)
		assert.NotNil(t, m.GetSegment(context.Background(), segment.ID))
	})
}

func TestGarbageCollector_recycleDroppedSegmentsBatch_CtxCanceledBeforeSegmentMeta(t *testing.T) {
	checkpointCatalog := MockCheckpointCatalog(t, 1000)
	m, dm, segment, _, _ := setupDroppedSegmentWithIndexForGC(t)
	ctx, cancel := context.WithCancel(context.Background())
	var indexMetaCalls atomic.Int32
	dm.IndexMeta.Catalog = &batchIndexDeleteCatalog{
		DataCoordCatalog: dm.IndexMeta.Catalog,
		dropSegmentIndexes: func(context.Context, []*model.SegmentIndex) error {
			indexMetaCalls.Inc()
			cancel()
			return nil
		},
	}
	cli := &batchRemoveChunkManager{rootPath: "root"}
	gc := NewGarbageCollector(m, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{Cli: cli, DropTolerance: 0, StreamingNodeCatalog: checkpointCatalog}))
	defer gc.Close()

	gc.RecycleDroppedSegments(ctx, nil)

	assert.Equal(t, int32(1), indexMetaCalls.Load())
	assert.Empty(t, dm.IndexMeta.GetAllSegmentIndexes(segment.ID))
	assert.NotNil(t, m.GetSegment(context.Background(), segment.ID))
}

func successfulRemoveResults(paths []string) []storage.RemoveResult {
	results := make([]storage.RemoveResult, len(paths))
	for i, path := range paths {
		results[i].Path = path
	}
	return results
}
