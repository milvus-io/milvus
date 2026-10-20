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

//go:build test

package datacoord

import (
	"context"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	uberatomic "go.uber.org/atomic"
	"google.golang.org/grpc"
)

func getFieldBinlogIDs(fieldID int64, logIDs ...int64) *datapb.FieldBinlog {
	l := &datapb.FieldBinlog{
		FieldID: fieldID,
		Binlogs: make([]*datapb.Binlog, 0, len(logIDs)),
	}
	for _, id := range logIDs {
		l.Binlogs = append(l.Binlogs, &datapb.Binlog{LogID: id})
	}
	err := binlog.CompressFieldBinlogs([]*datapb.FieldBinlog{l})
	if err != nil {
		panic(err)
	}
	return l
}

const MB = 1024 * 1024

// mockBroadcastAPIImpl is a mock implementation for broadcaster.BroadcastAPI interface
// This implementation has configurable behavior and uses enough code to be patchable by mockey
type mockBroadcastAPIImpl struct {
	broadcastResult *types.BroadcastAppendResult
	broadcastErr    error
	closeCalled     atomic.Bool
	// capturedMsg is the last message handed to Broadcast, for assertions on what the
	// caller actually put on the wire.
	capturedMsg message.BroadcastMutableMessage
}

func newMockBroadcastAPIImpl() *mockBroadcastAPIImpl {
	// Initialize with default success result
	mock := &mockBroadcastAPIImpl{
		broadcastResult: &types.BroadcastAppendResult{
			BroadcastID: 12345,
		},
		broadcastErr: nil,
	}
	mock.closeCalled.Store(false)
	return mock
}

func (m *mockBroadcastAPIImpl) Broadcast(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
	// Add operations to ensure the function is long enough for mockey
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	if msg == nil {
		return nil, errors.New("message is nil")
	}
	m.capturedMsg = msg
	if m.broadcastErr != nil {
		return nil, m.broadcastErr
	}
	if m.broadcastResult != nil {
		return m.broadcastResult, nil
	}
	return &types.BroadcastAppendResult{BroadcastID: 0}, nil
}

func (m *mockBroadcastAPIImpl) Close() {
	// Add operations to ensure the function is long enough for mockey
	m.closeCalled.Store(true)
	if m.closeCalled.Load() {
		// Already closed, do nothing
		return
	}
}

// captureBroadcastAPI is a BroadcastAPI mock that records the message passed
// to Broadcast so a test can assert its broadcast target vchannels.
type captureBroadcastAPI struct {
	captured message.BroadcastMutableMessage
}

func (c *captureBroadcastAPI) Broadcast(_ context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
	c.captured = msg
	return &types.BroadcastAppendResult{BroadcastID: 1}, nil
}

func (c *captureBroadcastAPI) Close() {}

// withSegmentIndexManifestWrites flips
// dataCoord.index.writeSegmentIndexToManifest for one test and restores it
// afterwards. enabled=true publishes completed StorageV3 artifacts to segment
// manifests; enabled=false is the legacy etcd-only behavior.
func withSegmentIndexManifestWrites(t *testing.T, enabled bool) {
	t.Helper()
	key := paramtable.Get().DataCoordCfg.WriteSegmentIndexToManifest.Key
	previous := paramtable.Get().DataCoordCfg.WriteSegmentIndexToManifest.GetValue()
	paramtable.Get().Save(key, strconv.FormatBool(enabled))
	t.Cleanup(func() { paramtable.Get().Save(key, previous) })
}

// fakeManifestStore is a stateful stand-in for the manifest objects: what a
// commit writes is exactly what a later read returns, so a reload round-trips
// through the manifest rather than through a fixture. Without that the "index
// survives a restart" claim would only be testing the projection helper.
type fakeManifestStore struct {
	mu          sync.Mutex
	revisions   map[string][]packed.ManifestIndexInfo
	failReads   bool
	readCount   int
	commitCount int
}

// failReadsFrom makes every subsequent manifest read fail, modeling a
// transient object-store error at the next boot. It toggles state inside the
// existing patch rather than re-mocking, which mockey forbids.
func (s *fakeManifestStore) failReadsFrom() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failReads = true
}

// Adapt the existing fake reader to callback delivery. Production recovery
// submits directly to the native executor; only this test stub uses a goroutine.
func mockManifestIndexSubmissions(t *testing.T) {
	t.Helper()
	mock := mockey.Mock(packed.SubmitManifestIndexInfos).To(func(ctx context.Context, io *packed.ManifestIOContext, manifest string, config *indexpb.StorageConfig, complete func([]packed.ManifestIndexInfo, error)) error {
		go func() {
			entries, err := packed.GetManifestIndexInfosAsync(ctx, io, manifest, config)
			complete(entries, err)
		}()
		return nil
	}).Build()
	t.Cleanup(func() { mock.UnPatch() })
}

func mockManifestUpdateResult(manifestPath string, updates *packed.ManifestUpdates) packed.ManifestUpdateResult {
	result := packed.ManifestUpdateResult{ManifestPath: manifestPath}
	if updates != nil && (len(updates.Indexes) > 0 || len(updates.DropIndexes) > 0) {
		value := len(updates.Indexes) > 0
		result.HasIndexes = &value
	}
	return result
}

// Adapt a mocked blocking commit to callback delivery. Install only alongside a
// CommitManifestUpdatesWithResultAsync mock, since the real wrapper calls the submit API.
func mockManifestUpdateSubmissions(t *testing.T) {
	t.Helper()
	mock := mockey.Mock(packed.SubmitManifestUpdates).To(func(ctx context.Context, io *packed.ManifestIOContext, base string, version int64, config *indexpb.StorageConfig, updates *packed.ManifestUpdates, complete func(packed.ManifestUpdateResult, error)) error {
		go func() {
			manifestPath, err := packed.CommitManifestUpdatesWithResultAsync(ctx, io, base, version, config, updates)
			complete(manifestPath, err)
		}()
		return nil
	}).Build()
	t.Cleanup(func() { mock.UnPatch() })
}

func newFakeManifestStore(t *testing.T) *fakeManifestStore {
	t.Helper()
	mockManifestUpdateSubmissions(t)
	mockManifestIndexSubmissions(t)
	s := &fakeManifestStore{revisions: make(map[string][]packed.ManifestIndexInfo)}

	commit := mockey.Mock(packed.CommitManifestUpdatesWithResultAsync).To(
		func(_ context.Context, _ *packed.ManifestIOContext, basePath string, version int64, _ *indexpb.StorageConfig, updates *packed.ManifestUpdates) (packed.ManifestUpdateResult, error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.commitCount++
			// A revision is a full snapshot resolved onto the one it opens at.
			current := s.revisions[packed.MarshalManifestPath(basePath, version)]
			next := make([]packed.ManifestIndexInfo, 0, len(current)+len(updates.Indexes))
			for _, entry := range current {
				dropped := false
				for _, drop := range updates.DropIndexes {
					if drop.IndexID == entry.IndexID {
						dropped = true
						break
					}
				}
				if !dropped {
					next = append(next, entry)
				}
			}
			next = append(next, updates.Indexes...)
			published := packed.MarshalManifestPath(basePath, version+1)
			s.revisions[published] = next
			hasIndexes := len(next) > 0
			return packed.ManifestUpdateResult{ManifestPath: published, HasIndexes: &hasIndexes}, nil
		}).Build()
	t.Cleanup(func() { commit.UnPatch() })

	read := mockey.Mock(packed.GetManifestIndexInfosAsync).To(
		func(_ context.Context, _ *packed.ManifestIOContext, manifestPath string, _ *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.readCount++
			if s.failReads {
				return nil, merr.WrapErrIoFailedReason("throttled")
			}
			entries := append([]packed.ManifestIndexInfo(nil), s.revisions[manifestPath]...)
			basePath, _, err := packed.UnmarshalManifestPath(manifestPath)
			if err != nil {
				return nil, err
			}
			for i := range entries {
				if strings.HasPrefix(entries[i].Path, "..") {
					entries[i].Path = path.Join(basePath, "_index", entries[i].Path)
				}
			}
			return entries, nil
		}).Build()
	t.Cleanup(func() { read.UnPatch() })
	return s
}

const restartCollID = UniqueID(300)

const restartSegID = UniqueID(8001)

const restartFieldID = UniqueID(101)

const restartIndexID = UniqueID(800)

const restartBuildID = UniqueID(8100)

type metaMemoryKV struct {
	memkv.MemoryKV
}

func NewMetaMemoryKV() *metaMemoryKV {
	return &metaMemoryKV{MemoryKV: *memkv.NewMemoryKV()}
}

func (mm *metaMemoryKV) WalkWithPrefix(ctx context.Context, prefix string, paginationSize int, fn func([]byte, []byte) error) error {
	keys, values, err := mm.LoadWithPrefix(context.TODO(), prefix)
	if err != nil {
		return err
	}

	for i, k := range keys {
		if err := fn([]byte(k), []byte(values[i])); err != nil {
			return err
		}
	}
	return nil
}

func (mm *metaMemoryKV) GetPath(key string) string {
	panic("implement me")
}

func (mm *metaMemoryKV) Watch(ctx context.Context, key string) clientv3.WatchChan {
	panic("implement me")
}

func (mm *metaMemoryKV) WatchWithPrefix(ctx context.Context, key string) clientv3.WatchChan {
	panic("implement me")
}

func (mm *metaMemoryKV) WatchWithRevision(ctx context.Context, key string, revision int64) clientv3.WatchChan {
	panic("implement me")
}

func (mm *metaMemoryKV) CompareVersionAndSwap(ctx context.Context, key string, version int64, target string) (bool, error) {
	panic("implement me")
}

// noDomainMetas is a DomainMetaLoader that builds no domain metas.
func noDomainMetas(context.Context, []int64) (SegmentIndexCatalog, CompactionProtection, error) {
	return nil, nil, nil
}

// newMetaWithLoader builds a meta over catalog and cm and loads the domain metas with the loader that newLoader returns for them.
func newMetaWithLoader(t *testing.T, catalog metastore.DataCoordCatalog, cm storage.ChunkManager, newLoader func(metastore.DataCoordCatalog, storage.ChunkManager) DomainMetaLoader) (*meta, error) {
	broker := broker.NewMockBroker(t)
	broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)
	m, err := newMeta(context.TODO(), catalog, cm, metacache.NewMetaStore(catalog), broker, newLoader(catalog, cm))
	if m != nil {
		t.Cleanup(m.closeManifestCommitExecutor)
	}
	return m, err
}

// newMemoryMetaWithLoader builds a meta over an in-memory catalog and loads the domain metas with the loader that newLoader returns for that catalog.
func newMemoryMetaWithLoader(t *testing.T, newLoader func(metastore.DataCoordCatalog, storage.ChunkManager) DomainMetaLoader) (*meta, error) {
	return newMetaWithLoader(t, datacoord.NewCatalog(NewMetaMemoryKV(), "", ""), nil, newLoader)
}

// withoutDomainMetas returns noDomainMetas for any catalog and chunk manager.
func withoutDomainMetas(metastore.DataCoordCatalog, storage.ChunkManager) DomainMetaLoader {
	return noDomainMetas
}

// newMemoryMeta builds a meta over an in-memory catalog without domain metas.
func newMemoryMeta(t *testing.T) (*meta, error) {
	return newMemoryMetaWithLoader(t, withoutDomainMetas)
}

// bootMetaWithLoader runs the real newMeta against a caller-owned catalog that holds collectionID,
// so two calls with the same catalog model a DataCoord restart. newLoader returns the loader of the domain metas.
func bootMetaWithLoader(t *testing.T, catalog metastore.DataCoordCatalog, collectionID int64, newLoader func(metastore.DataCoordCatalog, storage.ChunkManager) DomainMetaLoader) *meta {
	t.Helper()
	b := broker.NewMockBroker(t)
	b.EXPECT().ShowCollectionIDs(mock.Anything).Return(&rootcoordpb.ShowCollectionIDsResponse{
		Status:        merr.Success(),
		DbCollections: []*rootcoordpb.DBCollections{{DbName: "default", CollectionIDs: []int64{collectionID}}},
	}, nil)
	cm := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test-restart"))
	m, err := newMeta(context.TODO(), catalog, cm, metacache.NewMetaStore(catalog), b, newLoader(catalog, cm))
	require.NoError(t, err)
	t.Cleanup(m.closeManifestCommitExecutor)
	return m
}

func newMockAllocator(t *testing.T) *allocator.MockAllocator {
	counter := uberatomic.NewInt64(0)
	mockAllocator := allocator.NewMockAllocator(t)
	mockAllocator.EXPECT().AllocID(mock.Anything).RunAndReturn(func(ctx context.Context) (int64, error) {
		return counter.Inc(), nil
	}).Maybe()
	mockAllocator.EXPECT().AllocTimestamp(mock.Anything).RunAndReturn(func(ctx context.Context) (uint64, error) {
		return uint64(counter.Inc()), nil
	}).Maybe()
	mockAllocator.EXPECT().AllocN(mock.Anything).RunAndReturn(func(i int64) (int64, int64, error) {
		v := counter.Add(i)
		return v, v + i, nil
	}).Maybe()
	return mockAllocator
}

func newTestSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name:        "test",
		Description: "schema for test used",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "field1", IsPrimaryKey: false, Description: "field no.1", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "100"}}},
			{FieldID: 2, Name: "field2", IsPrimaryKey: false, Description: "field no.2", DataType: schemapb.DataType_FloatVector},
		},
	}
}

var collID = UniqueID(100)

var partID = UniqueID(200)

var indexID = UniqueID(300)

var fieldID = UniqueID(400)

var indexName = "_default_idx"

var segID = UniqueID(500)

var buildID = UniqueID(600)

var nodeID = UniqueID(700)

type legacyLocalPackedFixture struct {
	root             string
	legacyRoot       string
	physicalManifest string
	segment          *datapb.SegmentInfo
	cm               *storage.LocalChunkManager
	cfg              *indexpb.StorageConfig
	pks              []int64
	texts            []string
}

// The displaced variant models complete keys passed to the old double-rooted
// filesystem. Both variants use real writers and unchanged relative manifest
// references; only the physical namespace and catalog base differ.
func newLocalPackedFixture(t *testing.T, displaced bool) *legacyLocalPackedFixture {
	t.Helper()
	setLocalManifestLegacyPrefix(t, "files")
	root := t.TempDir()
	params := paramtable.Get()
	for _, setting := range []struct{ key, value, original string }{
		{params.CommonCfg.StorageType.Key, "local", params.CommonCfg.StorageType.GetValue()},
		{params.LocalStorageCfg.Path.Key, root, params.LocalStorageCfg.Path.GetValue()},
	} {
		require.NoError(t, params.Save(setting.key, setting.value))
		t.Cleanup(func() { require.NoError(t, params.Save(setting.key, setting.original)) })
	}
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: root}
	legacyRoot := filepath.Join(root, "files")
	catalogBase := "files/insert_log/1/2/3"
	if displaced {
		legacyRoot = filepath.Join(root, strings.TrimPrefix(root, string(filepath.Separator)))
		catalogBase = filepath.Join(root, "insert_log/1/2/3")
	}
	base := filepath.Join(legacyRoot, "insert_log/1/2/3")
	pks := []int64{11, 22, 33}
	texts := []string{strings.Repeat("first lob ", 32), strings.Repeat("second lob ", 32), strings.Repeat("third lob ", 32)}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "100", Type: arrow.PrimitiveTypes.Int64, Metadata: arrow.NewMetadata([]string{packed.ArrowFieldIdMetadataKey}, []string{"100"})},
		{Name: "101", Type: arrow.BinaryTypes.String, Metadata: arrow.NewMetadata([]string{packed.ArrowFieldIdMetadataKey}, []string{"101"})},
	}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	builder.Field(0).(*array.Int64Builder).AppendValues(pks, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues(texts, nil)
	record := builder.NewRecord()
	defer record.Release()
	writer, err := packed.NewFFISegmentWriter(schema, &packed.SegmentWriterConfig{
		SegmentPath:  base,
		WriterFormat: "parquet",
		TextColumns: []packed.TextColumnConfig{{
			FieldID: 101, LobBasePath: filepath.Join(legacyRoot, "insert_log/1/2/lobs/101"),
			InlineThreshold: 1, MaxLobFileBytes: 1 << 20, FlushThresholdBytes: 1 << 20,
		}},
	}, cfg, nil)
	require.NoError(t, err)
	closed := false
	t.Cleanup(func() {
		if !closed {
			out, closeErr := writer.Close()
			if out != nil {
				out.Destroy()
			}
			require.NoError(t, closeErr)
		}
	})
	require.NoError(t, writer.Write(record))
	output, err := writer.Close()
	closed = true
	require.NoError(t, err)
	defer output.Destroy()
	statsWriter := &storage.StatsWriter{}
	require.NoError(t, statsWriter.GenerateByData(100, schemapb.DataType_Int64, &storage.Int64FieldData{Data: pks}))
	statsPath := filepath.Join(base, "_stats/bloom_filter.100/42")
	require.NoError(t, packed.WriteFile(cfg, statsPath, statsWriter.GetBuffer()))
	physicalManifest, err := packed.CommitManifestUpdates(base, packed.ManifestEarliest, cfg, &packed.ManifestUpdates{
		NewFiles: output,
		Stats:    []packed.StatEntry{{Key: "bloom_filter.100", Files: []string{statsPath}, Metadata: map[string]string{"memory_size": "24"}}},
	})
	require.NoError(t, err)
	_, version, err := packed.UnmarshalManifestPath(physicalManifest)
	require.NoError(t, err)
	return &legacyLocalPackedFixture{
		root: root, legacyRoot: legacyRoot, physicalManifest: physicalManifest,
		segment: &datapb.SegmentInfo{
			ID: 3, CollectionID: 1, PartitionID: 2, State: commonpb.SegmentState_Flushed,
			StorageVersion: storage.StorageV3, NumOfRows: int64(len(pks)),
			ManifestPath: packed.MarshalManifestPath(catalogBase, version),
		},
		cm: storage.NewLocalChunkManager(objectstorage.RootPath(root)), cfg: cfg, pks: pks, texts: texts,
	}
}

func (f *legacyLocalPackedFixture) assertNoCanonicalLayout(t *testing.T) {
	t.Helper()
	_, err := os.Stat(filepath.Join(f.root, "insert_log"))
	require.ErrorIs(t, err, os.ErrNotExist, "legacy V3 compatibility must not create a canonical insert_log tree")
}

// Read the old segment's real data, stats and LOB payload in place. This does
// not exercise compaction between the legacy and canonical storage namespaces.
func (f *legacyLocalPackedFixture) assertReadable(t *testing.T, manifest, expectedRoot string) {
	t.Helper()
	base, _, err := packed.UnmarshalManifestPath(manifest)
	require.NoError(t, err)
	fragments, err := packed.ReadFragmentsFromManifest(manifest, f.cfg, []string{"100"})
	require.NoError(t, err)
	require.NotEmpty(t, fragments)
	for _, fragment := range fragments {
		require.True(t, strings.HasPrefix(fragment.FilePath, filepath.Join(base, "_data")+"/"), fragment.FilePath)
	}
	pkSchema := arrow.NewSchema([]arrow.Field{{Name: "100", Type: arrow.PrimitiveTypes.Int64}}, nil)
	reader, err := packed.NewFFIPackedReader(manifest, pkSchema, []string{"100"}, 1<<20, f.cfg, nil, packed.ExternalReaderContext{})
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	var gotPKs []int64
	for {
		record, readErr := reader.ReadNext()
		if readErr == io.EOF {
			break
		}
		require.NoError(t, readErr)
		gotPKs = append(gotPKs, record.Column(0).(*array.Int64).Int64Values()...)
	}
	require.Equal(t, f.pks, gotPKs)

	statsPaths, err := packed.NewStatsResolver(manifest, f.cfg).BloomFilterPaths(100)
	require.NoError(t, err)
	require.Equal(t, []string{filepath.Join(base, "_stats/bloom_filter.100/42")}, statsPaths)
	statsBytes, err := f.cm.Read(context.Background(), statsPaths[0])
	require.NoError(t, err)
	statsReader := &storage.StatsReader{}
	statsReader.SetBuffer(statsBytes)
	stats, err := statsReader.GetPrimaryKeyStats()
	require.NoError(t, err)
	assert.Equal(t, int64(100), stats.FieldID)
	assert.True(t, stats.MinPk.EQ(storage.NewInt64PrimaryKey(f.pks[0])))
	assert.True(t, stats.MaxPk.EQ(storage.NewInt64PrimaryKey(f.pks[len(f.pks)-1])))

	lobs, err := packed.GetManifestLobFiles(manifest, f.cfg)
	require.NoError(t, err)
	require.Len(t, lobs, 1, "the TEXT values must be stored out of line in a real Vortex file")
	lob := lobs[0]
	require.True(t, strings.HasPrefix(lob.Path, filepath.Join(expectedRoot, "insert_log/1/2/lobs/101/_data")+"/"), lob.Path)
	require.Equal(t, int64(101), lob.FieldID)
	require.Equal(t, int64(len(f.texts)), lob.TotalRows)
	lobSchema := arrow.NewSchema([]arrow.Field{{Name: "text_data", Type: arrow.BinaryTypes.String}}, nil)
	lobReader, err := packed.NewFFIPackedReaderWithFragments([]string{"text_data"}, "vortex", []packed.Fragment{{
		FilePath: lob.Path, StartRow: 0, EndRow: lob.TotalRows, RowCount: lob.TotalRows,
	}}, lobSchema, []string{"text_data"}, 1<<20, f.cfg, nil, packed.ExternalReaderContext{})
	require.NoError(t, err)
	defer func() { require.NoError(t, lobReader.Close()) }()
	var gotTexts []string
	for {
		record, readErr := lobReader.ReadNext()
		if readErr == io.EOF {
			break
		}
		require.NoError(t, readErr)
		column := record.Column(0).(*array.String)
		for i := 0; i < column.Len(); i++ {
			// ReadNext releases the preceding Arrow batch. Copy strings while
			// its backing buffer is still alive, including before the EOF read.
			gotTexts = append(gotTexts, strings.Clone(column.Value(i)))
		}
	}
	require.Equal(t, f.texts, gotTexts)
}

// Keep every fixture read beneath an opened root. The names returned by
// WalkDir are root-relative; they are never reopened as unchecked absolute
// filesystem paths.
func readLocalPackedFixtureFiles(t *testing.T, directory string) map[string][]byte {
	t.Helper()
	root, err := os.OpenRoot(directory)
	require.NoError(t, err)
	defer func() { require.NoError(t, root.Close()) }()
	rootFS := root.FS()
	files := make(map[string][]byte)
	require.NoError(t, fs.WalkDir(rootFS, ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		contents, err := fs.ReadFile(rootFS, name)
		if err == nil {
			files[name] = contents
		}
		return err
	}))
	return files
}

func setLocalManifestLegacyPrefix(t *testing.T, prefix string) {
	t.Helper()
	paramtable.Init()
	params := paramtable.Get()
	key := params.MinioCfg.RootPath.Key
	original := params.MinioCfg.RootPath.GetValue()
	require.NoError(t, params.Save(key, prefix))
	t.Cleanup(func() { require.NoError(t, params.Save(key, original)) })
}

func newLocalManifestTestMeta(t *testing.T, catalog *catalogmocks.DataCoordCatalog, cm storage.ChunkManager) *meta {
	t.Helper()
	executor := newManifestCommitExecutor(1)
	t.Cleanup(executor.close)
	store := metacache.NewMetaStore(catalog)
	return &meta{
		manifestCommitExecutor: executor,
		ctx:                    context.Background(),
		catalog:                catalog,
		metaStore:              store,
		chunkManager:           cm,
		segments:               NewSegmentsInfo(store),
		channelSync:            newChannelSync(),
	}
}

func loadAndNormalize(ctx context.Context, mt *meta, collectionIDs []int64) error {
	return mt.metaStore.LoadFromCatalog(ctx, collectionIDs, mt.segmentLoadNormalizer())
}

func assertLocalManifestCatalogReadOnly(t *testing.T, catalog *catalogmocks.DataCoordCatalog) {
	t.Helper()
	catalog.AssertNotCalled(t, "AlterSegments", mock.Anything, mock.Anything)
	for _, call := range catalog.Calls {
		assert.Contains(t, []string{"ListSegments", "ListChannelCheckpoint"}, call.Method,
			"loading local manifests must not call a catalog mutation")
	}
}

type mockMixCoord struct {
	rootcoordpb.UnimplementedRootCoordServer
	state commonpb.StateCode
	cnt   uberatomic.Int64
}

func (m *mockMixCoord) GetGcStatus(context.Context) (*datapb.GetGcStatusResponse, error) {
	return &datapb.GetGcStatusResponse{}, nil
}

func (m *mockMixCoord) BatchUpdateManifest(context.Context, *datapb.BatchUpdateManifestRequest) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (m *mockMixCoord) CommitBackfillResult(context.Context, *datapb.CommitBackfillResultRequest) (*datapb.CommitBackfillResultResponse, error) {
	return &datapb.CommitBackfillResultResponse{Status: merr.Success()}, nil
}

func (m *mockMixCoord) DescribeDatabase(ctx context.Context, in *rootcoordpb.DescribeDatabaseRequest) (*rootcoordpb.DescribeDatabaseResponse, error) {
	return &rootcoordpb.DescribeDatabaseResponse{
		Status:           merr.Success(),
		DbID:             1,
		DbName:           "default",
		CreatedTimestamp: 1,
	}, nil
}

func (m *mockMixCoord) GetRLSMetadata(context.Context, *rootcoordpb.GetRLSMetadataRequest) (*rootcoordpb.GetRLSMetadataResponse, error) {
	return &rootcoordpb.GetRLSMetadataResponse{Status: merr.Success()}, nil
}

func (m *mockMixCoord) Close() error {
	// TODO implement me
	panic("implement me")
}

func (m *mockMixCoord) RenameCollection(ctx context.Context, req *milvuspb.RenameCollectionRequest) (*commonpb.Status, error) {
	// TODO implement me
	panic("implement me")
}

func (m *mockMixCoord) CheckHealth(ctx context.Context, req *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error) {
	panic("implement me")
}

func (m *mockMixCoord) CreateAlias(ctx context.Context, req *milvuspb.CreateAliasRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockMixCoord) DropAlias(ctx context.Context, req *milvuspb.DropAliasRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockMixCoord) AlterAlias(ctx context.Context, req *milvuspb.AlterAliasRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockMixCoord) DescribeAlias(ctx context.Context, req *milvuspb.DescribeAliasRequest) (*milvuspb.DescribeAliasResponse, error) {
	panic("implement me")
}

func (m *mockMixCoord) ListAliases(ctx context.Context, req *milvuspb.ListAliasesRequest) (*milvuspb.ListAliasesResponse, error) {
	panic("implement me")
}

func (m *mockMixCoord) AddCollectionField(ctx context.Context, req *milvuspb.AddCollectionFieldRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockMixCoord) AddCollectionStructField(ctx context.Context, req *milvuspb.AddCollectionStructFieldRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockMixCoord) GetQuotaMetrics(ctx context.Context, req *internalpb.GetQuotaMetricsRequest) (*internalpb.GetQuotaMetricsResponse, error) {
	panic("implement me")
}

func (m *mockMixCoord) GetFeatureUsage(ctx context.Context, req *internalpb.GetFeatureUsageRequest) (*internalpb.FeatureUsageReport, error) {
	panic("implement me")
}

func (m *mockMixCoord) ClearReadTaskQueue(ctx context.Context, req *internalpb.ClearReadTaskQueueRequest) (*internalpb.ClearReadTaskQueueResponse, error) {
	return &internalpb.ClearReadTaskQueueResponse{Status: merr.Success()}, nil
}

func (m *mockMixCoord) ListLoadedSegments(ctx context.Context, req *querypb.ListLoadedSegmentsRequest) (*querypb.ListLoadedSegmentsResponse, error) {
	return &querypb.ListLoadedSegmentsResponse{
		Status: merr.Success(),
	}, nil
}

func (m *mockMixCoord) FlushAll(ctx context.Context, req *datapb.FlushAllRequest) (*datapb.FlushAllResponse, error) {
	panic("implement me")
}

func (m *mockMixCoord) ComputePhraseMatchSlop(ctx context.Context, req *querypb.ComputePhraseMatchSlopRequest) (*querypb.ComputePhraseMatchSlopResponse, error) {
	panic("implement me")
}

func (m *mockMixCoord) RefreshExternalCollection(ctx context.Context, req *datapb.RefreshExternalCollectionRequest) (*datapb.RefreshExternalCollectionResponse, error) {
	return &datapb.RefreshExternalCollectionResponse{
		Status: merr.Success(),
	}, nil
}

func (m *mockMixCoord) GetRefreshExternalCollectionProgress(ctx context.Context, req *datapb.GetRefreshExternalCollectionProgressRequest) (*datapb.GetRefreshExternalCollectionProgressResponse, error) {
	return &datapb.GetRefreshExternalCollectionProgressResponse{
		Status: merr.Success(),
	}, nil
}

func (m *mockMixCoord) ListRefreshExternalCollectionJobs(ctx context.Context, req *datapb.ListRefreshExternalCollectionJobsRequest) (*datapb.ListRefreshExternalCollectionJobsResponse, error) {
	return &datapb.ListRefreshExternalCollectionJobsResponse{
		Status: merr.Success(),
	}, nil
}

func newMockMixCoord() *mockMixCoord {
	return &mockMixCoord{state: commonpb.StateCode_Healthy}
}

func (m *mockMixCoord) GetTimeTickChannel(ctx context.Context, req *internalpb.GetTimeTickChannelRequest) (*milvuspb.StringResponse, error) {
	return nil, nil
}

func (m *mockMixCoord) Stop() error {
	m.state = commonpb.StateCode_Abnormal
	return nil
}

func (m *mockMixCoord) Register() error {
	return nil
}

func (m *mockMixCoord) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error) {
	return &milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			NodeID:    0,
			Role:      "",
			StateCode: m.state,
			ExtraInfo: []*commonpb.KeyValuePair{},
		},
		SubcomponentStates: []*milvuspb.ComponentInfo{},
		Status:             merr.Success(),
	}, nil
}

func (h *mockMixCoord) SyncQcFileResource(ctx context.Context, resources []*internalpb.FileResourceInfo, verion uint64) error {
	return nil
}

func (m *mockMixCoord) SyncDcFileResource(ctx context.Context, resources []*internalpb.FileResourceInfo, version uint64) error {
	return nil
}

func (m *mockMixCoord) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	panic("not implemented") // TODO: Implement
}

// DDL request
func (m *mockMixCoord) CreateCollection(ctx context.Context, req *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) DropCollection(ctx context.Context, req *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) HasCollection(ctx context.Context, req *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) DescribeCollection(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	// return not exist
	if req.CollectionID == -1 {
		err := merr.WrapErrCollectionNotFound(req.GetCollectionID())
		return &milvuspb.DescribeCollectionResponse{
			Status: merr.Status(err),
		}, nil
	}
	return &milvuspb.DescribeCollectionResponse{
		Status: merr.Success(),
		Schema: &schemapb.CollectionSchema{
			Name: "test",
		},
		CollectionID:        1314,
		VirtualChannelNames: []string{"vchan1"},
	}, nil
}

func (m *mockMixCoord) DescribeCollectionInternal(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	return m.DescribeCollection(ctx, req)
}

func (m *mockMixCoord) ShowCollections(ctx context.Context, req *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	return &milvuspb.ShowCollectionsResponse{
		Status:          merr.Success(),
		CollectionNames: []string{"test"},
	}, nil
}

func (m *mockMixCoord) ShowCollectionIDs(ctx context.Context, req *rootcoordpb.ShowCollectionIDsRequest) (*rootcoordpb.ShowCollectionIDsResponse, error) {
	return &rootcoordpb.ShowCollectionIDsResponse{
		Status: merr.Success(),
	}, nil
}

func (m *mockMixCoord) CreateDatabase(ctx context.Context, in *milvuspb.CreateDatabaseRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) DropDatabase(ctx context.Context, in *milvuspb.DropDatabaseRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) ListDatabases(ctx context.Context, in *milvuspb.ListDatabasesRequest) (*milvuspb.ListDatabasesResponse, error) {
	return &milvuspb.ListDatabasesResponse{
		Status: merr.Success(),
	}, nil
}

func (m *mockMixCoord) AlterDatabase(ctx context.Context, in *rootcoordpb.AlterDatabaseRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) AlterCollection(ctx context.Context, request *milvuspb.AlterCollectionRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) AlterCollectionField(ctx context.Context, request *milvuspb.AlterCollectionFieldRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) AlterCollectionSchema(ctx context.Context, request *milvuspb.AlterCollectionSchemaRequest) (*milvuspb.AlterCollectionSchemaResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) AddCollectionFunction(ctx context.Context, request *milvuspb.AddCollectionFunctionRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) AlterCollectionFunction(ctx context.Context, request *milvuspb.AlterCollectionFunctionRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) DropCollectionFunction(ctx context.Context, request *milvuspb.DropCollectionFunctionRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) CreatePartition(ctx context.Context, req *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) CreatePartitionV2(ctx context.Context, req *milvuspb.CreatePartitionRequest) (*rootcoordpb.CreatePartitionResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) DropPartition(ctx context.Context, req *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) HasPartition(ctx context.Context, req *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) ShowPartitions(ctx context.Context, req *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	return &milvuspb.ShowPartitionsResponse{
		Status:         merr.Success(),
		PartitionNames: []string{"_default"},
		PartitionIDs:   []int64{0},
	}, nil
}

func (m *mockMixCoord) ShowPartitionsInternal(ctx context.Context, req *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	return m.ShowPartitions(ctx, req)
}

// global timestamp allocator
func (m *mockMixCoord) AllocTimestamp(ctx context.Context, req *rootcoordpb.AllocTimestampRequest) (*rootcoordpb.AllocTimestampResponse, error) {
	if m.state != commonpb.StateCode_Healthy {
		return &rootcoordpb.AllocTimestampResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError}}, nil
	}

	val := m.cnt.Add(int64(req.Count))
	phy := time.Now().UnixNano() / int64(time.Millisecond)
	ts := tsoutil.ComposeTS(phy, val)
	return &rootcoordpb.AllocTimestampResponse{
		Status:    merr.Success(),
		Timestamp: ts,
		Count:     req.Count,
	}, nil
}

func (m *mockMixCoord) AllocID(ctx context.Context, req *rootcoordpb.AllocIDRequest) (*rootcoordpb.AllocIDResponse, error) {
	if m.state != commonpb.StateCode_Healthy {
		return &rootcoordpb.AllocIDResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError}}, nil
	}
	val := m.cnt.Add(int64(req.Count))
	return &rootcoordpb.AllocIDResponse{
		Status: merr.Success(),
		ID:     val,
		Count:  req.Count,
	}, nil
}

// segment
func (m *mockMixCoord) DescribeSegment(ctx context.Context, req *milvuspb.DescribeSegmentRequest) (*milvuspb.DescribeSegmentResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) ShowSegments(ctx context.Context, req *milvuspb.ShowSegmentsRequest) (*milvuspb.ShowSegmentsResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) GetPChannelInfo(ctx context.Context, req *rootcoordpb.GetPChannelInfoRequest) (*rootcoordpb.GetPChannelInfoResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) DescribeSegments(ctx context.Context, req *rootcoordpb.DescribeSegmentsRequest) (*rootcoordpb.DescribeSegmentsResponse, error) {
	panic("implement me")
}

func (m *mockMixCoord) GetDdChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: merr.Success(),
		Value:  "ddchannel",
	}, nil
}

func (m *mockMixCoord) UpdateChannelTimeTick(ctx context.Context, req *internalpb.ChannelTimeTickMsg) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) InvalidateCollectionMetaCache(ctx context.Context, req *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) SegmentFlushCompleted(ctx context.Context, in *datapb.SegmentFlushCompletedMsg) (*commonpb.Status, error) {
	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
}

func (m *mockMixCoord) AddNewSegment(ctx context.Context, in *datapb.SegmentMsg) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	return &internalpb.ShowConfigurationsResponse{
		Status: merr.Success(),
	}, nil
}

func (m *mockMixCoord) Init() error {
	return nil
}

func (m *mockMixCoord) Start() error {
	return nil
}

func (m *mockMixCoord) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	// TODO(dragondriver): change the id, though it's not important in ut
	nodeID := UniqueID(20210901)

	rootCoordTopology := metricsinfo.RootCoordTopology{
		Self: metricsinfo.RootCoordInfos{
			BaseComponentInfos: metricsinfo.BaseComponentInfos{
				Name: metricsinfo.ConstructComponentName(typeutil.RootCoordRole, nodeID),
				ID:   nodeID,
			},
		},
		Connections: metricsinfo.ConnTopology{
			Name: metricsinfo.ConstructComponentName(typeutil.RootCoordRole, nodeID),
			// TODO(dragondriver): fill ConnectedComponents if necessary
			ConnectedComponents: []metricsinfo.ConnectionInfo{},
		},
	}

	resp, err := metricsinfo.MarshalTopology(rootCoordTopology)
	if err != nil {
		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			Response:      "",
			ComponentName: metricsinfo.ConstructComponentName(typeutil.RootCoordRole, nodeID),
		}, nil
	}

	return &milvuspb.GetMetricsResponse{
		Status:        merr.Success(),
		Response:      resp,
		ComponentName: metricsinfo.ConstructComponentName(typeutil.RootCoordRole, nodeID),
	}, nil
}

func (m *mockMixCoord) GetDcMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) GetQcMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) GetDataCoordTopology(ctx context.Context, req *milvuspb.GetMetricsRequest) (*metricsinfo.DataCoordTopology, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) GetConnectedDataNodeMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) ([]metricsinfo.DataNodeInfos, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) GetQueryCoordTopology(ctx context.Context, req *milvuspb.GetMetricsRequest) (*metricsinfo.QueryCoordTopology, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) BackupRBAC(ctx context.Context, req *milvuspb.BackupRBACMetaRequest) (*milvuspb.BackupRBACMetaResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) RestoreRBAC(ctx context.Context, req *milvuspb.RestoreRBACMetaRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) BackupEzk(ctx context.Context, req *internalpb.BackupEzkRequest) (*internalpb.BackupEzkResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) CreateCredential(ctx context.Context, req *internalpb.CredentialInfo) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockMixCoord) UpdateCredential(ctx context.Context, req *internalpb.CredentialInfo) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockMixCoord) DeleteCredential(ctx context.Context, req *milvuspb.DeleteCredentialRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockMixCoord) ListCredUsers(ctx context.Context, req *milvuspb.ListCredUsersRequest) (*milvuspb.ListCredUsersResponse, error) {
	panic("implement me")
}

func (m *mockMixCoord) GetCredential(ctx context.Context, req *rootcoordpb.GetCredentialRequest) (*rootcoordpb.GetCredentialResponse, error) {
	panic("implement me")
}

func (m *mockMixCoord) CreateRole(ctx context.Context, req *milvuspb.CreateRoleRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockMixCoord) AlterRole(ctx context.Context, req *milvuspb.AlterRoleRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockMixCoord) DropRole(ctx context.Context, req *milvuspb.DropRoleRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockMixCoord) OperateUserRole(ctx context.Context, req *milvuspb.OperateUserRoleRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockMixCoord) SelectRole(ctx context.Context, req *milvuspb.SelectRoleRequest) (*milvuspb.SelectRoleResponse, error) {
	panic("implement me")
}

func (m *mockMixCoord) SelectUser(ctx context.Context, req *milvuspb.SelectUserRequest) (*milvuspb.SelectUserResponse, error) {
	panic("implement me")
}

func (m *mockMixCoord) OperatePrivilege(ctx context.Context, req *milvuspb.OperatePrivilegeRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockMixCoord) SelectGrant(ctx context.Context, req *milvuspb.SelectGrantRequest) (*milvuspb.SelectGrantResponse, error) {
	panic("implement me")
}

func (m *mockMixCoord) ListPolicy(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
	return &internalpb.ListPolicyResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}}, nil
}

func (m *mockMixCoord) CreatePrivilegeGroup(ctx context.Context, req *milvuspb.CreatePrivilegeGroupRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockMixCoord) DropPrivilegeGroup(ctx context.Context, req *milvuspb.DropPrivilegeGroupRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockMixCoord) ListPrivilegeGroups(ctx context.Context, req *milvuspb.ListPrivilegeGroupsRequest) (*milvuspb.ListPrivilegeGroupsResponse, error) {
	panic("implement me")
}

func (m *mockMixCoord) OperatePrivilegeGroup(ctx context.Context, req *milvuspb.OperatePrivilegeGroupRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) ActivateChecker(ctx context.Context, req *querypb.ActivateCheckerRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) DeactivateChecker(ctx context.Context, req *querypb.DeactivateCheckerRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) ListCheckers(ctx context.Context, req *querypb.ListCheckersRequest) (*querypb.ListCheckersResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) ShowLoadCollections(ctx context.Context, req *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) LoadCollection(ctx context.Context, req *querypb.LoadCollectionRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) ShowLoadPartitions(ctx context.Context, req *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) SyncNewCreatedPartition(ctx context.Context, req *querypb.SyncNewCreatedPartitionRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetPartitionStates(ctx context.Context, req *querypb.GetPartitionStatesRequest) (*querypb.GetPartitionStatesResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetLoadSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) LoadBalance(ctx context.Context, req *querypb.LoadBalanceRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetReplicas(ctx context.Context, req *milvuspb.GetReplicasRequest) (*milvuspb.GetReplicasResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetShardLeaders(ctx context.Context, req *querypb.GetShardLeadersRequest) (*querypb.GetShardLeadersResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) CreateResourceGroup(ctx context.Context, req *milvuspb.CreateResourceGroupRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) UpdateResourceGroups(ctx context.Context, req *querypb.UpdateResourceGroupsRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) DropResourceGroup(ctx context.Context, req *milvuspb.DropResourceGroupRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) TransferNode(ctx context.Context, req *milvuspb.TransferNodeRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) TransferReplica(ctx context.Context, req *querypb.TransferReplicaRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) ListResourceGroups(ctx context.Context, req *milvuspb.ListResourceGroupsRequest) (*milvuspb.ListResourceGroupsResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) DescribeResourceGroup(ctx context.Context, req *querypb.DescribeResourceGroupRequest) (*querypb.DescribeResourceGroupResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) ListQueryNode(ctx context.Context, req *querypb.ListQueryNodeRequest) (*querypb.ListQueryNodeResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetQueryNodeDistribution(ctx context.Context, req *querypb.GetQueryNodeDistributionRequest) (*querypb.GetQueryNodeDistributionResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) SuspendBalance(ctx context.Context, req *querypb.SuspendBalanceRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) ResumeBalance(ctx context.Context, req *querypb.ResumeBalanceRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) CheckBalanceStatus(ctx context.Context, req *querypb.CheckBalanceStatusRequest) (*querypb.CheckBalanceStatusResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) SuspendNode(ctx context.Context, req *querypb.SuspendNodeRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) ResumeNode(ctx context.Context, req *querypb.ResumeNodeRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) TransferSegment(ctx context.Context, req *querypb.TransferSegmentRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) TransferChannel(ctx context.Context, req *querypb.TransferChannelRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) CheckQueryNodeDistribution(ctx context.Context, req *querypb.CheckQueryNodeDistributionRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) UpdateLoadConfig(ctx context.Context, req *querypb.UpdateLoadConfigRequest) (*commonpb.Status, error) {
	panic("implement me")
}

// DataCoordServer
func (s *mockMixCoord) GetSegmentInfo(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) Flush(ctx context.Context, req *datapb.FlushRequest) (*datapb.FlushResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) AssignSegmentID(ctx context.Context, req *datapb.AssignSegmentIDRequest) (*datapb.AssignSegmentIDResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetSegmentStates(ctx context.Context, req *datapb.GetSegmentStatesRequest) (*datapb.GetSegmentStatesResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetInsertBinlogPaths(ctx context.Context, req *datapb.GetInsertBinlogPathsRequest) (*datapb.GetInsertBinlogPathsResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetCollectionStatistics(ctx context.Context, req *datapb.GetCollectionStatisticsRequest) (*datapb.GetCollectionStatisticsResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetPartitionStatistics(ctx context.Context, req *datapb.GetPartitionStatisticsRequest) (*datapb.GetPartitionStatisticsResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetSegmentInfoChannel(ctx context.Context, req *datapb.GetSegmentInfoChannelRequest) (*milvuspb.StringResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) SaveBinlogPaths(ctx context.Context, req *datapb.SaveBinlogPathsRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetRecoveryInfo(ctx context.Context, req *datapb.GetRecoveryInfoRequest) (*datapb.GetRecoveryInfoResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetRecoveryInfoV2(ctx context.Context, req *datapb.GetRecoveryInfoRequestV2) (*datapb.GetRecoveryInfoResponseV2, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetChannelRecoveryInfo(ctx context.Context, req *datapb.GetChannelRecoveryInfoRequest) (*datapb.GetChannelRecoveryInfoResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetFlushedSegments(ctx context.Context, req *datapb.GetFlushedSegmentsRequest) (*datapb.GetFlushedSegmentsResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetSegmentsByStates(ctx context.Context, req *datapb.GetSegmentsByStatesRequest) (*datapb.GetSegmentsByStatesResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) ManualCompaction(ctx context.Context, req *milvuspb.ManualCompactionRequest) (*milvuspb.ManualCompactionResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetCompactionState(ctx context.Context, req *milvuspb.GetCompactionStateRequest) (*milvuspb.GetCompactionStateResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetCompactionStateWithPlans(ctx context.Context, req *milvuspb.GetCompactionPlansRequest) (*milvuspb.GetCompactionPlansResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) WatchChannels(ctx context.Context, req *datapb.WatchChannelsRequest) (*datapb.WatchChannelsResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetFlushState(ctx context.Context, req *datapb.GetFlushStateRequest) (*milvuspb.GetFlushStateResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetFlushAllState(ctx context.Context, req *milvuspb.GetFlushAllStateRequest) (*milvuspb.GetFlushAllStateResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) DropVirtualChannel(ctx context.Context, req *datapb.DropVirtualChannelRequest) (*datapb.DropVirtualChannelResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) SetSegmentState(ctx context.Context, req *datapb.SetSegmentStateRequest) (*datapb.SetSegmentStateResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) UpdateSegmentStatistics(ctx context.Context, req *datapb.UpdateSegmentStatisticsRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) UpdateChannelCheckpoint(ctx context.Context, req *datapb.UpdateChannelCheckpointRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) MarkSegmentsDropped(ctx context.Context, req *datapb.MarkSegmentsDroppedRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) BroadcastAlteredCollection(ctx context.Context, req *datapb.AlterCollectionRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) GcConfirm(ctx context.Context, req *datapb.GcConfirmRequest) (*datapb.GcConfirmResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) CreateIndex(ctx context.Context, req *indexpb.CreateIndexRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) AlterIndex(ctx context.Context, req *indexpb.AlterIndexRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetIndexState(ctx context.Context, req *indexpb.GetIndexStateRequest) (*indexpb.GetIndexStateResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetSegmentIndexState(ctx context.Context, req *indexpb.GetSegmentIndexStateRequest) (*indexpb.GetSegmentIndexStateResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetIndexInfos(ctx context.Context, req *indexpb.GetIndexInfoRequest) (*indexpb.GetIndexInfoResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) DescribeIndex(ctx context.Context, req *indexpb.DescribeIndexRequest) (*indexpb.DescribeIndexResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetIndexStatistics(ctx context.Context, req *indexpb.GetIndexStatisticsRequest) (*indexpb.GetIndexStatisticsResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) DropIndex(ctx context.Context, req *indexpb.DropIndexRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetIndexBuildProgress(ctx context.Context, req *indexpb.GetIndexBuildProgressRequest) (*indexpb.GetIndexBuildProgressResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) ReportDataNodeTtMsgs(ctx context.Context, req *datapb.ReportDataNodeTtMsgsRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) GcControl(ctx context.Context, req *datapb.GcControlRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) ImportV2(ctx context.Context, req *internalpb.ImportRequestInternal) (*internalpb.ImportResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetImportProgress(ctx context.Context, req *internalpb.GetImportProgressRequest) (*internalpb.GetImportProgressResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) ListImports(ctx context.Context, req *internalpb.ListImportsRequestInternal) (*internalpb.ListImportsResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) ListIndexes(ctx context.Context, req *indexpb.ListIndexesRequest) (*indexpb.ListIndexesResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) AllocSegment(ctx context.Context, req *datapb.AllocSegmentRequest) (*datapb.AllocSegmentResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) NotifyDropPartition(ctx context.Context, channel string, collectionID int64, partitionIDs []int64) error {
	panic("implement me")
}

// RegisterStreamingCoordGRPCService registers the grpc service of streaming coordinator.
func (s *mockMixCoord) RegisterStreamingCoordGRPCService(server *grpc.Server) {
	panic("implement me")
}

// AddFileResource add file resource to datacoord
func (s *mockMixCoord) AddFileResource(ctx context.Context, req *milvuspb.AddFileResourceRequest) (*commonpb.Status, error) {
	panic("implement me")
}

// RemoveFileResource remove file resource from datacoord
func (s *mockMixCoord) RemoveFileResource(ctx context.Context, req *milvuspb.RemoveFileResourceRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) ListFileResources(ctx context.Context, req *milvuspb.ListFileResourcesRequest) (*milvuspb.ListFileResourcesResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetFileResources(ctx context.Context, resourceIDs ...int64) ([]*internalpb.FileResourceInfo, error) {
	panic("implement me")
}

func (s *mockMixCoord) RunAnalyzer(ctx context.Context, req *querypb.RunAnalyzerRequest) (*milvuspb.RunAnalyzerResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) ValidateAnalyzer(ctx context.Context, req *querypb.ValidateAnalyzerRequest) (*querypb.ValidateAnalyzerResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) TruncateCollection(ctx context.Context, req *milvuspb.TruncateCollectionRequest) (*milvuspb.TruncateCollectionResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) DropSegmentsByTime(ctx context.Context, collectionID int64, flushTsList map[string]uint64) error {
	panic("implement me")
}

func (s *mockMixCoord) ManualUpdateCurrentTarget(ctx context.Context, collectionID int64) error {
	panic("implement me")
}

// Snapshot related methods
func (s *mockMixCoord) CreateSnapshot(ctx context.Context, req *datapb.CreateSnapshotRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) DropSnapshot(ctx context.Context, req *datapb.DropSnapshotRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) ListSnapshots(ctx context.Context, req *datapb.ListSnapshotsRequest) (*datapb.ListSnapshotsResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) DescribeSnapshot(ctx context.Context, req *datapb.DescribeSnapshotRequest) (*datapb.DescribeSnapshotResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) RestoreSnapshot(ctx context.Context, req *datapb.RestoreSnapshotRequest) (*datapb.RestoreSnapshotResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) ExportSnapshot(ctx context.Context, req *datapb.ExportSnapshotRequest) (*datapb.ExportSnapshotResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetExportSnapshotState(ctx context.Context, req *datapb.GetExportSnapshotStateRequest) (*datapb.GetExportSnapshotStateResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetRestoreSnapshotState(ctx context.Context, req *datapb.GetRestoreSnapshotStateRequest) (*datapb.GetRestoreSnapshotStateResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) ListRestoreSnapshotJobs(ctx context.Context, req *datapb.ListRestoreSnapshotJobsRequest) (*datapb.ListRestoreSnapshotJobsResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) ClientHeartbeat(ctx context.Context, req *milvuspb.ClientHeartbeatRequest) (*milvuspb.ClientHeartbeatResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) DeleteClientCommand(ctx context.Context, req *milvuspb.DeleteClientCommandRequest) (*milvuspb.DeleteClientCommandResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) ListClientCommands(ctx context.Context, req *rootcoordpb.ListClientCommandsRequest) (*rootcoordpb.ListClientCommandsResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetClientTelemetry(ctx context.Context, req *milvuspb.GetClientTelemetryRequest) (*milvuspb.GetClientTelemetryResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) PushClientCommand(ctx context.Context, req *milvuspb.PushClientCommandRequest) (*milvuspb.PushClientCommandResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) PinSnapshotData(ctx context.Context, req *datapb.PinSnapshotDataRequest) (*datapb.PinSnapshotDataResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) UnpinSnapshotData(ctx context.Context, req *datapb.UnpinSnapshotDataRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) CommitImport(ctx context.Context, req *datapb.CommitImportRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) AbortImport(ctx context.Context, req *datapb.AbortImportRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) HandleCommitVchannel(ctx context.Context, req *datapb.HandleCommitVchannelRequest) (*commonpb.Status, error) {
	panic("implement me")
}

type mockHandler struct {
	meta *meta
}

func newMockHandler() *mockHandler {
	return &mockHandler{}
}

func (h *mockHandler) GetQueryVChanPositions(channel RWChannel, partitionID ...UniqueID) *datapb.VchannelInfo {
	return &datapb.VchannelInfo{
		CollectionID: channel.GetCollectionID(),
		ChannelName:  channel.GetName(),
	}
}

func (h *mockHandler) GetDataVChanPositions(channel RWChannel, partitionID UniqueID) *datapb.VchannelInfo {
	return &datapb.VchannelInfo{
		CollectionID: channel.GetCollectionID(),
		ChannelName:  channel.GetName(),
	}
}

func (h *mockHandler) CheckShouldDropChannel(channel string) bool {
	return false
}

func (h *mockHandler) FinishDropChannel(channel string, collectionID int64) error {
	return nil
}

func (h *mockHandler) GetCollection(_ context.Context, collectionID UniqueID) (*collectionInfo, error) {
	// empty schema
	if h.meta != nil {
		return h.meta.GetCollection(collectionID), nil
	}
	return &collectionInfo{ID: collectionID}, nil
}

func (h *mockHandler) GetCurrentSegmentsView(ctx context.Context, channel RWChannel, partitionIDs ...UniqueID) *SegmentsView {
	return nil
}

func (h *mockHandler) ListLoadedSegments(ctx context.Context) ([]int64, error) {
	return nil, nil
}

func (h *mockHandler) GenSnapshot(ctx context.Context, collectionID UniqueID, positions []*msgpb.MsgPosition) (*snapshotstorage.SnapshotData, error) {
	return &snapshotstorage.SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{
			Name:         "test_snapshot",
			CollectionId: collectionID,
		},
	}, nil
}

func (h *mockHandler) GetDeltaLogFromCompactTo(ctx context.Context, segmentID UniqueID) ([]*datapb.FieldBinlog, error) {
	return nil, nil
}

func newMockHandlerWithMeta(meta *meta) *mockHandler {
	return &mockHandler{
		meta: meta,
	}
}

func (*mockMixCoord) GetQueryViewLoadInfo(context.Context, *querypb.GetQueryViewLoadInfoRequest) (*querypb.GetQueryViewLoadInfoResponse, error) {
	return nil, merr.WrapErrServiceInternalMsg("GetQueryViewLoadInfo is not configured by this test")
}

func (*mockMixCoord) GetStreamingNodeQueryViewResources(context.Context, *datapb.GetStreamingNodeQueryViewResourcesRequest) (*datapb.GetStreamingNodeQueryViewResourcesResponse, error) {
	return nil, merr.WrapErrServiceInternalMsg("GetStreamingNodeQueryViewResources is not configured by this test")
}

// newSegmentsInfoWithSegments builds a store-backed SegmentsInfo pre-populated
// with the given segments, for use in test fixtures that used to build a
// SegmentsInfo struct literal directly (e.g. `&SegmentsInfo{segments: ...}`)
// back when the standalone in-memory map existed.
// newEmptyTestMeta returns a meta whose segments and metaStore share one
// empty MetaStore.
func newEmptyTestMeta() *meta {
	ms := metacache.NewMetaStore(nil)
	return &meta{metaStore: ms, segments: NewSegmentsInfo(ms)}
}

func newSegmentsInfoWithSegments(segs map[int64]*SegmentInfo) *SegmentsInfo {
	si := NewSegmentsInfo(metacache.NewMetaStore(nil))
	for id, seg := range segs {
		si.SetSegment(id, seg)
	}
	return si
}

func commitUpdates(segmentID int64, basePath string, operators ...UpdateOperator) SegmentManifestCommit {
	return SegmentManifestCommit{
		SegmentID:     segmentID,
		StorageConfig: &indexpb.StorageConfig{},
		Mutation: ManifestMutation{
			Type:    ManifestMutationCommitUpdates,
			Updates: &packed.ManifestUpdates{DeltaLogs: []packed.DeltaLogEntry{{Path: basePath + "/_delta/1", NumEntries: 1}}},
		},
		CatalogMutation: SegmentCatalogMutation{Operators: operators},
	}
}

type embeddedBroker struct{ broker.Broker }

// newTestGroup returns a STAGED mix-compaction segment change group with one new and one superseded segment.
func newTestGroup() *model.SegmentChangeGroup {
	return &model.SegmentChangeGroup{
		GroupID:              1,
		Source:               model.SegmentChangeSourceMixCompaction,
		CollectionID:         10,
		PartitionID:          100,
		SourceJobID:          999,
		State:                model.SegmentChangeStateStaged,
		NewSegmentIDs:        []int64{1001},
		SupersededSegmentIDs: []int64{2001},
		CreateTS:             123,
	}
}
