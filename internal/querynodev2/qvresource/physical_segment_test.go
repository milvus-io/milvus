package qvresource

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/pkoracle"
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type fakeCollectionRuntime struct {
	collectionID int64
	collection   *segments.Collection
	schema       *schemapb.CollectionSchema
}

func (r fakeCollectionRuntime) CollectionID() int64 { return r.collectionID }
func (fakeCollectionRuntime) DatabaseName() string  { return "default" }
func (r fakeCollectionRuntime) Schema() *schemapb.CollectionSchema {
	if r.schema != nil {
		return r.schema
	}
	return &schemapb.CollectionSchema{}
}
func (fakeCollectionRuntime) SchemaVersion() int64                     { return 1 }
func (fakeCollectionRuntime) CCollection() *segcore.CCollection        { return nil }
func (r fakeCollectionRuntime) PinnedCollection() *segments.Collection { return r.collection }

type fakeLoadedSegment struct {
	id          int64
	partitionID int64
	hits        []bool
	deletePKs   storage.PrimaryKeys
	deleteTS    []typeutil.Timestamp
	releases    int
	mu          sync.Mutex
}

func (s *fakeLoadedSegment) ID() int64        { return s.id }
func (s *fakeLoadedSegment) Partition() int64 { return s.partitionID }
func (s *fakeLoadedSegment) Delete(_ context.Context, pks storage.PrimaryKeys, timestamps []typeutil.Timestamp) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deletePKs = pks
	s.deleteTS = append([]typeutil.Timestamp(nil), timestamps...)
	return nil
}

func (s *fakeLoadedSegment) Release(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releases++
	return nil
}
func (s *fakeLoadedSegment) PkCandidateExist() bool { return s.hits != nil }
func (s *fakeLoadedSegment) BatchPkExist(*storage.BatchLocationsCache) []bool {
	return append([]bool(nil), s.hits...)
}

type fakeSegmentOperations struct {
	segment *fakeLoadedSegment
	failAt  string
	calls   []string
	mu      sync.Mutex
}

func (o *fakeSegmentOperations) call(name string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.calls = append(o.calls, name)
	if o.failAt == name {
		return merr.WrapErrServiceInternalMsg("test %s failure", name)
	}
	return nil
}

func (o *fakeSegmentOperations) NewSegment(context.Context, qnview.CollectionRuntime, *querypb.SegmentLoadInfo) (queryViewLoadedSegment, error) {
	if err := o.call("new"); err != nil {
		return nil, err
	}
	return o.segment, nil
}

func (o *fakeSegmentOperations) LoadSegment(context.Context, queryViewLoadedSegment, *querypb.SegmentLoadInfo) error {
	return o.call("load")
}

func (o *fakeSegmentOperations) ReopenSegment(context.Context, queryViewLoadedSegment, *querypb.SegmentLoadInfo) error {
	return o.call("reopen")
}

func (o *fakeSegmentOperations) LoadIndex(context.Context, queryViewLoadedSegment, *querypb.SegmentLoadInfo) error {
	return o.call("index")
}

func (o *fakeSegmentOperations) LoadDeltaLogs(context.Context, queryViewLoadedSegment, *querypb.SegmentLoadInfo, qnview.CollectionRuntime) error {
	return o.call("delta")
}

func (o *fakeSegmentOperations) LoadPKCandidate(context.Context, queryViewLoadedSegment, *querypb.SegmentLoadInfo, qnview.CollectionRuntime) error {
	return o.call("pk")
}

func TestPhysicalSegmentLoaderLoadAndUpdate(t *testing.T) {
	loaded := &fakeLoadedSegment{id: 100, partitionID: 10}
	operations := &fakeSegmentOperations{segment: loaded}
	loader := newQueryViewPhysicalSegmentLoader(operations)
	runtime := fakeCollectionRuntime{collectionID: 1, collection: &segments.Collection{}}
	info := &querypb.SegmentLoadInfo{
		CollectionID:  1,
		PartitionID:   10,
		SegmentID:     100,
		InsertChannel: "channel",
		DeltaPosition: &msgpb.MsgPosition{Timestamp: 99},
	}

	segment, err := loader.Load(context.Background(), info, runtime)
	require.NoError(t, err)
	assert.Equal(t, []string{"new", "load", "delta", "pk"}, operations.calls)
	assert.Equal(t, int64(100), segment.ID())
	assert.Equal(t, "channel", segment.VChannel())
	assert.Equal(t, uint64(99), segment.TransformStartAfterTimeTick())

	err = loader.Update(context.Background(), segment, runtime, qnview.SegmentLoadInfoSnapshot{
		CollectionID: 1,
		SegmentID:    100,
		LoadInfo:     info,
	}, qnview.SegmentUpdateReopen|qnview.SegmentUpdateLoadIndex)
	require.NoError(t, err)
	assert.Equal(t, []string{"new", "load", "delta", "pk", "reopen"}, operations.calls)
}

func TestPhysicalSegmentLoaderReleasesPartialLoad(t *testing.T) {
	loaded := &fakeLoadedSegment{id: 100, partitionID: 10}
	operations := &fakeSegmentOperations{segment: loaded, failAt: "delta"}
	loader := newQueryViewPhysicalSegmentLoader(operations)
	runtime := fakeCollectionRuntime{collectionID: 1, collection: &segments.Collection{}}

	segment, err := loader.Load(context.Background(), &querypb.SegmentLoadInfo{
		CollectionID: 1, PartitionID: 10, SegmentID: 100,
	}, runtime)
	require.Error(t, err)
	assert.Nil(t, segment)
	assert.Equal(t, 1, loaded.releases)
	assert.Equal(t, []string{"new", "load", "delta"}, operations.calls)
}

func TestPhysicalSegmentLoaderFailureStages(t *testing.T) {
	for _, stage := range []string{"new", "load", "pk"} {
		t.Run(stage, func(t *testing.T) {
			loaded := &fakeLoadedSegment{id: 100, partitionID: 10}
			operations := &fakeSegmentOperations{segment: loaded, failAt: stage}
			loader := newQueryViewPhysicalSegmentLoader(operations)
			segment, err := loader.Load(context.Background(), &querypb.SegmentLoadInfo{
				CollectionID: 1, PartitionID: 10, SegmentID: 100,
			}, fakeCollectionRuntime{collectionID: 1, collection: &segments.Collection{}})
			require.Error(t, err)
			assert.Nil(t, segment)
			if stage == "new" {
				assert.Zero(t, loaded.releases)
			} else {
				assert.Equal(t, 1, loaded.releases)
			}
		})
	}
}

func TestPhysicalSegmentLoaderValidatesInputs(t *testing.T) {
	loader := newQueryViewPhysicalSegmentLoader(&fakeSegmentOperations{})
	segment, err := loader.Load(context.Background(), nil, fakeCollectionRuntime{})
	require.Error(t, err)
	assert.Nil(t, segment)

	segment, err = loader.Load(context.Background(), &querypb.SegmentLoadInfo{CollectionID: 1}, nil)
	require.Error(t, err)
	assert.Nil(t, segment)

	err = loader.Update(context.Background(), nil, fakeCollectionRuntime{}, qnview.SegmentLoadInfoSnapshot{}, qnview.SegmentUpdateReopen)
	require.Error(t, err)

	unexpected := &unexpectedTransformSegment{}
	err = loader.Update(context.Background(), unexpected, fakeCollectionRuntime{}, qnview.SegmentLoadInfoSnapshot{
		CollectionID: 1,
		LoadInfo:     &querypb.SegmentLoadInfo{},
	}, qnview.SegmentUpdateReopen)
	require.Error(t, err)
}

type unexpectedTransformSegment struct{}

func (*unexpectedTransformSegment) ID() int64                           { return 1 }
func (*unexpectedTransformSegment) VChannel() string                    { return "" }
func (*unexpectedTransformSegment) PartitionID() int64                  { return 1 }
func (*unexpectedTransformSegment) TransformStartAfterTimeTick() uint64 { return 0 }
func (*unexpectedTransformSegment) AppliedTransformTimeTick() uint64    { return 0 }
func (*unexpectedTransformSegment) WaitTransformApplied(context.Context, uint64) error {
	return nil
}
func (*unexpectedTransformSegment) Release(context.Context) error { return nil }

func TestTransformSegmentCatchupDeleteAndRelease(t *testing.T) {
	loaded := &fakeLoadedSegment{id: 100, partitionID: 10, hits: []bool{true, false}}
	segment := newQueryViewTransformSegment(loaded, "channel", 10)
	assert.Equal(t, int64(100), segment.ID())
	assert.Equal(t, int64(10), segment.PartitionID())
	assert.Equal(t, "channel", segment.VChannel())
	assert.Equal(t, uint64(10), segment.AppliedTransformTimeTick())

	waited := make(chan error, 1)
	go func() { waited <- segment.WaitTransformApplied(context.Background(), 20) }()
	assert.Never(t, func() bool { return len(waited) > 0 }, 20*time.Millisecond, time.Millisecond)

	pks := storage.NewInt64PrimaryKeys(2)
	pks.MustAppend(storage.NewInt64PrimaryKey(1))
	pks.MustAppend(storage.NewInt64PrimaryKey(2))
	require.NoError(t, segment.ApplyDelete(context.Background(), 10, pks, 20))
	loaded.mu.Lock()
	require.NotNil(t, loaded.deletePKs)
	assert.Equal(t, 1, loaded.deletePKs.Len())
	assert.Equal(t, []typeutil.Timestamp{20}, loaded.deleteTS)
	loaded.mu.Unlock()

	segment.MarkTransformApplied(20)
	select {
	case err := <-waited:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for applied transform")
	}
	assert.Equal(t, uint64(20), segment.AppliedTransformTimeTick())
	require.NoError(t, segment.WaitTransformApplied(context.Background(), 0))
	require.NoError(t, segment.WaitTransformApplied(context.Background(), 20))
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, segment.WaitTransformApplied(canceledCtx, 30), context.Canceled)

	require.NoError(t, segment.ApplyDelete(context.Background(), common.AllPartitionsID, pks, 21))
	require.NoError(t, segment.ApplyDelete(context.Background(), 11, pks, 22))
	require.NoError(t, segment.Release(context.Background()))
	require.NoError(t, segment.Release(context.Background()))
	assert.Equal(t, 1, loaded.releases)
}

type fakeReservation struct{ released bool }

func (r *fakeReservation) Release() { r.released = true }

type fakeResourceLoader struct {
	info        *querypb.SegmentLoadInfo
	reservation *fakeReservation
}

func (l *fakeResourceLoader) ReserveLoadResource(_ context.Context, infos ...*querypb.SegmentLoadInfo) (segments.LoadResourceReservation, error) {
	l.info = infos[0]
	return l.reservation, nil
}

func TestSegmentResourceEstimatorDelegatesReservation(t *testing.T) {
	reservation := &fakeReservation{}
	loader := &fakeResourceLoader{reservation: reservation}
	estimator := newQueryViewSegmentResourceEstimator(loader)
	info := &querypb.SegmentLoadInfo{SegmentID: 100}

	got, err := estimator.Reserve(context.Background(), info, nil)
	require.NoError(t, err)
	assert.Same(t, reservation, got)
	assert.Same(t, info, loader.info)
}

func TestRealSegmentLoaderValidatesDetachedSegmentType(t *testing.T) {
	loader := realQueryViewSegmentLoader{}
	fake := &fakeLoadedSegment{id: 100, partitionID: 10}
	info := &querypb.SegmentLoadInfo{SegmentID: 100, Level: datapb.SegmentLevel_L1}
	internalRuntime := fakeCollectionRuntime{collectionID: 1, collection: &segments.Collection{}}
	externalRuntime := fakeCollectionRuntime{
		collectionID: 1,
		collection:   &segments.Collection{},
		schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{{FieldID: 100, ExternalField: "external_id"}},
		},
	}

	require.Error(t, loader.LoadSegment(context.Background(), fake, info))
	require.NoError(t, loader.LoadSegment(context.Background(), fake, &querypb.SegmentLoadInfo{Level: datapb.SegmentLevel_L0}))
	require.Error(t, loader.ReopenSegment(context.Background(), fake, info))
	require.Error(t, loader.LoadIndex(context.Background(), fake, info))
	require.Error(t, loader.LoadDeltaLogs(context.Background(), fake, info, internalRuntime))
	require.NoError(t, loader.LoadDeltaLogs(context.Background(), fake, info, externalRuntime))
	require.Error(t, loader.LoadPKCandidate(context.Background(), fake, info, internalRuntime))

	local := &queryViewLocalSegment{}
	got, err := asQueryViewLocalSegment(local)
	require.NoError(t, err)
	assert.Same(t, local, got)

	_, err = loader.NewSegment(context.Background(), fakeCollectionRuntime{collectionID: 1}, info)
	require.ErrorIs(t, err, merr.ErrCollectionNotFound)
}

type embeddedSegmentStub struct {
	segments.Segment
	id           int64
	partitionID  int64
	reopened     bool
	deleted      bool
	candidate    pkoracle.Candidate
	candidateSet bool
}

func (s *embeddedSegmentStub) ID() int64        { return s.id }
func (s *embeddedSegmentStub) Partition() int64 { return s.partitionID }
func (s *embeddedSegmentStub) Delete(context.Context, storage.PrimaryKeys, []typeutil.Timestamp) error {
	s.deleted = true
	return nil
}

func (s *embeddedSegmentStub) Reopen(context.Context, *querypb.SegmentLoadInfo) error {
	s.reopened = true
	return nil
}
func (s *embeddedSegmentStub) PkCandidateExist() bool { return s.candidateSet }
func (s *embeddedSegmentStub) BatchPkExist(*storage.BatchLocationsCache) []bool {
	return []bool{true}
}

func (s *embeddedSegmentStub) SetPKCandidate(candidate pkoracle.Candidate) {
	s.candidate = candidate
	s.candidateSet = true
}
func (*embeddedSegmentStub) Type() segments.SegmentType { return segments.SegmentTypeSealed }

type embeddedQueryViewLoaderStub struct {
	segments.QueryViewLoader
	loadCalled    bool
	deltaCalled   bool
	indexCalled   bool
	reserveCalled bool
	reservation   segments.LoadResourceReservation
}

func (l *embeddedQueryViewLoaderStub) LoadSegment(context.Context, segments.Segment, *querypb.SegmentLoadInfo) error {
	l.loadCalled = true
	return nil
}

func (l *embeddedQueryViewLoaderStub) LoadDeltaLogsWithoutResource(context.Context, segments.Segment, *querypb.SegmentLoadInfo) error {
	l.deltaCalled = true
	return nil
}

func (l *embeddedQueryViewLoaderStub) LoadIndex(context.Context, segments.Segment, *querypb.SegmentLoadInfo, int64) error {
	l.indexCalled = true
	return nil
}

func (*embeddedQueryViewLoaderStub) LoadBloomFilterSet(context.Context, int64, ...*querypb.SegmentLoadInfo) ([]*pkoracle.BloomFilterSet, error) {
	return nil, nil
}

func (l *embeddedQueryViewLoaderStub) ReserveLoadResource(context.Context, ...*querypb.SegmentLoadInfo) (segments.LoadResourceReservation, error) {
	l.reserveCalled = true
	return l.reservation, nil
}

func TestRealSegmentLoaderUsesDetachedSegmentPrimitives(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().CommonCfg.BloomFilterEnabled.Key, "false")
	t.Cleanup(func() { paramtable.Get().Reset(paramtable.Get().CommonCfg.BloomFilterEnabled.Key) })

	segment := &embeddedSegmentStub{id: 100, partitionID: 10, candidateSet: true}
	local := &queryViewLocalSegment{
		segment:      segment,
		collections:  &rejectingCollectionManager{collection: &segments.Collection{}},
		collectionID: 1,
	}
	loaderStub := &embeddedQueryViewLoaderStub{}
	loader := realQueryViewSegmentLoader{loader: loaderStub}
	info := &querypb.SegmentLoadInfo{CollectionID: 1, SegmentID: 100, Level: datapb.SegmentLevel_L1}
	runtime := fakeCollectionRuntime{collectionID: 1, collection: &segments.Collection{}}

	assert.Equal(t, int64(100), local.ID())
	assert.Equal(t, int64(10), local.Partition())
	assert.NotNil(t, local.Collection())
	require.NoError(t, local.Delete(context.Background(), storage.NewInt64PrimaryKeys(0), nil))
	assert.True(t, segment.deleted)
	assert.True(t, local.PkCandidateExist())
	assert.Equal(t, []bool{true}, local.BatchPkExist(nil))

	require.NoError(t, loader.LoadSegment(context.Background(), local, info))
	require.NoError(t, loader.ReopenSegment(context.Background(), local, info))
	require.NoError(t, loader.LoadIndex(context.Background(), local, info))
	require.NoError(t, loader.LoadDeltaLogs(context.Background(), local, info, runtime))
	require.NoError(t, loader.LoadPKCandidate(context.Background(), local, info, runtime))
	assert.True(t, loaderStub.loadCalled)
	assert.True(t, loaderStub.deltaCalled)
	assert.True(t, loaderStub.indexCalled)
	assert.True(t, segment.reopened)

	physical := NewQueryViewPhysicalSegmentLoader(loaderStub, &rejectingCollectionManager{}, nil)
	assert.NotNil(t, physical)
	reservation := &fakeReservation{}
	loaderStub.reservation = reservation
	estimator := NewQueryViewSegmentResourceEstimator(loaderStub)
	got, err := estimator.Reserve(context.Background(), info, runtime)
	require.NoError(t, err)
	assert.Same(t, reservation, got)
	assert.True(t, loaderStub.reserveCalled)
}

func TestRealSegmentLoaderPKCandidateBranches(t *testing.T) {
	paramtable.Init()
	segment := &embeddedSegmentStub{id: 100, partitionID: 10}
	local := &queryViewLocalSegment{segment: segment}
	loaderStub := &embeddedQueryViewLoaderStub{}
	loader := realQueryViewSegmentLoader{loader: loaderStub}
	info := &querypb.SegmentLoadInfo{CollectionID: 1, SegmentID: 100, PartitionID: 10}

	paramtable.Get().Save(paramtable.Get().CommonCfg.BloomFilterEnabled.Key, "false")
	require.NoError(t, loader.LoadPKCandidate(
		context.Background(), local, info,
		fakeCollectionRuntime{collectionID: 1, schema: &schemapb.CollectionSchema{}},
	))
	assert.False(t, segment.candidateSet)

	require.NoError(t, loader.LoadPKCandidate(
		context.Background(), local, info,
		fakeCollectionRuntime{collectionID: 1, schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{{FieldID: 100, ExternalField: "external_id"}},
		}},
	))
	assert.True(t, segment.candidateSet)
	segment.candidateSet = false

	paramtable.Get().Save(paramtable.Get().CommonCfg.BloomFilterEnabled.Key, "true")
	t.Cleanup(func() { paramtable.Get().Reset(paramtable.Get().CommonCfg.BloomFilterEnabled.Key) })
	require.Error(t, loader.LoadPKCandidate(
		context.Background(), local, info,
		fakeCollectionRuntime{collectionID: 1, schema: &schemapb.CollectionSchema{}},
	))
}

func TestRealSegmentLoaderNewSegmentPropagatesCreateError(t *testing.T) {
	schema := &schemapb.CollectionSchema{Name: "test"}
	collection := segments.NewCollectionWithoutSegcoreForTest(1, schema)
	loader := realQueryViewSegmentLoader{}
	loaded, err := loader.NewSegment(context.Background(), fakeCollectionRuntime{
		collectionID: 1,
		collection:   collection,
		schema:       schema,
	}, &querypb.SegmentLoadInfo{CollectionID: 1, SegmentID: 100})
	require.Error(t, err)
	assert.Nil(t, loaded)
}

func TestPrimaryKeyHelpersCoverVarcharAndFallbacks(t *testing.T) {
	varchar := storage.NewVarcharPrimaryKeys(1)
	varchar.MustAppend(storage.NewVarCharPrimaryKey("a"))
	created, ok := newPrimaryKeysLike(varchar)
	assert.True(t, ok)
	assert.Equal(t, schemapb.DataType_VarChar, created.Type())

	segment := &fakeLoadedSegment{hits: []bool{true, false}}
	int64Keys := storage.NewInt64PrimaryKeys(1)
	int64Keys.MustAppend(storage.NewInt64PrimaryKey(1))
	assert.Same(t, int64Keys, filterMaybeHitPrimaryKeys(segment, int64Keys), "hit-vector length mismatch must keep the original keys")
	assert.Equal(t, uint64(7), transformStartAfter(&querypb.SegmentLoadInfo{
		StartPosition: &msgpb.MsgPosition{Timestamp: 7},
	}))
}
