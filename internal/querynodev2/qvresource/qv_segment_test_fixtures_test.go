//go:build test && dynamic

package qvresource

import (
	"context"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
)

type fakeQVCollectionManager struct {
	putCollectionID int64
	putSchema       *schemapb.CollectionSchema
	putIndexMeta    *segcorepb.CollectionIndexMeta
	putLoadMeta     *querypb.LoadMetaInfo
	putCount        int
	refCollection   int64
	refCount        uint32
	unrefCollection int64
	unrefCount      uint32
	err             error
}

func (m *fakeQVCollectionManager) Get(collectionID int64) *segments.Collection {
	return nil
}

func (m *fakeQVCollectionManager) PutOrRef(collectionID int64, schema *schemapb.CollectionSchema, indexMeta *segcorepb.CollectionIndexMeta, loadMeta *querypb.LoadMetaInfo) error {
	m.putCollectionID = collectionID
	m.putSchema = schema
	m.putIndexMeta = indexMeta
	m.putLoadMeta = loadMeta
	m.putCount++
	return m.err
}

func (m *fakeQVCollectionManager) Ref(collectionID int64, count uint32) bool {
	m.refCollection = collectionID
	m.refCount = count
	return true
}

func (m *fakeQVCollectionManager) Unref(collectionID int64, count uint32) bool {
	m.unrefCollection = collectionID
	m.unrefCount = count
	return true
}

type fakeQVSegmentManager struct {
	removed []int64
}

func (m *fakeQVSegmentManager) Remove(_ context.Context, segmentID int64, scope querypb.DataScope) (int, int) {
	if scope == querypb.DataScope_All {
		m.removed = append(m.removed, segmentID)
	}
	return 0, 1
}

type fakeQVLoader struct {
	collectionID    int64
	version         int64
	infos           []*querypb.SegmentLoadInfo
	segment         qvLoadedSegment
	newCalled       bool
	loadCalled      bool
	reopenCalled    bool
	loadIndexCalled bool
	deltaCalled     bool
	pkCalled        bool
	err             error
}

type fakeQVResourceLoader struct {
	info        *querypb.SegmentLoadInfo
	reservation segments.LoadResourceReservation
	err         error
}

func (l *fakeQVResourceLoader) ReserveLoadResource(_ context.Context, infos ...*querypb.SegmentLoadInfo) (segments.LoadResourceReservation, error) {
	if len(infos) > 0 {
		l.info = infos[0]
	}
	return l.reservation, l.err
}

type fakeQVResourceReservation struct {
	released bool
}

func (r *fakeQVResourceReservation) Release() {
	r.released = true
}

type fakeQVLoadMetadataProvider struct {
	collection   *milvuspb.DescribeCollectionResponse
	partitionIDs []int64
	loadFields   []int64
	err          error
}

func (p *fakeQVLoadMetadataProvider) DescribeCollection(context.Context, int64) (*milvuspb.DescribeCollectionResponse, error) {
	return p.collection, p.err
}

func (p *fakeQVLoadMetadataProvider) GetQueryViewLoadInfo(context.Context, int64, qnview.QueryViewLoadInfoVersion) (qnview.QueryViewLoadInfo, error) {
	fields := make([]*messagespb.LoadFieldConfig, 0, len(p.loadFields))
	for _, fieldID := range p.loadFields {
		fields = append(fields, &messagespb.LoadFieldConfig{FieldId: fieldID})
	}
	return qnview.QueryViewLoadInfo{
		PartitionIDs: append([]int64(nil), p.partitionIDs...),
		LoadFields:   fields,
	}, p.err
}

func (l *fakeQVLoader) NewSegment(_ context.Context, _ qnview.CollectionRuntime, info *querypb.SegmentLoadInfo) (qvLoadedSegment, error) {
	l.newCalled = true
	l.collectionID = info.GetCollectionID()
	l.infos = []*querypb.SegmentLoadInfo{info}
	return l.segment, l.err
}

func (l *fakeQVLoader) LoadSegment(_ context.Context, segment qvLoadedSegment, info *querypb.SegmentLoadInfo) error {
	l.loadCalled = true
	l.infos = append(l.infos, info)
	return l.err
}

func (l *fakeQVLoader) ReopenSegment(_ context.Context, segment qvLoadedSegment, info *querypb.SegmentLoadInfo) error {
	l.reopenCalled = true
	l.infos = append(l.infos, info)
	return l.err
}

func (l *fakeQVLoader) LoadIndex(_ context.Context, segment qvLoadedSegment, info *querypb.SegmentLoadInfo, version int64) error {
	l.loadIndexCalled = true
	l.version = version
	l.infos = append(l.infos, info)
	return l.err
}

func (l *fakeQVLoader) LoadDeltaLogs(_ context.Context, segment qvLoadedSegment, info *querypb.SegmentLoadInfo) error {
	l.deltaCalled = true
	l.infos = append(l.infos, info)
	return l.err
}

func (l *fakeQVLoader) LoadPKCandidate(_ context.Context, segment qvLoadedSegment, info *querypb.SegmentLoadInfo) error {
	l.pkCalled = true
	l.infos = append(l.infos, info)
	return l.err
}

func (l *fakeQVLoader) Load(_ context.Context, collectionID int64, version int64, infos ...*querypb.SegmentLoadInfo) ([]qvLoadedSegment, error) {
	l.collectionID = collectionID
	l.version = version
	l.infos = infos
	return nil, assert.AnError
}

type fakeQVSegment struct {
	id           int64
	partitionID  int64
	deletedPKs   storage.PrimaryKeys
	deletedTS    []uint64
	candidateOK  bool
	hits         []bool
	released     bool
	releaseCount int
	err          error
}

type fakeQVCollectionRuntime struct {
	collectionID  int64
	databaseName  string
	schema        *schemapb.CollectionSchema
	schemaVersion int64
}

func (r fakeQVCollectionRuntime) CollectionID() int64 {
	return r.collectionID
}

func (r fakeQVCollectionRuntime) DatabaseName() string {
	return r.databaseName
}

func (r fakeQVCollectionRuntime) Schema() *schemapb.CollectionSchema {
	return r.schema
}

func (r fakeQVCollectionRuntime) SchemaVersion() int64 {
	return r.schemaVersion
}

func (r fakeQVCollectionRuntime) CCollection() *segcore.CCollection {
	return nil
}

func (s *fakeQVSegment) ID() int64 {
	return s.id
}

func (s *fakeQVSegment) Partition() int64 {
	return s.partitionID
}

func (s *fakeQVSegment) Delete(_ context.Context, pks storage.PrimaryKeys, timestamps []uint64) error {
	s.deletedPKs = pks
	s.deletedTS = timestamps
	return s.err
}

func (s *fakeQVSegment) Release(context.Context) error {
	s.released = true
	s.releaseCount++
	return s.err
}

func (s *fakeQVSegment) PkCandidateExist() bool {
	return s.candidateOK
}

func (s *fakeQVSegment) BatchPkExist(*storage.BatchLocationsCache) []bool {
	return append([]bool(nil), s.hits...)
}
