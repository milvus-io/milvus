package datacoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// answeringDrainer answers the one question datacoord asks, and records the
// collection it was asked about. The other methods are never reached from
// datacoord; they panic rather than returning a zero value so that a seam
// wired to the wrong method fails loudly here.
type answeringDrainer struct {
	allow             bool
	seenCollectionIDs []int64
	seenIndexNames    []string
}

func (d *answeringDrainer) AllowVectorIndexDropWhileLoaded(_ context.Context, collectionID int64, indexName string) bool {
	d.seenCollectionIDs = append(d.seenCollectionIDs, collectionID)
	d.seenIndexNames = append(d.seenIndexNames, indexName)
	return d.allow
}

func (d *answeringDrainer) AbortDropIndex(context.Context, *indexpb.DropIndexRequest) {
	panic("datacoord must never reach AbortDropIndex")
}

func (d *answeringDrainer) AfterCreateIndex(context.Context, *indexpb.CreateIndexRequest) {
	panic("datacoord must never reach AfterCreateIndex")
}

func (d *answeringDrainer) CollectionDraining(context.Context, int64) bool {
	panic("datacoord must never reach CollectionDraining")
}

func (d *answeringDrainer) BeginDropIndex(context.Context, *indexpb.DropIndexRequest) bool {
	panic("datacoord must not classify drops itself")
}

func (d *answeringDrainer) AfterDropIndex(context.Context, *indexpb.DropIndexRequest) {
	panic("datacoord must not report committed drops")
}

type drainProvider struct{ drainer extension.IndexDrainer }

func (drainProvider) Name() string                       { return "test" }
func (drainProvider) Requires() []extension.CapabilityID { return nil }
func (p drainProvider) Capabilities() extension.Capabilities {
	return extension.Capabilities{IndexDrain: p.drainer}
}

func installDrainer(t *testing.T, drainer extension.IndexDrainer) {
	t.Helper()
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)
	require.NoError(t, extension.SetProvider(drainProvider{drainer: drainer}))
}

// newLoadedVectorIndexServer builds a datacoord holding exactly one index, on
// a vector field of a collection that querycoord reports as loaded: the state
// in which milvus refuses a drop.
func newLoadedVectorIndexServer(t *testing.T) (*Server, *indexpb.DropIndexRequest) {
	const (
		collID    = UniqueID(1)
		fieldID   = UniqueID(10)
		indexID   = UniqueID(100)
		indexName = "vector_idx"
	)

	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.EXPECT().AlterIndexes(mock.Anything, mock.Anything).Return(nil).Maybe()

	b := broker.NewMockBroker(t)
	b.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		Status:         merr.Success(),
		DbName:         "test_db",
		CollectionName: "test_collection",
		Schema: &schemapb.CollectionSchema{
			Name: "test_collection",
			Fields: []*schemapb.FieldSchema{
				{FieldID: fieldID, Name: "vec", DataType: schemapb.DataType_FloatVector},
			},
		},
	}, nil)

	s := &Server{
		meta: &meta{
			catalog: catalog,
			indexMeta: &indexMeta{
				catalog: catalog,
				indexes: map[UniqueID]map[UniqueID]*model.Index{
					collID: {
						indexID: {
							CollectionID: collID,
							FieldID:      fieldID,
							IndexID:      indexID,
							IndexName:    indexName,
							TypeParams:   []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "128"}},
							IndexParams:  []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "IVF_FLAT"}},
						},
					},
				},
				segmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
			},
			segments: NewSegmentsInfo(),
		},
		broker:          b,
		allocator:       newMockAllocator(t),
		notifyIndexChan: make(chan UniqueID, 1),
	}

	mixCoord := mocks.NewMixCoord(t)
	mixCoord.EXPECT().ShowLoadCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
		Status:        merr.Success(),
		CollectionIDs: []int64{collID},
	}, nil)
	s.mixCoord = mixCoord

	RegisterDDLCallbacks(s)
	s.stateCode.Store(commonpb.StateCode_Healthy)

	return s, &indexpb.DropIndexRequest{CollectionID: collID, IndexName: indexName}
}

// With no provider installed datacoord must keep refusing: a loaded collection
// whose vector index disappeared is loaded and unqueryable.
func TestDropVectorIndexOnLoadedIsRefusedWithoutProvider(t *testing.T) {
	initStreamingSystem(t)
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)

	s, req := newLoadedVectorIndexServer(t)
	status, err := s.DropIndex(context.Background(), req)
	require.NoError(t, err)
	assert.False(t, merr.Ok(status))
	assert.Contains(t, status.GetReason(), "vector index cannot be dropped on loaded collection")

	assert.NotEmpty(t, s.meta.indexMeta.GetIndexesForCollection(req.GetCollectionID(), req.GetIndexName()),
		"a refused drop must leave the index in place")
}

// A drainer that refuses to take the drop over leaves the refusal exactly as
// it was: the capability being installed is not what changes datacoord's mind,
// its answer is.
func TestDropVectorIndexOnLoadedIsRefusedWhenDrainerDeclines(t *testing.T) {
	initStreamingSystem(t)
	drainer := &answeringDrainer{allow: false}
	installDrainer(t, drainer)

	s, req := newLoadedVectorIndexServer(t)
	status, err := s.DropIndex(context.Background(), req)
	require.NoError(t, err)
	assert.False(t, merr.Ok(status))
	assert.Contains(t, status.GetReason(), "vector index cannot be dropped on loaded collection")
	assert.Equal(t, []int64{req.GetCollectionID()}, drainer.seenCollectionIDs,
		"datacoord must ask about the collection it is refusing")
	assert.NotEmpty(t, s.meta.indexMeta.GetIndexesForCollection(req.GetCollectionID(), req.GetIndexName()),
		"a refused drop must leave the index in place")
}

// A drainer that takes the drop over suppresses the refusal, and the drop runs
// to completion: the index is marked deleted.
func TestDropVectorIndexOnLoadedProceedsWhenDrainerAllows(t *testing.T) {
	initStreamingSystem(t)
	drainer := &answeringDrainer{allow: true}
	installDrainer(t, drainer)

	s, req := newLoadedVectorIndexServer(t)
	status, err := s.DropIndex(context.Background(), req)
	require.NoError(t, err)
	assert.True(t, merr.Ok(status), "the drainer took the drop over, so it must not be refused")
	assert.Equal(t, []int64{req.GetCollectionID()}, drainer.seenCollectionIDs)
	assert.Equal(t, []string{req.GetIndexName()}, drainer.seenIndexNames,
		"the check must carry the request's index name: it is how a drainer mid-drain tells this drop from a concurrent second one")

	assert.Empty(t, s.meta.indexMeta.GetIndexesForCollection(req.GetCollectionID(), req.GetIndexName()),
		"an allowed drop must actually remove the index, not just skip the refusal")
}
