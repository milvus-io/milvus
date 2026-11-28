package qnview

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/querynodev2/pkoracle"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/bloomfilter"
	"github.com/milvus-io/milvus/internal/views/optimizer"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type pkCandidateRead struct {
	segments.ReadState
	candidate pkoracle.Candidate
}

func (r pkCandidateRead) PKCandidate() pkoracle.Candidate { return r.candidate }

func TestQueryViewSealedPKPruning(t *testing.T) {
	paramtable.Init()
	switchKey := paramtable.Get().QueryNodeCfg.EnableSegmentFilter.Key
	paramtable.Get().Save(switchKey, "true")
	t.Cleanup(func() { paramtable.Get().Reset(switchKey) })

	schema, err := typeutil.CreateSchemaHelper(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "other", DataType: schemapb.DataType_Int64},
		{FieldID: 102, Name: "vec", DataType: schemapb.DataType_FloatVector},
	}})
	require.NoError(t, err)
	key := qviews.QueryViewKey{}
	view := &viewpb.QueryViewOfQueryNode{Partitions: []*viewpb.QueryViewOfPartition{{
		SegmentIds: []int64{1, 2, 3, 4}, SegmentManifestVersions: []int64{11, 22, 33, 44},
	}}}
	// Segment 1 has range-only statistics after Bloom filter decoding fell back.
	// Segment 2 spans -1, but its real Bloom filter contains only -2 and 2.
	point := pkoracle.NewBloomFilterSet(1, 0, commonpb.SegmentState_Sealed)
	point.AddHistoricalStats(&storage.PkStatistics{
		PkFilter: bloomfilter.AlwaysTrueBloomFilter,
		MinPK:    storage.NewInt64PrimaryKey(-1), MaxPK: storage.NewInt64PrimaryKey(-1),
	})
	spanning := pkoracle.NewBloomFilterSet(2, 0, commonpb.SegmentState_Sealed)
	history := &storage.PkStatistics{PkFilter: bloomfilter.NewBloomFilterWithType(1024, 0.001, bloomfilter.BasicBFName)}
	require.NoError(t, history.UpdatePKRange(&storage.Int64FieldData{Data: []int64{-2, 2}}))
	spanning.AddHistoricalStats(history)
	binding := func(candidate pkoracle.Candidate) *segmentBinding {
		return &segmentBinding{prepared: &preparedSegmentRead{read: pkCandidateRead{candidate: candidate}}}
	}
	bindings := map[int64]*segmentBinding{
		1: binding(point), 2: binding(spanning), 3: binding(nil),
		4: binding(pkoracle.NewBloomFilterSet(4, 0, commonpb.SegmentState_Sealed)),
	}
	manager := &SegmentResourceManager{views: map[qviews.QueryViewKey]*viewResources{key: {bindings: bindings}}}
	h := NewQNQueryViewHandler(manager)
	scope := optimizer.LocalSegmentScope{Key: key, Partitions: view.GetPartitions()}
	prune := func(req *internalpb.RetrieveRequest) *viewpb.QueryViewOfQueryNode {
		optimization, err := h.localOptimizer.OptimizeRetrieve(t.Context(), req, scope)
		require.NoError(t, err)
		return qviews.FilterSelectedSegments(view, optimization.SegmentSelection)
	}
	request := func(expr string, hint int32) *internalpb.RetrieveRequest {
		plan, err := planparserv2.CreateRetrievePlan(schema, expr, nil)
		require.NoError(t, err)
		data, err := proto.Marshal(plan)
		require.NoError(t, err)
		return &internalpb.RetrieveRequest{SerializedExprPlan: data, PkFilter: hint}
	}
	for _, tc := range []struct {
		expr string
		hint int32
		want []int64
	}{
		{"id == -1", common.PkFilterHasPkFilter, []int64{1, 3, 4}},
		{"id IN [-1]", common.PkFilterNotChecked, []int64{1, 3, 4}},
		{"id <= -2 and other > 0", common.PkFilterHasPkFilter, []int64{2, 3, 4}},
		{"id == 9", common.PkFilterHasPkFilter, []int64{3, 4}},
		{"id == -1 or other > 0", common.PkFilterHasPkFilter, []int64{1, 2, 3, 4}},
		{"id == -1", common.PkFilterNoPkFilter, []int64{1, 2, 3, 4}},
	} {
		req := request(tc.expr, tc.hint)
		got := prune(req)
		require.Equal(t, tc.want, got.Partitions[0].SegmentIds)

		searchPlan, err := planparserv2.CreateSearchPlan(schema, tc.expr, "vec", nil, nil, nil)
		require.NoError(t, err)
		data, err := proto.Marshal(searchPlan)
		require.NoError(t, err)
		optimization, err := h.localOptimizer.OptimizeSearch(t.Context(), &internalpb.SearchRequest{SerializedExprPlan: data, PkFilter: tc.hint}, scope)
		require.NoError(t, err)
		require.Equal(t, tc.want, qviews.FilterSelectedSegments(view, optimization.SegmentSelection).Partitions[0].SegmentIds)
	}
	req := request("id == -1", common.PkFilterHasPkFilter)
	got := prune(req)
	require.Equal(t, []int64{11, 33, 44}, got.Partitions[0].SegmentManifestVersions)
	require.Equal(t, []int64{1, 2, 3, 4}, view.Partitions[0].SegmentIds)
	require.Same(t, view, prune(&internalpb.RetrieveRequest{SerializedExprPlan: []byte{0xff}}))
	delete(manager.views, key)
	require.Same(t, view, prune(req))
	manager.views[key] = &viewResources{bindings: bindings}
	paramtable.Get().Save(switchKey, "false")
	require.Same(t, view, prune(req))
}
