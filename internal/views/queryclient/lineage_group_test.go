package queryclient

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	commonpb "github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/views/queryclient/resolver"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	lineageS  = "by-dev-rootcoord-dml_0_100v0"
	lineageT1 = "by-dev-rootcoord-dml_1_100v1"
	lineageT2 = "by-dev-rootcoord-dml_2_100v2"
	plainA    = "by-dev-rootcoord-dml_3_100v3"

	lineageSwitch = uint64(100)
)

// lineageShards is the serving list [S, T1, T2] of a collection whose shard S
// was split at tick 100.
func lineageShards() []resolver.Shard {
	return []resolver.Shard{
		{VChannel: lineageS, Lineage: &resolver.ShardLineage{VChannels: []string{lineageT1, lineageT2}, SwitchTimeTick: lineageSwitch}},
		{VChannel: lineageT1},
		{VChannel: lineageT2},
	}
}

type lineageTestResolver struct {
	legacyResolver
	shards []resolver.Shard
}

func (r *lineageTestResolver) ResolveShards(context.Context, int64) ([]resolver.Shard, error) {
	return r.shards, nil
}

// lineageTestSN stands for the StreamingNodes of all shards: it answers the
// prefix sampling and Phase 1 the way the WAL adaptor does, and records every
// request in arrival order.
type lineageTestSN struct {
	QueryPlanClient
	mu sync.Mutex
	// What each target reports before the query: through and the Strong minimum.
	prefixes   map[string]*viewpb.GetMVCCTimestampResponse
	prefixErrs map[string][]error
	// Frontiers of each target at the barrier. Missing: (barrier, barrier),
	// which is what the adaptor returns after it had to sync up to the barrier.
	frontiers map[string]*viewpb.QueryPlanMVCC
	planErrs  map[string][]error
	queryNode bool // plans name one QueryNode
	streaming bool // plans also name the StreamingNode

	prefixReqs map[string][]*viewpb.GetMVCCTimestampRequest
	planReqs   map[string][]*viewpb.GetQueryPlanRequest
	events     []string
}

func newLineageTestSN() *lineageTestSN {
	return &lineageTestSN{
		prefixes: make(map[string]*viewpb.GetMVCCTimestampResponse), prefixErrs: make(map[string][]error),
		frontiers: make(map[string]*viewpb.QueryPlanMVCC), planErrs: make(map[string][]error), queryNode: true,
		prefixReqs: make(map[string][]*viewpb.GetMVCCTimestampRequest), planReqs: make(map[string][]*viewpb.GetQueryPlanRequest),
	}
}

func (c *lineageTestSN) setPrefix(vchannel string, through, strongMinimum uint64) {
	c.prefixes[vchannel] = &viewpb.GetMVCCTimestampResponse{Through: through, StrongMinimum: strongMinimum}
}

func (c *lineageTestSN) GetMVCCTimestamp(_ context.Context, shardID qviews.ShardID, req *viewpb.GetMVCCTimestampRequest) (*viewpb.GetMVCCTimestampResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, "prefix:"+shardID.VChannel)
	c.prefixReqs[shardID.VChannel] = append(c.prefixReqs[shardID.VChannel], proto.Clone(req).(*viewpb.GetMVCCTimestampRequest))
	if errs := c.prefixErrs[shardID.VChannel]; len(errs) > 0 {
		c.prefixErrs[shardID.VChannel] = errs[1:]
		return nil, errs[0]
	}
	return c.prefixes[shardID.VChannel], nil
}

func (c *lineageTestSN) GetQueryPlan(_ context.Context, shardID qviews.ShardID, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	vchannel := shardID.VChannel
	c.events = append(c.events, "plan:"+vchannel)
	c.planReqs[vchannel] = append(c.planReqs[vchannel], proto.Clone(req).(*viewpb.GetQueryPlanRequest))
	if errs := c.planErrs[vchannel]; len(errs) > 0 {
		c.planErrs[vchannel] = errs[1:]
		return nil, errs[0]
	}
	var mvcc *viewpb.QueryPlanMVCC
	switch source := req.GetMvcc().(type) {
	case *viewpb.GetQueryPlanRequest_BarrierMvcc:
		mvcc = c.frontiers[vchannel]
		if mvcc == nil {
			mvcc = &viewpb.QueryPlanMVCC{GrowingTimetick: source.BarrierMvcc, TransformingTimetick: source.BarrierMvcc}
		}
	case *viewpb.GetQueryPlanRequest_QueryPlanMvcc:
		// A proxy-provided MVCC is used as is.
		mvcc = source.QueryPlanMvcc
	default:
		mvcc = &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 90}
	}
	plan := &viewpb.QueryPlan{ShardId: (&qviews.ShardID{ReplicaID: 1, VChannel: vchannel}).IntoProto(), Version: &viewpb.QueryViewVersion{QueryVersion: 1}, Mvcc: mvcc}
	response := &viewpb.GetQueryPlanResponse{Plan: plan}
	if c.queryNode {
		plan.WorkNodes = append(plan.WorkNodes, &viewpb.QueryPlanWorkNode{Node: &viewpb.QueryPlanWorkNode_QueryNode{QueryNode: &viewpb.QueryWorkNode{NodeId: 11}}})
	}
	switch request := req.GetRequest().(type) {
	case *viewpb.GetQueryPlanRequest_LegacySearchRequest:
		plan.Request = &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: request.LegacySearchRequest}
	case *viewpb.GetQueryPlanRequest_LegacyRetrieveRequest:
		plan.Request = &viewpb.QueryPlan_LegacyRetrieveRequest{LegacyRetrieveRequest: request.LegacyRetrieveRequest}
	case *viewpb.GetQueryPlanRequest_StatisticsRequest:
		plan.Request = &viewpb.QueryPlan_StatisticsRequest{StatisticsRequest: request.StatisticsRequest}
		response.GrowingInlineResponse = &viewpb.GrowingInlineResponse{Response: &viewpb.GrowingInlineResponse_StatisticsResponse{StatisticsResponse: &viewpb.StatisticsOnViewResponse{RowCount: 1}}}
	}
	if c.streaming && req.GetStatisticsRequest() == nil {
		plan.WorkNodes = append(plan.WorkNodes, &viewpb.QueryPlanWorkNode{Node: &viewpb.QueryPlanWorkNode_StreamingNode{StreamingNode: &viewpb.StreamingWorkNode{Pchannel: "p"}}})
	}
	return response, nil
}

func (c *lineageTestSN) planRequests(vchannel string) []*viewpb.GetQueryPlanRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.planReqs[vchannel])
}

func (c *lineageTestSN) eventLog() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.events)
}

func (c *lineageTestSN) prefixCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, reqs := range c.prefixReqs {
		n += len(reqs)
	}
	return n
}

type lineageWorkerCall struct {
	streaming bool
	search    *viewpb.SearchOnViewRequest
	query     *viewpb.QueryOnViewRequest
	stats     *viewpb.StatisticsOnViewRequest
}

// lineageTestWorkers stands for the work nodes of Phase 2.
type lineageTestWorkers struct {
	ViewQueryServiceClient
	mu    sync.Mutex
	calls map[string][]lineageWorkerCall
}

func newLineageTestWorkers() *lineageTestWorkers {
	return &lineageTestWorkers{calls: make(map[string][]lineageWorkerCall)}
}

func (c *lineageTestWorkers) record(vchannel string, node qviews.WorkNode, call lineageWorkerCall) {
	c.mu.Lock()
	defer c.mu.Unlock()
	call.streaming = node.NodeType() == qviews.NodeTypeStreamingNode
	c.calls[vchannel] = append(c.calls[vchannel], call)
}

func (c *lineageTestWorkers) SearchOnView(_ context.Context, node qviews.WorkNode, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
	c.record(req.GetShardId().GetVchannel(), node, lineageWorkerCall{search: req})
	return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{Status: merr.Success()}}, nil
}

func (c *lineageTestWorkers) QueryOnView(_ context.Context, node qviews.WorkNode, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
	c.record(req.GetShardId().GetVchannel(), node, lineageWorkerCall{query: req})
	return &viewpb.QueryOnViewResponse{LegacyResults: &internalpb.RetrieveResults{Status: merr.Success()}}, nil
}

func (c *lineageTestWorkers) StatisticsOnView(_ context.Context, node qviews.WorkNode, req *viewpb.StatisticsOnViewRequest) (*viewpb.StatisticsOnViewResponse, error) {
	c.record(req.GetShardId().GetVchannel(), node, lineageWorkerCall{stats: req})
	return &viewpb.StatisticsOnViewResponse{RowCount: 10}, nil
}

func (c *lineageTestWorkers) of(vchannel string) []lineageWorkerCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.calls[vchannel])
}

func newLineageTestClient(sn *lineageTestSN, workers *lineageTestWorkers, shards []resolver.Shard) *legacyClient {
	return newLegacyClient(ViewQueryClientConfig{MaxRetries: 2}, sn, workers, &lineageTestResolver{shards: shards})
}

func requireBefore(t *testing.T, events []string, first, second string) {
	t.Helper()
	i, j := slices.Index(events, first), slices.Index(events, second)
	require.GreaterOrEqual(t, i, 0, "%s missing in %v", first, events)
	require.GreaterOrEqual(t, j, 0, "%s missing in %v", second, events)
	require.Less(t, i, j, "%s must precede %s in %v", first, second, events)
}

func sourceMVCC(readPoint, x1, x2 uint64) *viewpb.QueryPlanMVCC {
	return &viewpb.QueryPlanMVCC{
		GrowingTimetick: lineageSwitch, TransformingTimetick: lineageSwitch, ReadTimetick: readPoint,
		LineageTransformingTimeticks: map[string]uint64{lineageT1: x1, lineageT2: x2},
	}
}

// The worked example of the design: T_switch = 100, an upsert on T1 at 110, an
// insert on T2 at 120, a delete on T2 at 130; a Strong query arrives with
// through_1 = 125, through_2 = 135 and nothing pending.
func TestLineageGroupWorkedExample(t *testing.T) {
	setupTestReplicaRouting(t)
	sn, workers := newLineageTestSN(), newLineageTestWorkers()
	sn.setPrefix(lineageT1, 125, 125)
	sn.setPrefix(lineageT2, 135, 135)
	sn.frontiers[lineageT1] = &viewpb.QueryPlanMVCC{GrowingTimetick: 110, TransformingTimetick: 110}
	sn.frontiers[lineageT2] = &viewpb.QueryPlanMVCC{GrowingTimetick: 130, TransformingTimetick: 130}
	client := newLineageTestClient(sn, workers, lineageShards())

	result, err := client.Search(context.Background(), &LegacySearchRequest{
		Req: &internalpb.SearchRequest{CollectionID: 100, ConsistencyLevel: commonpb.ConsistencyLevel_Strong},
	})
	require.NoError(t, err)

	// Step 1: both targets are asked for their prefix, without waiting.
	for _, target := range []string{lineageT1, lineageT2} {
		require.Len(t, sn.prefixReqs[target], 1)
		require.True(t, sn.prefixReqs[target][0].GetPrefixOnly())
		require.Equal(t, target, sn.prefixReqs[target][0].GetVchannel())
	}
	require.Empty(t, sn.prefixReqs[lineageS])

	// Steps 2 and 3: B = 135; T1 and T2 plan with the barrier, so T1 syncs up to 135.
	for _, target := range []string{lineageT1, lineageT2} {
		reqs := sn.planRequests(target)
		require.Len(t, reqs, 1)
		require.EqualValues(t, 135, reqs[0].GetBarrierMvcc())
	}

	// Step 4: S plans with the proxy-provided pair (100, 100); step 5: its plan
	// carries B and {T1: 110, T2: 130}.
	sourceReqs := sn.planRequests(lineageS)
	require.Len(t, sourceReqs, 1)
	want := sourceMVCC(135, 110, 130)
	require.True(t, proto.Equal(want, sourceReqs[0].GetQueryPlanMvcc()), "%v", sourceReqs[0].GetQueryPlanMvcc())

	// S plans only after both targets returned their frontiers at B.
	events := sn.eventLog()
	for _, target := range []string{lineageT1, lineageT2} {
		requireBefore(t, events, "prefix:"+target, "plan:"+target)
		requireBefore(t, events, "plan:"+target, "plan:"+lineageS)
	}

	// Phase 2 on S: every holder of S's rows gets B and the per-stream ticks, and reads at B.
	sourceCalls := workers.of(lineageS)
	require.Len(t, sourceCalls, 1)
	require.True(t, proto.Equal(want, sourceCalls[0].search.GetMvcc()))
	require.EqualValues(t, 135, sourceCalls[0].search.GetLegacyReq().GetMvccTimestamp())
	// Phase 2 on the targets is what their own Phase 1 returned.
	for target, frontier := range map[string]uint64{lineageT1: 110, lineageT2: 130} {
		calls := workers.of(target)
		require.Len(t, calls, 1)
		require.True(t, proto.Equal(sn.frontiers[target], calls[0].search.GetMvcc()))
		require.EqualValues(t, frontier, calls[0].search.GetLegacyReq().GetMvccTimestamp())
	}

	// One plan per shard, in serving-list order; S reports B as its snapshot.
	require.Len(t, result.Plans, 3)
	require.Equal(t, []string{lineageS, lineageT1, lineageT2}, []string{result.Plans[0].ShardID.VChannel, result.Plans[1].ShardID.VChannel, result.Plans[2].ShardID.VChannel})
	require.Len(t, result.Results, 3)
	require.Equal(t, map[string]uint64{lineageS: 135}, result.Results[0].GetChannelsMvcc())
	require.Equal(t, map[string]uint64{lineageT1: 110}, result.Results[1].GetChannelsMvcc())
}

// The case from the design discussion: a delete on T1 was allocated at 127 and
// completed, but is not published when the query arrives. through_1 is still
// 125; the Strong minimum of T1 is 127.
func TestLineageGroupStrongIncludesPendingDelete(t *testing.T) {
	setupTestReplicaRouting(t)
	for name, tc := range map[string]struct {
		through2, strong2 uint64
		readPoint         uint64
	}{
		// With through_2 = 135 the other target already forces B past 127.
		"through_2 = 135": {135, 135, 135},
		// With an idle T2 at 120 only the Strong minimum of T1 pushes B to 127:
		// min(through) would have been 120 and excluded the completed delete.
		"through_2 = 120": {120, 120, 127},
	} {
		sn, workers := newLineageTestSN(), newLineageTestWorkers()
		sn.setPrefix(lineageT1, 125, 127)
		sn.setPrefix(lineageT2, tc.through2, tc.strong2)
		// T1 has to publish the delete at 127 before it can answer the barrier.
		sn.frontiers[lineageT1] = &viewpb.QueryPlanMVCC{GrowingTimetick: 127, TransformingTimetick: 127}
		client := newLineageTestClient(sn, workers, lineageShards())

		_, err := client.Query(context.Background(), &LegacyQueryRequest{
			Req: &internalpb.RetrieveRequest{CollectionID: 100, ConsistencyLevel: commonpb.ConsistencyLevel_Strong},
		})
		require.NoError(t, err, name)
		for _, target := range []string{lineageT1, lineageT2} {
			require.EqualValues(t, tc.readPoint, sn.planRequests(target)[0].GetBarrierMvcc(), name)
		}
		// T2 had nothing to return but the barrier itself.
		want := sourceMVCC(tc.readPoint, 127, tc.readPoint)
		require.True(t, proto.Equal(want, sn.planRequests(lineageS)[0].GetQueryPlanMvcc()), name)
		sourceCalls := workers.of(lineageS)
		require.Len(t, sourceCalls, 1, name)
		require.True(t, proto.Equal(want, sourceCalls[0].query.GetMvcc()), name)
		require.EqualValues(t, tc.readPoint, sourceCalls[0].query.GetLegacyReq().GetMvccTimestamp(), name)
	}
}

// The lower bound of B per consistency level.
func TestLineageReadPointLowerBounds(t *testing.T) {
	shards := lineageShards()
	groups, _, err := groupShards(shards)
	require.NoError(t, err)
	for name, tc := range map[string]struct {
		request     requestReadPoint
		readPoint   uint64
		prefixCalls int
	}{
		// through_1 = 125 (Strong minimum 127), through_2 = 135 (Strong minimum 135).
		"strong: larger Strong minimum":           {requestReadPoint{consistencyLevel: commonpb.ConsistencyLevel_Strong}, 135, 2},
		"session: guarantee above both targets":   {requestReadPoint{consistencyLevel: commonpb.ConsistencyLevel_Session, guaranteeTimestamp: 999}, 999, 2},
		"session: guarantee below strong minimum": {requestReadPoint{consistencyLevel: commonpb.ConsistencyLevel_Session, guaranteeTimestamp: 120}, 135, 2},
		"bounded: guarantee above min through":    {requestReadPoint{consistencyLevel: commonpb.ConsistencyLevel_Bounded, guaranteeTimestamp: 130}, 130, 2},
		"bounded: guarantee below min through":    {requestReadPoint{consistencyLevel: commonpb.ConsistencyLevel_Bounded, guaranteeTimestamp: 90}, 125, 2},
		"eventually: no lower bound":              {requestReadPoint{consistencyLevel: commonpb.ConsistencyLevel_Eventually, guaranteeTimestamp: 999}, 125, 2},
		"customized: guarantee newer than both":   {requestReadPoint{consistencyLevel: commonpb.ConsistencyLevel_Customized, guaranteeTimestamp: 200}, 200, 2},
		"customized: guarantee older":             {requestReadPoint{consistencyLevel: commonpb.ConsistencyLevel_Customized, guaranteeTimestamp: 50}, 125, 2},
		"fixed point (iterator, delete by expr)":  {requestReadPoint{consistencyLevel: commonpb.ConsistencyLevel_Bounded, guaranteeTimestamp: 999, barrierMVCC: 140}, 140, 0},
		"fixed point below the published prefix":  {requestReadPoint{barrierMVCC: 110}, 110, 0},
		"statistics without barrier: strong":      {requestReadPoint{}, 135, 2},
	} {
		sn := newLineageTestSN()
		sn.setPrefix(lineageT1, 125, 127)
		sn.setPrefix(lineageT2, 135, 135)
		client := newShardViewQueryClient(2, sn, newLineageTestWorkers())
		got, err := client.lineageReadPoint(context.Background(), shards, groups[0], tc.request)
		require.NoError(t, err, name)
		require.Equal(t, tc.readPoint, got, name)
		require.Equal(t, tc.prefixCalls, sn.prefixCalls(), name)
	}

	// A frontier pair cannot describe the one read point of a lineage group.
	client := newShardViewQueryClient(2, newLineageTestSN(), newLineageTestWorkers())
	_, err = client.lineageReadPoint(context.Background(), shards, groups[0], requestReadPoint{mvcc: &viewpb.QueryPlanMVCC{GrowingTimetick: 1, TransformingTimetick: 1}})
	require.Error(t, err)
	// Targets without any published prefix cannot fix a read point.
	sn := newLineageTestSN()
	sn.setPrefix(lineageT1, 0, 0)
	sn.setPrefix(lineageT2, 0, 0)
	_, err = newShardViewQueryClient(2, sn, newLineageTestWorkers()).lineageReadPoint(context.Background(), shards, groups[0], requestReadPoint{consistencyLevel: commonpb.ConsistencyLevel_Eventually})
	require.Error(t, err)
}

// Iterator pages, statistics and delete by expression arrive with a fixed
// point: it is B for the whole group and no prefix is sampled.
func TestLineageGroupUsesFixedPointAsReadPoint(t *testing.T) {
	setupTestReplicaRouting(t)
	sn, workers := newLineageTestSN(), newLineageTestWorkers()
	sn.frontiers[lineageT1] = &viewpb.QueryPlanMVCC{GrowingTimetick: 110, TransformingTimetick: 110}
	client := newLineageTestClient(sn, workers, lineageShards())

	_, err := client.Query(context.Background(), &LegacyQueryRequest{
		Req:         &internalpb.RetrieveRequest{CollectionID: 100, ConsistencyLevel: commonpb.ConsistencyLevel_Bounded, GuaranteeTimestamp: 999},
		BarrierMVCC: 140,
	})
	require.NoError(t, err)
	require.Zero(t, sn.prefixCalls())
	require.EqualValues(t, 140, sn.planRequests(lineageT1)[0].GetBarrierMvcc())
	require.EqualValues(t, 140, sn.planRequests(lineageT2)[0].GetBarrierMvcc())
	require.True(t, proto.Equal(sourceMVCC(140, 110, 140), sn.planRequests(lineageS)[0].GetQueryPlanMvcc()))

	// A fixed frontier pair is refused for a list with lineage, before any RPC.
	sn, workers = newLineageTestSN(), newLineageTestWorkers()
	client = newLineageTestClient(sn, workers, lineageShards())
	_, err = client.Search(context.Background(), &LegacySearchRequest{
		Req:  &internalpb.SearchRequest{CollectionID: 100},
		MVCC: &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 90},
	})
	require.Error(t, err)
	require.Empty(t, sn.eventLog())
}

func TestLineageGroupStatistics(t *testing.T) {
	setupTestReplicaRouting(t)
	for name, tc := range map[string]struct {
		barrier, readPoint uint64
		prefixCalls        int
	}{
		"strong sampling": {0, 135, 2},
		"fixed barrier":   {140, 140, 0},
	} {
		sn, workers := newLineageTestSN(), newLineageTestWorkers()
		sn.setPrefix(lineageT1, 125, 125)
		sn.setPrefix(lineageT2, 135, 135)
		sn.frontiers[lineageT1] = &viewpb.QueryPlanMVCC{GrowingTimetick: 110, TransformingTimetick: 110}
		client := newLineageTestClient(sn, workers, lineageShards())

		rows, err := client.Statistics(context.Background(), 100, &viewpb.StatisticsRequest{}, tc.barrier)
		require.NoError(t, err, name)
		// Per shard: one growing row counted in Phase 1 and ten on its QueryNode.
		require.EqualValues(t, 33, rows, name)
		require.Equal(t, tc.prefixCalls, sn.prefixCalls(), name)
		require.EqualValues(t, tc.readPoint, sn.planRequests(lineageT1)[0].GetBarrierMvcc(), name)
		want := sourceMVCC(tc.readPoint, 110, tc.readPoint)
		require.True(t, proto.Equal(want, sn.planRequests(lineageS)[0].GetQueryPlanMvcc()), name)
		sourceCalls := workers.of(lineageS)
		require.Len(t, sourceCalls, 1, name)
		require.True(t, proto.Equal(want, sourceCalls[0].stats.GetMvcc()), name)
	}
}

// Ordinary shards of the same list keep today's path next to a lineage group,
// and both node kinds of S get the plan MVCC.
func TestLineageGroupNextToOrdinaryShard(t *testing.T) {
	setupTestReplicaRouting(t)
	sn, workers := newLineageTestSN(), newLineageTestWorkers()
	sn.streaming = true
	sn.setPrefix(lineageT1, 125, 125)
	sn.setPrefix(lineageT2, 135, 135)
	sn.frontiers[lineageT1] = &viewpb.QueryPlanMVCC{GrowingTimetick: 120, TransformingTimetick: 110}
	shards := append([]resolver.Shard{{VChannel: plainA}}, lineageShards()...)
	client := &viewQueryClientImpl{shardClient: newShardViewQueryClient(2, sn, workers)}

	req := &internalpb.SearchRequest{CollectionID: 100, ConsistencyLevel: commonpb.ConsistencyLevel_Bounded, GuaranteeTimestamp: 90}
	plans, err := client.searchAllShards(context.Background(), shards, req, nil, 0, fakeSearchResultReducer{})
	require.NoError(t, err)
	require.Len(t, plans, 4)
	require.Equal(t, plainA, plans[0].ShardID.VChannel)
	require.Equal(t, lineageS, plans[1].ShardID.VChannel)

	// The ordinary shard samples by consistency level, as it always did.
	ordinary := sn.planRequests(plainA)
	require.Len(t, ordinary, 1)
	require.Equal(t, commonpb.ConsistencyLevel_Bounded, ordinary[0].GetConsistencyLevel())
	require.Zero(t, ordinary[0].GetBarrierMvcc())
	require.Nil(t, ordinary[0].GetQueryPlanMvcc())
	// Bounded: B = max(90, min(125, 135)) = 125.
	require.EqualValues(t, 125, sn.planRequests(lineageT1)[0].GetBarrierMvcc())
	want := sourceMVCC(125, 110, 125)
	require.True(t, proto.Equal(want, plans[1].Mvcc))

	// On S the QueryNode reads at B. The StreamingNode keeps the growing
	// frontier: its residual rows are the subject of the growing runtime.
	for _, call := range workers.of(lineageS) {
		require.True(t, proto.Equal(want, call.search.GetMvcc()))
		if call.streaming {
			require.EqualValues(t, lineageSwitch, call.search.GetLegacyReq().GetMvccTimestamp())
		} else {
			require.EqualValues(t, 125, call.search.GetLegacyReq().GetMvccTimestamp())
		}
	}
	require.Len(t, workers.of(lineageS), 2)
	// On a target each node kind reads at its own frontier, as today.
	for _, call := range workers.of(lineageT1) {
		if call.streaming {
			require.EqualValues(t, 120, call.search.GetLegacyReq().GetMvccTimestamp())
		} else {
			require.EqualValues(t, 110, call.search.GetLegacyReq().GetMvccTimestamp())
		}
	}

	// The retrieve fan-out of the native client takes the same path.
	sn, workers = newLineageTestSN(), newLineageTestWorkers()
	sn.setPrefix(lineageT1, 125, 125)
	sn.setPrefix(lineageT2, 135, 135)
	client = &viewQueryClientImpl{shardClient: newShardViewQueryClient(2, sn, workers)}
	queryPlans, err := client.queryAllShards(context.Background(), shards, &internalpb.RetrieveRequest{CollectionID: 100, ConsistencyLevel: commonpb.ConsistencyLevel_Eventually}, nil, 0, newLegacyQueryCollector())
	require.NoError(t, err)
	require.True(t, proto.Equal(sourceMVCC(125, 125, 125), queryPlans[1].Mvcc))
	require.Equal(t, commonpb.ConsistencyLevel_Eventually, sn.planRequests(plainA)[0].GetConsistencyLevel())
}

// Without lineage in the serving list nothing changes: the same RPCs with the
// same bytes, and no prefix sampling, whichever resolver supplies the list.
func TestServingListWithoutLineageSendsUnchangedRequests(t *testing.T) {
	setupTestReplicaRouting(t)
	marshal := func(m proto.Message) []byte {
		data, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
		require.NoError(t, err)
		return data
	}
	vchannels := []string{plainA, lineageT1}
	search := &internalpb.SearchRequest{CollectionID: 100, ConsistencyLevel: commonpb.ConsistencyLevel_Strong, PartitionIDs: []int64{7}, Topk: 3, GuaranteeTimestamp: 55}
	retrieve := &internalpb.RetrieveRequest{CollectionID: 100, ConsistencyLevel: commonpb.ConsistencyLevel_Bounded, PartitionIDs: []int64{7}, Limit: 3, GuaranteeTimestamp: 55}
	planMVCC := &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 90}

	for name, shardResolver := range map[string]resolver.ShardResolver{
		"resolver without lineage support":       &legacyResolver{vchannels: vchannels},
		"lineage resolver, list without lineage": &lineageTestResolver{shards: []resolver.Shard{{VChannel: plainA}, {VChannel: lineageT1}}},
	} {
		sn, workers := newLineageTestSN(), newLineageTestWorkers()
		client := newLegacyClient(ViewQueryClientConfig{MaxRetries: 2}, sn, workers, shardResolver)

		_, err := client.Search(context.Background(), &LegacySearchRequest{Req: search})
		require.NoError(t, err, name)
		_, err = client.Query(context.Background(), &LegacyQueryRequest{Req: retrieve})
		require.NoError(t, err, name)
		_, err = client.Query(context.Background(), &LegacyQueryRequest{Req: retrieve, BarrierMVCC: 77})
		require.NoError(t, err, name)
		_, err = client.Statistics(context.Background(), 100, &viewpb.StatisticsRequest{PartitionIds: []int64{7}}, 0)
		require.NoError(t, err, name)
		require.Zero(t, sn.prefixCalls(), name)

		for _, vchannel := range vchannels {
			shard := &viewpb.ShardID{ReplicaId: 1, Vchannel: vchannel}
			// Phase 1, exactly as the per-shard path builds it.
			wantPlans := []*viewpb.GetQueryPlanRequest{
				{
					CollectionId: 100, ShardId: shard, PartitionIds: []int64{7},
					Mvcc:    &viewpb.GetQueryPlanRequest_ConsistencyLevel{ConsistencyLevel: commonpb.ConsistencyLevel_Strong},
					Request: &viewpb.GetQueryPlanRequest_LegacySearchRequest{LegacySearchRequest: search},
				},
				{
					CollectionId: 100, ShardId: shard, PartitionIds: []int64{7},
					Mvcc:    &viewpb.GetQueryPlanRequest_ConsistencyLevel{ConsistencyLevel: commonpb.ConsistencyLevel_Bounded},
					Request: &viewpb.GetQueryPlanRequest_LegacyRetrieveRequest{LegacyRetrieveRequest: retrieve},
				},
				{
					CollectionId: 100, ShardId: shard, PartitionIds: []int64{7},
					Mvcc:    &viewpb.GetQueryPlanRequest_BarrierMvcc{BarrierMvcc: 77},
					Request: &viewpb.GetQueryPlanRequest_LegacyRetrieveRequest{LegacyRetrieveRequest: retrieve},
				},
				{
					CollectionId: 100, ShardId: shard,
					Mvcc:    &viewpb.GetQueryPlanRequest_ConsistencyLevel{ConsistencyLevel: commonpb.ConsistencyLevel_Strong},
					Request: &viewpb.GetQueryPlanRequest_StatisticsRequest{StatisticsRequest: &viewpb.StatisticsRequest{PartitionIds: []int64{7}}},
				},
			}
			gotPlans := sn.planRequests(vchannel)
			require.Len(t, gotPlans, len(wantPlans), name)
			for i := range wantPlans {
				require.Equal(t, marshal(wantPlans[i]), marshal(gotPlans[i]), "%s: plan request %d of %s", name, i, vchannel)
			}

			// Phase 2, exactly as the per-shard path builds it.
			calls := workers.of(vchannel)
			require.Len(t, calls, 4, name)
			wantSearch := proto.Clone(search).(*internalpb.SearchRequest)
			wantSearch.MvccTimestamp = 90
			require.Equal(t, marshal(&viewpb.SearchOnViewRequest{
				LegacyReq: wantSearch, ShardId: shard, Version: &viewpb.QueryViewVersion{QueryVersion: 1}, Mvcc: planMVCC,
			}), marshal(calls[0].search), name)
			wantRetrieve := proto.Clone(retrieve).(*internalpb.RetrieveRequest)
			wantRetrieve.MvccTimestamp = 90
			require.Equal(t, marshal(&viewpb.QueryOnViewRequest{
				LegacyReq: wantRetrieve, ShardId: shard, Version: &viewpb.QueryViewVersion{QueryVersion: 1}, Mvcc: planMVCC,
			}), marshal(calls[1].query), name)
			wantRetrieve.MvccTimestamp = 77
			require.Equal(t, marshal(&viewpb.QueryOnViewRequest{
				LegacyReq: wantRetrieve, ShardId: shard, Version: &viewpb.QueryViewVersion{QueryVersion: 1},
				Mvcc: &viewpb.QueryPlanMVCC{GrowingTimetick: 77, TransformingTimetick: 77},
			}), marshal(calls[2].query), name)
			require.Equal(t, marshal(&viewpb.StatisticsOnViewRequest{
				ShardId: shard, Version: &viewpb.QueryViewVersion{QueryVersion: 1}, Mvcc: planMVCC, PartitionIds: []int64{7},
			}), marshal(calls[3].stats), name)
		}
	}
}

func TestLineageGroupFailures(t *testing.T) {
	setupTestReplicaRouting(t)
	query := func(sn *lineageTestSN) error {
		client := newLineageTestClient(sn, newLineageTestWorkers(), lineageShards())
		_, err := client.Query(context.Background(), &LegacyQueryRequest{
			Req: &internalpb.RetrieveRequest{CollectionID: 100, ConsistencyLevel: commonpb.ConsistencyLevel_Strong},
		})
		return err
	}
	prepared := func() *lineageTestSN {
		sn := newLineageTestSN()
		sn.setPrefix(lineageT1, 125, 125)
		sn.setPrefix(lineageT2, 135, 135)
		return sn
	}

	t.Run("a retryable prefix failure is retried", func(t *testing.T) {
		sn := prepared()
		sn.prefixErrs[lineageT1] = []error{viewerror.NewOnShutdownError("wal is moving")}
		require.NoError(t, query(sn))
		require.Len(t, sn.prefixReqs[lineageT1], 2)
		require.EqualValues(t, 135, sn.planRequests(lineageT1)[0].GetBarrierMvcc())
	})

	t.Run("a persistent prefix failure fails the query before any plan", func(t *testing.T) {
		sn := prepared()
		sn.prefixErrs[lineageT1] = []error{viewerror.NewOnShutdownError("wal is moving"), viewerror.NewOnShutdownError("wal is moving")}
		require.Error(t, query(sn))
		require.Len(t, sn.prefixReqs[lineageT1], 2)
		for _, vchannel := range []string{lineageS, lineageT1, lineageT2} {
			require.Empty(t, sn.planRequests(vchannel))
		}
	})

	t.Run("a non-retryable prefix failure is not retried", func(t *testing.T) {
		sn := prepared()
		sn.prefixErrs[lineageT2] = []error{errors.New("boom")}
		require.Error(t, query(sn))
		require.Len(t, sn.prefixReqs[lineageT2], 1)
		require.Empty(t, sn.planRequests(lineageS))
	})

	t.Run("S is not planned when a target cannot establish the read point", func(t *testing.T) {
		sn := prepared()
		sn.planErrs[lineageT2] = []error{errors.New("barrier failed")}
		require.Error(t, query(sn))
		require.Empty(t, sn.planRequests(lineageS))
	})

	t.Run("a target that replans keeps the read point and S plans once", func(t *testing.T) {
		sn := prepared()
		sn.planErrs[lineageT1] = []error{viewerror.NewViewInvalidated("view replaced")}
		require.NoError(t, query(sn))
		reqs := sn.planRequests(lineageT1)
		require.Len(t, reqs, 2)
		require.EqualValues(t, 135, reqs[0].GetBarrierMvcc())
		require.EqualValues(t, 135, reqs[1].GetBarrierMvcc())
		require.Len(t, sn.planRequests(lineageS), 1)
	})

	t.Run("S that replans keeps the read point and the target ticks", func(t *testing.T) {
		sn := prepared()
		sn.frontiers[lineageT1] = &viewpb.QueryPlanMVCC{GrowingTimetick: 110, TransformingTimetick: 110}
		sn.planErrs[lineageS] = []error{viewerror.NewViewInvalidated("view replaced")}
		require.NoError(t, query(sn))
		reqs := sn.planRequests(lineageS)
		require.Len(t, reqs, 2)
		require.True(t, proto.Equal(reqs[0].GetQueryPlanMvcc(), reqs[1].GetQueryPlanMvcc()))
		require.True(t, proto.Equal(sourceMVCC(135, 110, 135), reqs[1].GetQueryPlanMvcc()))
		require.Len(t, sn.planRequests(lineageT1), 1)
	})

	t.Run("cancellation ends the group", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		client := newShardViewQueryClient(2, prepared(), newLineageTestWorkers())
		_, err := client.targetPrefix(ctx, lineageT1)
		require.ErrorIs(t, err, context.Canceled)
		frontiers := newTargetFrontiers(1)
		_, err = frontiers.wait(ctx)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestTargetFrontiersKeepFirstPlan(t *testing.T) {
	frontiers := newTargetFrontiers(2)
	frontiers.observe(lineageT1, 110)
	// A replan of the same target must not replace what S may already use.
	frontiers.observe(lineageT1, 135)
	select {
	case <-frontiers.ready:
		t.Fatal("ready before every target planned")
	default:
	}
	frontiers.observe(lineageT2, 130)
	ticks, err := frontiers.wait(context.Background())
	require.NoError(t, err)
	require.Equal(t, map[string]uint64{lineageT1: 110, lineageT2: 130}, ticks)
}

func TestGroupShards(t *testing.T) {
	groups, ordinary, err := groupShards([]resolver.Shard{{VChannel: "a"}, {VChannel: "b"}})
	require.NoError(t, err)
	require.Empty(t, groups)
	require.Equal(t, []int{0, 1}, ordinary)

	groups, ordinary, err = groupShards(nil)
	require.NoError(t, err)
	require.Empty(t, groups)
	require.Empty(t, ordinary)

	lineage := func(tick uint64, targets ...string) *resolver.ShardLineage {
		return &resolver.ShardLineage{VChannels: targets, SwitchTimeTick: tick}
	}
	groups, ordinary, err = groupShards([]resolver.Shard{
		{VChannel: "a"},
		{VChannel: "t2"},
		{VChannel: "s", Lineage: lineage(100, "t1", "t2")},
		{VChannel: "t1"},
		{VChannel: "u", Lineage: lineage(200, "v")},
		{VChannel: "v"},
	})
	require.NoError(t, err)
	require.Equal(t, []int{0}, ordinary)
	require.Len(t, groups, 2)
	require.Equal(t, 2, groups[0].source)
	require.Equal(t, []int{3, 1}, groups[0].targets)
	require.Equal(t, 4, groups[1].source)
	require.Equal(t, []int{5}, groups[1].targets)

	for name, shards := range map[string][]resolver.Shard{
		"target not served":     {{VChannel: "s", Lineage: lineage(100, "t1", "t2")}, {VChannel: "t1"}},
		"no targets":            {{VChannel: "s", Lineage: lineage(100)}},
		"no switch tick":        {{VChannel: "s", Lineage: lineage(0, "t1")}, {VChannel: "t1"}},
		"own vchannel":          {{VChannel: "s", Lineage: lineage(100, "s")}},
		"target listed twice":   {{VChannel: "s", Lineage: lineage(100, "t1", "t1")}, {VChannel: "t1"}},
		"target of two sources": {{VChannel: "s", Lineage: lineage(100, "t1")}, {VChannel: "u", Lineage: lineage(100, "t1")}, {VChannel: "t1"}},
		"target with lineage":   {{VChannel: "s", Lineage: lineage(100, "t1")}, {VChannel: "t1", Lineage: lineage(200, "t3")}, {VChannel: "t3"}},
	} {
		_, _, err := groupShards(shards)
		require.Error(t, err, name)
	}

	// A malformed list fails the request before anything is sent.
	sn := newLineageTestSN()
	client := newLineageTestClient(sn, newLineageTestWorkers(), []resolver.Shard{{VChannel: "s", Lineage: lineage(100, "t1")}})
	_, err = client.Statistics(context.Background(), 100, &viewpb.StatisticsRequest{}, 0)
	require.Error(t, err)
	_, err = client.Search(context.Background(), &LegacySearchRequest{Req: &internalpb.SearchRequest{CollectionID: 100}})
	require.Error(t, err)
	_, err = client.Query(context.Background(), &LegacyQueryRequest{Req: &internalpb.RetrieveRequest{CollectionID: 100}})
	require.Error(t, err)
	malformed := []resolver.Shard{{VChannel: "s", Lineage: lineage(100, "t1")}}
	native := &viewQueryClientImpl{shardClient: newShardViewQueryClient(2, sn, newLineageTestWorkers())}
	_, err = native.searchAllShards(context.Background(), malformed, &internalpb.SearchRequest{CollectionID: 100}, nil, 0, fakeSearchResultReducer{})
	require.Error(t, err)
	_, err = native.queryAllShards(context.Background(), malformed, &internalpb.RetrieveRequest{CollectionID: 100}, nil, 0, newLegacyQueryCollector())
	require.Error(t, err)
	require.Empty(t, sn.eventLog())
}

// The same example as master answers it. T1 is behind B (through_1 = 125), so
// its StreamingNode syncs and returns (B, B) rather than its capped frontiers.
// S is then told to wait T1's stream to B itself; the QueryNode is released by
// the published barrier, which reaches the stream as payload-free progress.
func TestLineageGroupTargetThatSyncsReportsBarrier(t *testing.T) {
	setupTestReplicaRouting(t)
	sn, workers := newLineageTestSN(), newLineageTestWorkers()
	sn.setPrefix(lineageT1, 125, 125)
	sn.setPrefix(lineageT2, 135, 135)
	// T2 already covers B and returns min(frontier, B); T1 has no entry here,
	// so the fake answers (barrier, barrier) like resolveBarrierMVCC after a sync.
	sn.frontiers[lineageT2] = &viewpb.QueryPlanMVCC{GrowingTimetick: 130, TransformingTimetick: 130}
	client := newLineageTestClient(sn, workers, lineageShards())

	_, err := client.Search(context.Background(), &LegacySearchRequest{
		Req: &internalpb.SearchRequest{CollectionID: 100, ConsistencyLevel: commonpb.ConsistencyLevel_Strong},
	})
	require.NoError(t, err)
	want := sourceMVCC(135, 135, 130)
	require.True(t, proto.Equal(want, sn.planRequests(lineageS)[0].GetQueryPlanMvcc()))
	sourceCalls := workers.of(lineageS)
	require.Len(t, sourceCalls, 1)
	require.True(t, proto.Equal(want, sourceCalls[0].search.GetMvcc()))
	require.EqualValues(t, 135, sourceCalls[0].search.GetLegacyReq().GetMvccTimestamp())
	// T1 itself executes at the barrier.
	targetCalls := workers.of(lineageT1)
	require.Len(t, targetCalls, 1)
	require.EqualValues(t, 135, targetCalls[0].search.GetMvcc().GetTransformingTimetick())
	require.EqualValues(t, 135, targetCalls[0].search.GetLegacyReq().GetMvccTimestamp())
}

func TestTargetPrefixBacksOffBetweenRetries(t *testing.T) {
	moving := viewerror.NewOnShutdownError("wal is moving")
	sn := newLineageTestSN()
	sn.setPrefix(lineageT1, 125, 125)
	sn.prefixErrs[lineageT1] = []error{moving, moving}
	client := newShardViewQueryClient(3, sn, newLineageTestWorkers())

	start := time.Now()
	response, err := client.targetPrefix(context.Background(), lineageT1)
	require.NoError(t, err)
	require.EqualValues(t, 125, response.GetThrough())
	require.Len(t, sn.prefixReqs[lineageT1], 3)
	// One delay, then a doubled one: a WAL that is moving gets time to settle.
	require.GreaterOrEqual(t, time.Since(start), 3*targetPrefixRetryDelay)

	// The caller's cancellation ends the wait between attempts.
	sn.prefixErrs[lineageT1] = []error{moving, moving, moving}
	ctx, cancel := context.WithTimeout(context.Background(), targetPrefixRetryDelay/2)
	defer cancel()
	_, err = client.targetPrefix(ctx, lineageT1)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// The last failure is returned without a trailing wait.
	sn = newLineageTestSN()
	sn.prefixErrs[lineageT1] = []error{moving}
	start = time.Now()
	_, err = newShardViewQueryClient(1, sn, newLineageTestWorkers()).targetPrefix(context.Background(), lineageT1)
	require.Error(t, err)
	require.Less(t, time.Since(start), targetPrefixRetryDelay)
}
