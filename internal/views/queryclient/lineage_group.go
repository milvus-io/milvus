package queryclient

import (
	"context"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	commonpb "github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/views/queryclient/resolver"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// requestReadPoint is how one request fixes its read point: an explicit
// executor MVCC, an explicit barrier, or the consistency level.
type requestReadPoint struct {
	consistencyLevel   commonpb.ConsistencyLevel
	guaranteeTimestamp uint64
	mvcc               *viewpb.QueryPlanMVCC
	barrierMVCC        uint64
}

// shardReadPoint is the read point one shard executes with. For an ordinary
// shard it is the request's own. onPlan, when set, observes every plan that
// Phase 1 returned for the shard.
type shardReadPoint struct {
	vchannel    string
	mvcc        *viewpb.QueryPlanMVCC
	barrierMVCC uint64
	onPlan      func(*viewpb.QueryPlan)
}

// runShard executes both phases of the shard at position index of the serving list.
type runShard func(ctx context.Context, index int, read shardReadPoint) error

// lineageGroup is a split source shard and its targets, by position in the
// serving list.
type lineageGroup struct {
	source  int
	lineage *resolver.ShardLineage
	targets []int
}

// groupShards separates the serving list into lineage groups and ordinary
// shards. A list without lineage yields no group.
func groupShards(shards []resolver.Shard) (groups []lineageGroup, ordinary []int, err error) {
	positions := make(map[string]int, len(shards))
	hasLineage := false
	for i, shard := range shards {
		positions[shard.VChannel] = i
		hasLineage = hasLineage || shard.Lineage != nil
	}
	if !hasLineage {
		ordinary = make([]int, len(shards))
		for i := range shards {
			ordinary[i] = i
		}
		return nil, ordinary, nil
	}
	grouped := make(map[int]bool, len(shards))
	for i, shard := range shards {
		if shard.Lineage == nil {
			continue
		}
		if len(shard.Lineage.VChannels) == 0 || shard.Lineage.SwitchTimeTick == 0 {
			return nil, nil, merr.WrapErrServiceInternalMsg("shard %s has an incomplete lineage", shard.VChannel)
		}
		group := lineageGroup{source: i, lineage: shard.Lineage}
		for _, vchannel := range shard.Lineage.VChannels {
			target, ok := positions[vchannel]
			if !ok {
				return nil, nil, merr.WrapErrServiceInternalMsg("lineage shard %s of %s is not in the serving list", vchannel, shard.VChannel)
			}
			// A shard is read at one point: it cannot be the target of two
			// sources, a source and a target at once, or listed twice.
			if target == i || shards[target].Lineage != nil || grouped[target] {
				return nil, nil, merr.WrapErrServiceInternalMsg("lineage shard %s of %s belongs to another lineage", vchannel, shard.VChannel)
			}
			grouped[target] = true
			group.targets = append(group.targets, target)
		}
		grouped[i] = true
		groups = append(groups, group)
	}
	for i := range shards {
		if !grouped[i] {
			ordinary = append(ordinary, i)
		}
	}
	return groups, ordinary, nil
}

// fanOutShards runs every shard of the serving list concurrently. Ordinary
// shards run both phases on their own with the request's read point. The
// shards of a lineage group are read at one common point, in the order that
// point requires.
func (s *shardViewQueryClient) fanOutShards(ctx context.Context, shards []resolver.Shard, request requestReadPoint, run runShard) error {
	groups, ordinary, err := groupShards(shards)
	if err != nil {
		return err
	}
	g, gCtx := errgroup.WithContext(ctx)
	for _, i := range ordinary {
		g.Go(func() error {
			return run(gCtx, i, shardReadPoint{vchannel: shards[i].VChannel, mvcc: request.mvcc, barrierMVCC: request.barrierMVCC})
		})
	}
	for _, group := range groups {
		g.Go(func() error {
			return s.runLineageGroup(gCtx, shards, group, request, run)
		})
	}
	return g.Wait()
}

// runLineageGroup reads a split source shard S and its targets at one point B:
//
//  1. B is fixed from the targets' published prefixes (lineageReadPoint);
//  2. every target plans with barrier_mvcc = B, so it covers B and returns its
//     frontiers at B;
//  3. S plans with the pair (T_switch, T_switch), its last event, and executes
//     with B and each target's transform frontier at B: the holders of S's
//     rows apply the targets' deletes through those ticks and read at B.
//
// S starts as soon as every target has planned once. Any plan a target
// obtains for B is a frontier at B, so a target that later replans does not
// invalidate what S was given.
func (s *shardViewQueryClient) runLineageGroup(ctx context.Context, shards []resolver.Shard, group lineageGroup, request requestReadPoint, run runShard) error {
	readPoint, err := s.lineageReadPoint(ctx, shards, group, request)
	if err != nil {
		return err
	}
	frontiers := newTargetFrontiers(len(group.targets))
	g, gCtx := errgroup.WithContext(ctx)
	for _, target := range group.targets {
		vchannel := shards[target].VChannel
		g.Go(func() error {
			return run(gCtx, target, shardReadPoint{
				vchannel:    vchannel,
				barrierMVCC: readPoint,
				onPlan: func(plan *viewpb.QueryPlan) {
					frontiers.observe(vchannel, plan.GetMvcc().GetTransformingTimetick())
				},
			})
		})
	}
	g.Go(func() error {
		ticks, err := frontiers.wait(gCtx)
		if err != nil {
			return err
		}
		switchTick := group.lineage.SwitchTimeTick
		return run(gCtx, group.source, shardReadPoint{
			vchannel: shards[group.source].VChannel,
			mvcc: &viewpb.QueryPlanMVCC{
				GrowingTimetick:              switchTick,
				TransformingTimetick:         switchTick,
				ReadTimetick:                 readPoint,
				LineageTransformingTimeticks: ticks,
			},
		})
	})
	return g.Wait()
}

// lineageReadPoint fixes the common read point B of a lineage group:
// B = max(lower bound, min over targets of through), where through is the
// complete published prefix of a target's PChannel.
//
//	Strong               lower bound = largest Strong minimum prefix of the targets
//	Session              lower bound = max(Strong minimum, request guarantee)
//	Bounded, Customized  lower bound = the request's guarantee timestamp
//	Eventually           no lower bound
//
// A request that already carries a fixed point (iterator page, statistics and
// delete by expression at a barrier) uses that point as B.
func (s *shardViewQueryClient) lineageReadPoint(ctx context.Context, shards []resolver.Shard, group lineageGroup, request requestReadPoint) (uint64, error) {
	if request.mvcc != nil {
		// A frontier pair fixes two timestamps per shard; a lineage group has one.
		return 0, merr.WrapErrServiceInternalMsg("a fixed executor MVCC cannot be applied to shard %s, which has lineage", shards[group.source].VChannel)
	}
	if request.barrierMVCC != 0 {
		return request.barrierMVCC, nil
	}
	prefixes := make([]*viewpb.GetMVCCTimestampResponse, len(group.targets))
	g, gCtx := errgroup.WithContext(ctx)
	for i, target := range group.targets {
		g.Go(func() (err error) {
			prefixes[i], err = s.targetPrefix(gCtx, shards[target].VChannel)
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return 0, err
	}
	through, strongMinimum := prefixes[0].GetThrough(), uint64(0)
	for _, prefix := range prefixes {
		through = min(through, prefix.GetThrough())
		strongMinimum = max(strongMinimum, prefix.GetStrongMinimum())
	}
	var lowerBound uint64
	switch request.consistencyLevel {
	case commonpb.ConsistencyLevel_Strong:
		lowerBound = strongMinimum
	case commonpb.ConsistencyLevel_Session:
		lowerBound = max(strongMinimum, request.guaranteeTimestamp)
	case commonpb.ConsistencyLevel_Bounded, commonpb.ConsistencyLevel_Customized:
		lowerBound = request.guaranteeTimestamp
	}
	readPoint := max(lowerBound, through)
	if readPoint == 0 {
		return 0, merr.WrapErrServiceInternalMsg("lineage shards of %s report no published prefix", shards[group.source].VChannel)
	}
	return readPoint, nil
}

// Delay before the first retry of a prefix sample; it doubles up to the cap.
const (
	targetPrefixRetryDelay    = 20 * time.Millisecond
	targetPrefixRetryMaxDelay = 500 * time.Millisecond
)

// targetPrefix samples a target's published prefix without waiting on the
// StreamingNode. Retryable failures (the WAL is moving) are retried here with
// a growing delay; the barrier established afterwards is what makes the read
// point safe.
func (s *shardViewQueryClient) targetPrefix(ctx context.Context, vchannel string) (*viewpb.GetMVCCTimestampResponse, error) {
	var lastErr error
	delay := targetPrefixRetryDelay
	for attempt := 0; attempt < max(s.maxRetries, 1); attempt++ {
		if attempt > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
			delay = min(2*delay, targetPrefixRetryMaxDelay)
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		response, err := s.queryPlanClient.GetMVCCTimestamp(ctx, qviews.ShardID{VChannel: vchannel}, &viewpb.GetMVCCTimestampRequest{Vchannel: vchannel, PrefixOnly: true})
		if err == nil {
			return response, nil
		}
		if !retryableQueryError(err) {
			return nil, err
		}
		lastErr = err
	}
	return nil, lastErr
}

// targetFrontiers collects each target's transform frontier at the read point
// from the first plan the target obtains.
type targetFrontiers struct {
	mu      sync.Mutex
	ticks   map[string]uint64
	missing int
	ready   chan struct{}
}

func newTargetFrontiers(targets int) *targetFrontiers {
	return &targetFrontiers{ticks: make(map[string]uint64, targets), missing: targets, ready: make(chan struct{})}
}

func (f *targetFrontiers) observe(vchannel string, tick uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, seen := f.ticks[vchannel]; seen {
		return
	}
	f.ticks[vchannel] = tick
	if f.missing--; f.missing == 0 {
		close(f.ready)
	}
}

func (f *targetFrontiers) wait(ctx context.Context) (map[string]uint64, error) {
	select {
	case <-f.ready:
		f.mu.Lock()
		defer f.mu.Unlock()
		ticks := make(map[string]uint64, len(f.ticks))
		for vchannel, tick := range f.ticks {
			ticks[vchannel] = tick
		}
		return ticks, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
