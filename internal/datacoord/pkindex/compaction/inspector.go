package compaction

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/datacoord/pkindex/manifest"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
)

// Inspector plans compactions. It keeps a summary of every manifest it was
// told of, scores the ones that changed on every inspection, and puts the
// best plans into the scheduler, at most Config.MaxConcurrent in flight and
// one per vchannel. It is the manifest.Observer of the manager and the GC,
// and the source of the in-flight outputs of the GC. It re-reads the
// configuration on every inspection.
type Inspector struct {
	ctx        context.Context
	taskCtx    context.Context // the parent context: tasks outlive Close
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	loadConfig func() Config
	alloc      IDAllocator
	committer  Committer
	sched      task.GlobalScheduler
	now        func() time.Time

	mu        sync.Mutex
	summaries map[manifest.Target]*summary
	dirty     map[manifest.Target]struct{}
	cooldown  map[manifest.Target]time.Time
	table     *taskTable
}

// New returns an inspector that is not running. Call Start.
func New(ctx context.Context, loadConfig func() Config, alloc IDAllocator, committer Committer, sched task.GlobalScheduler) *Inspector {
	loopCtx, cancel := context.WithCancel(ctx)
	return &Inspector{
		ctx: loopCtx, taskCtx: ctx, cancel: cancel, loadConfig: loadConfig, alloc: alloc, committer: committer, sched: sched, now: time.Now,
		summaries: map[manifest.Target]*summary{},
		dirty:     map[manifest.Target]struct{}{},
		cooldown:  map[manifest.Target]time.Time{},
		table:     newTaskTable(),
	}
}

func (i *Inspector) config() Config { return i.loadConfig().normalized() }

// Start runs one inspection every Config.InspectInterval until Close. The
// interval is read once at start.
func (i *Inspector) Start() {
	interval := i.config().InspectInterval
	i.wg.Add(1)
	go func() {
		defer i.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		mlog.Info(i.ctx, "pkindex compaction inspector started", mlog.Duration("interval", interval))
		for {
			select {
			case <-i.ctx.Done():
				return
			case now := <-ticker.C:
				i.inspect(now)
			}
		}
	}()
}

// Close stops the inspection loop. The tasks in the scheduler keep their own
// context and go on until the scheduler stops.
func (i *Inspector) Close() {
	i.cancel()
	i.wg.Wait()
}

// ManifestChanged records a manifest and marks its vchannel for inspection.
// It keeps the highest version per vchannel and returns at once; it never
// calls the manager (see manifest.Observer).
func (i *Inspector) ManifestChanged(_ context.Context, m *pkindexpb.Manifest) {
	i.observe(m, i.now())
}

func (i *Inspector) observe(m *pkindexpb.Manifest, now time.Time) {
	target := manifest.Target{CollectionID: m.GetCollectionId(), VChannel: m.GetVchannel()}
	i.mu.Lock()
	defer i.mu.Unlock()
	if old := i.summaries[target]; old != nil {
		if old.version >= m.GetVersion() {
			return
		}
		if _, dirty := i.dirty[target]; dirty {
			now = old.dirtyAt
		}
	}
	i.summaries[target] = summarize(m, now)
	i.dirty[target] = struct{}{}
}

// InFlightOutputs is the gc.InFlightOutputs of this inspector. A task records
// its output range when the dispatch succeeds, so a node may upload for a
// moment before its outputs are listed here; the grace window of the GC
// covers that moment.
func (i *Inspector) InFlightOutputs(_ context.Context, collectionID int64, vchannel string) ([]int64, error) {
	return i.table.inFlightOutputs(manifest.Target{CollectionID: collectionID, VChannel: vchannel}), nil
}

type candidate struct {
	target  manifest.Target
	score   float64
	dirtyAt time.Time
}

// inspect scores the changed vchannels and launches the best plans up to the
// free capacity. A vchannel with a task stays marked and is scored again
// after its task ends.
func (i *Inspector) inspect(now time.Time) {
	cfg := i.config()
	i.mu.Lock()
	cands := make([]candidate, 0, len(i.dirty))
	for target := range i.dirty {
		if i.table.has(target) {
			continue
		}
		if until, ok := i.cooldown[target]; ok {
			if now.Before(until) {
				continue
			}
			delete(i.cooldown, target)
		}
		s := i.summaries[target]
		c, ok := evaluate(s.groups, cfg)
		if !ok {
			delete(i.dirty, target)
			continue
		}
		cands = append(cands, candidate{target: target, score: c.score, dirtyAt: s.dirtyAt})
	}
	free := cfg.MaxConcurrent - i.table.len()
	i.mu.Unlock()

	sort.Slice(cands, func(a, b int) bool {
		if cands[a].score != cands[b].score {
			return cands[a].score > cands[b].score
		}
		return cands[a].dirtyAt.Before(cands[b].dirtyAt)
	})
	for _, c := range cands {
		if free <= 0 {
			break
		}
		if i.launch(c.target, cfg, now) {
			free--
		}
	}
	metrics.DataCoordPKIndexCompactionTaskNum.Set(float64(i.table.len()))
}

// launch reads the current manifest of target, plans on it and puts the task
// into the scheduler. It returns false when nothing was launched. It does not
// hold i.mu across Current or Enqueue: the manager calls the observer, which
// takes i.mu, while it holds its own key lock, and Current takes that lock.
func (i *Inspector) launch(target manifest.Target, cfg Config, now time.Time) bool {
	// A commit may report a newer manifest while Current runs. The marks are
	// cleared below only when no newer summary arrived than the one used.
	i.mu.Lock()
	var seen int64
	if s := i.summaries[target]; s != nil {
		seen = s.version
	}
	i.mu.Unlock()
	m, err := i.committer.Current(i.ctx, target)
	if err != nil {
		mlog.Warn(i.ctx, "pkindex compaction cannot read the manifest", mlog.FieldCollectionID(target.CollectionID), mlog.FieldVChannel(target.VChannel), mlog.Err(err))
		i.mu.Lock()
		i.cooldown[target] = now.Add(cfg.FailureCooldown)
		i.mu.Unlock()
		return false
	}
	if m == nil {
		i.mu.Lock()
		if s := i.summaries[target]; s != nil && s.version <= seen {
			delete(i.summaries, target)
			delete(i.dirty, target)
		}
		i.mu.Unlock()
		return false
	}
	i.observe(m, now)
	p, ok := makePlan(m, cfg)
	if !ok {
		i.mu.Lock()
		if s := i.summaries[target]; s == nil || s.version <= m.GetVersion() {
			delete(i.dirty, target)
		}
		i.mu.Unlock()
		return false
	}
	begin, _, err := i.alloc.AllocN(2)
	if err != nil {
		mlog.Warn(i.ctx, "pkindex compaction cannot allocate ids", mlog.FieldCollectionID(target.CollectionID), mlog.FieldVChannel(target.VChannel), mlog.Err(err))
		return false
	}
	t := newTask(i.taskCtx, p, begin, begin+1, cfg, i.alloc, i.committer, i.onTerminal)
	i.mu.Lock()
	ok = i.table.add(t)
	if ok {
		delete(i.dirty, target)
	}
	i.mu.Unlock()
	if !ok {
		return false
	}
	i.sched.Enqueue(t)
	mlog.Info(i.ctx, "pkindex compaction planned", t.logFields(mlog.Float64("score", p.score), mlog.Int64("baseVersion", p.baseVersion), mlog.Bool("dropTombstones", p.dropTombstones))...)
	return true
}

// onTerminal takes a finished or failed task out of the table and marks its
// vchannel for inspection: a success changed the manifest, and after a
// failure the same inputs are still there. A failed vchannel is not planned
// again before Config.FailureCooldown. It runs under the scheduler's per-task
// lock (see Task), so it only touches the inspector's own state.
func (i *Inspector) onTerminal(t *Task) {
	state := t.GetTaskState()
	cfg := i.config()
	i.mu.Lock()
	i.table.remove(t.taskID)
	if state == taskcommon.Failed {
		i.cooldown[t.target] = i.now().Add(cfg.FailureCooldown)
	}
	i.dirty[t.target] = struct{}{}
	i.mu.Unlock()

	result := "finished"
	if state == taskcommon.Failed {
		result = "failed"
	}
	metrics.DataCoordPKIndexCompactionTotal.WithLabelValues(t.rule.String(), result).Inc()
	if state == taskcommon.Finished {
		metrics.DataCoordPKIndexCompactionBytes.WithLabelValues("in").Add(float64(t.inputBytes))
		var out int64
		t.mu.Lock()
		for _, s := range t.outputs {
			out += s.GetSize()
		}
		t.mu.Unlock()
		metrics.DataCoordPKIndexCompactionBytes.WithLabelValues("out").Add(float64(out))
	}
	metrics.DataCoordPKIndexCompactionTaskNum.Set(float64(i.table.len()))
}
