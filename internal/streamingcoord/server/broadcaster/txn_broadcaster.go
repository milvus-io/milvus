package broadcaster

import (
	"context"
	"maps"
	"slices"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Bound each task so Begin + Commit fit one KV write. Bound member count so
// the complete group can be deleted in a single backend transaction.
const (
	maxTxnMemberBytes = 256 * 1024
	maxTxnMembers     = 64
	maxTxnChannels    = 128
)

type txnBroadcaster struct {
	mu             sync.Mutex // protects this handle, not recovered handles
	manager        *broadcastTaskManager
	group          *broadcastTxn
	guards         *lockGuards
	admission      *lockGuards
	admissionKey   string
	controlChannel string
	closed         bool
}

// broadcastTxn has two independent serializations: op admits members, mu writes
// snapshots. No code holding mu may acquire a task mutex or manager.mu.
type broadcastTxn struct {
	manager *broadcastTaskManager
	id      uint64
	op      chan struct{}
	tasks   []*broadcastTask // protected by op; append only until GC
	deleted bool             // protected by op
	mu      sync.Mutex
	begin   *streamingpb.BroadcastTask // last successfully persisted Begin; protected by mu
	beginID uint64
	guards  *lockGuards
}

func (bm *broadcastTaskManager) StartTxnBroadcastWithResourceKey(ctx context.Context, keys ...message.ResourceKey) (TxnBroadcaster, *TxnBroadcastResult, error) {
	ctx, cancel := contextutil.MergeContext(ctx, bm.txnCtx)
	defer cancel()
	if err := bm.checkClusterRole(ctx); err != nil {
		return nil, nil, err
	}
	control := streaming.WAL().ControlChannel()
	var admissionKey string
	business := make([]message.ResourceKey, 0, len(keys)+1)
	admissionCount := 0
	for _, key := range keys {
		if key.Domain != messagespb.ResourceDomain_ResourceDomainIdempotency {
			business = append(business, key)
			continue
		}
		admissionCount++
		if admissionCount > 1 || key.Shared {
			return nil, nil, merr.WrapErrParameterInvalidMsg("transaction requires at most one exclusive admission key")
		}
		admissionKey = key.Key
	}
	admission := &lockGuards{}
	if admissionKey != "" {
		admission = bm.resourceKeyLocker.Lock(message.ResourceKey{Domain: messagespb.ResourceDomain_ResourceDomainIdempotency, Key: admissionKey})
		bm.mu.Lock()
		id, _ := bm.idempotencyIndex.Get(txnAdmissionScope(admissionKey))
		previous := bm.tasks[id]
		bm.mu.Unlock()
		if previous != nil {
			admission.Unlock()
			result, err := txnResult(ctx, previous, true)
			return nil, result, err
		}
	}
	guards := bm.resourceKeyLocker.Lock(bm.appendSharedClusterRK(business...)...)
	fail := func(err error) (TxnBroadcaster, *TxnBroadcastResult, error) {
		guards.Unlock()
		admission.Unlock()
		return nil, nil, err
	}
	if !bm.lifetime.Add(typeutil.LifetimeStateWorking) {
		return fail(status.NewOnShutdownError("broadcaster is closing"))
	}
	defer bm.lifetime.Done()
	if err := bm.checkClusterRole(ctx); err != nil {
		return fail(err)
	}
	// The cluster S guard fences a concurrent replication configuration change.
	config, err := resource.Resource().StreamingCatalog().GetReplicateConfiguration(ctx)
	if err != nil {
		return fail(err)
	}
	if len(config.GetReplicateConfiguration().GetCrossClusterTopology()) != 0 {
		return fail(merr.WrapErrServiceUnavailable("broadcast transactions do not yet support cross-cluster replication"))
	}
	return &txnBroadcaster{manager: bm, guards: guards, admission: admission, admissionKey: admissionKey, controlChannel: control}, nil, nil
}

func (bm *broadcastTaskManager) RecoverTxnBroadcast(ctx context.Context, id uint64) (TxnBroadcaster, error) {
	if !bm.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("broadcaster is closing")
	}
	defer bm.lifetime.Done()
	if err := bm.checkClusterRole(ctx); err != nil {
		return nil, err
	}
	bm.mu.Lock()
	group := bm.txns[id]
	bm.mu.Unlock()
	if group == nil {
		return nil, merr.WrapErrParameterInvalidMsg("broadcast transaction %d does not exist or has expired", id)
	}
	return &txnBroadcaster{manager: bm, group: group}, nil
}

func (h *txnBroadcaster) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	if h.guards != nil {
		h.guards.Unlock()
		h.guards = nil
	}
	if h.admission != nil {
		h.admission.Unlock()
		h.admission = nil
	}
}

func (h *txnBroadcaster) BroadcastBegin(ctx context.Context, msg message.BroadcastMutableMessage) (*TxnBroadcastResult, error) {
	ctx, cancel := contextutil.MergeContext(ctx, h.manager.txnCtx)
	defer cancel()
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed || h.group != nil {
		return nil, merr.WrapErrParameterInvalidMsg("Begin requires an unused transaction handle")
	}
	tc := msg.BroadcastHeader().Txn
	if tc.GetTxnId() == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("Begin requires a nonzero BroadcastHeader transaction ID")
	}
	if key := message.BroadcastAdmissionKeyOf(msg); key != "" && key != h.admissionKey {
		return nil, merr.WrapErrParameterInvalidMsg("Begin admission identity differs from Start")
	}
	// The old message-type-scoped key is not an admission identity. Do not silently
	// promise pre-lock idempotency for a differently keyed message.
	if message.IdempotencyKeyOf(msg) != "" {
		return nil, merr.WrapErrParameterInvalidMsg("Begin idempotency must be supplied through its admission ResourceKey")
	}
	bm := h.manager
	if !bm.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("broadcaster is closing")
	}
	defer bm.lifetime.Done()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	msg, err := prepareTxnMessage(ctx, msg, tc.GetTxnId(), messagespb.BroadcastTxnKind_BROADCAST_TXN_KIND_BEGIN, 0, h.guards.ResourceKeys(), h.controlChannel)
	if err != nil {
		return nil, err
	}
	msg.OverwriteBroadcastAdmissionKey(h.admissionKey)
	if len(msg.BroadcastHeader().VChannels) > maxTxnChannels || proto.Size(msg.IntoMessageProto()) > maxTxnMemberBytes {
		return nil, merr.WrapErrParameterInvalidMsg("transaction member exceeds size limit")
	}
	bm.mu.Lock()
	if _, exists := bm.txns[tc.GetTxnId()]; exists {
		bm.mu.Unlock()
		return nil, merr.WrapErrParameterInvalidMsg("broadcast transaction ID %d is already used", tc.GetTxnId())
	}
	group := &broadcastTxn{manager: bm, id: tc.GetTxnId(), op: make(chan struct{}, 1), guards: h.guards}
	task := newBroadcastTaskFromBroadcastMessage(msg, bm.metrics, bm.ackScheduler)
	task.txn = group
	task.SetLogger(bm.Logger())
	group.tasks = []*broadcastTask{task}
	bm.txns[group.id] = group
	bm.tasks[msg.BroadcastHeader().BroadcastID] = task
	bm.idempotencyIndex.Add(txnAdmissionScope(h.admissionKey), msg.BroadcastHeader().BroadcastID)
	bm.mu.Unlock()
	h.group = group
	h.guards = nil
	admission := h.admission
	h.admission = nil
	group.submit(task, admission)
	return txnResult(ctx, task, false)
}

func (h *txnBroadcaster) BroadcastBody(ctx context.Context, msg message.BroadcastMutableMessage) (*TxnBroadcastResult, error) {
	return h.broadcastMember(ctx, msg, messagespb.BroadcastTxnKind_BROADCAST_TXN_KIND_BODY, false)
}

func (h *txnBroadcaster) BroadcastCommit(ctx context.Context, msg message.BroadcastMutableMessage, opts ...TxnCommitOption) (*TxnBroadcastResult, error) {
	for _, opt := range opts {
		if opt != EnsureTxnCompleted {
			return nil, merr.WrapErrParameterInvalidMsg("unknown transaction commit option")
		}
	}
	return h.broadcastMember(ctx, msg, messagespb.BroadcastTxnKind_BROADCAST_TXN_KIND_COMMIT, slices.Contains(opts, EnsureTxnCompleted))
}

func (h *txnBroadcaster) broadcastMember(ctx context.Context, msg message.BroadcastMutableMessage, kind messagespb.BroadcastTxnKind, ensure bool) (*TxnBroadcastResult, error) {
	ctx, cancel := contextutil.MergeContext(ctx, h.manager.txnCtx)
	defer cancel()
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed || h.group == nil {
		return nil, merr.WrapErrParameterInvalidMsg("transaction handle has no admitted Begin or is closed")
	}
	bm, group := h.manager, h.group
	if !bm.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("broadcaster is closing")
	}
	defer bm.lifetime.Done()
	if err := bm.checkClusterRole(ctx); err != nil {
		return nil, err
	}
	if id := msg.BroadcastHeader().Txn.GetTxnId(); id != 0 && id != group.id {
		return nil, merr.WrapErrParameterInvalidMsg("message transaction ID does not match handle")
	}
	if message.BroadcastAdmissionKeyOf(msg) != "" {
		return nil, merr.WrapErrParameterInvalidMsg("only Begin carries admission identity")
	}
	if err := group.acquire(ctx); err != nil {
		return nil, err
	}
	task, duplicated, err := group.admit(ctx, msg, kind, ensure)
	group.release()
	if err != nil {
		return nil, err
	}
	return txnResult(ctx, task, duplicated)
}

func (g *broadcastTxn) acquire(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-g.manager.broadcastScheduler.backgroundTaskNotifier.Context().Done():
		return status.NewOnShutdownError("broadcaster is closing")
	case g.op <- struct{}{}:
		return nil
	}
}
func (g *broadcastTxn) release() { <-g.op }

// admit holds op, but never mu while waiting for a previous member.
func (g *broadcastTxn) admit(ctx context.Context, msg message.BroadcastMutableMessage, kind messagespb.BroadcastTxnKind, ensure bool) (*broadcastTask, bool, error) {
	if g.deleted {
		return nil, false, merr.WrapErrParameterInvalidMsg("broadcast transaction has expired")
	}
	if kind == messagespb.BroadcastTxnKind_BROADCAST_TXN_KIND_BODY && message.IdempotencyKeyOf(msg) != "" {
		scope := idempotencyScopeOfMessage(msg)
		for _, task := range g.tasks[1:] {
			if task.IdempotencyScope() == scope {
				return task, true, nil
			}
		}
	}
	last := g.tasks[len(g.tasks)-1]
	if last.Header().Txn.GetKind() == messagespb.BroadcastTxnKind_BROADCAST_TXN_KIND_COMMIT {
		if kind == messagespb.BroadcastTxnKind_BROADCAST_TXN_KIND_COMMIT && (ensure || message.SameBroadcastOperation(last.BroadcastMessage(), msg)) {
			return last, true, nil
		}
		return nil, false, merr.WrapErrParameterInvalidMsg("broadcast transaction already has a different terminal operation")
	}
	if _, err := txnResult(ctx, last, false); err != nil {
		return nil, false, err
	}
	if kind == messagespb.BroadcastTxnKind_BROADCAST_TXN_KIND_BODY && len(g.tasks) >= maxTxnMembers-1 {
		return nil, false, merr.WrapErrParameterInvalidMsg("broadcast transaction member limit reached; Commit remains available")
	}
	prepared, err := prepareTxnMessage(ctx, msg, g.id, kind, uint32(len(g.tasks)), g.tasks[0].Header().ResourceKeys.Collect(), streaming.WAL().ControlChannel())
	if err != nil {
		return nil, false, err
	}
	if len(prepared.BroadcastHeader().VChannels) > maxTxnChannels || proto.Size(prepared.IntoMessageProto()) > maxTxnMemberBytes {
		return nil, false, merr.WrapErrParameterInvalidMsg("transaction member exceeds size limit")
	}
	task := newBroadcastTaskFromBroadcastMessage(prepared, g.manager.metrics, g.manager.ackScheduler)
	task.txn = g
	task.SetLogger(g.manager.Logger())
	g.tasks = append(g.tasks, task)
	g.manager.mu.Lock()
	g.manager.tasks[prepared.BroadcastHeader().BroadcastID] = task
	g.manager.mu.Unlock()
	g.submit(task, nil)
	return task, false, nil
}

func prepareTxnMessage(ctx context.Context, msg message.BroadcastMutableMessage, id uint64, kind messagespb.BroadcastTxnKind, seq uint32, keys []message.ResourceKey, control string) (message.BroadcastMutableMessage, error) {
	if supplied := msg.BroadcastHeader().ResourceKeys; len(supplied) != 0 && !maps.Equal(supplied, typeutil.NewSet(keys...)) {
		return nil, merr.WrapErrParameterInvalidMsg("message resource keys differ from the transaction's resources")
	}
	idOfBroadcast, err := resource.Resource().IDAllocator().Allocate(ctx)
	if err != nil {
		return nil, err
	}
	// The background task owns its properties, independently of caller retries.
	msg = message.NewBroadcastMutableMessageBeforeAppend(msg.Payload(), maps.Clone(msg.Properties().ToRawMap()))
	header := msg.BroadcastHeader()
	header.BroadcastID = idOfBroadcast
	header.ResourceKeys = typeutil.NewSet(keys...)
	header.Txn = &messagespb.BroadcastTxnContext{TxnId: id, Kind: kind, Sequence: seq}
	msg.OverwriteBroadcastHeader(header)
	msg = message.WithBroadcastControlChannel(msg, control)
	message.InjectTraceContext(ctx, msg)
	return msg, nil
}

// submit transfers execution to the manager before returning to a cancellable caller.
func (g *broadcastTxn) submit(task *broadcastTask, admission *lockGuards) {
	g.manager.txnWG.Add(1)
	go func() {
		defer g.manager.txnWG.Done()
		ctx := g.manager.broadcastScheduler.backgroundTaskNotifier.Context()
		if admission != nil {
			defer admission.Unlock()
		}
		if err := task.InitializeRecovery(ctx); err != nil {
			g.manager.Logger().Warn(ctx, "persist broadcast transaction member failed", mlog.Err(err))
			return
		}
		if admission != nil {
			admission.Unlock()
		} // index + durable group now identify Begin
		if _, err := g.manager.broadcastScheduler.AddTask(ctx, newPendingBroadcastTask(task)); err != nil {
			g.manager.Logger().Warn(ctx, "broadcast transaction member interrupted", mlog.Err(err))
		}
	}()
}

func txnResult(ctx context.Context, task *broadcastTask, duplicated bool) (*TxnBroadcastResult, error) {
	result, err := task.BlockUntilDone(ctx)
	if err != nil {
		return nil, err
	}
	if duplicated {
		result.Duplicated = task.BroadcastMessage()
	}
	return &TxnBroadcastResult{TxnID: task.Header().Txn.GetTxnId(), BroadcastResult: result}, nil
}

func txnAdmissionScope(key string) string {
	if key == "" {
		return ""
	}
	return "admission/" + key
}
