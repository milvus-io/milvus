package dml

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/proxy/dql"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	partialUpdateCASMaxRetryAttempts = 5
	partialUpdateCASRetryBackoff     = 10 * time.Millisecond
)

func (ut *UpsertTask) Execute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Upsert-Execute")
	defer sp.End()

	var ez *message.CipherConfig
	if hookutil.IsClusterEncryptionEnabled() {
		ez = hookutil.GetEzByCollProperties(ut.schema.GetProperties(), ut.collectionID).AsMessageConfig()
	}

	if ut.req.GetPartialUpdate() {
		return ut.executePartialUpdateWithCASRetry(ctx, ez)
	}
	return ut.appendUpsertAttempt(ctx, ez)
}

func (ut *UpsertTask) executePartialUpdateWithCASRetry(ctx context.Context, ez *message.CipherConfig) error {
	if ut.partialUpdateOriginalFields == nil {
		return merr.WrapErrServiceInternalMsg("partial update original fields are unavailable")
	}
	if ut.partialState == nil {
		return merr.WrapErrServiceInternalMsg("partial update row state is unavailable")
	}
	ut.requestSize = proto.Size(ut.req)
	ut.mutationSize = ut.upsertMsg.InsertMsg.Size() + ut.upsertMsg.DeleteMsg.Size()
	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			delay := partialUpdateCASRetryBackoff * (1 << min(attempt-1, 2))
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				err := merr.Wrap(ctx.Err(), "partial update canceled while waiting to retry CAS conflict")
				if ut.req.GetWriteCondition() != nil {
					return conditionalExecutionError(err)
				}
				return err
			case <-timer.C:
			}
			if err := ut.retainRejectedRows(ctx); err != nil {
				if ut.req.GetWriteCondition() != nil {
					return conditionalExecutionError(err)
				}
				return err
			}
		}
		if len(ut.partialState.writeRows) == 0 {
			return nil
		}
		err := ut.appendUpsertAttempt(ctx, ez)
		if err == nil {
			return nil
		}
		if !isPartialUpdateCASRetry(err) {
			if ut.req.GetWriteCondition() != nil && ut.partialState.appendAttempted {
				return conditionalExecutionError(err)
			}
			return err
		}
		if !ut.canRetryPartialUpdateCASConflict() || (ut.req.GetWriteCondition() != nil && attempt == partialUpdateCASMaxRetryAttempts-1) {
			return merr.WrapErrCollectionPartialUpdateConflictErr(err, "partial update conflict; request may have partially committed; automatic replay is unsafe")
		}
		if attempt == partialUpdateCASMaxRetryAttempts-1 {
			return projectPartialUpdateCASError(err, true)
		}
	}
}

// Only streaming admission failures are rebuild signals. A public merr projection
// owns the final decision, even when its diagnostic cause contains a CAS error.
func isPartialUpdateCASRetry(err error) bool {
	return err != nil && !merr.IsMilvusError(err) && status.AsStreamingError(err).IsPartialUpdateRetryableCAS()
}

func projectPartialUpdateCASError(err error, allowConflictRetry bool) error {
	if err == nil || !isPartialUpdateCASRetry(err) {
		return err
	}
	if !allowConflictRetry {
		return merr.WrapErrCollectionPartialUpdateConflictErr(
			err,
			"relative partial update conflicted with a concurrent write; automatic retry is unsafe",
		)
	}
	return merr.WrapErrServiceUnavailableErr(err, "partial update conflicted with a concurrent write")
}

// preparePartialUpdateRetryAttempt restores the original payload and rebuilds
// terms, Strong query snapshots, and DML state for one retry.
func (ut *UpsertTask) preparePartialUpdateRetryAttempt(ctx context.Context) error {
	if err := ut.prepareUpsert(ctx); err != nil {
		return err
	}
	if ut.partialState != nil && len(ut.partialState.writeRows) == 0 {
		return nil
	}
	if err := ut.insertPreExecute(ctx); err != nil {
		return err
	}
	if err := ut.deletePreExecute(ctx); err != nil {
		return err
	}
	ut.refreshMutationResultCounts()
	return nil
}

func cloneFieldDataList(fields []*schemapb.FieldData) []*schemapb.FieldData {
	if fields == nil {
		return nil
	}
	cloned := make([]*schemapb.FieldData, len(fields))
	for i, field := range fields {
		if field == nil {
			continue
		}
		cloned[i] = proto.Clone(field).(*schemapb.FieldData)
	}
	return cloned
}

// appendUpsertAttempt packs and appends one upsert, and routes again what a
// shard split refused (see shard_fenced_retry.go).
//
// Both halves are settled per message. A row or tombstone whose message
// committed is never sent again: sending a row again writes it twice, and
// sending a tombstone again to a vchannel whose transaction committed gives it
// a later tick than the row this upsert inserted there in the same transaction,
// deleting it.
//
// The two halves of a row stay together. The insert of a key and the tombstone
// of the same key are routed by the same table in the same attempt, so they go
// to the same vchannel; the messages of one vchannel in one append are one
// transaction, which commits at one time tick or not at all. A refusal
// therefore hands back both halves, and the next attempt routes both to the
// shard that owns the key now, again in one transaction. (An upsert that
// changes the primary key, as an auto-id upsert does, deletes one key and
// inserts another; the two may live on different shards, with or without a
// split.)
//
// A partial update does not enter this retry: its CAS proof binds the vchannels
// it read, so a row routed again would carry a proof for the wrong shard. It
// evicts the collection and fails instead (see partialUpdateFenceRefusal).
func (ut *UpsertTask) appendUpsertAttempt(ctx context.Context, ez *message.CipherConfig) error {
	logger := mlog.With(mlog.FieldCollectionName(ut.req.CollectionName))
	partialUpdate := ut.req.GetPartialUpdate()

	fence := newSplitFence()
	// A failed attempt leaves its error in the result (packInsertMessage does,
	// as it always did). An attempt that fails after rows landed is retried,
	// and when the retry succeeds that error must not stay behind.
	statusBefore := ut.result.GetStatus()
	pendingInserts := newPendingRows(int(ut.upsertMsg.InsertMsg.NumRows))
	pendingDeletes := newPendingRows(typeutil.GetSizeOfIDs(ut.deletePrimaryKeys()))
	attempt := 0
	// prepareFailed ends the request on a failure met before the first append,
	// and leaves a later one to retryPreparation. A partial update never
	// retries, so it never gets here past its first attempt.
	prepareFailed := func(err error) (bool, error) {
		if attempt > 1 {
			return fence.retryPreparation(ctx, ut.GetMetaCache(), ut.collectionID, err)
		}
		return false, err
	}
	appendErr := retry.Handle(ctx, func() (bool, error) {
		attempt++
		route, err := ut.writeRoute(ctx)
		if err != nil {
			logger.Warn(ctx, "resolve the write route failed", mlog.Err(err))
			return prepareFailed(err)
		}
		// On a collection that was never split the route cannot differ from
		// the one the proofs were taken against, so nothing is checked there.
		if partialUpdate && route.split() {
			if err := ut.checkPartialUpdateProofsCoverRoute(ctx, route); err != nil {
				return false, err
			}
		}
		insertMsgs, insertOffsets, err := ut.packInsertMessage(ctx, ez, route, pendingInserts)
		if err != nil {
			logger.Warn(ctx, "pack insert message failed", mlog.Err(err))
			return prepareFailed(err)
		}
		deleteMsgs, deleteOffsets, err := ut.packDeleteMessage(ctx, ez, route, pendingDeletes)
		if err != nil {
			logger.Warn(ctx, "pack delete message failed", mlog.Err(err))
			return prepareFailed(err)
		}

		messages := make([]message.MutableMessage, 0, len(insertMsgs)+len(deleteMsgs))
		messages = append(messages, insertMsgs...)
		messages = append(messages, deleteMsgs...)
		if partialUpdate {
			if err := ut.attachPartialUpdateCAS(messages); err != nil {
				logger.Warn(ctx, "attach partial update CAS metadata failed", mlog.Err(err))
				return false, err
			}
		}
		fence.routedBy(route)
		if ut.req.GetWriteCondition() != nil {
			if err := ctx.Err(); err != nil {
				return false, ut.conditionalWriteState().Cancel(merr.Wrap(err, "conditional upsert canceled before WAL submission"))
			}
			if !ut.conditionalWriteState().TryAppend() {
				return false, merr.Wrap(context.Canceled, "conditional upsert canceled before WAL submission")
			}
		}
		if ut.partialState != nil {
			ut.partialState.appendAttempted = true
		}
		resp := streaming.WAL().AppendMessages(ctx, messages...)
		invalidateMismatchedWriteSchema(ctx, ut.GetMetaCache(), ut.collectionID, resp)
		if ut.partialState != nil {
			if err := ut.recordPartialAppend(messages, resp); err != nil {
				return false, err
			}
		}

		// A partial update reports CAS outcomes the retry must not swallow, so
		// it keeps its own unwrapping and never retries refused rows.
		if partialUpdate {
			fence.observe(resp)
			return false, ut.partialUpdateFenceRefusal(ctx, resp)
		}

		err = fence.settle(resp, insertMsgs, insertOffsets, pendingInserts)
		// The responses are in message order: the inserts, then the deletes.
		var deleteResp streaming.AppendResponses
		if len(resp.Responses) > len(insertMsgs) {
			deleteResp.Responses = resp.Responses[len(insertMsgs):]
		}
		deleteErr := fence.settle(deleteResp, deleteMsgs, deleteOffsets, pendingDeletes)
		if err == nil {
			err = deleteErr
		}
		if err != nil {
			return false, err
		}
		if pendingInserts.done() && pendingDeletes.done() {
			return false, nil
		}
		return fence.refresh(ctx, ut.GetMetaCache(), ut.collectionID, nil)
	}, shardFencedRetryOptions()...)
	if appendErr != nil {
		logger.Warn(ctx, "append messages to wal failed", mlog.Err(appendErr))
		failure := appendErr
		if status.AsStreamingError(appendErr).IsSchemaVersionMismatch() {
			failure = merr.ErrCollectionSchemaMismatch
		}
		return fence.finalError(ctx, ut.collectionID, failure,
			writtenPart{what: "rows", pending: pendingInserts}, writtenPart{what: "deletions", pending: pendingDeletes})
	}
	if attempt > 1 {
		ut.result.Status = statusBefore
	}
	// Update result.Timestamp for session consistency: the highest tick any
	// attempt reached, since earlier attempts landed rows too.
	ut.result.Timestamp = fence.maxTimeTick
	return nil
}

// partialUpdateFenceRefusal returns the outcome of the append of a partial
// update. Without a refusal by a shard split it is what it always was. With
// one, the collection is evicted, so that the next request routes against the
// refreshed table, and the request fails:
//
//   - retriably, when sending the whole request again is safe: every operation
//     is a REPLACE, or no vchannel committed;
//   - as a partial update conflict, when a relative operation was committed on
//     some vchannels and refused on others: applying it again there would
//     apply it twice, which is what the CAS retry refuses to do as well.
func (ut *UpsertTask) partialUpdateFenceRefusal(ctx context.Context, resp streaming.AppendResponses) error {
	var refusal, otherFailure error
	landed := false
	for _, response := range resp.Responses {
		switch {
		case response.Error == nil:
			landed = true
		case isShardRefusal(response.Error):
			if refusal == nil {
				refusal = response.Error
			}
		case otherFailure == nil && !isPartialUpdateCASRetry(response.Error):
			otherFailure = response.Error
		}
	}
	if refusal == nil {
		return unwrapPartialUpdateAppendError(resp)
	}
	ut.GetMetaCache().RefreshCollectionByID(ctx, ut.collectionID)
	if landed && !ut.canRetryPartialUpdateCASConflict() {
		// Schema mismatch can trigger whole-RPC SDK replay even when its
		// public Retriable flag is false. Preserve the
		// original split conflict protection once a relative write committed.
		return merr.WrapErrCollectionPartialUpdateConflictErr(refusal,
			"relative partial update was applied on some shards and refused by a shard split on others; automatic retry is unsafe")
	}
	// A concurrent channel may have timed out or reported another non-CAS
	// failure. Its outcome cannot be made deterministic by a different channel's
	// split refusal, and its public classification must not be replaced by the
	// fence's retry projection.
	if otherFailure != nil {
		return otherFailure
	}
	return merr.WrapErrServiceUnavailableErr(refusal,
		"partial update reached a vchannel refused by a shard split; retry against the refreshed routing")
}

// checkPartialUpdateProofsCoverRoute refuses a partial update whose CAS proofs
// were taken against a routing that has changed since: a row routing to a
// vchannel it holds no proof for can only mean a split was applied between the
// proof and the append. That is a transient race, not a Milvus
// bug, so it evicts the collection and fails retriably; the client's retry
// reads and proves against the new shards.
func (ut *UpsertTask) checkPartialUpdateProofsCoverRoute(ctx context.Context, route *writeRoute) error {
	needed, err := ut.buildPartialUpdateCASGroups(route)
	if err != nil {
		return err
	}
	for vchannel := range needed {
		if ut.partialUpdateCASGroups[vchannel] != nil {
			continue
		}
		ut.GetMetaCache().RefreshCollectionByID(ctx, ut.collectionID)
		return merr.WrapErrServiceUnavailableMsg(
			"partial update: the collection's routing changed after its CAS proof was taken; vchannel %s has no proof", vchannel)
	}
	return nil
}

// writeRoute reads the route of one upsert attempt.
func (ut *UpsertTask) writeRoute(ctx context.Context) (*writeRoute, error) {
	return resolveWriteRoute(ctx, ut.GetMetaCache(), ut.collectionID)
}

// deletePrimaryKeys returns the keys the upsert's delete half tombstones:
// the ones queryPreExecute resolved, or the request's old ids when it did not.
func (ut *UpsertTask) deletePrimaryKeys() *schemapb.IDs {
	if ut.upsertMsg.DeleteMsg.PrimaryKeys == nil {
		// Fall back only when no delete subset was prepared; an empty subset
		// means no lookup IDs should be deleted.
		ut.upsertMsg.DeleteMsg.PrimaryKeys = ut.oldIDs
	}
	return ut.upsertMsg.DeleteMsg.PrimaryKeys
}

// unwrapPartialUpdateAppendError returns a CAS retry signal only when no
// vchannel reported a failure with an unknown or non-CAS outcome.
func unwrapPartialUpdateAppendError(resp streaming.AppendResponses) error {
	var casErr error
	for _, response := range resp.Responses {
		if response.Error == nil {
			continue
		}
		if !isPartialUpdateCASRetry(response.Error) {
			return response.Error
		}
		if casErr == nil {
			casErr = response.Error
		}
	}
	return casErr
}

func (ut *UpsertTask) packInsertMessage(ctx context.Context, ez *message.CipherConfig, route *writeRoute, pending *pendingRows) ([]message.MutableMessage, [][]int, error) {
	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy insertExecute upsert %d", ut.ID()))
	defer tr.Elapse("insert execute done when insertExecute")

	collID := ut.collectionID
	ut.upsertMsg.InsertMsg.CollectionID = collID
	log := mlog.With(
		mlog.FieldCollectionID(collID))

	log.Debug(ctx, "send insert request to virtual channels when insertExecute",
		mlog.String("collection", ut.req.GetCollectionName()),
		mlog.String("partition", ut.req.GetPartitionName()),
		mlog.FieldCollectionID(collID),
		mlog.Strings("virtual_channels", route.vchannels),
		mlog.FieldTaskID(ut.ID()),
		mlog.Duration("get cache duration", tr.RecordSpan()))

	// start to repack insert data
	var msgs []message.MutableMessage
	var msgOffsets [][]int
	var err error
	// Upsert never carries an idempotency key, so it passes no header decorator.
	if ut.partitionKeys == nil {
		msgs, msgOffsets, err = repackInsertDataForStreamingService(ut.TraceCtx(), ut.GetMetaCache(), route.vchannels, route.table, ut.upsertMsg.InsertMsg, ut.result, ez, ut.schemaVersion, ut.partialUpdateCASGroups, nil, pending)
	} else {
		msgs, msgOffsets, err = repackInsertDataWithPartitionKeyForStreamingService(ut.TraceCtx(), ut.GetMetaCache(), route.vchannels, route.table, ut.upsertMsg.InsertMsg, ut.result, ut.partitionKeys, ez, ut.schema.CollectionSchema, ut.schemaVersion, ut.partialUpdateCASGroups, nil, pending)
	}
	if err != nil {
		log.Warn(ctx, "assign segmentID and repack insert data failed", mlog.Err(err))
		ut.result.Status = merr.Status(err)
		return nil, nil, err
	}
	if s := ut.partialState; s != nil {
		if len(ut.upsertMsg.InsertMsg.HashValues) != len(s.writeRows) || typeutil.GetSizeOfIDs(ut.result.GetIDs()) != len(s.writeRows) {
			return nil, nil, merr.WrapErrServiceInternalMsg("partial update packed rows are not aligned")
		}
		// Preserve the actual packing route; result accounting must not resolve metadata after append.
		channels := make([]string, len(s.writeRows))
		for row, index := range ut.upsertMsg.InsertMsg.HashValues {
			if int(index) >= len(route.vchannels) {
				return nil, nil, merr.WrapErrServiceInternalMsg("partial update packed channel index %d is out of range", index)
			}
			channels[row] = route.vchannels[index]
		}
		s.writeChannels = channels
	}
	return msgs, msgOffsets, nil
}

func (ut *UpsertTask) packDeleteMessage(ctx context.Context, ez *message.CipherConfig, route *writeRoute, pending *pendingRows) ([]message.MutableMessage, [][]int, error) {
	primaryKeys := ut.deletePrimaryKeys()
	if typeutil.GetSizeOfIDs(primaryKeys) == 0 {
		return nil, nil, nil
	}
	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy deleteExecute upsert %d", ut.ID()))
	collID := ut.upsertMsg.DeleteMsg.CollectionID
	log := mlog.With(
		mlog.FieldCollectionID(collID))
	vChannels := route.vchannels
	result, offsets, numRows, err := repackPendingDeleteMsgs(
		ctx,
		route.table,
		primaryKeys,
		pending.pendingSet(),
		vChannels, ut.idAllocator,
		ut.BeginTs(),
		ut.upsertMsg.DeleteMsg.CollectionID, ut.upsertMsg.DeleteMsg.CollectionName,
		ut.upsertMsg.DeleteMsg.PartitionID, ut.upsertMsg.DeleteMsg.PartitionName,
		ut.upsertMsg.DeleteMsg.GetDbName(),
		ut.req.Namespace,
		ut.schema.CollectionSchema,
	)
	if err != nil {
		return nil, nil, err
	}

	var msgs []message.MutableMessage
	var msgOffsets [][]int
	for hashKey, deleteMsgs := range result {
		vchannel := vChannels[hashKey]
		for _, deleteMsg := range deleteMsgs {
			msg, err := message.NewDeleteMessageBuilderV1().
				WithHeader(&message.DeleteMessageHeader{
					CollectionId: ut.upsertMsg.DeleteMsg.CollectionID,
					Rows:         uint64(deleteMsg.NumRows),
				}).
				WithBody(deleteMsg.DeleteRequest).
				WithVChannel(vchannel).
				WithCipher(ez).
				BuildMutable()
			if err != nil {
				return nil, nil, err
			}
			msgs = append(msgs, msg)
			msgOffsets = append(msgOffsets, offsets[deleteMsg])
		}
	}

	log.Debug(ctx, "Proxy Upsert deleteExecute done",
		mlog.FieldCollectionID(collID),
		mlog.Strings("virtual_channels", vChannels),
		mlog.FieldTaskID(ut.ID()),
		mlog.Int64("numRows", numRows),
		mlog.Duration("prepare duration", tr.ElapseSpan()))

	return msgs, msgOffsets, nil
}

func (ut *UpsertTask) attachPartialUpdateCAS(messages []message.MutableMessage) error {
	groups := ut.partialUpdateCASGroups
	if len(groups) == 0 {
		return merr.WrapErrServiceInternalMsg("partial update CAS metadata snapshot is empty")
	}

	attached := make(map[string]struct{}, len(groups))
	for _, msg := range messages {
		if msg.MessageType() != message.MessageTypeInsert {
			continue
		}
		vchannel := msg.VChannel()
		_, ok := groups[vchannel]
		if !ok {
			return merr.WrapErrServiceInternalMsg("partial update insert has no CAS metadata for vchannel %s", vchannel)
		}
		if !message.HasPartialUpdateCAS(msg) {
			return merr.WrapErrServiceInternalMsg("partial update insert is missing CAS metadata for vchannel %s", vchannel)
		}
		if paramtable.Get().ProxyCfg.SplitChunkProxy.GetAsBool() {
			maxMessageSize := paramtable.Get().PulsarCfg.MaxMessageSize.GetAsInt()
			messageSize := msg.EstimateSize()
			if messageSize > maxMessageSize {
				return merr.WrapErrServiceInternalMsg(
					"partial update insert packer emitted oversized message for vchannel %s: size=%d, max=%d",
					vchannel,
					messageSize,
					maxMessageSize,
				)
			}
		}
		attached[vchannel] = struct{}{}
	}
	for vchannel := range groups {
		if _, ok := attached[vchannel]; !ok {
			return merr.WrapErrServiceInternalMsg("partial update CAS has no insert message for vchannel %s", vchannel)
		}
	}
	return nil
}

// preparePartialUpdateCASGroups resolves all possible write PChannel terms before
// reading. Strong reads bind their actual snapshots after the query succeeds.
func (ut *UpsertTask) preparePartialUpdateCASGroups(ctx context.Context) error {
	ut.partialUpdateCASGroups = nil
	route, err := ut.writeRoute(ctx)
	if err != nil {
		return err
	}
	groups, err := ut.buildPartialUpdateCASGroups(route)
	if err != nil {
		return err
	}
	pkSchema, err := typeutil.GetPrimaryFieldSchema(ut.schema.CollectionSchema)
	if err != nil {
		return err
	}
	if pkSchema.GetAutoID() {
		// Newly allocated PKs may target any shard that owns a key. Capture
		// terms before the single Strong read; namespace routing still fixes
		// one destination.
		_, fixedChannel, err := dql.NamespaceShardingChannelID(ut.schema.CollectionSchema, ut.req.Namespace, route.vchannels)
		if err != nil {
			return err
		}
		if !fixedChannel {
			for _, channel := range route.vchannels {
				if groups[channel] == nil {
					groups[channel] = &messagespb.PartialUpdateCAS{}
				}
			}
		}
	}

	terms := make(map[string]int64, len(groups))
	for vchannel, meta := range groups {
		pchannel := funcutil.ToPhysicalChannel(vchannel)
		term, ok := terms[pchannel]
		if !ok {
			info, err := streaming.ResolvePChannelInfo(ctx, vchannel)
			if err != nil {
				return err
			}
			if info.Term <= 0 {
				return merr.WrapErrServiceInternalMsg("partial update CAS resolved invalid term %d for vchannel %s", info.Term, vchannel)
			}
			term = info.Term
			terms[pchannel] = term
		}
		meta.ObservedPchannelTerm = term
	}
	ut.partialUpdateCASGroups = groups
	return nil
}

// bindPartialUpdateReadTimestamps publishes proofs only after every candidate write
// channel has a snapshot from this successful read attempt.
func (ut *UpsertTask) bindPartialUpdateReadTimestamps(channelReadTs *typeutil.ConcurrentMap[string, uint64]) error {
	if len(ut.partialUpdateCASGroups) == 0 {
		return merr.WrapErrServiceInternalMsg("partial update: query succeeded but CAS candidate write channel groups are empty")
	}
	for channel := range ut.partialUpdateCASGroups {
		ts, ok := channelReadTs.Get(channel)
		if !ok {
			return merr.WrapErrServiceInternalMsg("partial update: query succeeded but read timestamp is missing for candidate write channel %q", channel)
		}
		if ts == 0 {
			return merr.WrapErrServiceInternalMsg("partial update: query succeeded but read timestamp is zero for candidate write channel %q", channel)
		}
	}
	for channel, meta := range ut.partialUpdateCASGroups {
		meta.ReadTs, _ = channelReadTs.Get(channel)
		if ut.partialState != nil && meta.ReadTs > ut.partialState.timestamp {
			ut.partialState.timestamp = meta.ReadTs
		}
	}
	return nil
}

func (ut *UpsertTask) buildPartialUpdateCASGroups(route *writeRoute) (map[string]*messagespb.PartialUpdateCAS, error) {
	primaryFieldSchema, err := typeutil.GetPrimaryFieldSchema(ut.schema.CollectionSchema)
	if err != nil {
		return nil, err
	}
	primaryFieldData, err := typeutil.GetPrimaryFieldData(ut.req.GetFieldsData(), primaryFieldSchema)
	if err != nil {
		return nil, merr.WrapErrParameterInvalidMsg(err.Error())
	}
	originalIDs, err := parsePrimaryFieldData2IDs(primaryFieldData)
	if err != nil {
		return nil, err
	}

	originalIDs = ut.selectedPartialIDs(originalIDs)
	size := typeutil.GetSizeOfIDs(originalIDs)
	if size == 0 {
		if ut.partialState != nil && ut.partialState.writeRows != nil {
			return map[string]*messagespb.PartialUpdateCAS{}, nil
		}
		return nil, merr.WrapErrParameterInvalidMsg("partial update primary keys are empty")
	}
	vchannels := route.vchannels
	channelIndexes, err := ut.partialUpdateCASChannelIndexes(route.table, originalIDs, vchannels)
	if err != nil {
		return nil, err
	}
	groups := make(map[string]*messagespb.PartialUpdateCAS, len(vchannels))
	for _, channelIndex := range channelIndexes {
		vchannel := vchannels[channelIndex]
		group := groups[vchannel]
		if group == nil {
			group = &messagespb.PartialUpdateCAS{}
			groups[vchannel] = group
		}
	}
	return groups, nil
}

// partialUpdateCASChannelIndexes mirrors normal upsert routing so CAS proof
// and the corresponding DML transaction target the same vchannel.
func (ut *UpsertTask) partialUpdateCASChannelIndexes(table *routing.Table, ids *schemapb.IDs, vchannels []string) ([]uint32, error) {
	channelID, ok, err := dql.NamespaceShardingChannelID(ut.schema.CollectionSchema, ut.req.Namespace, vchannels)
	if err != nil {
		return nil, err
	}
	if !ok {
		return pkChannelIndexes(table, ids, vchannels)
	}
	if table != nil {
		return nil, errNamespacePlacementOnSplitCollection()
	}
	size := typeutil.GetSizeOfIDs(ids)
	channelIndexes := make([]uint32, size)
	for offset := range channelIndexes {
		channelIndexes[offset] = channelID
	}
	return channelIndexes, nil
}

// canRetryPartialUpdateCASConflict reports whether rebuilding the whole request
// after a deterministic CAS conflict preserves the requested operation semantics.
func (ut *UpsertTask) canRetryPartialUpdateCASConflict() bool {
	for _, op := range ut.req.GetFieldOps() {
		if op.GetOp() != schemapb.FieldPartialUpdateOp_REPLACE {
			return false
		}
	}
	return true
}
