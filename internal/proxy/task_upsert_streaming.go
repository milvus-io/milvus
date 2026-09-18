package proxy

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	partialUpdateCASMaxRetryAttempts = 5
	partialUpdateCASRetryBackoff     = 10 * time.Millisecond
)

func (ut *upsertTask) Execute(ctx context.Context) error {
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

func (ut *upsertTask) executePartialUpdateWithCASRetry(ctx context.Context, ez *message.CipherConfig) error {
	// A request-level retry can reapply relative operations on vchannels that
	// already committed before another vchannel rejected the CAS.
	if !ut.canRetryPartialUpdateCASConflict() {
		return projectPartialUpdateCASError(ut.appendUpsertAttempt(ctx, ez), false)
	}

	if ut.partialUpdateOriginalFields == nil {
		return merr.WrapErrServiceInternalMsg("partial update: original request fields are unavailable")
	}
	attempt := 0
	err := retry.Do(ctx, func() error {
		if attempt > 0 {
			if err := ut.preparePartialUpdateRetryAttempt(ctx); err != nil {
				return err
			}
		}
		attempt++
		return ut.appendUpsertAttempt(ctx, ez)
	},
		retry.Attempts(partialUpdateCASMaxRetryAttempts),
		retry.Sleep(partialUpdateCASRetryBackoff),
		retry.MaxSleepTime(4*partialUpdateCASRetryBackoff),
		retry.RetryErr(func(err error) bool {
			return status.AsStreamingError(err).IsPartialUpdateRetryableCAS()
		}),
	)
	return projectPartialUpdateCASError(err, true)
}

func projectPartialUpdateCASError(err error, allowConflictRetry bool) error {
	if err == nil || !status.AsStreamingError(err).IsPartialUpdateRetryableCAS() {
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
func (ut *upsertTask) preparePartialUpdateRetryAttempt(ctx context.Context) error {
	if err := ut.prepareUpsert(ctx); err != nil {
		return err
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

// appendUpsertAttempt packs and appends one upsert, re-routing what a shard
// split's fence refused (see shard_fenced_retry.go).
//
// Both halves are settled per message. A row or tombstone whose message
// committed is never sent again: re-sending a row writes it twice, and
// re-sending a tombstone to a vchannel whose transaction committed gives it a
// later tick than the row this upsert inserted there in the same transaction,
// deleting it. The key of an insert and of its delete route to the same shard,
// so they are refused -- and re-sent -- together. Neither half is ever sent to a
// vchannel a fence already refused.
//
// A partial update does not enter the fence retry: its CAS proof binds the
// vchannels it read, so a re-route would carry a proof for the wrong shard. It
// evicts the collection and fails retriably instead, and the client's retry
// reads and routes against the new shards.
func (ut *upsertTask) appendUpsertAttempt(ctx context.Context, ez *message.CipherConfig) error {
	logger := mlog.With(mlog.FieldCollectionName(ut.req.CollectionName))
	partialUpdate := ut.req.GetPartialUpdate()

	fence := newSplitFence()
	pendingInserts := newPendingRows(int(ut.upsertMsg.InsertMsg.NumRows), fence)
	pendingDeletes := newPendingRows(typeutil.GetSizeOfIDs(ut.deletePrimaryKeys()), fence)
	appendErr := retry.Handle(ctx, func() (bool, error) {
		route, err := ut.writeRoute(ctx)
		if err != nil {
			logger.Warn(ctx, "resolve the write route failed", mlog.Err(err))
			return false, err
		}
		insertMsgs, insertOffsets, err := ut.packInsertMessage(ctx, ez, route, pendingInserts)
		if err != nil {
			logger.Warn(ctx, "pack insert message failed", mlog.Err(err))
			return false, err
		}
		deleteMsgs, deleteOffsets, err := ut.packDeleteMessage(ctx, ez, route, pendingDeletes)
		if err != nil {
			logger.Warn(ctx, "pack delete message failed", mlog.Err(err))
			return false, err
		}
		deleteMsgs, deleteOffsets = pendingDeletes.dropFenced(deleteMsgs, deleteOffsets)

		messages := make([]message.MutableMessage, 0, len(insertMsgs)+len(deleteMsgs))
		messages = append(messages, insertMsgs...)
		messages = append(messages, deleteMsgs...)
		if partialUpdate {
			if err := ut.attachPartialUpdateCAS(messages); err != nil {
				logger.Warn(ctx, "attach partial update CAS metadata failed", mlog.Err(err))
				return false, err
			}
		}
		resp := streaming.WAL().AppendMessages(ctx, messages...)

		// A partial update reports CAS outcomes the fence logic must not swallow,
		// so it keeps its own unwrapping and never enters the fence retry.
		if partialUpdate {
			fence.observe(resp)
			return false, ut.partialUpdateFenceRefusal(ctx, resp)
		}

		insertDurable, err := fence.settle(resp, insertMsgs, insertOffsets)
		pendingInserts.settle(insertDurable)
		deleteResp := streaming.AppendResponses{Responses: resp.Responses[len(insertMsgs):]}
		deleteDurable, deleteErr := fence.settle(deleteResp, deleteMsgs, deleteOffsets)
		pendingDeletes.settle(deleteDurable)
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
		if status.AsStreamingError(appendErr).IsSchemaVersionMismatch() {
			return merr.ErrCollectionSchemaMismatch
		}
		return appendErr
	}
	// Update result.Timestamp for session consistency: the highest tick any
	// attempt reached, since earlier attempts landed rows too.
	ut.result.Timestamp = fence.maxTimeTick
	return nil
}

// partialUpdateFenceRefusal returns the outcome of a partial update's append.
// A vchannel a shard split fenced evicts the collection, so the client's retry
// routes against the split's targets, and fails the request retriably.
func (ut *upsertTask) partialUpdateFenceRefusal(ctx context.Context, resp streaming.AppendResponses) error {
	for _, response := range resp.Responses {
		if response.Error == nil || !status.AsStreamingError(response.Error).IsShardFenced() {
			continue
		}
		ut.GetMetaCache().RemoveCollectionsByID(ctx, ut.collectionID)
		return merr.WrapErrServiceUnavailableErr(response.Error,
			"partial update reached a vchannel fenced by a shard split; retry against the refreshed routing")
	}
	return unwrapPartialUpdateAppendError(resp)
}

// writeRoute reads the route of one upsert attempt.
func (ut *upsertTask) writeRoute(ctx context.Context) (*writeRoute, error) {
	return resolveWriteRoute(ctx, ut.GetMetaCache(), ut.req.GetDbName(), ut.req.GetCollectionName(), ut.collectionID)
}

// deletePrimaryKeys returns the keys the upsert's delete half tombstones:
// the ones queryPreExecute resolved, or the request's old ids when it did not.
func (ut *upsertTask) deletePrimaryKeys() *schemapb.IDs {
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
		if !status.AsStreamingError(response.Error).IsPartialUpdateRetryableCAS() {
			return response.Error
		}
		if casErr == nil {
			casErr = response.Error
		}
	}
	return casErr
}

func (ut *upsertTask) packInsertMessage(ctx context.Context, ez *message.CipherConfig, route *writeRoute, pending *pendingRows) ([]message.MutableMessage, [][]int, error) {
	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy insertExecute upsert %d", ut.ID()))
	defer tr.Elapse("insert execute done when insertExecute")

	collectionName := ut.upsertMsg.InsertMsg.CollectionName
	collID, err := ut.GetMetaCache().GetCollectionID(ctx, ut.req.GetDbName(), collectionName)
	if err != nil {
		return nil, nil, err
	}
	ut.upsertMsg.InsertMsg.CollectionID = collID
	log := mlog.With(
		mlog.FieldCollectionID(collID))
	getCacheDur := tr.RecordSpan()

	log.Debug(ctx, "send insert request to virtual channels when insertExecute",
		mlog.String("collection", ut.req.GetCollectionName()),
		mlog.String("partition", ut.req.GetPartitionName()),
		mlog.FieldCollectionID(collID),
		mlog.Strings("virtual_channels", route.vchannels),
		mlog.FieldTaskID(ut.ID()),
		mlog.Duration("get cache duration", getCacheDur))

	// start to repack insert data
	var msgs []message.MutableMessage
	var msgOffsets [][]int
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
	return msgs, msgOffsets, nil
}

func (ut *upsertTask) packDeleteMessage(ctx context.Context, ez *message.CipherConfig, route *writeRoute, pending *pendingRows) ([]message.MutableMessage, [][]int, error) {
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
		ut.req.GetDbName(),
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

func (ut *upsertTask) attachPartialUpdateCAS(messages []message.MutableMessage) error {
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
		if Params.ProxyCfg.SplitChunkProxy.GetAsBool() {
			maxMessageSize := Params.PulsarCfg.MaxMessageSize.GetAsInt()
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
func (ut *upsertTask) preparePartialUpdateCASGroups(ctx context.Context) error {
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
		_, fixedChannel, err := namespaceShardingChannelID(ut.schema.CollectionSchema, ut.req.Namespace, route.vchannels)
		if err != nil {
			return err
		}
		if !fixedChannel {
			for _, channel := range route.writable {
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
func (ut *upsertTask) bindPartialUpdateReadTimestamps(channelReadTs *typeutil.ConcurrentMap[string, uint64]) error {
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
	}
	return nil
}

func (ut *upsertTask) buildPartialUpdateCASGroups(route *writeRoute) (map[string]*messagespb.PartialUpdateCAS, error) {
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

	size := typeutil.GetSizeOfIDs(originalIDs)
	if size == 0 {
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
func (ut *upsertTask) partialUpdateCASChannelIndexes(table *routing.ResidueTable, ids *schemapb.IDs, vchannels []string) ([]uint32, error) {
	channelID, ok, err := namespaceShardingChannelID(ut.schema.CollectionSchema, ut.req.Namespace, vchannels)
	if err != nil {
		return nil, err
	}
	if !ok {
		return pkChannelIndexes(table, ids, vchannels)
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
func (ut *upsertTask) canRetryPartialUpdateCASConflict() bool {
	for _, op := range ut.req.GetFieldOps() {
		if op.GetOp() != schemapb.FieldPartialUpdateOp_REPLACE {
			return false
		}
	}
	return true
}
