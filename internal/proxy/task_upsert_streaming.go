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
		return merr.WrapErrServiceInternalMsg("partial update original fields snapshot is unavailable")
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
// term, read timestamp, query, and DML state for one retry.
func (ut *upsertTask) preparePartialUpdateRetryAttempt(ctx context.Context) error {
	fields := cloneFieldDataList(ut.partialUpdateOriginalFields)
	ut.req.FieldsData = fields
	ut.upsertMsg.InsertMsg.FieldsData = fields
	if err := genFunctionFields(ctx, ut.upsertMsg.InsertMsg, ut.schema, true); err != nil {
		return err
	}
	if err := ut.preparePartialUpdateCASGroups(ctx); err != nil {
		return err
	}
	if err := ut.queryPreExecute(ctx); err != nil {
		return err
	}
	ut.upsertMsg.InsertMsg.FieldsData = ut.insertFieldData
	ut.upsertMsg.DeleteMsg.PrimaryKeys = ut.deletePKs
	ut.upsertMsg.DeleteMsg.NumRows = int64(typeutil.GetSizeOfIDs(ut.deletePKs))
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

func (ut *upsertTask) refreshPartialUpdateReadTs(ctx context.Context) error {
	proxy, ok := ut.node.(*Proxy)
	if !ok || proxy == nil || proxy.tsoAllocator == nil {
		return merr.WrapErrServiceInternal("partial update read timestamp allocator is unavailable")
	}
	ts, err := proxy.tsoAllocator.AllocOne(ctx)
	if err != nil {
		return err
	}
	ut.partialUpdateReadTs = ts
	return nil
}

// appendUpsertAttempt packs and appends one upsert, retrying the rows a
// concurrent shard split refused.
//
// The two halves retry differently, because they fail differently. The INSERT
// half is retried by ROW: its messages commit independently, so re-sending the
// whole half after one shard was fenced would write the committed rows twice
// (see shard_fenced_retry.go). The DELETE half is re-packed whole, which is safe
// because applying a delete twice is indistinguishable from applying it once.
func (ut *upsertTask) appendUpsertAttempt(ctx context.Context, ez *message.CipherConfig) error {
	logger := mlog.With(mlog.FieldCollectionName(ut.req.CollectionName))

	var resp streaming.AppendResponses
	pending := newRowSet(allRowOffsets(int(ut.upsertMsg.InsertMsg.NumRows)))
	appendErr := retry.Handle(ctx, func() (bool, error) {
		insertMsgs, messageOffsets, err := ut.packInsertMessage(ctx, ez, pending)
		if err != nil {
			logger.Warn(ctx, "pack insert message failed", mlog.Err(err))
			return false, err
		}
		deleteMsgs, err := ut.packDeleteMessage(ctx, ez)
		if err != nil {
			logger.Warn(ctx, "pack delete message failed", mlog.Err(err))
			return false, err
		}

		messages := append(insertMsgs, deleteMsgs...)
		if ut.req.GetPartialUpdate() {
			if err := ut.attachPartialUpdateCAS(messages); err != nil {
				logger.Warn(ctx, "attach partial update CAS metadata failed", mlog.Err(err))
				return false, err
			}
		}
		resp = streaming.WAL().AppendMessages(ctx, messages...)

		// A partial update reports CAS outcomes the fence logic must not swallow,
		// so it keeps its own unwrapping and never enters the fence retry.
		if ut.req.GetPartialUpdate() {
			return false, unwrapPartialUpdateAppendError(resp)
		}

		// The response is positional, so the insert half is the leading slice. It
		// is clamped rather than assumed: a WAL that reports fewer responses than
		// messages tells us nothing about the rest, and UnwrapFirstError tolerated
		// that too.
		insertRespCount := min(len(insertMsgs), len(resp.Responses))
		insertResp := streaming.AppendResponses{Responses: resp.Responses[:insertRespCount]}
		refused, insertFenceErr, fatalErr := refusedRows(insertResp, messageOffsets)
		if fatalErr != nil {
			return false, fatalErr
		}
		deleteResp := streaming.AppendResponses{Responses: resp.Responses[insertRespCount:]}
		_, deleteFenceErr, fatalErr := refusedRows(deleteResp, nil)
		if fatalErr != nil {
			return false, fatalErr
		}
		if insertFenceErr == nil && deleteFenceErr == nil {
			return false, nil
		}
		// The rows that landed are done and drop out of the set; an empty set packs
		// no insert messages at all, which is what a run where only the delete half
		// was fenced needs.
		pending = newRowSet(refused)
		ut.getMetaCache().RemoveCollection(ctx, ut.req.GetDbName(), ut.req.GetCollectionName())
		if insertFenceErr != nil {
			return true, insertFenceErr
		}
		return true, deleteFenceErr
	}, retry.Attempts(shardFencedRetryAttempts))
	if appendErr != nil {
		logger.Warn(ctx, "append messages to wal failed", mlog.Err(appendErr))
		if status.AsStreamingError(appendErr).IsSchemaVersionMismatch() {
			return merr.ErrCollectionSchemaMismatch
		}
		return appendErr
	}
	// Update result.Timestamp for session consistency.
	ut.result.Timestamp = resp.MaxTimeTick()
	return nil
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

func (ut *upsertTask) packInsertMessage(ctx context.Context, ez *message.CipherConfig, pending rowSet) ([]message.MutableMessage, [][]int, error) {
	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy insertExecute upsert %d", ut.ID()))
	defer tr.Elapse("insert execute done when insertExecute")

	collectionName := ut.upsertMsg.InsertMsg.CollectionName
	collID, err := ut.getMetaCache().GetCollectionID(ctx, ut.req.GetDbName(), collectionName)
	if err != nil {
		return nil, nil, err
	}
	ut.upsertMsg.InsertMsg.CollectionID = collID
	log := mlog.With(
		mlog.FieldCollectionID(collID))
	getCacheDur := tr.RecordSpan()

	getMsgStreamDur := tr.RecordSpan()
	channelNames, err := ut.chMgr.GetVChannels(collID)
	if err != nil {
		log.Warn(ctx, "get vChannels failed when insertExecute",
			mlog.Err(err))
		ut.result.Status = merr.Status(err)
		return nil, nil, err
	}

	log.Debug(ctx, "send insert request to virtual channels when insertExecute",
		mlog.String("collection", ut.req.GetCollectionName()),
		mlog.String("partition", ut.req.GetPartitionName()),
		mlog.FieldCollectionID(collID),
		mlog.Strings("virtual_channels", channelNames),
		mlog.FieldTaskID(ut.ID()),
		mlog.Duration("get cache duration", getCacheDur),
		mlog.Duration("get msgStream duration", getMsgStreamDur))

	collInfo, err := ut.getMetaCache().GetCollectionInfo(ctx, ut.req.GetDbName(), collectionName, collID)
	if err != nil {
		return nil, nil, err
	}
	table := routingOf(collInfo)

	// start to repack insert data
	var msgs []message.MutableMessage
	var messageOffsets [][]int
	if ut.partitionKeys == nil {
		msgs, messageOffsets, err = repackInsertDataForStreamingService(ut.TraceCtx(), ut.getMetaCache(), table, channelNames, ut.upsertMsg.InsertMsg, ut.result, ez, ut.schemaVersion, ut.partialUpdateCASGroups, pending)
	} else {
		msgs, messageOffsets, err = repackInsertDataWithPartitionKeyForStreamingService(ut.TraceCtx(), ut.getMetaCache(), table, channelNames, ut.upsertMsg.InsertMsg, ut.result, ut.partitionKeys, ez, ut.schema.CollectionSchema, ut.schemaVersion, ut.partialUpdateCASGroups, pending)
	}
	if err != nil {
		log.Warn(ctx, "assign segmentID and repack insert data failed", mlog.Err(err))
		ut.result.Status = merr.Status(err)
		return nil, nil, err
	}
	return msgs, messageOffsets, nil
}

func (ut *upsertTask) packDeleteMessage(ctx context.Context, ez *message.CipherConfig) ([]message.MutableMessage, error) {
	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy deleteExecute upsert %d", ut.ID()))
	collID := ut.upsertMsg.DeleteMsg.CollectionID
	if ut.upsertMsg.DeleteMsg.PrimaryKeys == nil {
		// if primary keys are not set by queryPreExecute, use oldIDs to delete all given records
		ut.upsertMsg.DeleteMsg.PrimaryKeys = ut.oldIDs
	}
	log := mlog.With(
		mlog.FieldCollectionID(collID))
	// hash primary keys to channels
	vChannels, err := ut.chMgr.GetVChannels(collID)
	if err != nil {
		log.Warn(ctx, "get vChannels failed when deleteExecute", mlog.Err(err))
		ut.result.Status = merr.Status(err)
		return nil, err
	}
	table, err := ut.routingTable(ctx)
	if err != nil {
		return nil, err
	}
	result, numRows, err := repackDeleteMsgByHash(
		ctx,
		table,
		ut.upsertMsg.DeleteMsg.PrimaryKeys,
		vChannels, ut.idAllocator,
		ut.BeginTs(),
		ut.upsertMsg.DeleteMsg.CollectionID, ut.upsertMsg.DeleteMsg.CollectionName,
		ut.upsertMsg.DeleteMsg.PartitionID, ut.upsertMsg.DeleteMsg.PartitionName,
		ut.req.GetDbName(),
		ut.req.Namespace,
		ut.schema.CollectionSchema,
	)
	if err != nil {
		return nil, err
	}

	var msgs []message.MutableMessage
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
				return nil, err
			}
			msgs = append(msgs, msg)
		}
	}

	log.Debug(ctx, "Proxy Upsert deleteExecute done",
		mlog.FieldCollectionID(collID),
		mlog.Strings("virtual_channels", vChannels),
		mlog.FieldTaskID(ut.ID()),
		mlog.Int64("numRows", numRows),
		mlog.Duration("prepare duration", tr.ElapseSpan()))

	return msgs, nil
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
		attached[vchannel] = struct{}{}
	}
	for vchannel := range groups {
		if _, ok := attached[vchannel]; !ok {
			return merr.WrapErrServiceInternalMsg("partial update CAS has no insert message for vchannel %s", vchannel)
		}
	}
	return nil
}

// preparePartialUpdateCASGroups resolves all touched PChannel terms before it
// allocates the attempt read timestamp used by both query and CAS proof.
func (ut *upsertTask) preparePartialUpdateCASGroups(ctx context.Context) error {
	ut.partialUpdateCASGroups = nil
	ut.partialUpdateReadTs = 0
	groups, err := ut.buildPartialUpdateCASGroups(ctx)
	if err != nil {
		return err
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
	if err := ut.refreshPartialUpdateReadTs(ctx); err != nil {
		return err
	}
	for _, meta := range groups {
		meta.ReadTs = ut.partialUpdateReadTs
	}
	ut.partialUpdateCASGroups = groups
	return nil
}

func (ut *upsertTask) buildPartialUpdateCASGroups(ctx context.Context) (map[string]*messagespb.PartialUpdateCAS, error) {
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
	if ut.chMgr == nil {
		return nil, merr.WrapErrServiceInternalMsg("partial update channel manager is unavailable")
	}
	vchannels, err := ut.chMgr.GetVChannels(ut.collectionID)
	if err != nil {
		return nil, err
	}
	channelIndexes, err := ut.partialUpdateCASChannelIndexes(ctx, originalIDs, vchannels)
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

// routingTable returns the collection's derived routing table. Every routing
// site of an upsert reads it through here, so the insert half, the delete half
// and the partial-update CAS proof cannot disagree about which shard owns a key.
func (ut *upsertTask) routingTable(ctx context.Context) (*routing.Table, error) {
	collInfo, err := ut.getMetaCache().GetCollectionInfo(ctx, ut.req.GetDbName(), ut.req.GetCollectionName(), ut.collectionID)
	if err != nil {
		return nil, err
	}
	return routingOf(collInfo), nil
}

// partialUpdateCASChannelIndexes mirrors normal upsert routing so CAS proof
// and the corresponding DML transaction target the same vchannel.
func (ut *upsertTask) partialUpdateCASChannelIndexes(ctx context.Context, ids *schemapb.IDs, vchannels []string) ([]uint32, error) {
	table, err := ut.routingTable(ctx)
	if err != nil {
		return nil, err
	}
	channelID, ok, err := namespaceShardingChannelID(table, ut.schema.CollectionSchema, ut.req.Namespace, vchannels)
	if err != nil {
		return nil, err
	}
	if !ok {
		return deleteHashValues(table, ids, vchannels)
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
