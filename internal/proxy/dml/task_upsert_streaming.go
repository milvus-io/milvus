// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
func (ut *UpsertTask) preparePartialUpdateRetryAttempt(ctx context.Context) error {
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

func (ut *UpsertTask) appendUpsertAttempt(ctx context.Context, ez *message.CipherConfig) error {
	logger := mlog.With(mlog.FieldCollectionName(ut.req.CollectionName))

	messages, err := ut.packUpsertMessages(ctx, ez)
	if err != nil {
		logger.Warn(ctx, "failed to prepare Upsert WAL messages", mlog.Err(err))
		return err
	}

	if ut.req.GetPartialUpdate() {
		if err := ut.validatePartialUpdateCASCoverage(messages); err != nil {
			logger.Warn(ctx, "attach partial update CAS metadata failed", mlog.Err(err))
			return err
		}
	}
	resp := streaming.WAL().AppendMessages(ctx, messages...)
	invalidateMismatchedWriteSchema(ctx, ut.GetMetaCache(), ut.collectionID, resp)
	appendErr := resp.UnwrapFirstError()
	if ut.req.GetPartialUpdate() {
		appendErr = unwrapPartialUpdateAppendError(resp)
	}
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

func (ut *UpsertTask) packUpsertMessages(ctx context.Context, ez *message.CipherConfig) ([]message.MutableMessage, error) {
	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy insertExecute upsert %d", ut.ID()))
	defer tr.Elapse("insert execute done when insertExecute")

	collID := ut.collectionID
	ut.upsertMsg.InsertMsg.CollectionID = collID
	log := mlog.With(
		mlog.FieldCollectionID(collID))

	getMsgStreamDur := tr.RecordSpan()
	channelNames, err := ut.chMgr.GetVChannels(collID)
	if err != nil {
		log.Warn(ctx, "get vChannels failed when insertExecute",
			mlog.Err(err))
		ut.result.Status = merr.Status(err)
		return nil, err
	}

	log.Debug(ctx, "send insert request to virtual channels when insertExecute",
		mlog.String("collection", ut.req.GetCollectionName()),
		mlog.String("partition", ut.req.GetPartitionName()),
		mlog.FieldCollectionID(collID),
		mlog.Strings("virtual_channels", channelNames),
		mlog.FieldTaskID(ut.ID()),
		mlog.Duration("get msgStream duration", getMsgStreamDur))

	if err := validateUpsertNamespace(ut.req.Namespace, ut.schema.CollectionSchema); err != nil {
		return nil, err
	}
	return repackUpsertMessages(ctx, ut.GetMetaCache(), channelNames, ut.upsertMsg.InsertMsg, ut.result.GetIDs(), ut.partitionKeys, ut.schema.CollectionSchema, ut.schemaVersion, ez, ut.partialUpdateCASGroups, "", ut.upsertMsg.DeleteMsg.GetPrimaryKeys(), ut.upsertMsg.DeleteMsg.PartitionID)
}

func (ut *UpsertTask) validatePartialUpdateCASCoverage(messages []message.MutableMessage) error {
	groups := ut.partialUpdateCASGroups
	if len(groups) == 0 {
		return merr.WrapErrServiceInternalMsg("partial update CAS metadata snapshot is empty")
	}

	attached := make(map[string]struct{}, len(groups))
	for _, msg := range messages {
		vchannel := msg.VChannel()
		_, ok := groups[vchannel]
		if !ok {
			return merr.WrapErrServiceInternalMsg("partial update insert has no CAS metadata for vchannel %s", vchannel)
		}
		if !message.HasPartialUpdateCAS(msg) {
			return merr.WrapErrServiceInternalMsg("partial update insert is missing CAS metadata for vchannel %s", vchannel)
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
	groups, err := ut.buildPartialUpdateCASGroups()
	if err != nil {
		return err
	}
	pkSchema, err := typeutil.GetPrimaryFieldSchema(ut.schema.CollectionSchema)
	if err != nil {
		return err
	}
	if pkSchema.GetAutoID() {
		// Newly allocated PKs may target any shard. Capture terms before the
		// single Strong read; namespace routing still fixes one destination.
		vchannels, err := ut.chMgr.GetVChannels(ut.collectionID)
		if err != nil {
			return err
		}
		_, fixedChannel, err := dql.NamespaceShardingChannelID(ut.schema.CollectionSchema, ut.req.Namespace, vchannels)
		if err != nil {
			return err
		}
		if !fixedChannel {
			for _, channel := range vchannels {
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
	}
	return nil
}

func (ut *UpsertTask) buildPartialUpdateCASGroups() (map[string]*messagespb.PartialUpdateCAS, error) {
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
	channelIndexes, err := ut.partialUpdateCASChannelIndexes(originalIDs, vchannels)
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
func (ut *UpsertTask) partialUpdateCASChannelIndexes(ids *schemapb.IDs, vchannels []string) ([]uint32, error) {
	channelID, ok, err := dql.NamespaceShardingChannelID(ut.schema.CollectionSchema, ut.req.Namespace, vchannels)
	if err != nil {
		return nil, err
	}
	if !ok {
		return typeutil.HashPK2Channels(ids, vchannels)
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
