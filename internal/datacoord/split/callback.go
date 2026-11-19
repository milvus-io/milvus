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

package split

import (
	"context"
	"strconv"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// ShardDeclarer is the part of the DataView manager the callback uses.
type ShardDeclarer interface {
	DeclareShards(ctx context.Context, event dataview.DeclareShardsDataViewEvent) (*viewpb.DataVersion, error)
}

// residualWaitLogInterval is how often a callback that waits for the residual
// of a source says so and updates its wait gauge. A variable for tests.
var residualWaitLogInterval = 30 * time.Second

// Callback is the DataCoord half of the consumption of a SplitShard message.
// It runs on every cluster that consumes the message, the cluster that issued
// it and the clusters that received it through replication alike, and each of
// its steps may run again after a failure or a coordinator restart.
type Callback struct {
	Store *Store
	// DataView returns the DataView manager, nil while it is not recovered.
	DataView func() ShardDeclarer
}

// recordOf reads a split from the result of its broadcast. T_switch and the
// genesis time ticks are those of the copies in the WAL of this cluster. The
// residues of the targets and their modulus are those the message carries,
// the same on every cluster.
func recordOf(ctx context.Context, result message.BroadcastResultSplitShardMessageV2) (Record, error) {
	header := result.Message.Header()
	record := Record{
		TaskID:       header.GetSplitTaskId(),
		CollectionID: header.GetCollectionId(),
		Source:       header.GetSourceVchannel(),
	}
	if rh := result.Message.ReplicateHeader(); rh != nil {
		if rh.ClusterID == "" {
			return Record{}, merr.WrapErrServiceInternalMsg("replicated shard split %d has no source cluster", record.TaskID)
		}
		record.ReplicatedFrom = rh.ClusterID
	}
	source := result.Results[record.Source]
	if record.Source == "" || source == nil || source.TimeTick == 0 {
		return Record{}, merr.WrapErrServiceInternalMsg(
			"the result of shard split task %d carries no time tick for its source %q", record.TaskID, record.Source)
	}
	record.SwitchTimeTick = source.TimeTick
	for _, vchannel := range header.GetTargetVchannels() {
		target := result.Results[vchannel]
		if vchannel == "" || target == nil || target.TimeTick == 0 {
			return Record{}, merr.WrapErrServiceInternalMsg(
				"the result of shard split task %d carries no time tick for its target %q", record.TaskID, vchannel)
		}
		record.Targets = append(record.Targets, Target{VChannel: vchannel, GenesisTimeTick: target.TimeTick})
	}
	if len(record.Targets) == 0 {
		return Record{}, merr.WrapErrServiceInternalMsg("shard split task %d names no target", record.TaskID)
	}
	body, err := result.Message.Body(ctx)
	if err != nil {
		return Record{}, merr.Wrapf(err, "read the body of shard split task %d", record.TaskID)
	}
	if err := fillTargetResidues(&record, body.GetRouting()); err != nil {
		return Record{}, err
	}
	return record, nil
}

// fillTargetResidues copies, from the routing the message carries, the
// residues each target owns and their modulus. A message without routing
// leaves them empty: the rewrite of such a split refuses to start
// (RewritePlanner), nothing else reads them.
func fillTargetResidues(record *Record, routing *message.SplitShardRouting) error {
	if routing == nil || routing.GetRoutingModulus() == 0 {
		return nil
	}
	residues := make(map[string][]uint64, len(routing.GetShards()))
	for _, shard := range routing.GetShards() {
		residues[shard.GetVchannelName()] = shard.GetHashRouting().GetBuckets()
	}
	record.RoutingModulus = routing.GetRoutingModulus()
	for i := range record.Targets {
		owned, ok := residues[record.Targets[i].VChannel]
		if !ok || len(owned) == 0 {
			return merr.WrapErrServiceInternalMsg("the routing of shard split task %d gives no residue to its target %q",
				record.TaskID, record.Targets[i].VChannel)
		}
		record.Targets[i].Residues = owned
	}
	return nil
}

// OnAcked runs once every copy of the SplitShard message is appended, before
// RootCoord publishes the new routing. In this order:
//
//  1. Declare the targets in the DataView, each with its Transform frontier at
//     its pin. A flush on a target finds its shard from here on.
//  2. Record the split: T_switch, the targets and the state "window open".
//     The record is one durable write, and the pin of the Transform frontier
//     of the targets (Store.TransformPins) and the suspension of their L0
//     compaction (Store.IsL0CompactionSuspended) are read from it.
//
// Between 1 and 2, and before 1, DataCoord refuses the final flush of a
// segment of a target (Server.SaveBinlogPaths): accepted without the pin it
// would move the frontier of the target past T_switch.
//
// Both steps are idempotent. A recorded split keeps its record, so a callback
// entered again declares nothing and writes nothing.
func (c *Callback) OnAcked(ctx context.Context, result message.BroadcastResultSplitShardMessageV2) error {
	record, err := recordOf(ctx, result)
	if err != nil {
		return err
	}
	// A split that is recorded keeps the ticks of its record: the DataView was
	// declared with them, and another attempt may carry other ticks.
	if existing := c.Store.Get(record.TaskID); existing != nil && existing.GetState() >= InWindowOpenState {
		record.SwitchTimeTick = existing.GetSwitchTimeTick()
		for i, target := range existing.GetTargets() {
			if i < len(record.Targets) && record.Targets[i].VChannel == target.GetVchannel() {
				record.Targets[i].GenesisTimeTick = target.GetGenesisTimeTick()
			}
		}
	}

	manager := c.DataView()
	if manager == nil {
		return merr.WrapErrServiceNotReadyMsg("DataView manager is not ready to declare the targets of shard split task %d", record.TaskID)
	}
	event := dataview.DeclareShardsDataViewEvent{
		Source:          record.Source,
		Lineage:         &viewpb.QueryViewLineage{SwitchTimetick: record.SwitchTimeTick},
		CollectionID:    record.CollectionID,
		TransformStarts: make(map[string]uint64, len(record.Targets)),
	}
	for _, target := range record.Targets {
		event.VChannels = append(event.VChannels, target.VChannel)
		event.Lineage.Vchannels = append(event.Lineage.Vchannels, target.VChannel)
		event.TransformStarts[target.VChannel] = TransformPin(record.SwitchTimeTick, target.GenesisTimeTick)
	}
	version, err := manager.DeclareShards(ctx, event)
	if err != nil {
		return merr.Wrapf(err, "declare the targets of shard split task %d", record.TaskID)
	}
	if version == nil {
		// The DataViews of the collection are dropped: the collection is
		// gone, and a record written now would never be removed.
		mlog.Warn(ctx, "shard split of a collection whose DataView is dropped, it is not recorded",
			mlog.Int64("splitTaskID", record.TaskID), mlog.FieldCollectionID(record.CollectionID))
		return nil
	}

	if _, err := c.Store.Record(ctx, record); err != nil {
		return merr.Wrapf(err, "record shard split task %d", record.TaskID)
	}
	return nil
}

// InWindowOpenState is the first state in which a split is recorded by its
// ack callback.
const InWindowOpenState = datapb.SplitShardTaskState_SplitShardTaskWindowOpen

// OnApplied runs after RootCoord has published the new routing, still inside
// the ack callback. It returns when the residual of the source has flushed.
//
// The callback holds the broadcast resource key of the collection while it
// runs, on the cluster that issued the split and, through the ack callback
// scheduler, on a cluster that received it. Waiting here therefore keeps
// every other DDL of the collection behind the split until the source holds
// only flushed data. The write path does not wait: the routing is published
// before this step.
//
// A coordinator that restarts while it waits enters the callback again and
// waits again; the acknowledgement of the source is durable in the record.
//
// The wait has no end of its own. While it lasts, every DDL of the collection
// waits behind it, DropCollection and release included: releasing the key
// before the residual has flushed would let a truncate, a drop of a partition
// or a drop of the collection interleave with segments of the source that are
// still being flushed (spec 10.1), and nothing on master tells DataCoord that a
// drop is waiting for the key. The source acknowledgement can be held back by
// a StreamingNode that cannot flush the source or materialize its L0 data up
// to the fence (an owner poisoned in the L0 materializer, a node that is down
// or stuck); the remedy is on that node, as for a ManualFlush that never
// completes. The wait is visible: the gauge
// shard_split_residual_wait_seconds{collection_id} gives its age from the
// record of the split, which survives a restart, and after
// dataCoord.shardSplit.residualWaitAlertThreshold it is logged as an error at
// every interval.
func (c *Callback) OnApplied(ctx context.Context, result message.BroadcastResultSplitShardMessageV2) error {
	header := result.Message.Header()
	taskID := header.GetSplitTaskId()
	task := c.Store.Get(taskID)
	if task == nil {
		// The collection was gone when the callback checked it, so the split
		// was never recorded. There is no residual to wait for.
		return nil
	}
	since := time.Now()
	if task.GetRecordTime() > 0 {
		since = time.Unix(task.GetRecordTime(), 0)
	}
	collection := strconv.FormatInt(header.GetCollectionId(), 10)
	gauge := metrics.DataCoordShardSplitResidualWaitSeconds.WithLabelValues(collection)
	defer metrics.DataCoordShardSplitResidualWaitSeconds.DeleteLabelValues(collection)
	for {
		gauge.Set(time.Since(since).Seconds())
		waitCtx, cancel := context.WithTimeout(ctx, residualWaitLogInterval)
		err := c.Store.WaitResidualFlushed(waitCtx, taskID)
		cancel()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if waitCtx.Err() == nil {
			return err
		}
		waited := time.Since(since)
		fields := []mlog.Field{
			mlog.Int64("splitTaskID", taskID),
			mlog.FieldCollectionID(header.GetCollectionId()),
			mlog.String("source", header.GetSourceVchannel()),
			mlog.Duration("waited", waited),
		}
		threshold := paramtable.Get().DataCoordCfg.ShardSplitResidualWaitAlertThreshold.GetAsDuration(time.Second)
		if threshold > 0 && waited >= threshold {
			mlog.Error(ctx, "shard split still waits for the StreamingNode of its source to acknowledge the fence; "+
				"every DDL of the collection, drop included, waits behind it. Check that the StreamingNode of the source "+
				"is up and can flush the source and materialize its L0 data", fields...)
			continue
		}
		mlog.Warn(ctx, "shard split keeps the resource key of its collection until the residual of its source has flushed", fields...)
	}
}

// OnSourceConsumed runs when a StreamingNode acknowledges a copy of a
// SplitShard message from the consuming side. The copy on the source is
// acknowledged once the StreamingNode has flushed every segment the fence
// sealed and materialized the L0 data up to the fence, which is what the
// record keeps as "source acked". Copies on other vchannels are ignored.
//
// An error is returned to the StreamingNode, which sends the acknowledgement
// again.
func (c *Callback) OnSourceConsumed(ctx context.Context, msg message.ImmutableMessage, completed bool) error {
	if msg.MessageType() != message.MessageTypeSplitShard {
		return nil
	}
	split, err := message.AsImmutableSplitShardMessageV2(msg)
	if err != nil {
		return merr.Wrap(err, "read the acknowledged shard split message")
	}
	header := split.Header()
	if msg.VChannel() != header.GetSourceVchannel() {
		return nil
	}
	var proof *commonpb.ReplicateCheckpoint
	if rh := msg.ReplicateHeader(); rh != nil && !header.GetSupplementalSourceFence() {
		if rh.ClusterID == "" || rh.MessageID == nil || rh.TimeTick == 0 || rh.VChannel == "" || funcutil.IsControlChannel(rh.VChannel) {
			return merr.WrapErrServiceInternalMsg("replicated source of shard split %d has no original source WAL position", header.GetSplitTaskId())
		}
		proof = &commonpb.ReplicateCheckpoint{
			ClusterId: rh.ClusterID,
			Pchannel:  funcutil.ToPhysicalChannel(rh.VChannel),
			MessageId: rh.MessageID.IntoProto(),
			TimeTick:  rh.TimeTick,
		}
	}
	return c.Store.MarkSourceAcked(ctx, header.GetSplitTaskId(), header.GetCollectionId(),
		header.GetSourceVchannel(), header.GetTargetVchannels(), proof, completed)
}
