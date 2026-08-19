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

package streamingnode

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/txn"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// SaveRecoverySnapshot saves a WAL recovery snapshot in one compound
// operation: module upserts/removals, salvage checkpoint, and strictly last
// the consume checkpoint - the commit point of the snapshot.
// Nil or empty parts of the snapshot are skipped.
//
// The ops are staged into a txn.Builder (reusing the per-key encoders -
// buildSegmentAssignmentKey, getRemovalAndSaveForVChannel,
// buildSalvageCheckpointPath, buildConsumeCheckpointKey) and applied via
// txn.Commit: atomically in a single guarded txn when the whole op set fits
// the store's txn op limit, otherwise via the ordered chunked fallback. Either
// way the consume checkpoint is staged with CommitSave, so it is the last
// write to become visible. On the fallback path, component deltas written
// before a crash remain replay-safe through their own checkpoint_time_tick.
func (c *catalog) SaveRecoverySnapshot(ctx context.Context, pChannelName string, snapshot *metastore.WALRecoverySnapshot) error {
	if snapshot == nil {
		return nil
	}
	b := txn.New()
	if snapshot.PChannelControlMeta != nil {
		data, err := proto.Marshal(snapshot.PChannelControlMeta)
		if err != nil {
			return merr.WrapErrSerializationFailed(err, "marshal recovery control meta at pchannel %s", pChannelName)
		}
		b.Save(buildRecoveryControlKey(pChannelName), string(data))
	}
	// Aggregate every module mutation before adding the checkpoint commit
	// marker. Closed and tombstoned recovery metadata remains persisted until
	// the growing-module cleanup task explicitly includes its removal here.
	removes := make([]string, 0, len(snapshot.RemovedSegmentIDs)+len(snapshot.RemovedTransformLogs))
	vchannelSaves := make(map[string]string, len(snapshot.VChannels)+len(snapshot.VChannelBaseMetas))
	segmentSaves := make(map[string]string, len(snapshot.SegmentAssignments))
	transformLogSaves := make(map[string]string, len(snapshot.TransformLogMetas))
	for _, info := range snapshot.SegmentAssignments {
		key := buildSegmentAssignmentKey(pChannelName, info.GetSegmentId())
		data, err := proto.Marshal(info)
		if err != nil {
			return merr.WrapErrSerializationFailed(err, "marshal segment %d at pchannel %s", info.GetSegmentId(), pChannelName)
		}
		segmentSaves[key] = string(data)
	}
	for _, segmentID := range snapshot.RemovedSegmentIDs {
		removes = append(removes, buildSegmentAssignmentKey(pChannelName, segmentID))
	}
	for _, info := range snapshot.VChannels {
		vremoves, kvs, err := c.getRemovalAndSaveForVChannel(pChannelName, info)
		if err != nil {
			return err
		}
		removes = append(removes, vremoves...)
		for k, v := range kvs {
			vchannelSaves[k] = v
		}
	}
	for _, info := range snapshot.VChannelBaseMetas {
		data, err := marshalVChannelBaseMeta(pChannelName, info)
		if err != nil {
			return err
		}
		vchannelSaves[buildVChannelKey(pChannelName, info.GetVchannel())] = data
	}
	for vchannel, meta := range snapshot.TransformLogMetas {
		key, err := buildTransformLogKey(pChannelName, vchannel)
		if err != nil {
			return err
		}
		data, err := proto.Marshal(meta)
		if err != nil {
			return merr.WrapErrSerializationFailed(err, "marshal transform log meta %s at pchannel %s", vchannel, pChannelName)
		}
		transformLogSaves[key] = string(data)
	}
	for _, vchannel := range snapshot.RemovedTransformLogs {
		key, err := buildTransformLogKey(pChannelName, vchannel)
		if err != nil {
			return err
		}
		removes = append(removes, key)
	}
	// A vchannel cleanup also removes its transform-log meta. Keep the
	// vchannel removal last so the chunked fallback never exposes an orphaned
	// transform log for a vchannel that has already disappeared.
	for _, info := range snapshot.RemovedVChannels {
		removes = append(removes, buildVChannelKey(pChannelName, info.GetVchannel()))
		for _, schema := range info.GetCollectionInfo().GetSchemas() {
			removes = append(removes, buildVChannelSchemaKey(
				pChannelName,
				info.GetVchannel(),
				schema.GetCheckpointTimeTick(),
			))
		}
	}
	for _, r := range removes {
		b.Remove(r)
	}
	// Persist vchannel ownership before its dependent segment/transform-log
	// metadata on the chunked fallback path. Atomic commits are unaffected.
	for k, v := range vchannelSaves {
		b.Save(k, v)
	}
	for k, v := range segmentSaves {
		b.Save(k, v)
	}
	for k, v := range transformLogSaves {
		b.Save(k, v)
	}
	// The salvage checkpoint must be persisted before the consume checkpoint
	// to guarantee ordering across a crash in between. It is a plain save
	// staged after the bulk saves (still coalesced into the same save run), so
	// on the fallback path txn.Commit always flushes it before the CommitSave
	// below.
	if snapshot.SalvageCheckpoint != nil {
		key := buildSalvageCheckpointPath(pChannelName, snapshot.SalvageCheckpoint.GetClusterId())
		data, err := proto.Marshal(snapshot.SalvageCheckpoint)
		if err != nil {
			return merr.WrapErrSerializationFailed(err, "marshal salvage checkpoint at pchannel %s", pChannelName)
		}
		b.Save(key, string(data))
	}
	// The consume checkpoint is the commit point of the snapshot: staging it
	// with CommitSave makes it the last write to become visible, after every
	// other part of the snapshot has landed.
	if snapshot.ConsumeCheckpoint != nil {
		key := buildConsumeCheckpointKey(pChannelName)
		data, err := proto.Marshal(snapshot.ConsumeCheckpoint)
		if err != nil {
			return merr.WrapErrSerializationFailed(err, "marshal consume checkpoint at pchannel %s", pChannelName)
		}
		b.CommitSave(key, string(data))
	}
	return txn.Commit(ctx, c.metaKV, b)
}
