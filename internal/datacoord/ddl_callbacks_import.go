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

package datacoord

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// importV1AckCallback handles the ack callback for import messages.
func (c *DDLCallbacks) importV1AckCallback(ctx context.Context, result message.BroadcastResultImportMessageV1) error {
	body := result.Message.MustBody()

	// Ensure Schema.DbName is populated from the broadcast message's DbName,
	// matching the behavior in master where this was set before calling ImportV2.
	if body.Schema != nil {
		body.Schema.DbName = body.DbName
	}

	// Process each vchannel with its own TimeTick (not deprecated MsgBase)
	// Each vchannel gets its own import job with the corresponding TimeTick
	vchannels := make([]string, 0, len(result.Results))
	for vchannel := range result.Results {
		if funcutil.IsControlChannel(vchannel) {
			continue
		}
		vchannels = append(vchannels, vchannel)
	}

	// Call createImportJobFromAck directly instead of ImportV2
	// ImportV2 is only for proxy broadcast, not for ack callback
	importResp, err := c.createImportJobFromAck(ctx, &internalpb.ImportRequestInternal{
		DbID:           0, // already deprecated.
		CollectionID:   body.GetCollectionID(),
		CollectionName: body.GetCollectionName(),
		PartitionIDs:   body.GetPartitionIDs(),
		ChannelNames:   vchannels,
		Schema:         body.GetSchema(),
		Files: lo.Map(body.GetFiles(), func(file *msgpb.ImportFile, _ int) *internalpb.ImportFile {
			return &internalpb.ImportFile{
				Id:    file.GetId(),
				Paths: file.GetPaths(),
			}
		}),
		Options:       funcutil.Map2KeyValuePair(body.GetOptions()),
		DataTimestamp: result.GetMaxTimeTick(), // TODO: use per-vchannel TimeTick in future, must be supported for CDC.
		JobID:         body.GetJobID(),
	})

	err = merr.CheckRPCCall(importResp, err)
	if errors.Is(err, merr.ErrCollectionNotFound) {
		log.Ctx(ctx).Warn("import job creation failed because of collection not found, skip it",
			zap.Strings("vchannels", vchannels),
			zap.String("job_id", importResp.GetJobID()), zap.Error(err))
		return nil
	}
	return err
}

// validateImportRequest validates the import request before broadcasting.
// This includes all validation logic previously done in CheckCallback and Proxy.
func (s *Server) validateImportRequest(ctx context.Context, files []*msgpb.ImportFile, options []*commonpb.KeyValuePair) error {
	// Validate timeout
	_, err := importutilv2.GetTimeoutTs(options)
	if err != nil {
		return err
	}

	// Validate binlog import files if it's a backup
	if importutilv2.IsBackup(options) {
		err = ValidateBinlogImportRequest(ctx, s.meta.chunkManager, files, options)
		if err != nil {
			return err
		}
	}

	// Validate max import job count
	err = ValidateMaxImportJobExceed(ctx, s.importMeta)
	if err != nil {
		return err
	}

	// Validate channel assignment availability and replication configuration
	balancer, err := balance.GetWithContext(ctx)
	if err != nil {
		return err
	}
	channelAssignment, err := balancer.GetLatestChannelAssignment()
	if err != nil {
		return err
	}

	// Import in replicating cluster is not supported yet
	if channelAssignment.ReplicateConfiguration != nil && len(channelAssignment.ReplicateConfiguration.GetClusters()) > 1 {
		return merr.WrapErrImportFailed("import in replicating cluster is not supported yet")
	}

	return nil
}

// broadcastImport broadcasts the import message to all vchannels.
// This method is called from the new ImportV2 flow where proxy calls DataCoord directly.
func (s *Server) broadcastImport(ctx context.Context,
	collectionName string,
	collectionID int64,
	partitionIDs []int64,
	files []*internalpb.ImportFile,
	options []*commonpb.KeyValuePair,
	schema *schemapb.CollectionSchema,
	jobID int64,
	vchannels []string,
) error {
	// Convert files to msgpb format for validation
	msgFiles := lo.Map(files, func(file *internalpb.ImportFile, _ int) *msgpb.ImportFile {
		return &msgpb.ImportFile{
			Id:    file.GetId(),
			Paths: file.GetPaths(),
		}
	})

	// Validate the request before broadcasting
	if err := s.validateImportRequest(ctx, msgFiles, options); err != nil {
		return errors.Wrap(err, "failed to validate import request")
	}

	// Get database name from collection metadata via broker
	// This is safer than extracting from schema which may be stale
	broadcaster, err := s.startBroadcastWithCollectionID(ctx, collectionID)
	if err != nil {
		return errors.Wrap(err, "failed to start broadcast with collection id")
	}
	defer broadcaster.Close()

	coll, err := s.broker.DescribeCollectionInternal(ctx, collectionID)
	if err := merr.CheckRPCCall(coll, err); err != nil {
		return err
	}
	// Build import message without deprecated MsgBase
	msg := message.NewImportMessageBuilderV1().
		WithHeader(&message.ImportMessageHeader{}).
		WithBody(&msgpb.ImportMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Import,
				Timestamp: 0,
			},
			DbName:         coll.DbName,
			CollectionName: collectionName,
			CollectionID:   collectionID,
			PartitionIDs:   partitionIDs,
			Options:        funcutil.KeyValuePair2Map(options),
			Files:          msgFiles,
			Schema:         schema, // TODO: should we use the schema from the collection?
			JobID:          jobID,
		}).
		WithBroadcast(vchannels).
		MustBuildBroadcast()

	// Broadcast the message
	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

func (c *DDLCallbacks) registerImportCallbacks() {
	registry.RegisterImportV1AckCallback(c.importV1AckCallback)
	registry.RegisterCommitImportV2AckCallback(c.commitImportV2AckCallback)
	registry.RegisterRollbackImportV2AckCallback(c.rollbackImportV2AckCallback)
}

// commitImportV2AckCallback handles the ack callback for CommitImport WAL message.
// It transitions the import job from Uncommitted → Committing state.
// Concurrency safety is guaranteed by the broadcaster framework's resource key lock
// (exclusive collection-level lock), so no CAS is needed here.
func (c *DDLCallbacks) commitImportV2AckCallback(ctx context.Context, result message.BroadcastResultCommitImportMessageV2) error {
	header := result.Message.Header()
	jobID := header.GetJobId()
	log.Ctx(ctx).Info("CommitImport broadcast ack received", zap.Int64("jobID", jobID))

	job := c.importMeta.GetJob(ctx, jobID)
	if job == nil {
		log.Ctx(ctx).Warn("CommitImport: job not found, skipping", zap.Int64("jobID", jobID))
		return nil
	}
	if job.GetState() != internalpb.ImportJobState_Uncommitted {
		log.Ctx(ctx).Info("CommitImport: job not in Uncommitted state, no-op",
			zap.Int64("jobID", jobID), zap.String("state", job.GetState().String()))
		return nil
	}

	if err := c.importMeta.UpdateJob(ctx, jobID,
		UpdateJobState(internalpb.ImportJobState_Committing),
	); err != nil {
		return err
	}

	uncommittedDuration := job.GetTR().RecordSpan()
	log.Ctx(ctx).Info("import job uncommitted stage done",
		zap.Int64("jobID", jobID),
		zap.Duration("jobTimeCost/uncommitted", uncommittedDuration))
	return nil
}

// rollbackImportV2AckCallback handles the ack callback for RollbackImport WAL message.
// It transitions the import job to Failed state.
// Concurrency safety is guaranteed by the broadcaster framework's resource key lock
// (exclusive collection-level lock), so no CAS is needed here.
// Segment cleanup is handled by the import inspector (processFailed), not here.
func (c *DDLCallbacks) rollbackImportV2AckCallback(ctx context.Context, result message.BroadcastResultRollbackImportMessageV2) error {
	header := result.Message.Header()
	jobID := header.GetJobId()
	log.Ctx(ctx).Info("RollbackImport broadcast ack received", zap.Int64("jobID", jobID))

	job := c.importMeta.GetJob(ctx, jobID)
	if job == nil {
		log.Ctx(ctx).Warn("RollbackImport: job not found, skipping", zap.Int64("jobID", jobID))
		return nil
	}
	state := job.GetState()
	if state == internalpb.ImportJobState_Committing ||
		state == internalpb.ImportJobState_Completed ||
		state == internalpb.ImportJobState_Failed {
		log.Ctx(ctx).Info("RollbackImport: job already in terminal/committed state, no-op",
			zap.Int64("jobID", jobID), zap.String("state", state.String()))
		return nil
	}

	return c.importMeta.UpdateJob(ctx, jobID, UpdateJobState(internalpb.ImportJobState_Failed))
}
