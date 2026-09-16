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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
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
//
// All of this runs before the broadcaster's idempotency lookup, which cannot happen
// until the resource keys are held inside Broadcast. A retry therefore has to pass
// these checks again before it can resolve to its original jobID, and not all of them
// are a pure function of the request: ValidateMaxImportJobExceed counts in-flight jobs
// and ValidateBinlogImportRequest lists the backup files in object storage. A retry
// sent while the job limit is saturated -- by the original request among others -- or
// after the backup files changed is rejected here rather than returning the original
// jobID. Retrying the same key once the limit frees up resolves normally; minting a
// fresh key instead is what would import the data twice.
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
		return merr.WrapErrOperationNotSupportedMsg("import in replicating cluster is not supported yet")
	}

	return nil
}

// jobIDFromDuplicatedBroadcast recovers the original import jobID from the broadcast
// message the broadcaster returned on an idempotency hit. The broadcaster does not
// know about import-specific structures, so the decode happens here.
//
// The request payload is deliberately NOT compared against the original: keeping the
// key unique per logical request is the client's contract, and enforcing it
// server-side would mean inventing an equality predicate over file lists whose false
// mismatches would reject legitimate retries -- pushing the caller to mint a new key
// and import the data twice, the very outcome this feature exists to prevent.
//
// The collectionID comparison is not such a predicate and is not a semantic guard: the
// idempotency key is scoped to this collection's ID, so a hit already means both
// broadcasts targeted it. It is checked as an invariant, to fail loudly on an encoding
// or scoping bug rather than hand back a jobID for another collection's import.
func jobIDFromDuplicatedBroadcast(msg message.BroadcastMutableMessage, collectionID int64) (int64, error) {
	importMsg, err := message.AsBroadcastImportMessageV1(msg)
	if err != nil {
		return 0, merr.Wrap(err, "malformed duplicated import broadcast message")
	}
	body, err := importMsg.Body()
	if err != nil {
		return 0, merr.Wrap(err, "malformed duplicated import broadcast message body")
	}
	if body.GetCollectionID() != collectionID {
		return 0, merr.WrapErrServiceInternalMsg(
			"idempotency scope resolved to an import into collection %d, not %d",
			body.GetCollectionID(), collectionID)
	}
	return body.GetJobID(), nil
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
	idempotencyKey string,
) (duplicatedJobID int64, duplicated bool, err error) {
	// Convert files to msgpb format for validation
	msgFiles := lo.Map(files, func(file *internalpb.ImportFile, _ int) *msgpb.ImportFile {
		return &msgpb.ImportFile{
			Id:    file.GetId(),
			Paths: file.GetPaths(),
		}
	})

	// Validate the request before broadcasting
	if err := s.validateImportRequest(ctx, msgFiles, options); err != nil {
		return 0, false, merr.Wrap(err, "failed to validate import request")
	}

	// Get database name from collection metadata via broker
	// This is safer than extracting from schema which may be stale
	broadcaster, err := s.startBroadcastWithCollectionID(ctx, collectionID)
	if err != nil {
		return 0, false, merr.Wrap(err, "failed to start broadcast with collection id")
	}
	defer broadcaster.Close()

	coll, err := s.broker.DescribeCollectionInternal(ctx, collectionID)
	if err := merr.CheckRPCCall(coll.Status, err); err != nil {
		return 0, false, err
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
		// Scoped to the collection by ID, so the same client key stays a distinct
		// operation against another collection, and a rename does not move the key off
		// the collection it was bound to: a retry naming the renamed collection still
		// resolves to its original job. A retry still naming the OLD collection never
		// reaches here -- the proxy resolves the name first -- so it fails rather than
		// importing twice. The broadcaster adds the message type; everything else about
		// the dedup identity is this scope.
		WithIdempotencyKey(message.NewCollectionScopedIdempotencyKey(collectionID, idempotencyKey)).
		WithBroadcast(vchannels).
		MustBuildBroadcast()

	// Broadcast the message
	result, err := broadcaster.Broadcast(ctx, msg)
	if err != nil {
		return 0, false, err
	}
	if result.Duplicated == nil {
		return 0, false, nil
	}
	// The broadcaster resolved this idempotency key to an earlier broadcast, so no
	// new job was created; recover what that broadcast carried.
	originalJobID, err := jobIDFromDuplicatedBroadcast(result.Duplicated, collectionID)
	if err != nil {
		return 0, false, err
	}
	// Never log the raw key: it is client-controlled and may carry sensitive data.
	log.Ctx(ctx).Info("import broadcast deduplicated by idempotency key",
		zap.Int64("collectionID", collectionID),
		zap.Int64("jobID", originalJobID),
		zap.String("idempotencyKeyFingerprint", message.IdempotencyKeyFingerprint(idempotencyKey)))
	return originalJobID, true, nil
}
