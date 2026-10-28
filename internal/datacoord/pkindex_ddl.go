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
	"fmt"
	"slices"

	"github.com/blang/semver/v4"
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// pkIndexStreamingNodeVersions is the session version range of the streaming
// nodes that handle a primary key index CreateIndex on a data vchannel. It is
// a variable so that tests can change it.
var pkIndexStreamingNodeVersions = semver.MustParseRange(">=3.1.0-0")

// isPKIndex reports whether idx is a primary key index.
func isPKIndex(idx *model.Index) bool {
	return common.GetIndexType(idx.IndexParams) == common.PrimaryKeyIndexType
}

// checkPKIndexCreate checks a PRIMARY_KEY create request before the
// broadcast: the switch, the field, the collection kind, the streaming node
// versions, and the precheck that the collection is empty. It runs under the
// collection lock of the broadcast, so no other DDL of the collection runs
// at the same time. Writes still can: each streaming node decides again at
// the position of the CreateIndex.
func (s *Server) checkPKIndexCreate(ctx context.Context, coll *milvuspb.DescribeCollectionResponse, req *indexpb.CreateIndexRequest) error {
	if !paramtable.Get().StreamingCfg.PKIndexEnabled.GetAsBool() {
		return merr.WrapErrParameterInvalidMsg("%s index is disabled, set %s to true to enable it",
			common.PrimaryKeyIndexType, paramtable.Get().StreamingCfg.PKIndexEnabled.Key)
	}
	schema := coll.GetSchema()
	field := typeutil.GetField(schema, req.GetFieldID())
	checker, err := indexparamcheck.GetIndexCheckerMgrInstance().GetChecker(common.PrimaryKeyIndexType)
	if err != nil {
		return err
	}
	if err := checker.CheckValidDataType(indexparamcheck.IndexPrimaryKey, field); err != nil {
		return err
	}
	// An external collection gets its rows from a refresh job, not from the
	// writes of the WAL, so its index would miss them.
	if typeutil.IsExternalCollection(schema) {
		return merr.WrapErrParameterInvalidMsg("%s index is not supported on the external collection %d",
			common.PrimaryKeyIndexType, req.GetCollectionID())
	}
	if err := s.checkPKIndexStreamingNodeVersions(ctx); err != nil {
		return err
	}
	return s.checkPKIndexCollectionEmpty(ctx, req.GetCollectionID())
}

// checkPKIndexStreamingNodeVersions rejects the create while a streaming node
// is older than pkIndexStreamingNodeVersions, for example in a rolling
// upgrade. An old node would ack the CreateIndex without a decision.
func (s *Server) checkPKIndexStreamingNodeVersions(ctx context.Context) error {
	if s.session == nil {
		return merr.WrapErrServiceNotReadyMsg("the session of datacoord is not ready")
	}
	all, _, err := s.session.GetSessions(ctx, typeutil.StreamingNodeRole)
	if err != nil {
		return merr.WrapErrServiceUnavailableErr(err, "list the streaming node sessions")
	}
	supported, _, err := s.session.GetSessionsWithVersionRange(typeutil.StreamingNodeRole, pkIndexStreamingNodeVersions)
	if err != nil {
		return merr.WrapErrServiceUnavailableErr(err, "list the streaming node sessions")
	}
	for key, session := range all {
		if _, ok := supported[key]; !ok {
			return merr.WrapErrServiceNotReadyMsg("%s index needs every streaming node to support it, streaming node %d has version %s (a rolling upgrade may still be in progress)",
				common.PrimaryKeyIndexType, session.ServerID, session.Version.String())
		}
	}
	return nil
}

// checkPKIndexCollectionEmpty is the precheck that the collection is empty.
// It counts every segment that is not dropped, also a growing or an
// importing segment, and does not look at row counts: a collection whose rows
// were all deleted is not empty, as for the write history of a vchannel. A
// truncate drops the segments, so the collection is empty again after it.
// It also rejects the create while a snapshot restore copies segments into
// the collection: the copy adds rows without a write of the WAL.
func (s *Server) checkPKIndexCollectionEmpty(ctx context.Context, collectionID int64) error {
	segments := s.meta.SelectSegments(ctx, WithCollection(collectionID), SegmentFilterFunc(func(segment *SegmentInfo) bool {
		return segment.GetState() != commonpb.SegmentState_Dropped
	}))
	if len(segments) > 0 {
		return merr.WrapErrParameterInvalidMsg("%s index can only be created on an empty collection, collection %d is not empty (%d segments)",
			common.PrimaryKeyIndexType, collectionID, len(segments))
	}
	if s.copySegmentMeta != nil {
		running := s.copySegmentMeta.CountJobBy(ctx, WithCopyJobCollectionID(collectionID),
			WithoutCopyJobStates(datapb.CopySegmentJobState_CopySegmentJobCompleted, datapb.CopySegmentJobState_CopySegmentJobFailed))
		if running > 0 {
			return merr.WrapErrParameterInvalidMsg("%s index can only be created on an empty collection, a snapshot restore into collection %d is running",
				common.PrimaryKeyIndexType, collectionID)
		}
	}
	return nil
}

// pkIndexStateOf turns the acks of a primary key index CreateIndex into the
// state of the index. A data vchannel whose ack carries no result counts as
// not empty: an old streaming node acks without one. The control channel
// takes no decision and is skipped.
func pkIndexStateOf(results map[string]*message.AppendResult) (commonpb.IndexState, string) {
	var notEmpty, disabled, unknown []string
	for vchannel, result := range results {
		if funcutil.IsControlChannel(vchannel) {
			continue
		}
		ack := &pkindexpb.CreateIndexAckResult{}
		if result == nil || result.Extra == nil || result.Extra.UnmarshalTo(ack) != nil {
			unknown = append(unknown, vchannel)
			continue
		}
		switch ack.GetOutcome() {
		case pkindexpb.CreateIndexAckResult_ENABLED:
		case pkindexpb.CreateIndexAckResult_NOT_EMPTY:
			notEmpty = append(notEmpty, vchannel)
		case pkindexpb.CreateIndexAckResult_DISABLED:
			disabled = append(disabled, vchannel)
		default:
			unknown = append(unknown, vchannel)
		}
	}
	if len(notEmpty)+len(disabled)+len(unknown) == 0 {
		return commonpb.IndexState_Finished, ""
	}
	reason := ""
	add := func(vchannels []string, what string) {
		if len(vchannels) == 0 {
			return
		}
		slices.Sort(vchannels)
		if reason != "" {
			reason += "; "
		}
		reason += fmt.Sprintf("%s on vchannels %v", what, vchannels)
	}
	add(notEmpty, "collection is not empty")
	add(disabled, "primary key index is disabled on the streaming node")
	add(unknown, "streaming node gave no result")
	return commonpb.IndexState_Failed, reason
}

// notifyPKIndexFailed asks the compensation of the index inspector to drop
// the failed primary key indexes of a collection. It does not block.
func (s *Server) notifyPKIndexFailed(ctx context.Context, collectionID, indexID int64) {
	mlog.Warn(ctx, "primary key index creation failed, it is dropped soon",
		mlog.FieldCollectionID(collectionID), mlog.FieldIndexID(indexID))
	select {
	case s.pkIndexCompensateCh <- collectionID:
	default:
	}
}

// pkIndexCreateResult returns the error of the create RPC of the primary key
// index indexID, after the callback of its CreateIndex wrote its state.
// A streaming node decided at the position of the CreateIndex, so a vchannel
// that was not empty there is not a race: the rows were written by the user
// before the create, and a retry fails the same way.
func (s *Server) pkIndexCreateResult(collectionID, indexID int64) error {
	for _, index := range s.meta.indexMeta.GetIndexesForCollection(collectionID, "") {
		if index.IndexID == indexID && index.State == commonpb.IndexState_Failed {
			return merr.WrapErrParameterInvalidMsg("%s", index.FailReason)
		}
	}
	return nil
}

// buildDropIndexMessage builds the DropIndex of indexIDs of a collection.
// When indexIDs hold the primary key index, the header carries its type and
// the message goes to every vchannel, so that each streaming node closes
// its Index at the position of the drop. Otherwise it goes to the control
// channel only.
func (s *Server) buildDropIndexMessage(ctx context.Context, collectionID int64, indexIDs []int64, hasPKIndex bool) (message.BroadcastMutableMessage, error) {
	header := &message.DropIndexMessageHeader{CollectionId: collectionID, IndexIds: indexIDs}
	builder := message.NewDropIndexMessageBuilderV2().WithBody(&message.DropIndexMessageBody{})
	if !hasPKIndex {
		return builder.WithHeader(header).WithControlChannelBroadcast().MustBuildBroadcast(), nil
	}
	coll, err := s.broker.DescribeCollectionInternal(ctx, collectionID)
	if err := merr.CheckRPCCall(coll, err); err != nil {
		return nil, err
	}
	header.IndexType = common.PrimaryKeyIndexType
	return builder.WithHeader(header).WithBroadcast(coll.GetVirtualChannelNames()).MustBuildBroadcast(), nil
}

// errNotPrimaryCluster reports that the cluster is a secondary of a
// replication, which does not compensate a failed primary key index: the
// DropIndex of the primary reaches it through the replication.
var errNotPrimaryCluster = errors.New("the cluster is not the primary of the replication")

// dropFailedPKIndex broadcasts the DropIndex of a primary key index whose
// CreateIndex failed. It re-reads the index under the collection lock and
// does nothing when the index is deleted or not Failed, so a repeated call is
// safe. A collection that is gone needs no drop: the drop of the collection
// marks its indexes deleted. It returns errNotPrimaryCluster on a secondary.
func (s *Server) dropFailedPKIndex(ctx context.Context, idx *model.Index) error {
	role, _, err := s.getReplicationRole(ctx)
	if err != nil {
		return err
	}
	if role != replicateutil.RolePrimary {
		return errNotPrimaryCluster
	}
	broadcaster, err := s.startBroadcastWithCollectionID(ctx, idx.CollectionID)
	if err != nil {
		if errors.Is(err, merr.ErrCollectionNotFound) {
			return nil
		}
		return err
	}
	defer broadcaster.Close()

	var cur *model.Index
	for _, index := range s.meta.indexMeta.GetIndexesForCollection(idx.CollectionID, "") {
		if index.IndexID == idx.IndexID {
			cur = index
		}
	}
	if cur == nil || cur.State != commonpb.IndexState_Failed {
		return nil
	}
	msg, err := s.buildDropIndexMessage(ctx, idx.CollectionID, []int64{idx.IndexID}, true)
	if err != nil {
		if errors.Is(err, merr.ErrCollectionNotFound) {
			return nil
		}
		return err
	}
	if _, err := broadcaster.Broadcast(ctx, msg); err != nil {
		return err
	}
	mlog.Info(ctx, "failed primary key index dropped",
		mlog.FieldCollectionID(idx.CollectionID), mlog.FieldIndexID(idx.IndexID), mlog.String("failReason", cur.FailReason))
	return nil
}
