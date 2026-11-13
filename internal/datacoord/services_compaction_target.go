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
	"cmp"
	"context"
	"slices"
	"time"

	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/datacoord/compaction/targets"
	"github.com/milvus-io/milvus/internal/datacoord/ddl"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func validateCompactionTargetReplication(ctx context.Context) error {
	_, replicating, err := ddl.GetReplicationRole(ctx)
	if err != nil {
		return err
	}
	if replicating {
		return merr.WrapErrOperationNotSupportedMsg("compaction targets are not supported on a replicating cluster")
	}
	return nil
}

func (s *Server) compactionTargetsReady() error {
	if !s.targetCompactionEnabled {
		return merr.WrapErrOperationNotSupportedMsg("target-based compaction is disabled")
	}
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return err
	}
	if s.compactionTargetMeta == nil || s.meta == nil {
		return merr.WrapErrServiceNotReadyMsg("compaction target metadata is initializing")
	}
	return nil
}

// DropCollectionCompactionTargets is called by the control-channel ACK after
// RootCoord has persisted the collection tombstone.
func (s *Server) DropCollectionCompactionTargets(ctx context.Context, collectionID int64) error {
	if !s.targetCompactionEnabled {
		return nil
	}
	if s.compactionTargetMeta == nil {
		return merr.WrapErrServiceNotReadyMsg("compaction target metadata is initializing")
	}
	return s.compactionTargetMeta.CloseCollectionScope(ctx, collectionID)
}

func (s *Server) recoverCompactionTargetScopes(ctx context.Context) error {
	if s.compactionTargetMeta == nil {
		return nil
	}
	for _, id := range s.compactionTargetMeta.ActiveCollectionIDs() {
		if s.meta.GetCollection(id).IsCreated() {
			continue
		}
		if s.collectionDropped == nil {
			return merr.WrapErrServiceNotReadyMsg("compaction target collection lifecycle is unavailable")
		}
		dropped, err := s.collectionDropped(ctx, id)
		if err != nil {
			return merr.Wrapf(err, "recover compaction target scope for collection %d", id)
		}
		if dropped {
			if err := s.compactionTargetMeta.CloseCollectionScope(ctx, id); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Server) tryLockCompactionTargetScope(ctx context.Context, id int64) (func(), error) {
	collection := s.meta.GetCollection(id)
	if !collection.IsCreated() {
		return nil, nil
	}
	dbName, name := collection.DatabaseName, collection.Schema.GetName()
	if dbName == "" || name == "" {
		return nil, merr.WrapErrServiceUnavailableMsg("compaction target collection identity is unavailable")
	}
	// A DropCollection can retire channel data before its control ACK
	// persists the collection tombstone. Order the final satisfaction
	// observation against the existing DDL resource lock.
	release, err := broadcast.TryLockResourceKeys(ctx,
		message.NewSharedDBNameResourceKey(dbName),
		message.NewSharedCollectionNameResourceKey(dbName, name))
	if err != nil || release == nil {
		return nil, err
	}
	// Rename takes database locks. Revalidate the stable ID after the
	// acquisition in case the name changed before those locks were held.
	current := s.meta.GetCollection(id)
	if !current.IsCreated() || current.DatabaseName != dbName || current.Schema.GetName() != name {
		release()
		return nil, nil
	}
	return release, nil
}

func (s *Server) CreateCompactionTarget(ctx context.Context, req *datapb.CreateCompactionTargetRequest) (*datapb.CreateCompactionTargetResponse, error) {
	record, err := s.createCompactionTarget(ctx, req)
	resp := &datapb.CreateCompactionTargetResponse{Status: merr.Status(err)}
	if err == nil {
		resp.TargetId = record.GetTargetID()
		resp.TotalRows = record.GetTotalRows()
	}
	return resp, nil
}

func (s *Server) createCompactionTarget(ctx context.Context, req *datapb.CreateCompactionTargetRequest) (*datapb.CompactionTarget, error) {
	if err := s.compactionTargetsReady(); err != nil {
		return nil, err
	}
	resource := req.GetCollection()
	if resource.GetCollectionId() <= 0 || resource.GetDbName() == "" || resource.GetCollectionName() == "" || resource.GetDeleted() {
		return nil, merr.WrapErrParameterInvalidMsg("a resolved collection identity is required")
	}

	rewrite := req.GetRewrite()
	if rewrite == nil && req.GetInputTargetSizeBytes() <= 0 {
		return nil, merr.WrapErrParameterInvalidMsg("a rewrite or a positive input_target_size_bytes is required")
	}
	roots, err := targets.NormalizeRewriteRoots(rewrite.GetRootSegmentIds())
	if err != nil {
		return nil, err
	}
	validate := func() error {
		if err := s.compactionTargetsReady(); err != nil {
			return err
		}
		collection := s.meta.GetCollection(resource.GetCollectionId())
		if !collection.IsCreated() || !proto.Equal(liveCompactionTargetResource(collection), resource) {
			return merr.WrapErrServiceUnavailableMsg("collection changed during compaction target creation")
		}
		if collection.IsExternal() {
			return merr.WrapErrParameterInvalidMsg("compaction is not supported for external collection")
		}
		if s.checkCompactionTargetReplication != nil {
			if err := s.checkCompactionTargetReplication(ctx); err != nil {
				return err
			}
		}
		return nil
	}
	if err := validate(); err != nil {
		return nil, err
	}
	if rewrite == nil {
		return nil, merr.WrapErrOperationNotSupportedMsg("OPTIMIZE compaction targets are unavailable")
	}
	candidate, err := targets.NewRewriteRecord(ctx, s.allocator, resource.GetCollectionId(), roots)
	if err != nil {
		return nil, err
	}
	observation, err := targets.ObserveRewrite(ctx, s.meta, candidate, true)
	if err != nil {
		return nil, err
	}
	candidate.TotalRows = proto.Int64(observation.Rows)
	collection := s.meta.GetCollection(resource.GetCollectionId())
	if !collection.IsCreated() {
		return nil, merr.WrapErrServiceUnavailableMsg("collection changed during compaction target creation")
	}
	scope := &metastore.CompactionTargetCollectionVersion{
		DatabaseID: collection.DatabaseID,
		Revision:   resource.GetRevision(),
	}
	return s.compactionTargetMeta.Create(ctx, candidate, scope, validate)
}

func (s *Server) readCompactionTarget(ctx context.Context, id int64) (*datapb.CompactionTarget, error) {
	if err := s.compactionTargetsReady(); err != nil {
		return nil, err
	}
	if id <= 0 {
		return nil, merr.WrapErrParameterInvalidMsg("target_id must be positive")
	}
	record, err := s.compactionTargetMeta.Read(ctx, id)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, merr.Wrapf(merr.ErrCompactionTargetNotFound, "target ID %d", id)
	}
	return record, nil
}

func (s *Server) GetCompactionTargetBinding(ctx context.Context, req *datapb.GetCompactionTargetBindingRequest) (*datapb.GetCompactionTargetBindingResponse, error) {
	record, err := s.readCompactionTarget(ctx, req.GetTargetId())
	if err != nil {
		return &datapb.GetCompactionTargetBindingResponse{Status: merr.Status(err)}, nil
	}
	resource, err := s.compactionTargetResource(ctx, record.GetCollectionID())
	return &datapb.GetCompactionTargetBindingResponse{Status: merr.Status(err), Resource: resource}, nil
}

func liveCompactionTargetResource(collection *meta.CollectionInfo) *datapb.CompactionTargetResource {
	dbID := collection.DatabaseID
	if dbID == util.NonDBID {
		dbID = util.DefaultDBID
	}
	return &datapb.CompactionTargetResource{
		CollectionId: collection.ID, DbId: dbID, DbName: collection.DatabaseName,
		CollectionName: collection.Schema.GetName(), Revision: collection.UpdateTimestamp,
	}
}

func (s *Server) compactionTargetResource(ctx context.Context, id int64) (*datapb.CompactionTargetResource, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if id < 0 {
		return nil, merr.WrapErrDataIntegrityMsg("invalid compaction target collection ID %d", id)
	}
	if id == 0 {
		return &datapb.CompactionTargetResource{}, nil
	}
	collection := s.meta.GetCollection(id)
	if collection.IsCreated() {
		resource := liveCompactionTargetResource(collection)
		if resource.GetCollectionName() == "" || resource.GetDbName() == "" {
			return nil, merr.WrapErrServiceUnavailableMsg("compaction target collection identity is unavailable")
		}
		return resource, nil
	}
	if s.collectionDropped == nil {
		return nil, merr.WrapErrServiceNotReadyMsg("compaction target collection lifecycle is unavailable")
	}
	dropped, err := s.collectionDropped(ctx, id)
	if err != nil {
		return nil, err
	}
	if !dropped {
		return nil, merr.WrapErrServiceUnavailableMsg("compaction target collection identity is unavailable")
	}
	resource := &datapb.CompactionTargetResource{CollectionId: id, Deleted: true}
	// Keep database identity only while authoritative metadata still supplies it.
	if collection != nil {
		resource.DbId = collection.DatabaseID
		resource.DbName = collection.DatabaseName
		resource.Revision = collection.UpdateTimestamp
	}
	return resource, nil
}

func (s *Server) DescribeCompactionTarget(ctx context.Context, req *datapb.DescribeCompactionTargetRequest) (*datapb.DescribeCompactionTargetResponse, error) {
	target, err := s.describeCompactionTarget(ctx, req)
	if ctx.Err() != nil {
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &datapb.DescribeCompactionTargetResponse{Status: merr.Status(err), Target: target}, nil
}

func (s *Server) describeCompactionTarget(ctx context.Context, req *datapb.DescribeCompactionTargetRequest) (*datapb.CompactionTargetReadModel, error) {
	record, err := s.readCompactionTarget(ctx, req.GetTargetId())
	if err != nil {
		return nil, err
	}
	resource, err := s.compactionTargetResource(ctx, record.GetCollectionID())
	if err != nil {
		return nil, err
	}
	if !proto.Equal(resource, req.GetCollection()) {
		return nil, merr.WrapErrServiceUnavailableMsg("compaction target resource changed during authorization")
	}
	target, err := compactionTargetReadModel(record, resource)
	if err != nil {
		return nil, err
	}
	var rows int64
	var observed bool
	var observationErr error
	switch {
	case !resource.GetDeleted() && targets.IsRewrite(record):
		observation, err := targets.ObserveRewrite(ctx, s.meta, record, false)
		rows, observed, observationErr = observation.Rows, true, err
	case targets.IsStandingSort(record) && record.GetEnabled():
		observation, err := targets.ObserveSort(ctx, s.meta)
		rows, observed, observationErr = observation.Rows, true, err
	case targets.IsStandingStorage(record) && record.GetEnabled():
		rows, observationErr = targets.ObserveStorage(ctx, s.meta, record)
		observed = true
	}
	if observed && observationErr == nil {
		target.RemainingRows = proto.Int64(rows)
		target.CheckedAt = tsoutil.ComposeTSByTime(time.Now())
	} else if observationErr != nil && ctx.Err() == nil {
		target.Message = "remaining rows could not be observed"
		mlog.RatedWarn(ctx, rate.Limit(1), "describe compaction target observation failed",
			mlog.Int64("targetID", record.GetTargetID()), mlog.Err(observationErr))
	}
	return target, nil
}

func compactionTargetReadModel(record *datapb.CompactionTarget, resource *datapb.CompactionTargetResource) (*datapb.CompactionTargetReadModel, error) {
	target := &datapb.CompactionTargetReadModel{
		TargetId: record.GetTargetID(), Resource: resource, Intent: record.GetIntent(),
		State: record.GetState(), CreatedAt: record.GetActivatedAtTS(),
		StoppedAt: record.GetInactivatedAtTS(), StopReason: record.GetStopReason(),
		TotalRows: record.TotalRows, Rewrite: record.GetRewrite(), Optimize: record.GetOptimize(),
	}
	if targets.HasStandingCondition(record) {
		target.Enabled = record.Enabled
		for _, key := range publicStandingConditionKeys(record.GetIntent()) {
			if value, ok := record.GetProperties()[key]; ok {
				if target.Parameters == nil {
					target.Parameters = make(map[string]string)
				}
				target.Parameters[key] = value
			}
		}
	}
	switch target.GetState() {
	case datapb.TargetState_TARGET_STATE_ACTIVE:
		if target.GetStoppedAt() != 0 || target.GetStopReason() != "" {
			return nil, merr.WrapErrDataIntegrityMsg("active compaction target %d has stop fields", target.GetTargetId())
		}
	case datapb.TargetState_TARGET_STATE_INACTIVE:
		if target.GetStopReason() == "" {
			target.StopReason = "unknown"
		}
	default:
		return nil, merr.WrapErrDataIntegrityMsg("compaction target %d has invalid state %d", target.GetTargetId(), target.GetState())
	}
	if target.TotalRows != nil && target.GetTotalRows() < 0 {
		return nil, merr.WrapErrDataIntegrityMsg("compaction target %d has an invalid total_rows", target.GetTargetId())
	}
	return target, nil
}

func publicStandingConditionKeys(intent datapb.TargetIntent) []string {
	switch intent {
	case datapb.TargetIntent_INTENT_INDEX_VERSION:
		return []string{"vector_mode", "vector_version", "scalar_mode", "scalar_version"}
	case datapb.TargetIntent_INTENT_STORAGE_VERSION:
		return []string{"storage_version"}
	case datapb.TargetIntent_INTENT_STORAGE_FORMAT:
		return []string{"storage_format"}
	default:
		return nil
	}
}

func (s *Server) ListCompactionTargets(ctx context.Context, req *datapb.ListCompactionTargetsRequest) (*datapb.ListCompactionTargetsResponse, error) {
	result, err := s.listCompactionTargets(ctx, req)
	if ctx.Err() != nil {
		return nil, status.Errorf(status.FromContextError(ctx.Err()).Code(), "list compaction targets exceeded its request budget; narrow the filters")
	}
	resp := &datapb.ListCompactionTargetsResponse{Status: merr.Status(err), Targets: result}
	limit := min(Params.RootCoordGrpcServerCfg.ServerMaxSendSize.GetAsInt(), Params.RootCoordGrpcClientCfg.ClientMaxRecvSize.GetAsInt())
	if limit > 0 && proto.Size(resp) > limit {
		return nil, status.Error(codes.ResourceExhausted, "list compaction targets result is too large; narrow the filters")
	}
	return resp, nil
}

func (s *Server) listCompactionTargets(ctx context.Context, req *datapb.ListCompactionTargetsRequest) ([]*datapb.CompactionTargetReadModel, error) {
	if err := s.compactionTargetsReady(); err != nil {
		return nil, err
	}
	if _, explicit := req.GetCollectionFilter().(*datapb.ListCompactionTargetsRequest_CollectionId); explicit && req.GetCollectionId() <= 0 {
		return nil, merr.WrapErrParameterInvalidMsg("collection_id must be positive")
	}
	if _, explicit := req.GetCollectionFilter().(*datapb.ListCompactionTargetsRequest_CollectionName); explicit && req.GetCollectionName() == "" {
		return nil, merr.WrapErrParameterInvalidMsg("collection_name must not be empty")
	}
	for _, state := range req.GetStates() {
		if state != datapb.TargetState_TARGET_STATE_ACTIVE && state != datapb.TargetState_TARGET_STATE_INACTIVE {
			return nil, merr.WrapErrParameterInvalidMsg("unknown compaction target state %d", state)
		}
	}

	records, err := s.compactionTargetMeta.List(ctx)
	if err != nil {
		return nil, err
	}
	resources := make(map[int64]*datapb.CompactionTargetResource)
	var result []*datapb.CompactionTargetReadModel
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		collectionID := record.GetCollectionID()
		if req.GetCollectionId() > 0 && collectionID != req.GetCollectionId() {
			continue
		}
		if !req.GetIncludeSystem() && !targets.IsRewrite(record) && record.GetOptimize() == nil {
			continue
		}
		if len(req.GetStates()) > 0 && !slices.Contains(req.GetStates(), record.GetState()) {
			continue
		}
		resource, found := resources[collectionID]
		if !found {
			resource, err = s.compactionTargetResource(ctx, collectionID)
			if err != nil {
				return nil, err
			}
			resources[collectionID] = resource
		}
		if (req.GetDbName() != "" && resource.GetDbName() != req.GetDbName()) ||
			(req.GetCollectionName() != "" && resource.GetCollectionName() != req.GetCollectionName()) {
			continue
		}
		target, err := compactionTargetReadModel(record, resource)
		if err != nil {
			return nil, err
		}
		result = append(result, target)
	}
	slices.SortFunc(result, func(a, b *datapb.CompactionTargetReadModel) int {
		if byTime := cmp.Compare(b.GetCreatedAt(), a.GetCreatedAt()); byTime != 0 {
			return byTime
		}
		return cmp.Compare(b.GetTargetId(), a.GetTargetId())
	})
	return result, nil
}

func (s *Server) DropCompactionTarget(ctx context.Context, req *datapb.DropCompactionTargetRequest) (*datapb.DropCompactionTargetResponse, error) {
	if err := s.compactionTargetsReady(); err != nil {
		return &datapb.DropCompactionTargetResponse{Status: merr.Status(err)}, nil
	}
	if req.GetTargetId() <= 0 {
		return &datapb.DropCompactionTargetResponse{Status: merr.Status(merr.WrapErrParameterInvalidMsg("target_id must be positive"))}, nil
	}
	err := s.compactionTargetMeta.Stop(ctx, req.GetTargetId(), targets.StopOperatorDropped, func(record *datapb.CompactionTarget) (bool, error) {
		resource, err := s.compactionTargetResource(ctx, record.GetCollectionID())
		if err != nil {
			return false, err
		}
		if !proto.Equal(resource, req.GetCollection()) {
			return false, merr.WrapErrServiceUnavailableMsg("compaction target resource changed during authorization")
		}
		if !targets.IsRewrite(record) || record.GetCollectionID() == 0 {
			return false, merr.WrapErrOperationNotSupportedMsg("target family does not support operator drop")
		}
		if resource.GetDeleted() && record.GetState() == datapb.TargetState_TARGET_STATE_ACTIVE {
			return false, merr.WrapErrServiceNotReadyMsg("compaction target scope is closing")
		}
		return true, nil
	})
	return &datapb.DropCompactionTargetResponse{Status: merr.Status(err)}, nil
}
