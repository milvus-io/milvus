// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package datanode

import (
	"bytes"
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datanode/importv3"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"google.golang.org/protobuf/proto"
)

func (node *DataNode) createImportV3WorkerTask(
	_ context.Context,
	taskID, runID int64,
	execute importv3.Run,
) (*commonpb.Status, error) {
	if node.importV3TaskMgr == nil {
		return merr.Status(merr.WrapErrServiceNotReadyMsg("import V3 task manager is not initialized")), nil
	}
	if err := node.importV3TaskMgr.Add(taskID, runID, execute); err != nil {
		return merr.Status(err), nil
	}
	return merr.Success(), nil
}

func (node *DataNode) queryImportV3WorkerTask(
	_ context.Context,
	taskID, runID int64,
	taskType taskcommon.Type,
) (*workerpb.QueryTaskResponse, error) {
	if node.importV3TaskMgr == nil {
		return &workerpb.QueryTaskResponse{Status: merr.Status(merr.WrapErrServiceNotReadyMsg("import V3 task manager is not initialized"))}, nil
	}
	snapshot, ok := node.importV3TaskMgr.Query(taskID, runID)
	if !ok {
		return &workerpb.QueryTaskResponse{Status: merr.Status(merr.WrapErrNodeNotFound(node.GetNodeID(),
			"cannot find current import V3 task run"))}, nil
	}
	properties := taskcommon.NewProperties(nil)
	properties.AppendTaskState(importV3TaskCommonState(snapshot.State))
	properties.AppendReason(snapshot.Reason)

	var payload any
	switch taskType {
	case taskcommon.Reshard:
		payload = &datapb.QueryReshardTaskResponse{
			Status:       merr.Success(),
			State:        reshardTaskState(snapshot.State),
			Reason:       snapshot.Reason,
			ResultRef:    resultRef(snapshot.Result),
			ResultDigest: resultDigest(snapshot.Result),
			FailureCode:  snapshot.FailureCode,
		}
	case taskcommon.ImportV3:
		payload = &datapb.QueryImportTaskV3Response{
			Status:       merr.Success(),
			State:        importTaskV3State(snapshot.State),
			Reason:       snapshot.Reason,
			ResultRef:    resultRef(snapshot.Result),
			ResultDigest: resultDigest(snapshot.Result),
			FailureCode:  snapshot.FailureCode,
		}
	default:
		return &workerpb.QueryTaskResponse{Status: merr.Status(merr.WrapErrServiceInternalMsg(
			"invalid V3 task type %q", taskType))}, nil
	}
	// Keep the concrete proto types at the boundary so wrapQueryTaskResult can
	// enforce the existing GetStatus payload contract.
	switch result := payload.(type) {
	case *datapb.QueryReshardTaskResponse:
		return wrapQueryTaskResult(result, properties)
	case *datapb.QueryImportTaskV3Response:
		return wrapQueryTaskResult(result, properties)
	default:
		panic("unreachable import V3 query payload")
	}
}

func (node *DataNode) dropImportV3WorkerTask(taskID, runID int64) (*commonpb.Status, error) {
	if node.importV3TaskMgr == nil {
		return merr.Success(), nil
	}
	// Best effort and idempotent.  A stale run must not cancel a newer run;
	// TaskManager.Drop returns false for both stale and already-absent tasks.
	node.importV3TaskMgr.Drop(taskID, runID)
	return merr.Success(), nil
}

func (node *DataNode) executeReshardTask(ctx context.Context, req *datapb.ReshardTaskRequest, runID int64) (*importv3.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if req == nil || req.GetRunId() != runID || req.GetTaskPlanRef() == "" || len(req.GetTaskPlanDigest()) == 0 || req.GetOutputPrefix() == "" || req.GetStorageConfig() == nil || req.GetTaskSlot() <= 0 {
		return nil, merr.WrapErrImportSysFailedMsg("invalid or incomplete ReshardTask request")
	}
	plan, err := node.readReshardTaskPlan(ctx, req.GetStorageConfig(), req.GetTaskPlanRef(), req.GetTaskPlanDigest())
	if err != nil {
		return nil, err
	}
	if plan.GetTaskId() != req.GetTaskId() || plan.GetJobId() != req.GetJobId() {
		return nil, merr.WrapErrDataIntegrityMsg("ReshardTask plan identity mismatch")
	}
	// The executor is intentionally strict: a valid plan that cannot yet be
	// transformed into immutable fragments must fail with a typed protocol
	// error, never report an empty successful manifest.
	return nil, merr.WrapErrImportSysFailedMsg("ReshardTask fragment executor is unavailable for plan %d", plan.GetTaskId())
}

func (node *DataNode) executeImportTaskV3(ctx context.Context, req *datapb.ImportTaskV3Request, runID int64) (*importv3.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if req == nil || req.GetRunId() != runID || req.GetTaskPlanRef() == "" || len(req.GetTaskPlanDigest()) == 0 || req.GetOutputPrefix() == "" || req.GetStorageConfig() == nil || req.GetTaskSlot() <= 0 || req.GetMergeFanIn() < 2 || req.GetMergeFanIn() > 1024 {
		return nil, merr.WrapErrImportSysFailedMsg("invalid or incomplete ImportTaskV3 request")
	}
	plan, err := node.readImportTaskPlan(ctx, req.GetStorageConfig(), req.GetTaskPlanRef(), req.GetTaskPlanDigest())
	if err != nil {
		return nil, err
	}
	if plan.GetTaskId() != req.GetTaskId() || plan.GetJobId() != req.GetJobId() || plan.GetPlanningGeneration() != req.GetPlanningGeneration() || plan.GetMergeFanIn() != req.GetMergeFanIn() {
		return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 plan/request mismatch")
	}
	return nil, merr.WrapErrImportSysFailedMsg("ImportTaskV3 merge executor is unavailable for plan %d", plan.GetTaskId())
}

func (node *DataNode) readReshardTaskPlan(ctx context.Context, cfg *indexpb.StorageConfig, ref string, digest []byte) (*datapb.ReshardTaskPlan, error) {
	cm, err := node.storageFactory.NewChunkManager(ctx, cfg)
	if err != nil {
		return nil, err
	}
	payload, err := cm.Read(ctx, ref)
	if err != nil {
		return nil, err
	}
	// TODO(import-v3): validate the persisted plan digest here when the shared
	// digest helper is finalized. The first implementation deliberately does not
	// calculate SHA-256.
	_ = digest
	plan := &datapb.ReshardTaskPlan{}
	if err := proto.Unmarshal(payload, plan); err != nil {
		return nil, merr.WrapErrDataIntegrityMsg("decode ReshardTask plan %s: %s", ref, err.Error())
	}
	return plan, nil
}

func (node *DataNode) readImportTaskPlan(ctx context.Context, cfg *indexpb.StorageConfig, ref string, digest []byte) (*datapb.ImportTaskPlan, error) {
	cm, err := node.storageFactory.NewChunkManager(ctx, cfg)
	if err != nil {
		return nil, err
	}
	payload, err := cm.Read(ctx, ref)
	if err != nil {
		return nil, err
	}
	// TODO(import-v3): validate the persisted plan digest here; do not add a
	// second ad-hoc SHA-256 implementation at this boundary.
	_ = digest
	plan := &datapb.ImportTaskPlan{}
	if err := proto.Unmarshal(payload, plan); err != nil {
		return nil, merr.WrapErrDataIntegrityMsg("decode ImportTaskV3 plan %s: %s", ref, err.Error())
	}
	return plan, nil
}

func importV3TaskCommonState(state importv3.State) taskcommon.State {
	switch state {
	case importv3.StatePending:
		return taskcommon.Init
	case importv3.StateRunning:
		return taskcommon.InProgress
	case importv3.StateRetry:
		return taskcommon.Retry
	case importv3.StateCompleted:
		return taskcommon.Finished
	case importv3.StateFailed:
		return taskcommon.Failed
	default:
		return taskcommon.None
	}
}

func reshardTaskState(state importv3.State) datapb.ReshardTask_State {
	switch state {
	case importv3.StatePending:
		return datapb.ReshardTask_Pending
	case importv3.StateRunning:
		return datapb.ReshardTask_Running
	case importv3.StateRetry:
		return datapb.ReshardTask_Retry
	case importv3.StateCompleted:
		return datapb.ReshardTask_Completed
	case importv3.StateFailed:
		return datapb.ReshardTask_Failed
	default:
		return datapb.ReshardTask_None
	}
}

func importTaskV3State(state importv3.State) datapb.ImportTaskV3_State {
	switch state {
	case importv3.StatePending:
		return datapb.ImportTaskV3_Pending
	case importv3.StateRunning:
		return datapb.ImportTaskV3_Running
	case importv3.StateRetry:
		return datapb.ImportTaskV3_Retry
	case importv3.StateCompleted:
		return datapb.ImportTaskV3_Completed
	case importv3.StateFailed:
		return datapb.ImportTaskV3_Failed
	default:
		return datapb.ImportTaskV3_None
	}
}

func resultRef(result *importv3.Result) string {
	if result == nil {
		return ""
	}
	return result.Ref
}

func resultDigest(result *importv3.Result) []byte {
	if result == nil {
		return nil
	}
	return append([]byte(nil), result.Digest...)
}
