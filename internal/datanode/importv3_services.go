// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package datanode

// The Import V3 RPC boundary. It validates each request, builds the matching
// importv3 worker task, and submits it to the node task manager, which queues
// it until the slot scheduler admits it. All task execution lives in the
// importv3 package.

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datanode/importv3"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// submitImportV3WorkerTask queues one import V3-family worker task in the task
// manager, which holds it until the slot scheduler admits it into execution.
func (node *DataNode) submitImportV3WorkerTask(_ context.Context, task importv3.Task) (*commonpb.Status, error) {
	if node.importV3TaskMgr == nil || node.importV3Scheduler == nil {
		return merr.Status(merr.WrapErrServiceNotReadyMsg("import V3 task manager is not initialized")), nil
	}
	if err := node.importV3TaskMgr.Submit(task); err != nil {
		return merr.Status(err), nil
	}
	return merr.Success(), nil
}

// submitReshardTask validates the request and queues one ReshardTask run.
func (node *DataNode) submitReshardTask(ctx context.Context, req *datapb.ReshardTaskRequest) (*commonpb.Status, error) {
	if err := ctx.Err(); err != nil {
		return merr.Status(err), nil
	}
	if req == nil || req.GetRunId() <= 0 || req.GetStorageConfig() == nil || req.GetSlot() <= 0 || req.GetPlan() == nil {
		return merr.Status(merr.WrapErrImportSysFailedMsg("invalid or incomplete ReshardTask request")), nil
	}
	cm, err := node.storageFactory.NewChunkManager(ctx, req.GetStorageConfig())
	if err != nil {
		return merr.Status(err), nil
	}
	pluginContext, err := hookutil.GetCPluginContext(req.GetPluginContext(), req.GetPlan().GetCollectionId())
	if err != nil {
		return merr.Status(err), nil
	}
	return node.submitImportV3WorkerTask(ctx, importv3.NewReshardTask(req, cm, pluginContext, node.importV3Metrics))
}

// submitImportTaskV3 validates the request and queues one ImportTaskV3 run.
func (node *DataNode) submitImportTaskV3(ctx context.Context, req *datapb.ImportTaskV3Request) (*commonpb.Status, error) {
	if err := ctx.Err(); err != nil {
		return merr.Status(err), nil
	}
	if req == nil || req.GetRunId() <= 0 || req.GetStorageConfig() == nil || req.GetSlot() <= 0 || req.GetPlan() == nil {
		return merr.Status(merr.WrapErrImportSysFailedMsg("invalid or incomplete ImportTaskV3 request")), nil
	}
	pluginContext, err := hookutil.GetCPluginContext(req.GetPluginContext(), req.GetPlan().GetCollectionId())
	if err != nil {
		return merr.Status(err), nil
	}
	// The merged segment's stats blobs are uploaded through the packed writer's
	// BlobsWriter, so the task needs a chunk manager to build that uploader.
	cm, err := node.storageFactory.NewChunkManager(ctx, req.GetStorageConfig())
	if err != nil {
		return merr.Status(err), nil
	}
	return node.submitImportV3WorkerTask(ctx, importv3.NewImportTask(req, cm, pluginContext))
}

func (node *DataNode) queryImportV3WorkerTask(
	ctx context.Context,
	taskID, runID int64,
	taskType taskcommon.Type,
) (*workerpb.QueryTaskResponse, error) {
	if node.importV3TaskMgr == nil {
		return &workerpb.QueryTaskResponse{Status: merr.Status(merr.WrapErrServiceNotReadyMsg("import V3 task manager is not initialized"))}, nil
	}
	snapshot, ok := node.importV3TaskMgr.Query(taskID, runID)
	if !ok {
		// A task queued for a slot is already in the manager and answers
		// Pending through the generic path below, so DataCoord waits for its
		// admission instead of treating the run as lost. Only a truly absent
		// run is not found.
		return &workerpb.QueryTaskResponse{Status: merr.Status(merr.WrapErrNodeNotFound(node.GetNodeID(),
			"cannot find current import V3 task run"))}, nil
	}
	properties := taskcommon.NewProperties(nil)
	properties.AppendTaskState(taskcommon.FromImportState(snapshot.State))
	properties.AppendReason(snapshot.Reason)

	var payload any
	switch taskType {
	case taskcommon.Reshard:
		response := &datapb.QueryReshardTaskResponse{
			Status: merr.Success(), State: snapshot.State, Reason: snapshot.Reason,
		}
		if progress, ok := snapshot.Progress.(*importv3.ReshardProgress); ok {
			response.SourceProgresses = progress.SourceProgresses()
		}
		payload = response
	case taskcommon.ImportV3:
		response := &datapb.QueryImportTaskV3Response{
			Status: merr.Success(), State: snapshot.State, Reason: snapshot.Reason,
		}
		if snapshot.Result != nil {
			segments, ok := snapshot.Result.([]*datapb.SegmentResult)
			if !ok {
				return &workerpb.QueryTaskResponse{Status: merr.Status(merr.WrapErrServiceInternalMsg(
					"import V3 task %d run %d carries an unexpected result payload %T", taskID, runID, snapshot.Result))}, nil
			}
			response.Segments = segments
		}
		if snapshot.State == datapb.ImportTaskStateV2_Completed {
			resultBytes := proto.Size(response)
			grpcLimitBytes := min(
				paramtable.Get().DataNodeGrpcServerCfg.ServerMaxSendSize.GetAsInt(),
				paramtable.Get().DataNodeGrpcClientCfg.ClientMaxRecvSize.GetAsInt(),
			)
			if resultBytes >= grpcLimitBytes/2 {
				mlog.Warn(ctx, "import V3 result is close to the gRPC result limit",
					mlog.Int64("taskID", taskID),
					mlog.Int64("runID", runID),
					mlog.Int("segmentCount", len(response.GetSegments())),
					mlog.Int("resultBytes", resultBytes),
					mlog.Int("grpcResultLimitBytes", grpcLimitBytes))
			}
		}
		payload = response
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
		// Defensive: a new task type must not crash the DataNode in the
		// query path; surface it as an error so DataCoord retries.
		return &workerpb.QueryTaskResponse{Status: merr.Status(merr.WrapErrServiceInternalMsg(
			"unsupported import V3 query payload %T", payload))}, nil
	}
}

func (node *DataNode) dropImportV3WorkerTask(taskID, runID int64) (*commonpb.Status, error) {
	// The manager owns both a queued and a started run, so one Drop covers
	// both. It is best effort and idempotent; a stale run must not cancel a
	// newer run (TaskManager.Drop returns false for both stale and
	// already-absent tasks).
	if node.importV3TaskMgr == nil {
		return merr.Success(), nil
	}
	node.importV3TaskMgr.Drop(taskID, runID)
	return merr.Success(), nil
}
