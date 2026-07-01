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

package proxy

import (
	"context"
	"strconv"

	"go.opentelemetry.io/otel"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (node *Proxy) CreateSnapshot(ctx context.Context, req *milvuspb.CreateSnapshotRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-CreateSnapshot")
	defer sp.End()

	log := mlog.With(
		mlog.String("snapshotName", req.GetName()),
		mlog.String("collectionName", req.GetCollectionName()),
	)

	method := "CreateSnapshot"
	tr := timerecord.NewTimeRecorder(method)
	log.Info(ctx, rpcReceived(method))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.TotalLabel, req.GetDbName(), req.GetCollectionName()).Inc()
	t := &createSnapshotTask{
		req:       req,
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		mixCoord:  node.mixCoord,
	}

	err := node.sched.ddQueue.Enqueue(t)
	if err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.AbandonLabel, req.GetDbName(), req.GetCollectionName()).Inc()
		log.Warn(ctx, "CreateSnapshot failed to Enqueue",
			mlog.Err(err))
		return merr.Status(err), nil
	}

	if err := t.WaitToFinish(); err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, failMetricLabel(err), req.GetDbName(), req.GetCollectionName()).Inc()
		log.Warn(ctx, "CreateSnapshot failed to WaitToFinish",
			mlog.Err(err))
		return merr.Status(err), nil
	}

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.SuccessLabel, req.GetDbName(), req.GetCollectionName()).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return t.result, nil
}

func (node *Proxy) DropSnapshot(ctx context.Context, req *milvuspb.DropSnapshotRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-DropSnapshot")
	defer sp.End()

	log := mlog.With(
		mlog.String("snapshotName", req.GetName()),
	)

	method := "DropSnapshot"
	tr := timerecord.NewTimeRecorder(method)
	log.Info(ctx, rpcReceived(method))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.TotalLabel, req.GetDbName(), req.GetCollectionName()).Inc()
	t := &dropSnapshotTask{
		req:       req,
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		mixCoord:  node.mixCoord,
	}

	err := node.sched.ddQueue.Enqueue(t)
	if err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.AbandonLabel, req.GetDbName(), req.GetCollectionName()).Inc()
		log.Warn(ctx, "DropSnapshot failed to Enqueue",
			mlog.Err(err))
		return merr.Status(err), nil
	}

	if err := t.WaitToFinish(); err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, failMetricLabel(err), req.GetDbName(), req.GetCollectionName()).Inc()
		log.Warn(ctx, "DropSnapshot failed to WaitToFinish",
			mlog.Err(err))
		return merr.Status(err), nil
	}

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.SuccessLabel, req.GetDbName(), req.GetCollectionName()).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return t.result, nil
}

func (node *Proxy) DescribeSnapshot(ctx context.Context, req *milvuspb.DescribeSnapshotRequest) (*milvuspb.DescribeSnapshotResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-DescribeSnapshot")
	defer sp.End()

	log := mlog.With(
		mlog.String("snapshotName", req.GetName()),
	)

	method := "DescribeSnapshot"
	tr := timerecord.NewTimeRecorder(method)
	log.Info(ctx, rpcReceived(method))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.TotalLabel, req.GetDbName(), req.GetCollectionName()).Inc()
	t := &describeSnapshotTask{
		req:       req,
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		mixCoord:  node.mixCoord,
	}

	err := node.sched.ddQueue.Enqueue(t)
	if err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.AbandonLabel, req.GetDbName(), req.GetCollectionName()).Inc()
		log.Warn(ctx, "DescribeSnapshot failed to Enqueue",
			mlog.Err(err))
		return &milvuspb.DescribeSnapshotResponse{
			Status: merr.Status(err),
		}, nil
	}

	if err := t.WaitToFinish(); err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, failMetricLabel(err), req.GetDbName(), req.GetCollectionName()).Inc()
		log.Warn(ctx, "DescribeSnapshot failed to WaitToFinish",
			mlog.Err(err))
		return &milvuspb.DescribeSnapshotResponse{
			Status: merr.Status(err),
		}, nil
	}

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.SuccessLabel, req.GetDbName(), req.GetCollectionName()).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return t.result, nil
}

func (node *Proxy) ListSnapshots(ctx context.Context, req *milvuspb.ListSnapshotsRequest) (*milvuspb.ListSnapshotsResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-ListSnapshots")
	defer sp.End()

	log := mlog.With(
		mlog.String("collectionName", req.GetCollectionName()))

	method := "ListSnapshots"
	tr := timerecord.NewTimeRecorder(method)
	log.Info(ctx, rpcReceived(method))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.TotalLabel, req.GetDbName(), req.GetCollectionName()).Inc()
	t := &listSnapshotsTask{
		req:       req,
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		mixCoord:  node.mixCoord,
	}

	err := node.sched.ddQueue.Enqueue(t)
	if err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.AbandonLabel, req.GetDbName(), req.GetCollectionName()).Inc()
		log.Warn(ctx, "ListSnapshots failed to Enqueue",
			mlog.Err(err))
		return &milvuspb.ListSnapshotsResponse{
			Status: merr.Status(err),
		}, nil
	}

	if err := t.WaitToFinish(); err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, failMetricLabel(err), req.GetDbName(), req.GetCollectionName()).Inc()
		log.Warn(ctx, "ListSnapshots failed to WaitToFinish",
			mlog.Err(err))
		return &milvuspb.ListSnapshotsResponse{
			Status: merr.Status(err),
		}, nil
	}

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.SuccessLabel, req.GetDbName(), req.GetCollectionName()).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return t.result, nil
}

func (node *Proxy) RestoreExternalSnapshot(ctx context.Context, req *milvuspb.RestoreExternalSnapshotRequest) (*milvuspb.RestoreExternalSnapshotResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-RestoreExternalSnapshot")
	defer sp.End()

	if req == nil {
		err := merr.WrapErrParameterInvalidMsg("restore external snapshot request is nil")
		return &milvuspb.RestoreExternalSnapshotResponse{Status: merr.Status(err)}, nil
	}

	method := "RestoreExternalSnapshot"
	tr := timerecord.NewTimeRecorder(method)
	log := mlog.With(
		mlog.String("targetDb", req.GetDbName()),
		mlog.String("targetCollection", req.GetTargetCollectionName()),
		mlog.Bool("snapshotMetadataURISet", req.GetSnapshotMetadataUri() != ""),
		mlog.Bool("externalSpecSet", req.GetExternalSpec() != ""),
	)
	log.Info(ctx, rpcReceived(method))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.TotalLabel, req.GetDbName(), req.GetTargetCollectionName()).Inc()
	if req.GetSnapshotMetadataUri() == "" {
		err := merr.WrapErrParameterInvalidMsg("snapshot_metadata_uri is required for restore external snapshot")
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.FailLabel, req.GetDbName(), req.GetTargetCollectionName()).Inc()
		return &milvuspb.RestoreExternalSnapshotResponse{Status: merr.Status(err)}, nil
	}
	resp, err := node.mixCoord.RestoreSnapshot(ctx, &datapb.RestoreSnapshotRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_RestoreExternalSnapshot),
		),
		TargetDbName:         req.GetDbName(),
		TargetCollectionName: req.GetTargetCollectionName(),
		External:             true,
		SnapshotS3Location:   req.GetSnapshotMetadataUri(),
		ExternalSpec:         req.GetExternalSpec(),
	})
	if err = merr.CheckRPCCall(resp, err); err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.FailLabel, req.GetDbName(), req.GetTargetCollectionName()).Inc()
		log.Warn(ctx, "RestoreExternalSnapshot failed", mlog.Err(err))
		return &milvuspb.RestoreExternalSnapshotResponse{Status: merr.Status(err)}, nil
	}

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.SuccessLabel, req.GetDbName(), req.GetTargetCollectionName()).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return &milvuspb.RestoreExternalSnapshotResponse{
		Status: resp.GetStatus(),
		JobId:  resp.GetJobId(),
	}, nil
}

func (node *Proxy) ExportSnapshot(ctx context.Context, req *milvuspb.ExportSnapshotRequest) (*milvuspb.ExportSnapshotResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-ExportSnapshot")
	defer sp.End()

	if req == nil {
		err := merr.WrapErrParameterInvalidMsg("export snapshot request is nil")
		return &milvuspb.ExportSnapshotResponse{Status: merr.Status(err)}, nil
	}

	method := "ExportSnapshot"
	tr := timerecord.NewTimeRecorder(method)
	log := mlog.With(
		mlog.String("snapshotName", req.GetName()),
		mlog.String("dbName", req.GetDbName()),
		mlog.String("collectionName", req.GetCollectionName()),
		mlog.Bool("targetS3PathSet", req.GetTargetS3Path() != ""),
		mlog.Bool("externalSpecSet", req.GetExternalSpec() != ""),
	)
	log.Info(ctx, rpcReceived(method))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.TotalLabel, req.GetDbName(), req.GetCollectionName()).Inc()
	if err := ValidateSnapshotName(req.GetName()); err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.FailLabel, req.GetDbName(), req.GetCollectionName()).Inc()
		return &milvuspb.ExportSnapshotResponse{Status: merr.Status(err)}, nil
	}
	if req.GetCollectionName() == "" {
		err := merr.WrapErrParameterInvalidMsg("collection_name is required for export snapshot")
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.FailLabel, req.GetDbName(), req.GetCollectionName()).Inc()
		return &milvuspb.ExportSnapshotResponse{Status: merr.Status(err)}, nil
	}
	if req.GetTargetS3Path() == "" {
		err := merr.WrapErrParameterInvalidMsg("target_s3_path is required for export snapshot")
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.FailLabel, req.GetDbName(), req.GetCollectionName()).Inc()
		return &milvuspb.ExportSnapshotResponse{Status: merr.Status(err)}, nil
	}

	collectionID, err := globalMetaCache.GetCollectionID(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.FailLabel, req.GetDbName(), req.GetCollectionName()).Inc()
		log.Warn(ctx, "ExportSnapshot failed to resolve collection", mlog.Err(err))
		return &milvuspb.ExportSnapshotResponse{Status: merr.Status(err)}, nil
	}
	resp, err := node.mixCoord.ExportSnapshot(ctx, &datapb.ExportSnapshotRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_ExportSnapshot),
		),
		Name:         req.GetName(),
		CollectionId: collectionID,
		TargetS3Path: req.GetTargetS3Path(),
		ExternalSpec: req.GetExternalSpec(),
	})
	if err = merr.CheckRPCCall(resp, err); err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.FailLabel, req.GetDbName(), req.GetCollectionName()).Inc()
		log.Warn(ctx, "ExportSnapshot failed", mlog.Err(err))
		return &milvuspb.ExportSnapshotResponse{Status: merr.Status(err)}, nil
	}

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.SuccessLabel, req.GetDbName(), req.GetCollectionName()).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return &milvuspb.ExportSnapshotResponse{
		Status:              resp.GetStatus(),
		SnapshotMetadataUri: resp.GetSnapshotMetadataUri(),
	}, nil
}

func (node *Proxy) RestoreSnapshot(ctx context.Context, req *milvuspb.RestoreSnapshotRequest) (*milvuspb.RestoreSnapshotResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-RestoreSnapshot")
	defer sp.End()

	log := mlog.With(
		mlog.String("snapshotName", req.GetName()),
	)

	method := "RestoreSnapshot"
	tr := timerecord.NewTimeRecorder(method)
	log.Info(ctx, rpcReceived(method))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.TotalLabel, req.GetDbName(), req.GetCollectionName()).Inc()
	t := &restoreSnapshotTask{
		req:       req,
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		mixCoord:  node.mixCoord,
	}

	err := node.sched.ddQueue.Enqueue(t)
	if err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.AbandonLabel, req.GetDbName(), req.GetCollectionName()).Inc()
		log.Warn(ctx, "RestoreSnapshot failed to Enqueue",
			mlog.Err(err))
		return &milvuspb.RestoreSnapshotResponse{Status: merr.Status(err)}, nil
	}

	if err := t.WaitToFinish(); err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, failMetricLabel(err), req.GetDbName(), req.GetCollectionName()).Inc()
		log.Warn(ctx, "RestoreSnapshot failed to WaitToFinish",
			mlog.Err(err))
		return &milvuspb.RestoreSnapshotResponse{Status: merr.Status(err)}, nil
	}

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.SuccessLabel, req.GetDbName(), req.GetCollectionName()).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return t.result, nil
}

func (node *Proxy) GetRestoreSnapshotState(ctx context.Context, req *milvuspb.GetRestoreSnapshotStateRequest) (*milvuspb.GetRestoreSnapshotStateResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetRestoreSnapshotState")
	defer sp.End()

	log := mlog.With(
		mlog.Int64("jobID", req.GetJobId()),
	)

	method := "GetRestoreSnapshotState"
	tr := timerecord.NewTimeRecorder(method)
	log.Info(ctx, rpcReceived(method))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.TotalLabel, "", "").Inc()
	t := &getRestoreSnapshotStateTask{
		req:       req,
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		mixCoord:  node.mixCoord,
	}

	err := node.sched.ddQueue.Enqueue(t)
	if err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.AbandonLabel, "", "").Inc()
		log.Warn(ctx, "GetRestoreSnapshotState failed to Enqueue",
			mlog.Err(err))
		return &milvuspb.GetRestoreSnapshotStateResponse{
			Status: merr.Status(err),
		}, nil
	}

	if err := t.WaitToFinish(); err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, failMetricLabel(err), "", "").Inc()
		log.Warn(ctx, "GetRestoreSnapshotState failed to WaitToFinish",
			mlog.Err(err))
		return &milvuspb.GetRestoreSnapshotStateResponse{
			Status: merr.Status(err),
		}, nil
	}

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.SuccessLabel, "", "").Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return t.result, nil
}

func (node *Proxy) ListRestoreSnapshotJobs(ctx context.Context, req *milvuspb.ListRestoreSnapshotJobsRequest) (*milvuspb.ListRestoreSnapshotJobsResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-ListRestoreSnapshotJobs")
	defer sp.End()

	log := mlog.With(
		mlog.String("collectionName", req.GetCollectionName()),
	)

	method := "ListRestoreSnapshotJobs"
	tr := timerecord.NewTimeRecorder(method)
	log.Info(ctx, rpcReceived(method))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.TotalLabel, req.GetDbName(), req.GetCollectionName()).Inc()
	t := &listRestoreSnapshotJobsTask{
		req:       req,
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		mixCoord:  node.mixCoord,
	}

	err := node.sched.ddQueue.Enqueue(t)
	if err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.AbandonLabel, req.GetDbName(), req.GetCollectionName()).Inc()
		log.Warn(ctx, "ListRestoreSnapshotJobs failed to Enqueue",
			mlog.Err(err))
		return &milvuspb.ListRestoreSnapshotJobsResponse{
			Status: merr.Status(err),
		}, nil
	}

	if err := t.WaitToFinish(); err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, failMetricLabel(err), req.GetDbName(), req.GetCollectionName()).Inc()
		log.Warn(ctx, "ListRestoreSnapshotJobs failed to WaitToFinish",
			mlog.Err(err))
		return &milvuspb.ListRestoreSnapshotJobsResponse{
			Status: merr.Status(err),
		}, nil
	}

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.SuccessLabel, req.GetDbName(), req.GetCollectionName()).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return t.result, nil
}

func (node *Proxy) PinSnapshotData(ctx context.Context, req *milvuspb.PinSnapshotDataRequest) (*milvuspb.PinSnapshotDataResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-PinSnapshotData")
	defer sp.End()

	log := mlog.With(
		mlog.String("snapshotName", req.GetName()),
		mlog.String("collectionName", req.GetCollectionName()),
		mlog.String("dbName", req.GetDbName()),
	)

	method := "PinSnapshotData"
	tr := timerecord.NewTimeRecorder(method)
	log.Info(ctx, rpcReceived(method))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.TotalLabel, req.GetDbName(), req.GetCollectionName()).Inc()
	t := &pinSnapshotDataTask{
		req:       req,
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		mixCoord:  node.mixCoord,
	}

	if err := node.sched.ddQueue.Enqueue(t); err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.AbandonLabel, req.GetDbName(), req.GetCollectionName()).Inc()
		log.Warn(ctx, "PinSnapshotData failed to Enqueue",
			mlog.Err(err))
		return &milvuspb.PinSnapshotDataResponse{
			Status: merr.Status(err),
		}, nil
	}

	if err := t.WaitToFinish(); err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, failMetricLabel(err), req.GetDbName(), req.GetCollectionName()).Inc()
		log.Warn(ctx, "PinSnapshotData failed to WaitToFinish",
			mlog.Err(err))
		return &milvuspb.PinSnapshotDataResponse{
			Status: merr.Status(err),
		}, nil
	}

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.SuccessLabel, req.GetDbName(), req.GetCollectionName()).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return t.result, nil
}

func (node *Proxy) UnpinSnapshotData(ctx context.Context, req *milvuspb.UnpinSnapshotDataRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-UnpinSnapshotData")
	defer sp.End()

	log := mlog.With(
		mlog.Int64("pinID", req.GetPinId()),
	)

	method := "UnpinSnapshotData"
	tr := timerecord.NewTimeRecorder(method)
	log.Info(ctx, rpcReceived(method))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.TotalLabel, "", "").Inc()
	t := &unpinSnapshotDataTask{
		req:       req,
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		mixCoord:  node.mixCoord,
	}

	if err := node.sched.ddQueue.Enqueue(t); err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.AbandonLabel, "", "").Inc()
		log.Warn(ctx, "UnpinSnapshotData failed to Enqueue",
			mlog.Err(err))
		return merr.Status(err), nil
	}

	if err := t.WaitToFinish(); err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, failMetricLabel(err), "", "").Inc()
		log.Warn(ctx, "UnpinSnapshotData failed to WaitToFinish",
			mlog.Err(err))
		return merr.Status(err), nil
	}

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.SuccessLabel, "", "").Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return t.result, nil
}
