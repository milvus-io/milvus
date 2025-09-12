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
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func (node *Proxy) CreateSnapshot(ctx context.Context, req *milvuspb.CreateSnapshotRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-CreateSnapshot")
	defer sp.End()

	log := log.Ctx(ctx).With(
		zap.String("snapshotName", req.GetName()),
		zap.String("collectionName", req.GetCollectionName()),
	)

	method := "CreateSnapshot"
	tr := timerecord.NewTimeRecorder(method)
	log.Info(rpcReceived(method))

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
		log.Warn("CreateSnapshot failed to Enqueue",
			zap.Error(err))
		return merr.Status(err), nil
	}

	if err := t.WaitToFinish(); err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.FailLabel, req.GetDbName(), req.GetCollectionName()).Inc()
		log.Warn("CreateSnapshot failed to WaitToFinish",
			zap.Error(err))
		return merr.Status(err), nil
	}

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.SuccessLabel, req.GetDbName(), req.GetCollectionName()).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return t.result, nil
}

func (node *Proxy) DropSnapshot(ctx context.Context, req *milvuspb.DropSnapshotRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-DropSnapshot")
	defer sp.End()

	log := log.Ctx(ctx).With(
		zap.String("snapshotName", req.GetName()),
	)

	method := "DropSnapshot"
	tr := timerecord.NewTimeRecorder(method)
	log.Info(rpcReceived(method))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.TotalLabel, "", "").Inc()
	t := &dropSnapshotTask{
		req:       req,
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		mixCoord:  node.mixCoord,
	}

	err := node.sched.ddQueue.Enqueue(t)
	if err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.AbandonLabel, "", "").Inc()
		log.Warn("DropSnapshot failed to Enqueue",
			zap.Error(err))
		return merr.Status(err), nil
	}

	if err := t.WaitToFinish(); err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.FailLabel, "", "").Inc()
		log.Warn("DropSnapshot failed to WaitToFinish",
			zap.Error(err))
		return merr.Status(err), nil
	}

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.SuccessLabel, "", "").Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return t.result, nil
}

func (node *Proxy) DescribeSnapshot(ctx context.Context, req *milvuspb.DescribeSnapshotRequest) (*milvuspb.DescribeSnapshotResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-DescribeSnapshot")
	defer sp.End()

	log := log.Ctx(ctx).With(
		zap.String("snapshotName", req.GetName()),
	)

	method := "DescribeSnapshot"
	tr := timerecord.NewTimeRecorder(method)
	log.Info(rpcReceived(method))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.TotalLabel, "", "").Inc()
	t := &describeSnapshotTask{
		req:       req,
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		mixCoord:  node.mixCoord,
	}

	err := node.sched.ddQueue.Enqueue(t)
	if err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.AbandonLabel, "", "").Inc()
		log.Warn("DescribeSnapshot failed to Enqueue",
			zap.Error(err))
		return &milvuspb.DescribeSnapshotResponse{
			Status: merr.Status(err),
		}, nil
	}

	if err := t.WaitToFinish(); err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.FailLabel, "", "").Inc()
		log.Warn("DescribeSnapshot failed to WaitToFinish",
			zap.Error(err))
		return &milvuspb.DescribeSnapshotResponse{
			Status: merr.Status(err),
		}, nil
	}

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.SuccessLabel, "", "").Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return t.result, nil
}

func (node *Proxy) ListSnapshots(ctx context.Context, req *milvuspb.ListSnapshotsRequest) (*milvuspb.ListSnapshotsResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-ListSnapshots")
	defer sp.End()

	log := log.Ctx(ctx).With(
		zap.String("collectionName", req.GetCollectionName()))

	method := "ListSnapshots"
	tr := timerecord.NewTimeRecorder(method)
	log.Info(rpcReceived(method))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.TotalLabel, req.GetCollectionName(), "").Inc()
	t := &listSnapshotsTask{
		req:       req,
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		mixCoord:  node.mixCoord,
	}

	err := node.sched.ddQueue.Enqueue(t)
	if err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.AbandonLabel, req.GetCollectionName(), "").Inc()
		log.Warn("ListSnapshots failed to Enqueue",
			zap.Error(err))
		return &milvuspb.ListSnapshotsResponse{
			Status: merr.Status(err),
		}, nil
	}

	if err := t.WaitToFinish(); err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.FailLabel, req.GetCollectionName(), "").Inc()
		log.Warn("ListSnapshots failed to WaitToFinish",
			zap.Error(err))
		return &milvuspb.ListSnapshotsResponse{
			Status: merr.Status(err),
		}, nil
	}

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.SuccessLabel, req.GetCollectionName(), "").Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return t.result, nil
}

func (node *Proxy) RestoreSnapshot(ctx context.Context, req *milvuspb.RestoreSnapshotRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-RestoreSnapshot")
	defer sp.End()

	log := log.Ctx(ctx).With(
		zap.String("snapshotName", req.GetName()),
	)

	method := "RestoreSnapshot"
	tr := timerecord.NewTimeRecorder(method)
	log.Info(rpcReceived(method))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.TotalLabel, "", "").Inc()
	t := &restoreSnapshotTask{
		req:       req,
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		mixCoord:  node.mixCoord,
	}

	err := node.sched.ddQueue.Enqueue(t)
	if err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.AbandonLabel, "", "").Inc()
		log.Warn("RestoreSnapshot failed to Enqueue",
			zap.Error(err))
		return merr.Status(err), nil
	}

	if err := t.WaitToFinish(); err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.FailLabel, "", "").Inc()
		log.Warn("RestoreSnapshot failed to WaitToFinish",
			zap.Error(err))
		return merr.Status(err), nil
	}

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.SuccessLabel, "", "").Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return t.result, nil
}
