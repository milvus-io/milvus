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

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy/dml"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

// Update accepts one literal assignment set for a list of primary keys.
func (node *Proxy) Update(ctx context.Context, request *milvuspb.UpdateRequest) (*milvuspb.MutationResult, error) {
	metrics.GetStats(ctx).
		SetNodeID(paramtable.GetNodeID()).
		SetInboundLabel(metrics.UpdateLabel).
		SetDatabaseName(request.GetDbName()).
		SetCollectionName(request.GetCollectionName())

	if !paramtable.Get().ProxyCfg.PKUpdateEnabled.GetAsBool() {
		return failedMutationResult(merr.Wrap(merr.ErrServiceUnimplemented, "PK-update admission is disabled (proxy.pkUpdate.enabled=false)")), nil
	}
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return failedMutationResult(err), nil
	}
	if err := checkExternalCollectionBlockedForWrite(ctx, node.GetMetaCache(), request.GetDbName(), request.GetCollectionName(), "update"); err != nil {
		return failedMutationResult(err), nil
	}
	tr := timerecord.NewTimeRecorder("Update")
	result, err := dml.Update(ctx, node.GetMetaCache(), request)
	if err != nil {
		return failedMutationResult(err), nil
	}

	requestSize := proto.Size(request)
	rateCol.Add(internalpb.RateType_DMLInsert.String(), float64(requestSize))

	dbName := request.GetDbName()
	username := GetCurUserFromContextOrDefault(ctx)
	nodeID := paramtable.GetStringNodeID()
	v := hookutil.GetExtension().Report(map[string]any{
		hookutil.OpTypeKey:          hookutil.OpTypeUpdate,
		hookutil.DatabaseKey:        dbName,
		hookutil.UsernameKey:        username,
		hookutil.RequestDataSizeKey: requestSize,
		hookutil.SuccessCntKey:      result.GetUpdateCnt(),
		hookutil.FailCntKey:         len(result.GetErrIndex()),
	})
	SetReportValue(result.GetStatus(), v)
	metrics.ProxyReportValue.WithLabelValues(nodeID, hookutil.OpTypeUpdate, dbName, username).Add(float64(v))
	metrics.ProxyMutationLatency.WithLabelValues(nodeID, metrics.UpdateLabel, dbName, request.GetCollectionName()).
		Observe(float64(tr.ElapseSpan().Milliseconds()))
	return result, nil
}
