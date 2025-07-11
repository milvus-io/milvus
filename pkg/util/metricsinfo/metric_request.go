// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package metricsinfo

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	// MetricTypeKey are the key of metric type in GetMetrics request.
	MetricTypeKey = common.MetricTypeKey

	// SystemInfoMetrics means users request for system information metrics.
	SystemInfoMetrics = "system_info"

	// CollectionStorageMetrics means users request for collection storage metrics.
	CollectionStorageMetrics = "collection_storage"

	// MetricRequestTypeKey is a key for identify request type.
	MetricRequestTypeKey = "req_type"

	// MetricRequestParamsSeparator is a separator that parameter value will be joined be separator
	MetricRequestParamsSeparator = ","

	// SegmentKey request for get segments from the datanode/querynode/datacoord/querycoord
	SegmentKey = "segments"

	// ChannelKey request for get channels from the datanode/querynode/datacoord/querycoord
	ChannelKey = "channels"

	// DistKey request for segment/channel/leader view distribution on querycoord
	// DistKey request for get segments on the datacoord
	DistKey = "dist"

	// TargetKey request for segment/channel target on the querycoord
	TargetKey = "qc_target"

	// AllTaskKey request for get all tasks on the querycoord
	AllTaskKey = "tasks_all"

	// ReplicaKey request for get replica on the querycoord
	ReplicaKey = "replica"

	// ResourceGroupKey request for get resource groups on the querycoord
	ResourceGroupKey = "resource_group"

	// ImportTaskKey request for get import tasks from the datacoord
	ImportTaskKey = "import_tasks"

	// CompactionTaskKey request for get compaction tasks from the datacoord
	CompactionTaskKey = "compaction_tasks"

	// BuildIndexTaskKey request for get building index tasks from the datacoord
	BuildIndexTaskKey = "build_index_tasks"

	// IndexKey request for get index list/detail from the datacoord
	IndexKey = "index"

	// SyncTaskKey request for get sync tasks from the datanode
	SyncTaskKey = "sync_tasks"

	// MetricRequestParamVerboseKey as a request parameter decide to whether return verbose value
	MetricRequestParamVerboseKey = "verbose"

	MetricRequestParamTargetScopeKey = "target_scope"

	MetricRequestParamCollectionIDKey = "collection_id"

	MetricRequestParamINKey  = "in"
	MetricsRequestParamsInDC = "dc"
	MetricsRequestParamsInQC = "qc"
	MetricsRequestParamsInDN = "dn"
	MetricsRequestParamsInQN = "qn"

	MetricRequestProcessInRoleKey = "ProcessRole"
)

var (
	RequestParamsInDC = &commonpb.KeyValuePair{Key: MetricRequestParamINKey, Value: MetricsRequestParamsInDC}
	RequestParamsInQC = &commonpb.KeyValuePair{Key: MetricRequestParamINKey, Value: MetricsRequestParamsInQC}
	RequestParamsInDN = &commonpb.KeyValuePair{Key: MetricRequestParamINKey, Value: MetricsRequestParamsInDN}
	RequestParamsInQN = &commonpb.KeyValuePair{Key: MetricRequestParamINKey, Value: MetricsRequestParamsInQN}

	RequestProcessInDCRole = &commonpb.KeyValuePair{Key: MetricRequestProcessInRoleKey, Value: typeutil.DataCoordRole}
	RequestProcessInQCRole = &commonpb.KeyValuePair{Key: MetricRequestProcessInRoleKey, Value: typeutil.QueryCoordRole}
)

type MetricsRequestAction func(ctx context.Context, req *milvuspb.GetMetricsRequest, jsonReq gjson.Result) (string, error)

type MetricsRequest struct {
	metricsReqType2Action map[string]MetricsRequestAction
	lock                  sync.Mutex
}

func NewMetricsRequest() *MetricsRequest {
	return &MetricsRequest{
		metricsReqType2Action: make(map[string]MetricsRequestAction),
	}
}

func (mr *MetricsRequest) RegisterMetricsRequest(reqType string, action MetricsRequestAction) {
	mr.lock.Lock()
	defer mr.lock.Unlock()
	_, ok := mr.metricsReqType2Action[reqType]
	if ok {
		log.Info("metrics request type already exists", zap.String("reqType", reqType))
		return
	}

	mr.metricsReqType2Action[reqType] = action
}

func (mr *MetricsRequest) ExecuteMetricsRequest(ctx context.Context, req *milvuspb.GetMetricsRequest) (string, error) {
	jsonReq := gjson.Parse(req.Request)
	reqType, err := ParseMetricRequestType(jsonReq)
	if err != nil {
		log.Warn("failed to parse metric type", zap.Error(err))
		return "", err
	}

	mr.lock.Lock()
	action, ok := mr.metricsReqType2Action[reqType]
	if !ok {
		mr.lock.Unlock()
		log.Warn("unimplemented metric request type", zap.String("req_type", reqType))
		return "", errors.New(MsgUnimplementedMetric)
	}
	mr.lock.Unlock()

	actionRet, err := action(ctx, req, jsonReq)
	if err != nil {
		msg := fmt.Sprintf("failed to execute %s", reqType)
		log.Warn(msg, zap.Error(err))
		return "", err
	}
	return actionRet, nil
}

func RequestWithVerbose(jsonReq gjson.Result) bool {
	v := jsonReq.Get(MetricRequestParamVerboseKey)
	if !v.Exists() {
		return false
	}
	return v.Bool()
}

// ParseMetricRequestType returns the metric type of req
func ParseMetricRequestType(jsonRet gjson.Result) (string, error) {
	v := jsonRet.Get(MetricRequestTypeKey)
	if v.Exists() {
		return v.String(), nil
	}

	v = jsonRet.Get(MetricTypeKey)
	if v.Exists() {
		return v.String(), nil
	}

	return "", fmt.Errorf("%s or %s not found in request", MetricTypeKey, MetricRequestTypeKey)
}

func ParseMetricProcessInRole(jsonRet gjson.Result) (string, error) {
	v := jsonRet.Get(MetricRequestProcessInRoleKey)
	if v.Exists() {
		return v.String(), nil
	}

	return "", fmt.Errorf("%s not found in request", MetricRequestProcessInRoleKey)
}

func GetCollectionIDFromRequest(jsonReq gjson.Result) int64 {
	v := jsonReq.Get(MetricRequestParamCollectionIDKey)
	if !v.Exists() {
		return 0
	}
	return v.Int()
}

// ConstructRequestByMetricType constructs a request according to the metric type
func ConstructRequestByMetricType(metricType string) (*milvuspb.GetMetricsRequest, error) {
	m := make(map[string]interface{})
	m[MetricTypeKey] = metricType
	binary, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("failed to construct request by metric type %s: %s", metricType, err.Error())
	}
	// TODO:: switch metricType to different msgType and return err when metricType is not supported
	return &milvuspb.GetMetricsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_SystemInfo),
		),
		Request: string(binary),
	}, nil
}

func ConstructGetMetricsRequest(m map[string]interface{}) (*milvuspb.GetMetricsRequest, error) {
	binary, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("failed to construct request: %s", err.Error())
	}

	return &milvuspb.GetMetricsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_SystemInfo),
		),
		Request: string(binary),
	}, nil
}
