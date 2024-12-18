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

package rootcoord

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// EqualKeyPairArray check whether 2 KeyValuePairs are equal
func EqualKeyPairArray(p1 []*commonpb.KeyValuePair, p2 []*commonpb.KeyValuePair) bool {
	if len(p1) != len(p2) {
		return false
	}
	m1 := make(map[string]string)
	for _, p := range p1 {
		m1[p.Key] = p.Value
	}
	for _, p := range p2 {
		val, ok := m1[p.Key]
		if !ok {
			return false
		}
		if val != p.Value {
			return false
		}
	}
	return ContainsKeyPairArray(p1, p2)
}

func ContainsKeyPairArray(src []*commonpb.KeyValuePair, target []*commonpb.KeyValuePair) bool {
	m1 := make(map[string]string)
	for _, p := range target {
		m1[p.Key] = p.Value
	}
	for _, p := range src {
		val, ok := m1[p.Key]
		if !ok {
			return false
		}
		if val != p.Value {
			return false
		}
	}
	return true
}

// EncodeMsgPositions serialize []*MsgPosition into string
func EncodeMsgPositions(msgPositions []*msgstream.MsgPosition) (string, error) {
	if len(msgPositions) == 0 {
		return "", nil
	}
	resByte, err := json.Marshal(msgPositions)
	if err != nil {
		return "", err
	}
	return string(resByte), nil
}

// DecodeMsgPositions deserialize string to []*MsgPosition
func DecodeMsgPositions(str string, msgPositions *[]*msgstream.MsgPosition) error {
	if str == "" || str == "null" {
		return nil
	}
	return json.Unmarshal([]byte(str), msgPositions)
}

func Int64TupleSliceToMap(s []common.Int64Tuple) map[int]common.Int64Tuple {
	ret := make(map[int]common.Int64Tuple, len(s))
	for i, e := range s {
		ret[i] = e
	}
	return ret
}

func Int64TupleMapToSlice(s map[int]common.Int64Tuple) []common.Int64Tuple {
	ret := make([]common.Int64Tuple, 0, len(s))
	for _, e := range s {
		ret = append(ret, e)
	}
	return ret
}

func CheckMsgType(got, expect commonpb.MsgType) error {
	if got != expect {
		return fmt.Errorf("invalid msg type, expect %s, but got %s", expect, got)
	}
	return nil
}

type TimeTravelRequest interface {
	GetBase() *commonpb.MsgBase
	GetTimeStamp() Timestamp
}

func getTravelTs(req TimeTravelRequest) Timestamp {
	if req.GetTimeStamp() == 0 {
		return typeutil.MaxTimestamp
	}
	return req.GetTimeStamp()
}

func isMaxTs(ts Timestamp) bool {
	return ts == typeutil.MaxTimestamp
}

func getCollectionRateLimitConfigDefaultValue(configKey string) float64 {
	switch configKey {
	case common.CollectionInsertRateMaxKey:
		return Params.QuotaConfig.DMLMaxInsertRatePerCollection.GetAsFloat()
	case common.CollectionInsertRateMinKey:
		return Params.QuotaConfig.DMLMinInsertRatePerCollection.GetAsFloat()
	case common.CollectionUpsertRateMaxKey:
		return Params.QuotaConfig.DMLMaxUpsertRatePerCollection.GetAsFloat()
	case common.CollectionUpsertRateMinKey:
		return Params.QuotaConfig.DMLMinUpsertRatePerCollection.GetAsFloat()
	case common.CollectionDeleteRateMaxKey:
		return Params.QuotaConfig.DMLMaxDeleteRatePerCollection.GetAsFloat()
	case common.CollectionDeleteRateMinKey:
		return Params.QuotaConfig.DMLMinDeleteRatePerCollection.GetAsFloat()
	case common.CollectionBulkLoadRateMaxKey:
		return Params.QuotaConfig.DMLMaxBulkLoadRatePerCollection.GetAsFloat()
	case common.CollectionBulkLoadRateMinKey:
		return Params.QuotaConfig.DMLMinBulkLoadRatePerCollection.GetAsFloat()
	case common.CollectionQueryRateMaxKey:
		return Params.QuotaConfig.DQLMaxQueryRatePerCollection.GetAsFloat()
	case common.CollectionQueryRateMinKey:
		return Params.QuotaConfig.DQLMinQueryRatePerCollection.GetAsFloat()
	case common.CollectionSearchRateMaxKey:
		return Params.QuotaConfig.DQLMaxSearchRatePerCollection.GetAsFloat()
	case common.CollectionSearchRateMinKey:
		return Params.QuotaConfig.DQLMinSearchRatePerCollection.GetAsFloat()
	case common.CollectionDiskQuotaKey:
		return Params.QuotaConfig.DiskQuotaPerCollection.GetAsFloat()
	default:
		return float64(0)
	}
}

func getCollectionRateLimitConfig(properties map[string]string, configKey string) float64 {
	return getRateLimitConfig(properties, configKey, getCollectionRateLimitConfigDefaultValue(configKey))
}

func getRateLimitConfig(properties map[string]string, configKey string, configValue float64) float64 {
	megaBytes2Bytes := func(v float64) float64 {
		return v * 1024.0 * 1024.0
	}
	toBytesIfNecessary := func(rate float64) float64 {
		switch configKey {
		case common.CollectionInsertRateMaxKey:
			return megaBytes2Bytes(rate)
		case common.CollectionInsertRateMinKey:
			return megaBytes2Bytes(rate)
		case common.CollectionUpsertRateMaxKey:
			return megaBytes2Bytes(rate)
		case common.CollectionUpsertRateMinKey:
			return megaBytes2Bytes(rate)
		case common.CollectionDeleteRateMaxKey:
			return megaBytes2Bytes(rate)
		case common.CollectionDeleteRateMinKey:
			return megaBytes2Bytes(rate)
		case common.CollectionBulkLoadRateMaxKey:
			return megaBytes2Bytes(rate)
		case common.CollectionBulkLoadRateMinKey:
			return megaBytes2Bytes(rate)
		case common.CollectionQueryRateMaxKey:
			return rate
		case common.CollectionQueryRateMinKey:
			return rate
		case common.CollectionSearchRateMaxKey:
			return rate
		case common.CollectionSearchRateMinKey:
			return rate
		case common.CollectionDiskQuotaKey:
			return megaBytes2Bytes(rate)

		default:
			return float64(0)
		}
	}

	v, ok := properties[configKey]
	if ok {
		rate, err := strconv.ParseFloat(v, 64)
		if err != nil {
			log.Warn("invalid configuration for collection dml rate",
				zap.String("config item", configKey),
				zap.String("config value", v))
			return configValue
		}

		rateInBytes := toBytesIfNecessary(rate)
		if rateInBytes < 0 {
			return configValue
		}
		return rateInBytes
	}

	return configValue
}

func getQueryCoordMetrics(ctx context.Context, queryCoord types.QueryCoordClient) (*metricsinfo.QueryCoordTopology, error) {
	req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
	if err != nil {
		return nil, err
	}

	rsp, err := queryCoord.GetMetrics(ctx, req)
	if err = merr.CheckRPCCall(rsp, err); err != nil {
		return nil, err
	}
	queryCoordTopology := &metricsinfo.QueryCoordTopology{}
	if err := metricsinfo.UnmarshalTopology(rsp.GetResponse(), queryCoordTopology); err != nil {
		return nil, err
	}

	return queryCoordTopology, nil
}

func getDataCoordMetrics(ctx context.Context, dataCoord types.DataCoordClient) (*metricsinfo.DataCoordTopology, error) {
	req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
	if err != nil {
		return nil, err
	}

	rsp, err := dataCoord.GetMetrics(ctx, req)
	if err = merr.CheckRPCCall(rsp, err); err != nil {
		return nil, err
	}
	dataCoordTopology := &metricsinfo.DataCoordTopology{}
	if err = metricsinfo.UnmarshalTopology(rsp.GetResponse(), dataCoordTopology); err != nil {
		return nil, err
	}

	return dataCoordTopology, nil
}

func getProxyMetrics(ctx context.Context, proxies proxyutil.ProxyClientManagerInterface) ([]*metricsinfo.ProxyInfos, error) {
	resp, err := proxies.GetProxyMetrics(ctx)
	if err != nil {
		return nil, err
	}

	ret := make([]*metricsinfo.ProxyInfos, 0, len(resp))
	for _, rsp := range resp {
		proxyMetric := &metricsinfo.ProxyInfos{}
		err = metricsinfo.UnmarshalComponentInfos(rsp.GetResponse(), proxyMetric)
		if err != nil {
			return nil, err
		}
		ret = append(ret, proxyMetric)
	}

	return ret, nil
}

func CheckTimeTickLagExceeded(ctx context.Context, queryCoord types.QueryCoordClient, dataCoord types.DataCoordClient, maxDelay time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, GetMetricsTimeout)
	defer cancel()

	now := time.Now()
	group := &errgroup.Group{}
	queryNodeTTDelay := typeutil.NewConcurrentMap[string, time.Duration]()
	dataNodeTTDelay := typeutil.NewConcurrentMap[string, time.Duration]()

	group.Go(func() error {
		queryCoordTopology, err := getQueryCoordMetrics(ctx, queryCoord)
		if err != nil {
			return err
		}

		for _, queryNodeMetric := range queryCoordTopology.Cluster.ConnectedNodes {
			qm := queryNodeMetric.QuotaMetrics
			if qm != nil {
				if qm.Fgm.NumFlowGraph > 0 && qm.Fgm.MinFlowGraphChannel != "" {
					minTt, _ := tsoutil.ParseTS(qm.Fgm.MinFlowGraphTt)
					delay := now.Sub(minTt)

					if delay.Milliseconds() >= maxDelay.Milliseconds() {
						queryNodeTTDelay.Insert(qm.Fgm.MinFlowGraphChannel, delay)
					}
				}
			}
		}
		return nil
	})

	// get Data cluster metrics
	group.Go(func() error {
		dataCoordTopology, err := getDataCoordMetrics(ctx, dataCoord)
		if err != nil {
			return err
		}

		for _, dataNodeMetric := range dataCoordTopology.Cluster.ConnectedDataNodes {
			dm := dataNodeMetric.QuotaMetrics
			if dm != nil {
				if dm.Fgm.NumFlowGraph > 0 && dm.Fgm.MinFlowGraphChannel != "" {
					minTt, _ := tsoutil.ParseTS(dm.Fgm.MinFlowGraphTt)
					delay := now.Sub(minTt)

					if delay.Milliseconds() >= maxDelay.Milliseconds() {
						dataNodeTTDelay.Insert(dm.Fgm.MinFlowGraphChannel, delay)
					}
				}
			}
		}
		return nil
	})

	err := group.Wait()
	if err != nil {
		return err
	}

	var maxLagChannel string
	var maxLag time.Duration
	findMaxLagChannel := func(params ...*typeutil.ConcurrentMap[string, time.Duration]) {
		for _, param := range params {
			param.Range(func(k string, v time.Duration) bool {
				if v > maxLag {
					maxLag = v
					maxLagChannel = k
				}
				return true
			})
		}
	}

	var errStr string
	findMaxLagChannel(queryNodeTTDelay)
	if maxLag > 0 && len(maxLagChannel) != 0 {
		errStr = fmt.Sprintf("query max timetick lag:%s on channel:%s", maxLag, maxLagChannel)
	}
	maxLagChannel = ""
	maxLag = 0
	findMaxLagChannel(dataNodeTTDelay)
	if maxLag > 0 && len(maxLagChannel) != 0 {
		if errStr != "" {
			errStr += ", "
		}
		errStr += fmt.Sprintf("data max timetick lag:%s on channel:%s", maxLag, maxLagChannel)
	}
	if errStr != "" {
		return fmt.Errorf("max timetick lag execced threhold: %s", errStr)
	}

	return nil
}
