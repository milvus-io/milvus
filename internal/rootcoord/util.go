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
	"encoding/json"
	"fmt"
	"strconv"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var logger = log.L().WithOptions(zap.Fields(zap.String("role", typeutil.RootCoordRole)))

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
	return true
}

// GetFieldSchemaByID return field schema by id
func GetFieldSchemaByID(coll *model.Collection, fieldID typeutil.UniqueID) (*model.Field, error) {
	for _, f := range coll.Fields {
		if f.FieldID == fieldID {
			return f, nil
		}
	}
	return nil, fmt.Errorf("field id = %d not found", fieldID)
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

func failStatus(code commonpb.ErrorCode, reason string) *commonpb.Status {
	return &commonpb.Status{
		ErrorCode: code,
		Reason:    reason,
	}
}

func succStatus() *commonpb.Status {
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}
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
	megaBytes2Bytes := func(v float64) float64 {
		return v * 1024.0 * 1024.0
	}
	toBytesIfNecessary := func(rate float64) float64 {
		switch configKey {
		case common.CollectionInsertRateMaxKey:
			return megaBytes2Bytes(rate)
		case common.CollectionInsertRateMinKey:
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
			return getCollectionRateLimitConfigDefaultValue(configKey)
		}

		rateInBytes := toBytesIfNecessary(rate)
		if rateInBytes < 0 {
			return getCollectionRateLimitConfigDefaultValue(configKey)
		}
		return rateInBytes
	}

	return getCollectionRateLimitConfigDefaultValue(configKey)
}
