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
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/ratelimitutil"
	"github.com/stretchr/testify/assert"
)

func TestMultiRateLimiter(t *testing.T) {
	collectionID := int64(1)
	t.Run("test multiRateLimiter", func(t *testing.T) {
		bak := Params.QuotaConfig.QuotaAndLimitsEnabled
		paramtable.Get().Save(Params.QuotaConfig.QuotaAndLimitsEnabled.Key, "true")
		multiLimiter := NewMultiRateLimiter()
		multiLimiter.collectionLimiters[collectionID] = newRateLimiter(false)
		for _, rt := range internalpb.RateType_value {
			if IsDDLRequest(internalpb.RateType(rt)) {
				multiLimiter.globalDDLLimiter.limiters.Insert(internalpb.RateType(rt), ratelimitutil.NewLimiter(ratelimitutil.Limit(5), 1))
			} else {
				multiLimiter.collectionLimiters[collectionID].limiters.Insert(internalpb.RateType(rt), ratelimitutil.NewLimiter(ratelimitutil.Limit(1000), 1))
			}
		}
		for _, rt := range internalpb.RateType_value {
			if IsDDLRequest(internalpb.RateType(rt)) {
				errCode := multiLimiter.Check(collectionID, internalpb.RateType(rt), 1)
				assert.Equal(t, commonpb.ErrorCode_Success, errCode)
				errCode = multiLimiter.Check(collectionID, internalpb.RateType(rt), 5)
				assert.Equal(t, commonpb.ErrorCode_Success, errCode)
				errCode = multiLimiter.Check(collectionID, internalpb.RateType(rt), 5)
				assert.Equal(t, commonpb.ErrorCode_RateLimit, errCode)
			} else {
				errCode := multiLimiter.Check(collectionID, internalpb.RateType(rt), 1)
				assert.Equal(t, commonpb.ErrorCode_Success, errCode)
				errCode = multiLimiter.Check(collectionID, internalpb.RateType(rt), math.MaxInt)
				assert.Equal(t, commonpb.ErrorCode_Success, errCode)
				errCode = multiLimiter.Check(collectionID, internalpb.RateType(rt), math.MaxInt)
				assert.Equal(t, commonpb.ErrorCode_RateLimit, errCode)
			}

		}
		Params.QuotaConfig.QuotaAndLimitsEnabled = bak
	})

	t.Run("test global static limit", func(t *testing.T) {
		bak := Params.QuotaConfig.QuotaAndLimitsEnabled
		paramtable.Get().Save(Params.QuotaConfig.QuotaAndLimitsEnabled.Key, "true")
		multiLimiter := NewMultiRateLimiter()
		multiLimiter.collectionLimiters[1] = newRateLimiter(false)
		multiLimiter.collectionLimiters[2] = newRateLimiter(false)
		multiLimiter.collectionLimiters[3] = newRateLimiter(false)
		for _, rt := range internalpb.RateType_value {
			if IsDDLRequest(internalpb.RateType(rt)) {
				multiLimiter.globalDDLLimiter.limiters.Insert(internalpb.RateType(rt), ratelimitutil.NewLimiter(ratelimitutil.Limit(5), 1))
			} else {
				multiLimiter.globalDDLLimiter.limiters.Insert(internalpb.RateType(rt), ratelimitutil.NewLimiter(ratelimitutil.Limit(2), 1))
				multiLimiter.collectionLimiters[1].limiters.Insert(internalpb.RateType(rt), ratelimitutil.NewLimiter(ratelimitutil.Limit(2), 1))
				multiLimiter.collectionLimiters[2].limiters.Insert(internalpb.RateType(rt), ratelimitutil.NewLimiter(ratelimitutil.Limit(2), 1))
				multiLimiter.collectionLimiters[3].limiters.Insert(internalpb.RateType(rt), ratelimitutil.NewLimiter(ratelimitutil.Limit(2), 1))
			}
		}
		for _, rt := range internalpb.RateType_value {
			if IsDDLRequest(internalpb.RateType(rt)) {
				errCode := multiLimiter.Check(1, internalpb.RateType(rt), 1)
				assert.Equal(t, commonpb.ErrorCode_Success, errCode)
				errCode = multiLimiter.Check(1, internalpb.RateType(rt), 5)
				assert.Equal(t, commonpb.ErrorCode_Success, errCode)
				errCode = multiLimiter.Check(1, internalpb.RateType(rt), 5)
				assert.Equal(t, commonpb.ErrorCode_RateLimit, errCode)
			} else {
				errCode := multiLimiter.Check(1, internalpb.RateType(rt), 1)
				assert.Equal(t, commonpb.ErrorCode_Success, errCode)
				errCode = multiLimiter.Check(2, internalpb.RateType(rt), 1)
				assert.Equal(t, commonpb.ErrorCode_Success, errCode)
				errCode = multiLimiter.Check(3, internalpb.RateType(rt), 1)
				assert.Equal(t, commonpb.ErrorCode_RateLimit, errCode)
			}
		}
		Params.QuotaConfig.QuotaAndLimitsEnabled = bak
	})

	t.Run("not enable quotaAndLimit", func(t *testing.T) {
		multiLimiter := NewMultiRateLimiter()
		multiLimiter.collectionLimiters[collectionID] = newRateLimiter(false)
		bak := Params.QuotaConfig.QuotaAndLimitsEnabled
		paramtable.Get().Save(Params.QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
		for _, rt := range internalpb.RateType_value {
			errCode := multiLimiter.Check(collectionID, internalpb.RateType(rt), 1)
			assert.Equal(t, commonpb.ErrorCode_Success, errCode)
		}
		Params.QuotaConfig.QuotaAndLimitsEnabled = bak
	})

	t.Run("test limit", func(t *testing.T) {
		run := func(insertRate float64) {
			bakInsertRate := Params.QuotaConfig.DMLMaxInsertRate
			paramtable.Get().Save(Params.QuotaConfig.DMLMaxInsertRate.Key, fmt.Sprintf("%f", insertRate))
			multiLimiter := NewMultiRateLimiter()
			bak := Params.QuotaConfig.QuotaAndLimitsEnabled
			paramtable.Get().Save(Params.QuotaConfig.QuotaAndLimitsEnabled.Key, "true")
			errCode := multiLimiter.Check(collectionID, internalpb.RateType_DMLInsert, 1*1024*1024)
			assert.Equal(t, commonpb.ErrorCode_Success, errCode)
			Params.QuotaConfig.QuotaAndLimitsEnabled = bak
			Params.QuotaConfig.DMLMaxInsertRate = bakInsertRate
		}
		run(math.MaxFloat64)
		run(math.MaxFloat64 / 1.2)
		run(math.MaxFloat64 / 2)
		run(math.MaxFloat64 / 3)
		run(math.MaxFloat64 / 10000)
	})

	t.Run("test set rates", func(t *testing.T) {
		multiLimiter := NewMultiRateLimiter()
		zeroRates := make([]*internalpb.Rate, 0, len(internalpb.RateType_value))
		for _, rt := range internalpb.RateType_value {
			zeroRates = append(zeroRates, &internalpb.Rate{
				Rt: internalpb.RateType(rt), R: 0,
			})
		}

		err := multiLimiter.SetRates([]*proxypb.CollectionRate{
			{
				Collection: 1,
				Rates:      zeroRates,
			},
			{
				Collection: 2,
				Rates:      zeroRates,
			},
		})
		assert.NoError(t, err)
	})

	t.Run("test quota states", func(t *testing.T) {
		multiLimiter := NewMultiRateLimiter()
		zeroRates := make([]*internalpb.Rate, 0, len(internalpb.RateType_value))
		for _, rt := range internalpb.RateType_value {
			zeroRates = append(zeroRates, &internalpb.Rate{
				Rt: internalpb.RateType(rt), R: 0,
			})
		}

		err := multiLimiter.SetRates([]*proxypb.CollectionRate{
			{
				Collection: 1,
				Rates:      zeroRates,
				States: []milvuspb.QuotaState{
					milvuspb.QuotaState_DenyToWrite,
				},
				Codes: []commonpb.ErrorCode{
					commonpb.ErrorCode_DiskQuotaExhausted,
				},
			},
			{
				Collection: 2,
				Rates:      zeroRates,

				States: []milvuspb.QuotaState{
					milvuspb.QuotaState_DenyToRead,
				},
				Codes: []commonpb.ErrorCode{
					commonpb.ErrorCode_ForceDeny,
				},
			},
		})
		assert.NoError(t, err)

		states, codes := multiLimiter.GetQuotaStates()
		assert.Len(t, states, 2)
		assert.Len(t, codes, 2)
		assert.Contains(t, codes, GetQuotaErrorString(commonpb.ErrorCode_DiskQuotaExhausted))
		assert.Contains(t, codes, GetQuotaErrorString(commonpb.ErrorCode_ForceDeny))
	})
}

func TestRateLimiter(t *testing.T) {
	t.Run("test limit", func(t *testing.T) {
		limiter := newRateLimiter(false)
		for _, rt := range internalpb.RateType_value {
			limiter.limiters.Insert(internalpb.RateType(rt), ratelimitutil.NewLimiter(ratelimitutil.Limit(1000), 1))
		}
		for _, rt := range internalpb.RateType_value {
			ok, _ := limiter.limit(internalpb.RateType(rt), 1)
			assert.False(t, ok)
			ok, _ = limiter.limit(internalpb.RateType(rt), math.MaxInt)
			assert.False(t, ok)
			ok, _ = limiter.limit(internalpb.RateType(rt), math.MaxInt)
			assert.True(t, ok)
		}
	})

	t.Run("test setRates", func(t *testing.T) {
		limiter := newRateLimiter(false)
		for _, rt := range internalpb.RateType_value {
			limiter.limiters.Insert(internalpb.RateType(rt), ratelimitutil.NewLimiter(ratelimitutil.Limit(1000), 1))
		}

		zeroRates := make([]*internalpb.Rate, 0, len(internalpb.RateType_value))
		for _, rt := range internalpb.RateType_value {
			zeroRates = append(zeroRates, &internalpb.Rate{
				Rt: internalpb.RateType(rt), R: 0,
			})
		}
		err := limiter.setRates(&proxypb.CollectionRate{
			Collection: 1,
			Rates:      zeroRates,
		})
		assert.NoError(t, err)
		for _, rt := range internalpb.RateType_value {
			for i := 0; i < 100; i++ {
				ok, _ := limiter.limit(internalpb.RateType(rt), 1)
				assert.True(t, ok)
			}
		}

		err = limiter.setRates(&proxypb.CollectionRate{
			Collection: 1,
			States:     []milvuspb.QuotaState{milvuspb.QuotaState_DenyToRead, milvuspb.QuotaState_DenyToWrite},
			Codes:      []commonpb.ErrorCode{commonpb.ErrorCode_DiskQuotaExhausted, commonpb.ErrorCode_DiskQuotaExhausted},
		})
		assert.NoError(t, err)
		assert.Equal(t, limiter.quotaStates.Len(), 2)

		err = limiter.setRates(&proxypb.CollectionRate{
			Collection: 1,
			States:     []milvuspb.QuotaState{},
		})
		assert.NoError(t, err)
		assert.Equal(t, limiter.quotaStates.Len(), 0)
	})

	t.Run("test get error code", func(t *testing.T) {
		limiter := newRateLimiter(false)
		for _, rt := range internalpb.RateType_value {
			limiter.limiters.Insert(internalpb.RateType(rt), ratelimitutil.NewLimiter(ratelimitutil.Limit(1000), 1))
		}

		zeroRates := make([]*internalpb.Rate, 0, len(internalpb.RateType_value))
		for _, rt := range internalpb.RateType_value {
			zeroRates = append(zeroRates, &internalpb.Rate{
				Rt: internalpb.RateType(rt), R: 0,
			})
		}
		err := limiter.setRates(&proxypb.CollectionRate{
			Collection: 1,
			Rates:      zeroRates,
			States: []milvuspb.QuotaState{
				milvuspb.QuotaState_DenyToWrite,
				milvuspb.QuotaState_DenyToRead,
			},
			Codes: []commonpb.ErrorCode{
				commonpb.ErrorCode_DiskQuotaExhausted,
				commonpb.ErrorCode_ForceDeny,
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_ForceDeny, limiter.getErrorCode(internalpb.RateType_DQLQuery))
		assert.Equal(t, commonpb.ErrorCode_DiskQuotaExhausted, limiter.getErrorCode(internalpb.RateType_DMLInsert))
	})

	t.Run("tests refresh rate by config", func(t *testing.T) {
		limiter := newRateLimiter(false)

		etcdCli, _ := etcd.GetEtcdClient(
			Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
			Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
			Params.EtcdCfg.Endpoints.GetAsStrings(),
			Params.EtcdCfg.EtcdTLSCert.GetValue(),
			Params.EtcdCfg.EtcdTLSKey.GetValue(),
			Params.EtcdCfg.EtcdTLSCACert.GetValue(),
			Params.EtcdCfg.EtcdTLSMinVersion.GetValue())

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		// avoid production precision issues when comparing 0-terminated numbers
		newRate := fmt.Sprintf("%.3f1", rand.Float64())
		etcdCli.KV.Put(ctx, "by-dev/config/quotaAndLimits/ddl/collectionRate", newRate)
		etcdCli.KV.Put(ctx, "by-dev/config/quotaAndLimits/ddl/partitionRate", "invalid")

		assert.Eventually(t, func() bool {
			limit, _ := limiter.limiters.Get(internalpb.RateType_DDLCollection)
			return newRate == limit.Limit().String()
		}, 20*time.Second, time.Second)

		limit, _ := limiter.limiters.Get(internalpb.RateType_DDLPartition)
		assert.Equal(t, "+inf", limit.Limit().String())
	})
}
