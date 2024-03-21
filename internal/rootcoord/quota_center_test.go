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
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/ratelimitutil"
	"github.com/milvus-io/milvus/pkg/util/testutils"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func TestQuotaCenter(t *testing.T) {
	paramtable.Init()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	core, err := NewCore(ctx, nil)
	assert.NoError(t, err)
	core.tsoAllocator = newMockTsoAllocator()

	pcm := proxyutil.NewMockProxyClientManager(t)
	pcm.EXPECT().GetProxyMetrics(mock.Anything).Return(nil, nil).Maybe()

	dc := mocks.NewMockDataCoordClient(t)
	dc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(nil, nil).Maybe()

	collectionIDToPartitionIDs := map[int64][]int64{
		1: {},
		2: {},
		3: {},
	}

	collectionIDToDBID := typeutil.NewConcurrentMap[int64, int64]()
	collectionIDToDBID.Insert(1, 0)
	collectionIDToDBID.Insert(2, 0)
	collectionIDToDBID.Insert(3, 0)

	t.Run("test QuotaCenter", func(t *testing.T) {
		qc := mocks.NewMockQueryCoordClient(t)
		meta := mockrootcoord.NewIMetaTable(t)

		meta.EXPECT().GetCollectionByIDWithMaxTs(mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Maybe()
		quotaCenter := NewQuotaCenter(pcm, qc, dc, core.tsoAllocator, meta)
		quotaCenter.Start()
		time.Sleep(10 * time.Millisecond)
		quotaCenter.stop()
	})

	t.Run("test QuotaCenter stop", func(t *testing.T) {
		qc := mocks.NewMockQueryCoordClient(t)
		meta := mockrootcoord.NewIMetaTable(t)

		paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaCenterCollectInterval.Key, "1")
		defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaCenterCollectInterval.Key)

		qc.ExpectedCalls = nil
		// mock query coord stuck for  at most 10s
		qc.EXPECT().GetMetrics(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, gmr *milvuspb.GetMetricsRequest, co ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
			counter := 0
			for {
				select {
				case <-ctx.Done():
					return nil, merr.ErrCollectionNotFound
				default:
					if counter < 10 {
						time.Sleep(1 * time.Second)
						counter++
					}
				}
			}
		})

		meta.EXPECT().GetCollectionByID(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Maybe()
		quotaCenter := NewQuotaCenter(pcm, qc, dc, core.tsoAllocator, meta)
		quotaCenter.Start()
		time.Sleep(3 * time.Second)

		// assert stop won't stuck more than 5s
		start := time.Now()
		quotaCenter.stop()
		assert.True(t, time.Since(start).Seconds() <= 5)
	})

	t.Run("test collectMetrics", func(t *testing.T) {
		qc := mocks.NewMockQueryCoordClient(t)
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByIDWithMaxTs(mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Maybe()
		meta.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return([]*model.Database{
			{
				Name: "default",
				ID:   1,
			},
		}, nil).Maybe()

		qc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{Status: merr.Success()}, nil)
		quotaCenter := NewQuotaCenter(pcm, qc, dc, core.tsoAllocator, meta)
		err = quotaCenter.collectMetrics()
		assert.Error(t, err) // for empty response

		quotaCenter = NewQuotaCenter(pcm, qc, dc, core.tsoAllocator, meta)
		err = quotaCenter.collectMetrics()
		assert.Error(t, err)

		dc.ExpectedCalls = nil
		dc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{
			Status: merr.Status(errors.New("mock error")),
		}, nil)

		quotaCenter = NewQuotaCenter(pcm, qc, dc, core.tsoAllocator, meta)
		err = quotaCenter.collectMetrics()
		assert.Error(t, err)

		dc.ExpectedCalls = nil
		dc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(nil, errors.New("mock error"))
		qc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(nil, fmt.Errorf("mock err"))
		quotaCenter = NewQuotaCenter(pcm, qc, dc, core.tsoAllocator, meta)
		err = quotaCenter.collectMetrics()
		assert.Error(t, err)

		qc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{
			Status: merr.Status(err),
		}, nil)
		quotaCenter = NewQuotaCenter(pcm, qc, dc, core.tsoAllocator, meta)
		err = quotaCenter.collectMetrics()
		assert.Error(t, err)
	})

	t.Run("test force deny reading collection", func(t *testing.T) {
		qc := mocks.NewMockQueryCoordClient(t)
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByIDWithMaxTs(mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Maybe()
		quotaCenter := NewQuotaCenter(pcm, qc, dc, core.tsoAllocator, meta)

		quotaCenter.readableCollections = map[int64]map[int64][]int64{
			0: collectionIDToPartitionIDs,
		}
		err := quotaCenter.resetAllCurrentRates()
		assert.NoError(t, err)

		quotaCenter.forceDenyReading(commonpb.ErrorCode_ForceDeny)

		for collectionID := range collectionIDToPartitionIDs {
			collectionLimiters := quotaCenter.rateLimiter.GetCollectionLimiters(0, collectionID)
			assert.NotNil(t, collectionLimiters)

			limiters := collectionLimiters.GetLimiters()
			assert.NotNil(t, limiters)

			for _, rt := range []internalpb.RateType{
				internalpb.RateType_DQLSearch,
				internalpb.RateType_DQLQuery,
			} {
				ret, ok := limiters.Get(rt)
				assert.True(t, ok)
				assert.Equal(t, ret.Limit(), Limit(0))
			}
		}
	})

	t.Run("test force deny writing", func(t *testing.T) {
		qc := mocks.NewMockQueryCoordClient(t)
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().
			GetCollectionByIDWithMaxTs(mock.Anything, mock.Anything).
			Return(nil, merr.ErrCollectionNotFound).
			Maybe()

		quotaCenter := NewQuotaCenter(pcm, qc, dc, core.tsoAllocator, meta)
		quotaCenter.collectionIDToDBID = typeutil.NewConcurrentMap[int64, int64]()
		quotaCenter.collectionIDToDBID.Insert(1, 0)
		quotaCenter.collectionIDToDBID.Insert(2, 0)
		quotaCenter.collectionIDToDBID.Insert(3, 0)

		quotaCenter.writableCollections = map[int64]map[int64][]int64{
			0: collectionIDToPartitionIDs,
		}

		err := quotaCenter.resetAllCurrentRates()
		assert.NoError(t, err)

		err = quotaCenter.forceDenyWriting(commonpb.ErrorCode_ForceDeny, false, nil, []int64{4}, nil)
		assert.Error(t, err)

		err = quotaCenter.forceDenyWriting(commonpb.ErrorCode_ForceDeny, false, nil, []int64{1, 2, 3}, nil)
		assert.NoError(t, err)

		for collectionID := range collectionIDToPartitionIDs {
			collectionLimiters := quotaCenter.rateLimiter.GetCollectionLimiters(0, collectionID)
			assert.NotNil(t, collectionLimiters)

			limiters := collectionLimiters.GetLimiters()
			assert.NotNil(t, limiters)

			for _, rt := range []internalpb.RateType{
				internalpb.RateType_DMLInsert,
				internalpb.RateType_DMLUpsert,
				internalpb.RateType_DMLDelete,
				internalpb.RateType_DMLBulkLoad,
			} {
				ret, ok := limiters.Get(rt)
				assert.True(t, ok)
				assert.Equal(t, ret.Limit(), Limit(0))
			}
		}
	})

	t.Run("test calculateRates", func(t *testing.T) {
		forceBak := Params.QuotaConfig.ForceDenyWriting.GetValue()
		paramtable.Get().Save(Params.QuotaConfig.ForceDenyWriting.Key, "false")
		defer func() {
			paramtable.Get().Save(Params.QuotaConfig.ForceDenyWriting.Key, forceBak)
		}()

		qc := mocks.NewMockQueryCoordClient(t)
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByIDWithMaxTs(mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Maybe()
		quotaCenter := NewQuotaCenter(pcm, qc, dc, core.tsoAllocator, meta)
		quotaCenter.clearMetrics()
		err = quotaCenter.calculateRates()
		assert.NoError(t, err)
		alloc := newMockTsoAllocator()
		alloc.GenerateTSOF = func(count uint32) (typeutil.Timestamp, error) {
			return 0, fmt.Errorf("mock tso err")
		}
		quotaCenter.tsoAllocator = alloc
		quotaCenter.clearMetrics()
		err = quotaCenter.calculateRates()
		assert.Error(t, err)
	})

	t.Run("test getTimeTickDelayFactor factors", func(t *testing.T) {
		qc := mocks.NewMockQueryCoordClient(t)
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByIDWithMaxTs(mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Maybe()
		quotaCenter := NewQuotaCenter(pcm, qc, dc, core.tsoAllocator, meta)
		type ttCase struct {
			maxTtDelay     time.Duration
			curTt          time.Time
			fgTt           time.Time
			expectedFactor float64
		}
		t0 := time.Now()
		ttCases := []ttCase{
			{10 * time.Second, t0, t0.Add(1 * time.Second), 1},
			{10 * time.Second, t0, t0, 1},
			{10 * time.Second, t0.Add(1 * time.Second), t0, 0.9},
			{10 * time.Second, t0.Add(2 * time.Second), t0, 0.8},
			{10 * time.Second, t0.Add(5 * time.Second), t0, 0.5},
			{10 * time.Second, t0.Add(7 * time.Second), t0, 0.3},
			{10 * time.Second, t0.Add(9 * time.Second), t0, 0.1},
			{10 * time.Second, t0.Add(10 * time.Second), t0, 0},
			{10 * time.Second, t0.Add(100 * time.Second), t0, 0},
		}

		backup := Params.QuotaConfig.MaxTimeTickDelay.GetValue()

		for _, c := range ttCases {
			paramtable.Get().Save(Params.QuotaConfig.MaxTimeTickDelay.Key, fmt.Sprintf("%f", c.maxTtDelay.Seconds()))
			fgTs := tsoutil.ComposeTSByTime(c.fgTt, 0)
			quotaCenter.queryNodeMetrics = map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics{
				1: {
					Fgm: metricsinfo.FlowGraphMetric{
						NumFlowGraph:        1,
						MinFlowGraphTt:      fgTs,
						MinFlowGraphChannel: "dml",
					},
				},
			}
			curTs := tsoutil.ComposeTSByTime(c.curTt, 0)
			factors := quotaCenter.getTimeTickDelayFactor(curTs)
			for _, factor := range factors {
				assert.True(t, math.Abs(factor-c.expectedFactor) < 0.01)
			}
		}

		Params.Save(Params.QuotaConfig.MaxTimeTickDelay.Key, backup)
	})

	t.Run("test TimeTickDelayFactor factors", func(t *testing.T) {
		qc := mocks.NewMockQueryCoordClient(t)
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByIDWithMaxTs(mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Maybe()
		quotaCenter := NewQuotaCenter(pcm, qc, dc, core.tsoAllocator, meta)
		type ttCase struct {
			delay          time.Duration
			expectedFactor float64
		}
		ttCases := []ttCase{
			{0 * time.Second, 1},
			{1 * time.Second, 0.9},
			{2 * time.Second, 0.8},
			{5 * time.Second, 0.5},
			{7 * time.Second, 0.3},
			{9 * time.Second, 0.1},
			{10 * time.Second, 0},
			{100 * time.Second, 0},
		}

		backup := Params.QuotaConfig.MaxTimeTickDelay.GetValue()
		paramtable.Get().Save(Params.QuotaConfig.DMLLimitEnabled.Key, "true")
		paramtable.Get().Save(Params.QuotaConfig.TtProtectionEnabled.Key, "true")
		paramtable.Get().Save(Params.QuotaConfig.MaxTimeTickDelay.Key, "10.0")
		paramtable.Get().Save(Params.QuotaConfig.DMLMaxInsertRatePerCollection.Key, "100.0")
		paramtable.Get().Save(Params.QuotaConfig.DMLMinInsertRatePerCollection.Key, "0.0")
		paramtable.Get().Save(Params.QuotaConfig.DMLMaxUpsertRatePerCollection.Key, "100.0")
		paramtable.Get().Save(Params.QuotaConfig.DMLMinUpsertRatePerCollection.Key, "0.0")
		paramtable.Get().Save(Params.QuotaConfig.DMLMaxDeleteRatePerCollection.Key, "100.0")
		paramtable.Get().Save(Params.QuotaConfig.DMLMinDeleteRatePerCollection.Key, "0.0")
		forceBak := Params.QuotaConfig.ForceDenyWriting.GetValue()
		paramtable.Get().Save(Params.QuotaConfig.ForceDenyWriting.Key, "false")
		defer func() {
			paramtable.Get().Save(Params.QuotaConfig.ForceDenyWriting.Key, forceBak)
		}()

		alloc := newMockTsoAllocator()
		quotaCenter.tsoAllocator = alloc
		for _, c := range ttCases {
			minTS := tsoutil.ComposeTSByTime(time.Now(), 0)
			hackCurTs := tsoutil.ComposeTSByTime(time.Now().Add(c.delay), 0)
			alloc.GenerateTSOF = func(count uint32) (typeutil.Timestamp, error) {
				return hackCurTs, nil
			}
			quotaCenter.queryNodeMetrics = map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics{
				1: {
					Fgm: metricsinfo.FlowGraphMetric{
						NumFlowGraph:        1,
						MinFlowGraphTt:      minTS,
						MinFlowGraphChannel: "dml",
					},
					Effect: metricsinfo.NodeEffect{
						CollectionIDs: []int64{1, 2, 3},
					},
				},
			}
			quotaCenter.dataNodeMetrics = map[UniqueID]*metricsinfo.DataNodeQuotaMetrics{
				11: {
					Fgm: metricsinfo.FlowGraphMetric{
						NumFlowGraph:        1,
						MinFlowGraphTt:      minTS,
						MinFlowGraphChannel: "dml",
					},
					Effect: metricsinfo.NodeEffect{
						CollectionIDs: []int64{1, 2, 3},
					},
				},
			}
			quotaCenter.writableCollections = map[int64]map[int64][]int64{
				0: collectionIDToPartitionIDs,
			}
			quotaCenter.collectionIDToDBID = collectionIDToDBID
			err = quotaCenter.resetAllCurrentRates()
			assert.NoError(t, err)

			err = quotaCenter.calculateWriteRates()
			assert.NoError(t, err)

			limit, ok := quotaCenter.rateLimiter.GetCollectionLimiters(0, 1).GetLimiters().Get(internalpb.RateType_DMLDelete)
			assert.True(t, ok)
			assert.NotNil(t, limit)

			deleteFactor := float64(limit.Limit()) / Params.QuotaConfig.DMLMaxInsertRatePerCollection.GetAsFloat()
			assert.True(t, math.Abs(deleteFactor-c.expectedFactor) < 0.01)
		}
		Params.Save(Params.QuotaConfig.MaxTimeTickDelay.Key, backup)
	})

	t.Run("test calculateReadRates", func(t *testing.T) {
		qc := mocks.NewMockQueryCoordClient(t)
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByIDWithMaxTs(mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Maybe()
		meta.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return([]*model.Database{
			{
				ID:   0,
				Name: "default",
			},
		}, nil).Maybe()
		quotaCenter := NewQuotaCenter(pcm, qc, dc, core.tsoAllocator, meta)
		quotaCenter.clearMetrics()
		quotaCenter.collectionIDToDBID = collectionIDToDBID
		quotaCenter.readableCollections = map[int64]map[int64][]int64{
			0: collectionIDToPartitionIDs,
		}
		quotaCenter.dbs.Insert("default", 0)
		quotaCenter.collections.Insert("col1", 1)
		quotaCenter.collections.Insert("col2", 2)
		quotaCenter.collections.Insert("col3", 3)
		colSubLabel := ratelimitutil.GetCollectionSubLabel("default", "col1")
		quotaCenter.proxyMetrics = map[UniqueID]*metricsinfo.ProxyQuotaMetrics{
			1: {Rms: []metricsinfo.RateMetric{
				{Label: internalpb.RateType_DQLSearch.String(), Rate: 100},
				{Label: internalpb.RateType_DQLQuery.String(), Rate: 100},
				{Label: ratelimitutil.FormatSubLabel(internalpb.RateType_DQLSearch.String(), colSubLabel), Rate: 100},
				{Label: ratelimitutil.FormatSubLabel(internalpb.RateType_DQLQuery.String(), colSubLabel), Rate: 100},
			}},
		}

		paramtable.Get().Save(Params.QuotaConfig.ForceDenyReading.Key, "false")
		paramtable.Get().Save(Params.QuotaConfig.QueueProtectionEnabled.Key, "true")
		paramtable.Get().Save(Params.QuotaConfig.QueueLatencyThreshold.Key, "100")
		paramtable.Get().Save(Params.QuotaConfig.DQLLimitEnabled.Key, "true")
		paramtable.Get().Save(Params.QuotaConfig.DQLMaxQueryRatePerCollection.Key, "500")
		paramtable.Get().Save(Params.QuotaConfig.DQLMaxSearchRatePerCollection.Key, "500")

		checkLimiter := func() {
			for db, collections := range quotaCenter.readableCollections {
				for collection := range collections {
					if collection != 1 {
						continue
					}
					limiters := quotaCenter.rateLimiter.GetCollectionLimiters(db, collection).GetLimiters()
					searchLimit, _ := limiters.Get(internalpb.RateType_DQLSearch)
					assert.Equal(t, Limit(100.0*0.9), searchLimit.Limit())

					queryLimit, _ := limiters.Get(internalpb.RateType_DQLQuery)
					assert.Equal(t, Limit(100.0*0.9), queryLimit.Limit())
				}
			}
		}

		err := quotaCenter.resetAllCurrentRates()
		assert.NoError(t, err)

		quotaCenter.queryNodeMetrics = map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics{
			1: {SearchQueue: metricsinfo.ReadInfoInQueue{
				AvgQueueDuration: Params.QuotaConfig.QueueLatencyThreshold.GetAsDuration(time.Second),
			}, Effect: metricsinfo.NodeEffect{
				NodeID:        1,
				CollectionIDs: []int64{1, 2, 3},
			}},
		}

		err = quotaCenter.calculateReadRates()
		assert.NoError(t, err)
		checkLimiter()

		paramtable.Get().Save(Params.QuotaConfig.NQInQueueThreshold.Key, "100")
		quotaCenter.queryNodeMetrics = map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics{
			1: {
				SearchQueue: metricsinfo.ReadInfoInQueue{
					UnsolvedQueue: Params.QuotaConfig.NQInQueueThreshold.GetAsInt64(),
				},
			},
		}
		err = quotaCenter.calculateReadRates()
		assert.NoError(t, err)
		checkLimiter()

		paramtable.Get().Save(Params.QuotaConfig.ResultProtectionEnabled.Key, "true")
		paramtable.Get().Save(Params.QuotaConfig.MaxReadResultRate.Key, "1")
		quotaCenter.proxyMetrics = map[UniqueID]*metricsinfo.ProxyQuotaMetrics{
			1: {
				Rms: []metricsinfo.RateMetric{
					{Label: internalpb.RateType_DQLSearch.String(), Rate: 100},
					{Label: internalpb.RateType_DQLQuery.String(), Rate: 100},
					{Label: ratelimitutil.FormatSubLabel(internalpb.RateType_DQLSearch.String(), colSubLabel), Rate: 100},
					{Label: ratelimitutil.FormatSubLabel(internalpb.RateType_DQLQuery.String(), colSubLabel), Rate: 100},
					{Label: metricsinfo.ReadResultThroughput, Rate: 1.2},
				}},
		}
		quotaCenter.queryNodeMetrics = map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics{1: {SearchQueue: metricsinfo.ReadInfoInQueue{}}}
		err = quotaCenter.calculateReadRates()
		assert.NoError(t, err)
		checkLimiter()
	})

	t.Run("test calculateWriteRates", func(t *testing.T) {
		qc := mocks.NewMockQueryCoordClient(t)
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByIDWithMaxTs(mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Maybe()
		quotaCenter := NewQuotaCenter(pcm, qc, dc, core.tsoAllocator, meta)
		err = quotaCenter.calculateWriteRates()
		assert.NoError(t, err)

		// force deny
		paramtable.Get().Save(Params.QuotaConfig.ForceDenyWriting.Key, "true")
		quotaCenter.writableCollections = map[int64]map[int64][]int64{
			0: collectionIDToPartitionIDs,
		}
		quotaCenter.collectionIDToDBID = collectionIDToDBID
		quotaCenter.collectionIDToDBID = collectionIDToDBID
		quotaCenter.resetAllCurrentRates()
		err = quotaCenter.calculateWriteRates()
		assert.NoError(t, err)
		limiters := quotaCenter.rateLimiter.GetRootLimiters().GetLimiters()
		a, _ := limiters.Get(internalpb.RateType_DMLInsert)
		assert.Equal(t, Limit(0), a.Limit())
		b, _ := limiters.Get(internalpb.RateType_DMLUpsert)
		assert.Equal(t, Limit(0), b.Limit())
		c, _ := limiters.Get(internalpb.RateType_DMLDelete)
		assert.Equal(t, Limit(0), c.Limit())

		paramtable.Get().Reset(Params.QuotaConfig.ForceDenyWriting.Key)

		// disable tt delay protection
		disableTtBak := Params.QuotaConfig.TtProtectionEnabled.GetValue()
		paramtable.Get().Save(Params.QuotaConfig.TtProtectionEnabled.Key, "false")
		quotaCenter.resetAllCurrentRates()
		quotaCenter.queryNodeMetrics = make(map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics)
		quotaCenter.queryNodeMetrics[0] = &metricsinfo.QueryNodeQuotaMetrics{
			Hms: metricsinfo.HardwareMetrics{
				Memory:      100,
				MemoryUsage: 100,
			},
			Effect: metricsinfo.NodeEffect{CollectionIDs: []int64{1, 2, 3}},
		}
		err = quotaCenter.calculateWriteRates()
		assert.NoError(t, err)
		for db, collections := range quotaCenter.writableCollections {
			for collection := range collections {
				states := quotaCenter.rateLimiter.GetCollectionLimiters(db, collection).GetQuotaStates()
				code, _ := states.Get(milvuspb.QuotaState_DenyToWrite)
				assert.Equal(t, commonpb.ErrorCode_MemoryQuotaExhausted, code)
			}
		}
		paramtable.Get().Save(Params.QuotaConfig.TtProtectionEnabled.Key, disableTtBak)
	})

	t.Run("test MemoryFactor factors", func(t *testing.T) {
		qc := mocks.NewMockQueryCoordClient(t)
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByIDWithMaxTs(mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Maybe()
		quotaCenter := NewQuotaCenter(pcm, qc, dc, core.tsoAllocator, meta)
		type memCase struct {
			lowWater       float64
			highWater      float64
			memUsage       uint64
			memTotal       uint64
			expectedFactor float64
		}
		memCases := []memCase{
			{0.8, 0.9, 10, 100, 1},
			{0.8, 0.9, 80, 100, 1},
			{0.8, 0.9, 82, 100, 0.8},
			{0.8, 0.9, 85, 100, 0.5},
			{0.8, 0.9, 88, 100, 0.2},
			{0.8, 0.9, 90, 100, 0},

			{0.85, 0.95, 25, 100, 1},
			{0.85, 0.95, 85, 100, 1},
			{0.85, 0.95, 87, 100, 0.8},
			{0.85, 0.95, 90, 100, 0.5},
			{0.85, 0.95, 93, 100, 0.2},
			{0.85, 0.95, 95, 100, 0},
		}

		quotaCenter.writableCollections = map[int64]map[int64][]int64{
			0: collectionIDToPartitionIDs,
		}
		for _, c := range memCases {
			paramtable.Get().Save(Params.QuotaConfig.QueryNodeMemoryLowWaterLevel.Key, fmt.Sprintf("%f", c.lowWater))
			paramtable.Get().Save(Params.QuotaConfig.QueryNodeMemoryHighWaterLevel.Key, fmt.Sprintf("%f", c.highWater))
			quotaCenter.queryNodeMetrics = map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics{
				1: {
					Hms: metricsinfo.HardwareMetrics{
						MemoryUsage: c.memUsage,
						Memory:      c.memTotal,
					},
					Effect: metricsinfo.NodeEffect{
						NodeID:        1,
						CollectionIDs: []int64{1, 2, 3},
					},
				},
			}
			factors := quotaCenter.getMemoryFactor()

			for _, factor := range factors {
				assert.True(t, math.Abs(factor-c.expectedFactor) < 0.01)
			}
		}

		paramtable.Get().Reset(Params.QuotaConfig.QueryNodeMemoryLowWaterLevel.Key)
		paramtable.Get().Reset(Params.QuotaConfig.QueryNodeMemoryHighWaterLevel.Key)
	})

	t.Run("test GrowingSegmentsSize factors", func(t *testing.T) {
		qc := mocks.NewMockQueryCoordClient(t)
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByIDWithMaxTs(mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Maybe()
		quotaCenter := NewQuotaCenter(pcm, qc, dc, core.tsoAllocator, meta)
		defaultRatio := Params.QuotaConfig.GrowingSegmentsSizeMinRateRatio.GetAsFloat()
		tests := []struct {
			low            float64
			high           float64
			growingSize    int64
			memTotal       uint64
			expectedFactor float64
		}{
			{0.8, 0.9, 10, 100, 1},
			{0.8, 0.9, 80, 100, 1},
			{0.8, 0.9, 82, 100, 0.8},
			{0.8, 0.9, 85, 100, 0.5},
			{0.8, 0.9, 88, 100, defaultRatio},
			{0.8, 0.9, 90, 100, defaultRatio},

			{0.85, 0.95, 25, 100, 1},
			{0.85, 0.95, 85, 100, 1},
			{0.85, 0.95, 87, 100, 0.8},
			{0.85, 0.95, 90, 100, 0.5},
			{0.85, 0.95, 93, 100, defaultRatio},
			{0.85, 0.95, 95, 100, defaultRatio},
		}

		quotaCenter.writableCollections = map[int64]map[int64][]int64{
			0: collectionIDToPartitionIDs,
		}
		paramtable.Get().Save(Params.QuotaConfig.GrowingSegmentsSizeProtectionEnabled.Key, "true")
		for _, test := range tests {
			paramtable.Get().Save(Params.QuotaConfig.GrowingSegmentsSizeLowWaterLevel.Key, fmt.Sprintf("%f", test.low))
			paramtable.Get().Save(Params.QuotaConfig.GrowingSegmentsSizeHighWaterLevel.Key, fmt.Sprintf("%f", test.high))
			quotaCenter.queryNodeMetrics = map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics{
				1: {
					Hms: metricsinfo.HardwareMetrics{
						Memory: test.memTotal,
					},
					Effect: metricsinfo.NodeEffect{
						NodeID:        1,
						CollectionIDs: []int64{1, 2, 3},
					},
					GrowingSegmentsSize: test.growingSize,
				},
			}
			factors := quotaCenter.getGrowingSegmentsSizeFactor()

			for _, factor := range factors {
				assert.True(t, math.Abs(factor-test.expectedFactor) < 0.01)
			}
		}
		paramtable.Get().Reset(Params.QuotaConfig.GrowingSegmentsSizeLowWaterLevel.Key)
		paramtable.Get().Reset(Params.QuotaConfig.GrowingSegmentsSizeHighWaterLevel.Key)
	})

	t.Run("test checkDiskQuota", func(t *testing.T) {
		qc := mocks.NewMockQueryCoordClient(t)
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByIDWithMaxTs(mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Maybe()
		quotaCenter := NewQuotaCenter(pcm, qc, dc, core.tsoAllocator, meta)
		quotaCenter.checkDiskQuota()

		checkLimiter := func(notEquals ...int64) {
			for db, collections := range quotaCenter.writableCollections {
				for collection := range collections {
					limiters := quotaCenter.rateLimiter.GetCollectionLimiters(db, collection).GetLimiters()
					if lo.Contains(notEquals, collection) {
						a, _ := limiters.Get(internalpb.RateType_DMLInsert)
						assert.NotEqual(t, Limit(0), a.Limit())
						b, _ := limiters.Get(internalpb.RateType_DMLUpsert)
						assert.NotEqual(t, Limit(0), b.Limit())
						c, _ := limiters.Get(internalpb.RateType_DMLDelete)
						assert.NotEqual(t, Limit(0), c.Limit())
					} else {
						a, _ := limiters.Get(internalpb.RateType_DMLInsert)
						assert.Equal(t, Limit(0), a.Limit())
						b, _ := limiters.Get(internalpb.RateType_DMLUpsert)
						assert.Equal(t, Limit(0), b.Limit())
						c, _ := limiters.Get(internalpb.RateType_DMLDelete)
						assert.Equal(t, Limit(0), c.Limit())
					}
				}
			}
		}

		// total DiskQuota exceeded
		paramtable.Get().Save(Params.QuotaConfig.DiskQuota.Key, "99")
		paramtable.Get().Save(Params.QuotaConfig.DiskQuotaPerCollection.Key, "90")
		quotaCenter.dataCoordMetrics = &metricsinfo.DataCoordQuotaMetrics{
			TotalBinlogSize: 300 * 1024 * 1024,
			CollectionBinlogSize: map[int64]int64{
				1: 100 * 1024 * 1024,
				2: 100 * 1024 * 1024,
				3: 100 * 1024 * 1024,
			},
		}
		quotaCenter.writableCollections = map[int64]map[int64][]int64{
			0: collectionIDToPartitionIDs,
		}
		quotaCenter.collectionIDToDBID = collectionIDToDBID
		quotaCenter.resetAllCurrentRates()
		quotaCenter.checkDiskQuota()
		checkLimiter()
		paramtable.Get().Reset(Params.QuotaConfig.DiskQuota.Key)
		paramtable.Get().Reset(Params.QuotaConfig.DiskQuotaPerCollection.Key)

		// collection DiskQuota exceeded
		colQuotaBackup := Params.QuotaConfig.DiskQuotaPerCollection.GetValue()
		paramtable.Get().Save(Params.QuotaConfig.DiskQuotaPerCollection.Key, "30")
		quotaCenter.dataCoordMetrics = &metricsinfo.DataCoordQuotaMetrics{CollectionBinlogSize: map[int64]int64{
			1: 20 * 1024 * 1024, 2: 30 * 1024 * 1024, 3: 60 * 1024 * 1024,
		}}
		quotaCenter.writableCollections = map[int64]map[int64][]int64{
			0: collectionIDToPartitionIDs,
		}
		quotaCenter.resetAllCurrentRates()
		quotaCenter.checkDiskQuota()
		checkLimiter(1)
		paramtable.Get().Save(Params.QuotaConfig.DiskQuotaPerCollection.Key, colQuotaBackup)
	})

	t.Run("test setRates", func(t *testing.T) {
		qc := mocks.NewMockQueryCoordClient(t)
		pcm.EXPECT().GetProxyCount().Return(1)
		pcm.EXPECT().SetRates(mock.Anything, mock.Anything).Return(nil)
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByIDWithMaxTs(mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Maybe()
		quotaCenter := NewQuotaCenter(pcm, qc, dc, core.tsoAllocator, meta)
		quotaCenter.writableCollections = map[int64]map[int64][]int64{
			0: collectionIDToPartitionIDs,
		}
		quotaCenter.readableCollections = map[int64]map[int64][]int64{
			0: collectionIDToPartitionIDs,
		}
		quotaCenter.resetAllCurrentRates()
		collectionID := int64(1)
		limitNode := quotaCenter.rateLimiter.GetCollectionLimiters(0, collectionID)
		limitNode.GetLimiters().Insert(internalpb.RateType_DMLInsert, ratelimitutil.NewLimiter(100, 100))
		limitNode.GetQuotaStates().Insert(milvuspb.QuotaState_DenyToWrite, commonpb.ErrorCode_MemoryQuotaExhausted)
		limitNode.GetQuotaStates().Insert(milvuspb.QuotaState_DenyToRead, commonpb.ErrorCode_ForceDeny)
		err = quotaCenter.sendRatesToProxy()
		assert.NoError(t, err)
	})

	t.Run("test recordMetrics", func(t *testing.T) {
		qc := mocks.NewMockQueryCoordClient(t)
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByIDWithMaxTs(mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Maybe()
		quotaCenter := NewQuotaCenter(pcm, qc, dc, core.tsoAllocator, meta)
		quotaCenter.writableCollections = map[int64]map[int64][]int64{
			0: collectionIDToPartitionIDs,
		}
		quotaCenter.readableCollections = map[int64]map[int64][]int64{
			0: collectionIDToPartitionIDs,
		}
		quotaCenter.resetAllCurrentRates()
		collectionID := int64(1)
		limitNode := quotaCenter.rateLimiter.GetCollectionLimiters(0, collectionID)
		limitNode.GetQuotaStates().Insert(milvuspb.QuotaState_DenyToWrite, commonpb.ErrorCode_MemoryQuotaExhausted)
		limitNode.GetQuotaStates().Insert(milvuspb.QuotaState_DenyToRead, commonpb.ErrorCode_ForceDeny)
		quotaCenter.recordMetrics()
	})

	t.Run("test guaranteeMinRate", func(t *testing.T) {
		qc := mocks.NewMockQueryCoordClient(t)
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByIDWithMaxTs(mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Maybe()
		quotaCenter := NewQuotaCenter(pcm, qc, dc, core.tsoAllocator, meta)
		quotaCenter.readableCollections = map[int64]map[int64][]int64{
			0: collectionIDToPartitionIDs,
		}
		quotaCenter.resetAllCurrentRates()
		minRate := Limit(100)
		collectionID := int64(1)
		limitNode := quotaCenter.rateLimiter.GetCollectionLimiters(0, collectionID)
		limitNode.GetLimiters().Insert(internalpb.RateType_DQLSearch, ratelimitutil.NewLimiter(50, 50))
		quotaCenter.guaranteeMinRate(float64(minRate), internalpb.RateType_DQLSearch, limitNode)
		limiter, _ := limitNode.GetLimiters().Get(internalpb.RateType_DQLSearch)
		assert.EqualValues(t, minRate, limiter.Limit())
	})

	t.Run("test diskAllowance", func(t *testing.T) {
		tests := []struct {
			name            string
			totalDiskQuota  string
			collDiskQuota   string
			totalDiskUsage  int64   // in MB
			collDiskUsage   int64   // in MB
			expectAllowance float64 // in bytes
		}{
			{"test max", "-1", "-1", 100, 100, math.MaxFloat64},
			{"test total quota exceeded", "100", "-1", 100, 100, 0},
			{"test coll quota exceeded", "-1", "20", 100, 20, 0},
			{"test not exceeded", "100", "20", 80, 10, 10 * 1024 * 1024},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				collection := UniqueID(0)
				meta := mockrootcoord.NewIMetaTable(t)
				meta.EXPECT().GetCollectionByIDWithMaxTs(mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Maybe()
				quotaCenter := NewQuotaCenter(pcm, nil, dc, core.tsoAllocator, meta)
				quotaCenter.resetAllCurrentRates()
				quotaBackup := Params.QuotaConfig.DiskQuota.GetValue()
				colQuotaBackup := Params.QuotaConfig.DiskQuotaPerCollection.GetValue()
				paramtable.Get().Save(Params.QuotaConfig.DiskQuota.Key, test.totalDiskQuota)
				paramtable.Get().Save(Params.QuotaConfig.DiskQuotaPerCollection.Key, test.collDiskQuota)
				quotaCenter.diskMu.Lock()
				quotaCenter.dataCoordMetrics = &metricsinfo.DataCoordQuotaMetrics{}
				quotaCenter.dataCoordMetrics.CollectionBinlogSize = map[int64]int64{collection: test.collDiskUsage * 1024 * 1024}
				quotaCenter.totalBinlogSize = test.totalDiskUsage * 1024 * 1024
				quotaCenter.diskMu.Unlock()
				allowance := quotaCenter.diskAllowance(collection)
				assert.Equal(t, test.expectAllowance, allowance)
				paramtable.Get().Save(Params.QuotaConfig.DiskQuota.Key, quotaBackup)
				paramtable.Get().Save(Params.QuotaConfig.DiskQuotaPerCollection.Key, colQuotaBackup)
			})
		}
	})

	t.Run("test reset current rates", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByIDWithMaxTs(mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Maybe()
		quotaCenter := NewQuotaCenter(pcm, nil, dc, core.tsoAllocator, meta)
		quotaCenter.readableCollections = map[int64]map[int64][]int64{
			0: {1: {}},
		}
		quotaCenter.writableCollections = map[int64]map[int64][]int64{
			0: {1: {}},
		}
		quotaCenter.collectionIDToDBID = collectionIDToDBID
		quotaCenter.resetAllCurrentRates()

		limiters := quotaCenter.rateLimiter.GetCollectionLimiters(0, 1).GetLimiters()

		getRate := func(m *typeutil.ConcurrentMap[internalpb.RateType, *ratelimitutil.Limiter], key internalpb.RateType) float64 {
			v, _ := m.Get(key)
			return float64(v.Limit())
		}

		assert.Equal(t, getRate(limiters, internalpb.RateType_DMLInsert), Params.QuotaConfig.DMLMaxInsertRatePerCollection.GetAsFloat())
		assert.Equal(t, getRate(limiters, internalpb.RateType_DMLUpsert), Params.QuotaConfig.DMLMaxUpsertRatePerCollection.GetAsFloat())
		assert.Equal(t, getRate(limiters, internalpb.RateType_DMLDelete), Params.QuotaConfig.DMLMaxDeleteRatePerCollection.GetAsFloat())
		assert.Equal(t, getRate(limiters, internalpb.RateType_DMLBulkLoad), Params.QuotaConfig.DMLMaxBulkLoadRatePerCollection.GetAsFloat())
		assert.Equal(t, getRate(limiters, internalpb.RateType_DQLSearch), Params.QuotaConfig.DQLMaxSearchRatePerCollection.GetAsFloat())
		assert.Equal(t, getRate(limiters, internalpb.RateType_DQLQuery), Params.QuotaConfig.DQLMaxQueryRatePerCollection.GetAsFloat())

		meta.ExpectedCalls = nil
		meta.EXPECT().GetCollectionByIDWithMaxTs(mock.Anything, mock.Anything).Return(&model.Collection{
			Properties: []*commonpb.KeyValuePair{
				{
					Key:   common.CollectionInsertRateMaxKey,
					Value: "1",
				},

				{
					Key:   common.CollectionDeleteRateMaxKey,
					Value: "2",
				},

				{
					Key:   common.CollectionBulkLoadRateMaxKey,
					Value: "3",
				},

				{
					Key:   common.CollectionQueryRateMaxKey,
					Value: "4",
				},

				{
					Key:   common.CollectionSearchRateMaxKey,
					Value: "5",
				},
				{
					Key:   common.CollectionUpsertRateMaxKey,
					Value: "6",
				},
			},
		}, nil)
		quotaCenter.resetAllCurrentRates()
		limiters = quotaCenter.rateLimiter.GetCollectionLimiters(0, 1).GetLimiters()
		assert.Equal(t, getRate(limiters, internalpb.RateType_DMLInsert), float64(1*1024*1024))
		assert.Equal(t, getRate(limiters, internalpb.RateType_DMLDelete), float64(2*1024*1024))
		assert.Equal(t, getRate(limiters, internalpb.RateType_DMLBulkLoad), float64(3*1024*1024))
		assert.Equal(t, getRate(limiters, internalpb.RateType_DQLQuery), float64(4))
		assert.Equal(t, getRate(limiters, internalpb.RateType_DQLSearch), float64(5))
		assert.Equal(t, getRate(limiters, internalpb.RateType_DMLUpsert), float64(6*1024*1024))
	})
}

type QuotaCenterSuite struct {
	testutils.PromMetricsSuite

	core *Core

	pcm  *proxyutil.MockProxyClientManager
	dc   *mocks.MockDataCoordClient
	qc   *mocks.MockQueryCoordClient
	meta *mockrootcoord.IMetaTable
}

func (s *QuotaCenterSuite) SetupSuite() {
	paramtable.Init()

	var err error
	s.core, err = NewCore(context.Background(), nil)

	s.Require().NoError(err)
}

func (s *QuotaCenterSuite) SetupTest() {
	s.pcm = proxyutil.NewMockProxyClientManager(s.T())
	s.dc = mocks.NewMockDataCoordClient(s.T())
	s.qc = mocks.NewMockQueryCoordClient(s.T())
	s.meta = mockrootcoord.NewIMetaTable(s.T())
}

func (s *QuotaCenterSuite) getEmptyQCMetricsRsp() string {
	metrics := &metricsinfo.QueryCoordTopology{
		Cluster: metricsinfo.QueryClusterTopology{},
	}

	resp, err := metricsinfo.MarshalTopology(metrics)
	s.Require().NoError(err)
	return resp
}

func (s *QuotaCenterSuite) getEmptyDCMetricsRsp() string {
	metrics := &metricsinfo.DataCoordTopology{
		Cluster: metricsinfo.DataClusterTopology{},
	}

	resp, err := metricsinfo.MarshalTopology(metrics)
	s.Require().NoError(err)
	return resp
}

func (s *QuotaCenterSuite) TestSyncMetricsSuccess() {
	pcm := s.pcm
	dc := s.dc
	qc := s.qc
	meta := s.meta
	core := s.core

	call := meta.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return([]*model.Database{
		{
			ID:   1,
			Name: "default",
		},
	}, nil)
	defer call.Unset()

	s.Run("querycoord_cluster", func() {
		pcm.EXPECT().GetProxyMetrics(mock.Anything).Return(nil, nil).Once()
		dc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{
			Status:   merr.Status(nil),
			Response: s.getEmptyDCMetricsRsp(),
		}, nil).Once()

		metrics := &metricsinfo.QueryCoordTopology{
			Cluster: metricsinfo.QueryClusterTopology{
				ConnectedNodes: []metricsinfo.QueryNodeInfos{
					{BaseComponentInfos: metricsinfo.BaseComponentInfos{ID: 1}, QuotaMetrics: &metricsinfo.QueryNodeQuotaMetrics{Effect: metricsinfo.NodeEffect{NodeID: 1, CollectionIDs: []int64{100, 200}}}},
					{BaseComponentInfos: metricsinfo.BaseComponentInfos{ID: 2}, QuotaMetrics: &metricsinfo.QueryNodeQuotaMetrics{Effect: metricsinfo.NodeEffect{NodeID: 2, CollectionIDs: []int64{200, 300}}}},
				},
			},
		}

		resp, err := metricsinfo.MarshalTopology(metrics)
		s.Require().NoError(err)

		qc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{
			Status:   merr.Status(nil),
			Response: resp,
		}, nil).Once()
		meta.EXPECT().GetCollectionByIDWithMaxTs(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, i int64) (*model.Collection, error) {
			return &model.Collection{CollectionID: i, DBID: 1}, nil
		}).Times(3)

		quotaCenter := NewQuotaCenter(pcm, qc, dc, core.tsoAllocator, meta)

		err = quotaCenter.collectMetrics()
		s.Require().NoError(err)

		s.ElementsMatch([]int64{100, 200, 300}, lo.Keys(quotaCenter.readableCollections[1]))
		nodes := lo.Keys(quotaCenter.queryNodeMetrics)
		s.ElementsMatch([]int64{1, 2}, nodes)
	})

	s.Run("datacoord_cluster", func() {
		pcm.EXPECT().GetProxyMetrics(mock.Anything).Return(nil, nil).Once()
		qc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{
			Status:   merr.Status(nil),
			Response: s.getEmptyQCMetricsRsp(),
		}, nil).Once()

		metrics := &metricsinfo.DataCoordTopology{
			Cluster: metricsinfo.DataClusterTopology{
				ConnectedDataNodes: []metricsinfo.DataNodeInfos{
					{BaseComponentInfos: metricsinfo.BaseComponentInfos{ID: 1}, QuotaMetrics: &metricsinfo.DataNodeQuotaMetrics{Effect: metricsinfo.NodeEffect{NodeID: 1, CollectionIDs: []int64{100, 200}}}},
					{BaseComponentInfos: metricsinfo.BaseComponentInfos{ID: 2}, QuotaMetrics: &metricsinfo.DataNodeQuotaMetrics{Effect: metricsinfo.NodeEffect{NodeID: 2, CollectionIDs: []int64{200, 300}}}},
				},
			},
		}

		resp, err := metricsinfo.MarshalTopology(metrics)
		s.Require().NoError(err)

		dc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{
			Status:   merr.Status(nil),
			Response: resp,
		}, nil).Once()
		meta.EXPECT().GetCollectionByIDWithMaxTs(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, i int64) (*model.Collection, error) {
			return &model.Collection{CollectionID: i, DBID: 1}, nil
		}).Times(3)

		quotaCenter := NewQuotaCenter(pcm, qc, dc, core.tsoAllocator, meta)

		err = quotaCenter.collectMetrics()
		s.Require().NoError(err)

		s.ElementsMatch([]int64{100, 200, 300}, lo.Keys(quotaCenter.writableCollections[1]))
		nodes := lo.Keys(quotaCenter.dataNodeMetrics)
		s.ElementsMatch([]int64{1, 2}, nodes)
	})
}

func (s *QuotaCenterSuite) TestSyncMetricsFailure() {
	pcm := s.pcm
	dc := s.dc
	qc := s.qc
	meta := s.meta
	core := s.core
	call := meta.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return([]*model.Database{
		{
			ID:   1,
			Name: "default",
		},
	}, nil)
	defer call.Unset()

	s.Run("querycoord_failure", func() {
		pcm.EXPECT().GetProxyMetrics(mock.Anything).Return(nil, nil).Once()
		dc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{
			Status:   merr.Status(nil),
			Response: s.getEmptyDCMetricsRsp(),
		}, nil).Once()
		qc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(nil, errors.New("mock")).Once()

		quotaCenter := NewQuotaCenter(pcm, qc, dc, core.tsoAllocator, meta)

		err := quotaCenter.collectMetrics()
		s.Error(err)
	})

	s.Run("querycoord_bad_response", func() {
		pcm.EXPECT().GetProxyMetrics(mock.Anything).Return(nil, nil).Once()
		dc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{
			Status:   merr.Status(nil),
			Response: s.getEmptyDCMetricsRsp(),
		}, nil).Once()

		qc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{
			Status:   merr.Status(nil),
			Response: "abc",
		}, nil).Once()

		quotaCenter := NewQuotaCenter(pcm, qc, dc, core.tsoAllocator, meta)

		err := quotaCenter.collectMetrics()
		s.Error(err)
	})

	s.Run("datacoord_failure", func() {
		pcm.EXPECT().GetProxyMetrics(mock.Anything).Return(nil, nil).Once()
		qc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{
			Status:   merr.Status(nil),
			Response: s.getEmptyQCMetricsRsp(),
		}, nil).Once()

		dc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(nil, errors.New("mocked")).Once()

		quotaCenter := NewQuotaCenter(pcm, qc, dc, core.tsoAllocator, meta)
		err := quotaCenter.collectMetrics()
		s.Error(err)
	})

	s.Run("datacoord_bad_response", func() {
		pcm.EXPECT().GetProxyMetrics(mock.Anything).Return(nil, nil).Once()
		qc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{
			Status:   merr.Status(nil),
			Response: s.getEmptyQCMetricsRsp(),
		}, nil).Once()

		dc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{
			Status:   merr.Status(nil),
			Response: "abc",
		}, nil).Once()

		quotaCenter := NewQuotaCenter(pcm, qc, dc, core.tsoAllocator, meta)
		err := quotaCenter.collectMetrics()
		s.Error(err)
	})

	s.Run("proxy_manager_return_failure", func() {
		qc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{
			Status:   merr.Status(nil),
			Response: s.getEmptyQCMetricsRsp(),
		}, nil).Once()
		dc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{
			Status:   merr.Status(nil),
			Response: s.getEmptyDCMetricsRsp(),
		}, nil).Once()

		pcm.EXPECT().GetProxyMetrics(mock.Anything).Return(nil, errors.New("mocked")).Once()

		quotaCenter := NewQuotaCenter(pcm, qc, dc, core.tsoAllocator, meta)
		err := quotaCenter.collectMetrics()
		s.Error(err)
	})

	s.Run("proxy_manager_bad_response", func() {
		qc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{
			Status:   merr.Status(nil),
			Response: s.getEmptyQCMetricsRsp(),
		}, nil).Once()
		dc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{
			Status:   merr.Status(nil),
			Response: s.getEmptyDCMetricsRsp(),
		}, nil).Once()

		pcm.EXPECT().GetProxyMetrics(mock.Anything).Return([]*milvuspb.GetMetricsResponse{
			{
				Status:   merr.Status(nil),
				Response: "abc",
			},
		}, nil).Once()

		quotaCenter := NewQuotaCenter(pcm, qc, dc, core.tsoAllocator, meta)
		err := quotaCenter.collectMetrics()
		s.Error(err)
	})
}

func (s *QuotaCenterSuite) TestNodeOffline() {
	pcm := s.pcm
	dc := s.dc
	qc := s.qc
	meta := s.meta
	core := s.core

	call := meta.EXPECT().GetCollectionByIDWithMaxTs(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, i int64) (*model.Collection, error) {
		return &model.Collection{CollectionID: i, DBID: 1}, nil
	}).Maybe()
	defer call.Unset()

	dbCall := meta.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return([]*model.Database{
		{
			ID:   1,
			Name: "default",
		},
	}, nil)
	defer dbCall.Unset()

	metrics.RootCoordTtDelay.Reset()
	Params.Save(Params.QuotaConfig.TtProtectionEnabled.Key, "true")
	defer Params.Reset(Params.QuotaConfig.TtProtectionEnabled.Key)

	// proxy
	pcm.EXPECT().GetProxyMetrics(mock.Anything).Return(nil, nil)

	// qc first time
	qcMetrics := &metricsinfo.QueryCoordTopology{
		Cluster: metricsinfo.QueryClusterTopology{
			ConnectedNodes: []metricsinfo.QueryNodeInfos{
				{
					BaseComponentInfos: metricsinfo.BaseComponentInfos{ID: 1},
					QuotaMetrics: &metricsinfo.QueryNodeQuotaMetrics{
						Fgm: metricsinfo.FlowGraphMetric{NumFlowGraph: 2, MinFlowGraphChannel: "dml_0"},
						Effect: metricsinfo.NodeEffect{
							NodeID: 1, CollectionIDs: []int64{100, 200},
						},
					},
				},
				{
					BaseComponentInfos: metricsinfo.BaseComponentInfos{ID: 2},
					QuotaMetrics: &metricsinfo.QueryNodeQuotaMetrics{
						Fgm: metricsinfo.FlowGraphMetric{NumFlowGraph: 2, MinFlowGraphChannel: "dml_0"},
						Effect: metricsinfo.NodeEffect{
							NodeID: 2, CollectionIDs: []int64{100, 200},
						},
					},
				},
			},
		},
	}
	resp, err := metricsinfo.MarshalTopology(qcMetrics)
	s.Require().NoError(err)

	qc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{
		Status:   merr.Status(nil),
		Response: resp,
	}, nil).Once()

	// dc first time
	dcMetrics := &metricsinfo.DataCoordTopology{
		Cluster: metricsinfo.DataClusterTopology{
			ConnectedDataNodes: []metricsinfo.DataNodeInfos{
				{
					BaseComponentInfos: metricsinfo.BaseComponentInfos{ID: 3},
					QuotaMetrics: &metricsinfo.DataNodeQuotaMetrics{
						Fgm:    metricsinfo.FlowGraphMetric{NumFlowGraph: 2, MinFlowGraphChannel: "dml_0"},
						Effect: metricsinfo.NodeEffect{NodeID: 3, CollectionIDs: []int64{100, 200}},
					},
				},
				{
					BaseComponentInfos: metricsinfo.BaseComponentInfos{ID: 4},
					QuotaMetrics: &metricsinfo.DataNodeQuotaMetrics{
						Fgm:    metricsinfo.FlowGraphMetric{NumFlowGraph: 2, MinFlowGraphChannel: "dml_0"},
						Effect: metricsinfo.NodeEffect{NodeID: 4, CollectionIDs: []int64{200, 300}},
					},
				},
			},
		},
	}

	resp, err = metricsinfo.MarshalTopology(dcMetrics)
	s.Require().NoError(err)
	dc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{
		Status:   merr.Status(nil),
		Response: resp,
	}, nil).Once()

	quotaCenter := NewQuotaCenter(pcm, qc, dc, core.tsoAllocator, meta)
	err = quotaCenter.collectMetrics()
	s.Require().NoError(err)

	quotaCenter.getTimeTickDelayFactor(tsoutil.ComposeTSByTime(time.Now(), 0))

	s.CollectCntEqual(metrics.RootCoordTtDelay, 4)

	// qc second time
	qcMetrics = &metricsinfo.QueryCoordTopology{
		Cluster: metricsinfo.QueryClusterTopology{
			ConnectedNodes: []metricsinfo.QueryNodeInfos{
				{
					BaseComponentInfos: metricsinfo.BaseComponentInfos{ID: 2},
					QuotaMetrics: &metricsinfo.QueryNodeQuotaMetrics{
						Fgm:    metricsinfo.FlowGraphMetric{NumFlowGraph: 2, MinFlowGraphChannel: "dml_0"},
						Effect: metricsinfo.NodeEffect{NodeID: 2, CollectionIDs: []int64{200, 300}},
					},
				},
			},
		},
	}
	resp, err = metricsinfo.MarshalTopology(qcMetrics)
	s.Require().NoError(err)

	qc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{
		Status:   merr.Status(nil),
		Response: resp,
	}, nil).Once()

	// dc second time
	dcMetrics = &metricsinfo.DataCoordTopology{
		Cluster: metricsinfo.DataClusterTopology{
			ConnectedDataNodes: []metricsinfo.DataNodeInfos{
				{
					BaseComponentInfos: metricsinfo.BaseComponentInfos{ID: 4},
					QuotaMetrics: &metricsinfo.DataNodeQuotaMetrics{
						Fgm:    metricsinfo.FlowGraphMetric{NumFlowGraph: 2, MinFlowGraphChannel: "dml_0"},
						Effect: metricsinfo.NodeEffect{NodeID: 2, CollectionIDs: []int64{200, 300}},
					},
				},
			},
		},
	}

	resp, err = metricsinfo.MarshalTopology(dcMetrics)
	s.Require().NoError(err)
	dc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{
		Status:   merr.Status(nil),
		Response: resp,
	}, nil).Once()

	err = quotaCenter.collectMetrics()
	s.Require().NoError(err)

	quotaCenter.getTimeTickDelayFactor(tsoutil.ComposeTSByTime(time.Now(), 0))
	s.CollectCntEqual(metrics.RootCoordTtDelay, 2)
}

func TestQuotaCenterSuite(t *testing.T) {
	suite.Run(t, new(QuotaCenterSuite))
}
