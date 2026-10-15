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

package segments

import (
	"context"
	"sync"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func newReportTestLoader() *segmentLoader {
	loader := &segmentLoader{
		manager:                   NewManager(),
		loadingSegments:           typeutil.NewConcurrentMap[int64, *loadResult](),
		committedResourceNotifier: syncutil.NewVersionedNotifier(),
		duf: &diskUsageFetcher{
			usage: atomic.NewInt64(123),
			err:   atomic.NewError(nil),
		},
	}
	loader.duf.sampled.Store(true)
	return loader
}

func TestLoadResourceReportRetainsSuccessfulLoadUntilCleanup(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	key := params.QueryNodeCfg.TieredEvictionEnabled.Key
	oldValue := params.QueryNodeCfg.TieredEvictionEnabled.GetValue()
	require.NoError(t, params.Save(key, "false"))
	t.Cleanup(func() { require.NoError(t, params.Save(key, oldValue)) })
	patch := mockey.Mock(hardware.GetUsedMemoryCount).Return(uint64(100)).Build()
	defer patch.UnPatch()
	loader := newReportTestLoader()
	ctx := context.Background()
	infos := loader.prepare(ctx, SegmentTypeSealed, &querypb.SegmentLoadInfo{
		SegmentID: 5, CollectionID: 7, NumOfRows: 11,
	})
	require.Len(t, infos, 1)
	result, _ := loader.loadingSegments.Get(5)
	loader.mut.Lock()
	result.resourceInfo.EstimatedMemoryBytes = 50
	result.resourceInfo.EstimatedDiskBytes = 60
	loader.committedResource = LoadResource{MemorySize: 50, DiskSize: 60}
	loader.mut.Unlock()
	loader.notifyLoadFinish(infos...)

	report, err := loader.GetLoadResourceReport(ctx)
	require.NoError(t, err)
	require.Len(t, report.GetLoadingSegments(), 1, "success notification must not imply resource cleanup")
	require.EqualValues(t, 100, report.GetMemoryUsageBytes())
	require.EqualValues(t, 123, report.GetDiskUsageBytes())
	require.EqualValues(t, 50, report.GetLoadingMemoryBytes())
	require.EqualValues(t, 60, report.GetLoadingDiskBytes())
	require.EqualValues(t, 11, report.GetLoadingSegments()[0].GetNumRows())
	// Returned protobufs must not alias mutable loader metadata.
	report.LoadingSegments[0].EstimatedMemoryBytes = 999
	report, err = loader.GetLoadResourceReport(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 50, report.GetLoadingSegments()[0].GetEstimatedMemoryBytes())

	loader.freeRequestResource(requestResourceResult{Resource: LoadResource{MemorySize: 50, DiskSize: 60}})
	report, err = loader.GetLoadResourceReport(ctx)
	require.NoError(t, err)
	require.Zero(t, report.GetLoadingMemoryBytes())
	require.Len(t, report.GetLoadingSegments(), 1, "resource free must precede final unregister")
	loader.unregister(infos...)
	report, err = loader.GetLoadResourceReport(ctx)
	require.NoError(t, err)
	require.Empty(t, report.GetLoadingSegments())
}

func TestLoadResourceReportConcurrentPrepare(t *testing.T) {
	paramtable.Init()
	patch := mockey.Mock(hardware.GetUsedMemoryCount).Return(uint64(100)).Build()
	defer patch.UnPatch()
	loader := newReportTestLoader()
	info := &querypb.SegmentLoadInfo{SegmentID: 5, CollectionID: 7, NumOfRows: 11}
	var wg sync.WaitGroup
	registered := atomic.NewInt32(0)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			registered.Add(int32(len(loader.prepare(context.Background(), SegmentTypeSealed, info))))
		}()
	}
	wg.Wait()
	require.EqualValues(t, 1, registered.Load())
	report, err := loader.GetLoadResourceReport(context.Background())
	require.NoError(t, err)
	require.Len(t, report.GetLoadingSegments(), 1)
	loader.unregister(info)
}

func TestLoadResourceReportUnavailable(t *testing.T) {
	paramtable.Init()
	loader := newReportTestLoader()
	loader.duf.err.Store(merr.WrapErrServiceInternalMsg("disk sample unavailable"))
	report, err := loader.GetLoadResourceReport(context.Background())
	require.Error(t, err)
	require.Nil(t, report)
	loader.duf.err.Store(nil)
	loader.duf.usage.Store(-1)
	report, err = loader.GetLoadResourceReport(context.Background())
	require.Error(t, err)
	require.Nil(t, report)
	loader.duf.usage.Store(0)
	patch := mockey.Mock(hardware.GetUsedMemoryCount).Return(uint64(0)).Build()
	defer patch.UnPatch()
	report, err = loader.GetLoadResourceReport(context.Background())
	require.Error(t, err)
	require.Nil(t, report, "failed memory sampling must not look like an idle node")
}

func TestLoadResourceReportTieredConfiguration(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	key := params.QueryNodeCfg.TieredEvictionEnabled.Key
	previous := params.QueryNodeCfg.TieredEvictionEnabled.GetValue()
	t.Cleanup(func() { require.NoError(t, params.Save(key, previous)) })
	patch := mockey.Mock(hardware.GetUsedMemoryCount).Return(uint64(100)).Build()
	defer patch.UnPatch()
	loader := newReportTestLoader()
	for _, enabled := range []bool{false, true} {
		require.NoError(t, params.Save(key, map[bool]string{false: "false", true: "true"}[enabled]))
		report, err := loader.GetLoadResourceReport(context.Background())
		require.NoError(t, err)
		require.Equal(t, enabled, report.GetTieredEvictionEnabled())
	}
}

func (suite *SegmentLoaderDetailSuite) TestLoadResourceReportEstimates() {
	infos := []*querypb.SegmentLoadInfo{
		{SegmentID: 101, CollectionID: suite.collectionID, NumOfRows: 30},
		{SegmentID: 100, CollectionID: suite.collectionID, NumOfRows: 10},
	}
	// Mock only the native estimator, retaining aggregation/registration paths.
	patch := mockey.Mock(estimateLoadingResourceUsageOfSegment).Return(&ResourceUsage{MemorySize: 50, DiskSize: 60}, nil).Build()
	defer func() { patch.UnPatch() }()
	suite.loader.prepare(context.Background(), SegmentTypeSealed, infos...)
	defer suite.loader.unregister(infos...)
	usage, _, err := suite.loader.estimateSegmentLoadingResourceUsage(context.Background(), infos...)
	suite.Require().NoError(err)
	suite.EqualValues(100, usage.MemorySize)
	suite.loader.duf.usage.Store(0)
	suite.loader.duf.err.Store(nil)
	suite.loader.duf.sampled.Store(true)
	report, err := suite.loader.GetLoadResourceReport(context.Background())
	suite.Require().NoError(err)
	suite.Require().Len(report.GetLoadingSegments(), 2)
	suite.EqualValues(100, report.GetLoadingSegments()[0].GetSegmentId())
	suite.EqualValues(50, report.GetLoadingSegments()[0].GetEstimatedMemoryBytes())
	suite.EqualValues(60, report.GetLoadingSegments()[1].GetEstimatedDiskBytes())
	// A separate delta/retry request for the same ID must not replace the
	// registered load's full estimate with a different request's estimate.
	patch.UnPatch()
	patch = mockey.Mock(estimateLoadingResourceUsageOfSegment).Return(&ResourceUsage{MemorySize: 1, DiskSize: 2}, nil).Build()
	_, _, err = suite.loader.estimateSegmentLoadingResourceUsage(context.Background(),
		&querypb.SegmentLoadInfo{SegmentID: 100, CollectionID: suite.collectionID})
	suite.Require().NoError(err)
	report, err = suite.loader.GetLoadResourceReport(context.Background())
	suite.Require().NoError(err)
	suite.EqualValues(50, report.GetLoadingSegments()[0].GetEstimatedMemoryBytes())
}

func (suite *SegmentLoaderDetailSuite) TestLoadResourceReportRequestFailureCleanup() {
	for _, cancelled := range []bool{false, true} {
		suite.Run(map[bool]string{false: "rejected", true: "cancelled"}[cancelled], func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if cancelled {
				cancel()
			}
			patch := mockey.Mock((*segmentLoader).requestResource).To(func(loader *segmentLoader, ctx context.Context, infos ...*querypb.SegmentLoadInfo) (requestResourceResult, error) {
				suite.Equal(1, loader.loadingSegments.Len(), "prepare precedes resource request")
				if ctx.Err() != nil {
					return requestResourceResult{}, ctx.Err()
				}
				return requestResourceResult{}, merr.WrapErrSegmentRequestResourceFailed("Memory")
			}).Build()
			defer patch.UnPatch()
			_, err := suite.loader.Load(ctx, suite.collectionID, SegmentTypeSealed, 1,
				&querypb.SegmentLoadInfo{SegmentID: suite.segmentID, CollectionID: suite.collectionID, NumOfRows: 11})
			suite.Error(err)
			suite.Zero(suite.loader.loadingSegments.Len(), "failed/cancelled Load must unregister on return")
		})
	}
}
