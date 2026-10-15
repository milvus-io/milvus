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

package querynodev2

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type reportTestLoader struct {
	segments.Loader
	report *querypb.QueryNodeLoadResourceReport
	err    error
	calls  int
}

func (loader *reportTestLoader) GetLoadResourceReport(context.Context) (*querypb.QueryNodeLoadResourceReport, error) {
	loader.calls++
	return loader.report, loader.err
}

func TestInitialDistributionRequestIsFullEvenAtZeroVersion(t *testing.T) {
	require.True(t, hasDataDistributionChange(&querypb.GetDataDistributionRequest{LastUpdateTs: 0}, 0))
	tracker := newDataDistributionDeltaTracker()
	require.False(t, tracker.canBuildDelta(&querypb.GetDataDistributionRequest{SupportDelta: true}))
}

func (suite *ServiceSuite) TestGetDataDistribution_LoadResourceReportWithoutDistChange() {
	original := suite.node.loader
	loader := &reportTestLoader{
		Loader: original,
		report: &querypb.QueryNodeLoadResourceReport{
			MemoryUsageBytes: 100, DiskUsageBytes: 200, LoadingMemoryBytes: 50,
			LoadingSegments: []*querypb.LoadingSegmentInfo{{SegmentId: 5, CollectionId: 7, NumRows: 11}},
		},
	}
	suite.node.loader = loader
	defer func() { suite.node.loader = original }()
	ctx := context.Background()
	initial, err := suite.node.GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{SupportDelta: true})
	suite.Require().NoError(err)
	suite.Require().NoError(merr.Error(initial.GetStatus()))
	suite.False(initial.GetIsDelta(), "initial distribution must be a complete snapshot")
	suite.Require().NotNil(initial.GetLoadResourceReport())
	suite.True(initial.GetLoadResourceReportSupported())
	suite.Len(initial.GetLoadResourceReport().GetLoadingSegments(), 1)

	// No segment/channel update: only loading-resource state changes.
	loader.report = &querypb.QueryNodeLoadResourceReport{MemoryUsageBytes: 150, DiskUsageBytes: 200}
	unchanged, err := suite.node.GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{LastUpdateTs: math.MaxInt64})
	suite.Require().NoError(err)
	suite.Require().NoError(merr.Error(unchanged.GetStatus()))
	suite.Equal(initial.GetLastModifyTs(), unchanged.GetLastModifyTs())
	suite.Empty(unchanged.GetSegments())
	suite.Require().NotNil(unchanged.GetLoadResourceReport())
	suite.True(unchanged.GetLoadResourceReportSupported())
	suite.EqualValues(150, unchanged.GetLoadResourceReport().GetMemoryUsageBytes())
	suite.Empty(unchanged.GetLoadResourceReport().GetLoadingSegments())
	suite.Positive(unchanged.GetMemCapacityInMB())
	suite.Positive(unchanged.GetCpuNum())
	suite.Equal(2, loader.calls)

	loader.err = merr.WrapErrServiceInternalMsg("resource snapshot unavailable")
	unknown, err := suite.node.GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{LastUpdateTs: math.MaxInt64})
	suite.NoError(err)
	suite.NoError(merr.Error(unknown.GetStatus()))
	suite.Nil(unknown.GetLoadResourceReport(), "resource sampling failure must not report a known idle node")
	suite.True(unknown.GetLoadResourceReportSupported())
}

func (suite *ServiceSuite) TestGetDataDistribution_LegacyLoaderResourceReportAbsent() {
	original := suite.node.loader
	suite.node.loader = segments.NewMockLoader(suite.T())
	defer func() { suite.node.loader = original }()
	resp, err := suite.node.GetDataDistribution(context.Background(), &querypb.GetDataDistributionRequest{LastUpdateTs: math.MaxInt64})
	suite.NoError(err)
	suite.NoError(merr.Error(resp.GetStatus()))
	suite.Nil(resp.GetLoadResourceReport())
	suite.False(resp.GetLoadResourceReportSupported())
}

func (suite *ServiceSuite) TestGetDataDistribution_FirstResourceReportUnavailable() {
	original := suite.node.loader
	suite.node.loader = &reportTestLoader{Loader: original, err: merr.WrapErrServiceInternalMsg("first resource snapshot unavailable")}
	defer func() { suite.node.loader = original }()
	resp, err := suite.node.GetDataDistribution(context.Background(), &querypb.GetDataDistributionRequest{})
	suite.NoError(err)
	suite.NoError(merr.Error(resp.GetStatus()))
	suite.False(resp.GetIsDelta())
	suite.True(resp.GetLoadResourceReportSupported(), "first failed snapshot must not look like a legacy node")
	suite.Nil(resp.GetLoadResourceReport())
}
