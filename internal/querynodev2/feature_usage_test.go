// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package querynodev2

import (
	"context"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/featureusage"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (suite *ServiceSuite) TestGetFeatureUsage() {
	ctx := context.Background()
	req := &internalpb.GetFeatureUsageRequest{}

	wasEnabled := featureusage.Enabled()
	featureusage.SetEnabled(true)
	defer featureusage.SetEnabled(wasEnabled)
	featureusage.Hit(featureusage.FeatureTwoStageSearch)
	featureusage.Hit(featureusage.FeatureTwoStageSearch)
	featureusage.Hit(featureusage.FeatureSegmentPrune)

	resp, err := suite.node.GetFeatureUsage(ctx, req)
	suite.NoError(err)
	suite.NoError(merr.Error(resp.GetStatus()))
	suite.Equal(typeutil.QueryNodeRole, resp.GetRole())
	suite.Equal(suite.node.GetNodeID(), resp.GetNodeId())
	suite.NotZero(resp.GetNodeStartTime())
	suite.NotZero(resp.GetCollectedAt())

	byName := make(map[string]*internalpb.FeatureEntry)
	configEntries := 0
	for _, e := range resp.GetEntries() {
		byName[e.Name] = e
		if e.Group == featureusage.GroupConfig {
			configEntries++
			suite.EqualValues(1, e.Value)
			suite.Zero(e.LastUsedAt)
		}
	}
	// Every config entry is key=true or key=false, and exactly one of the pair is present.
	cfg := &paramtable.Get().QueryNodeCfg
	for _, item := range []*paramtable.ParamItem{&cfg.EnableDisk, &cfg.EnableInterminSegmentIndex, &cfg.EnableSegmentPrune, &cfg.MmapVectorIndex} {
		on, off := byName[item.Key+"=true"], byName[item.Key+"=false"]
		suite.True((on != nil) != (off != nil), item.Key)
		if item.GetAsBool() {
			suite.NotNil(on, item.Key)
		} else {
			suite.NotNil(off, item.Key)
		}
	}
	suite.Equal(20, configEntries, "one entry per reported QueryNode boolean switch")

	// The config group is read by a system outside this repository, so the set
	// of reported keys is pinned here: dropping one, or letting a paramtable
	// rename slip through, is a change the consumer sees.
	wantConfigKeys := []string{
		"queryNode.enableDisk",
		"queryNode.segcore.interimIndex.enableIndex",
		"queryNode.segcore.tieredStorage.evictionEnabled",
		"queryNode.segcore.tieredStorage.backgroundEvictionEnabled",
		"queryNode.segcore.multipleChunkedEnable",
		"queryNode.segcore.enableGeometryCache",
		"queryNode.segcore.enableGISSplitFusion",
		"queryNode.mmap.vectorField",
		"queryNode.mmap.vectorIndex",
		"queryNode.mmap.scalarField",
		"queryNode.mmap.scalarIndex",
		"queryNode.mmap.growingMmapEnabled",
		"queryNode.mmap.jsonShredding",
		"queryNode.exprCache.enabled",
		"queryNode.enableSegmentPrune",
		"queryNode.enableSegmentFilter",
		"queryNode.skipGrowingSegmentBF",
		"queryNode.search.enableResultZeroCopy",
		"queryNode.preferFieldDataWhenIndexHasRawData",
		"queryNode.idfOracle.preload",
	}
	gotConfigKeys := make([]string, 0, len(wantConfigKeys))
	for _, e := range resp.GetEntries() {
		if e.Group == featureusage.GroupConfig {
			gotConfigKeys = append(gotConfigKeys, strings.TrimSuffix(strings.TrimSuffix(e.Name, "=true"), "=false"))
		}
	}
	suite.ElementsMatch(wantConfigKeys, gotConfigKeys,
		"the reported QueryNode configuration keys changed; update the list deliberately")

	// Execution-path counters: only the QueryNode-owned ones, all present, hit ones counted.
	suite.Require().Contains(byName, featureusage.FeatureTwoStageSearch.Name())
	suite.Equal(featureusage.GroupRequest, byName[featureusage.FeatureTwoStageSearch.Name()].Group)
	suite.GreaterOrEqual(byName[featureusage.FeatureTwoStageSearch.Name()].Value, int64(2))
	suite.NotZero(byName[featureusage.FeatureTwoStageSearch.Name()].LastUsedAt)
	suite.GreaterOrEqual(byName[featureusage.FeatureSegmentPrune.Name()].Value, int64(1))
	suite.Contains(byName, featureusage.FeatureRunAnalyzer.Name())
	suite.Contains(byName, featureusage.FeatureBruteForceSearch.Name())
	suite.NotContains(byName, featureusage.FeatureIterator.Name(), "proxy counters are not reported by a QueryNode")
	suite.NotContains(byName, featureusage.FeatureImportCSV.Name(), "coordinator counters are not reported by a QueryNode")

	// Not healthy: error status, no entries.
	suite.node.UpdateStateCode(commonpb.StateCode_Abnormal)
	resp, err = suite.node.GetFeatureUsage(ctx, req)
	suite.NoError(err)
	suite.Error(merr.Error(resp.GetStatus()))
	suite.Empty(resp.GetEntries())
}
