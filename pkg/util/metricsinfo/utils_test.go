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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
)

func TestFillDeployMetricsWithEnv(t *testing.T) {
	var m DeployMetrics

	commit := "commit"
	t.Setenv(GitCommitEnvKey, commit)

	deploy := "deploy"
	t.Setenv(DeployModeEnvKey, deploy)

	version := "version"
	t.Setenv(GitBuildTagsEnvKey, version)

	goVersion := "go"
	t.Setenv(MilvusUsedGoVersion, goVersion)

	buildTime := "build"
	t.Setenv(MilvusBuildTimeEnvKey, buildTime)

	FillDeployMetricsWithEnv(&m)
	assert.NotNil(t, m)
	assert.Equal(t, commit, m.SystemVersion)
	assert.Equal(t, deploy, m.DeployMode)
	assert.Equal(t, version, m.BuildVersion)
	assert.Equal(t, goVersion, m.UsedGoVersion)
	assert.Equal(t, buildTime, m.BuildTime)
}

func TestNewSlowQueryWithSearchRequest(t *testing.T) {
	request := &milvuspb.SearchRequest{
		DbName:                "test_db",
		CollectionName:        "test_collection",
		PartitionNames:        []string{"partition1", "partition2"},
		ConsistencyLevel:      commonpb.ConsistencyLevel_Bounded,
		UseDefaultConsistency: true,
		GuaranteeTimestamp:    123456789,
		SearchParams:          []*commonpb.KeyValuePair{{Key: "param1", Value: "value1"}},
		SubReqs:               []*milvuspb.SubSearchRequest{{Dsl: "dsl1", SearchParams: []*commonpb.KeyValuePair{{Key: "param2", Value: "value2"}}, Nq: 10}},
		Dsl:                   "dsl2",
		Nq:                    20,
		OutputFields:          []string{"field1", "field2"},
	}
	user := "test_user"
	cost := time.Duration(100) * time.Millisecond

	slowQuery := NewSlowQueryWithSearchRequest(request, user, cost, "")

	assert.NotNil(t, slowQuery)
	assert.Equal(t, "proxy", slowQuery.Role)
	assert.Equal(t, "test_db", slowQuery.Database)
	assert.Equal(t, "test_collection", slowQuery.Collection)
	assert.Equal(t, "partition1,partition2", slowQuery.Partitions)
	assert.Equal(t, "Bounded", slowQuery.ConsistencyLevel)
	assert.True(t, slowQuery.UseDefaultConsistency)
	assert.Equal(t, uint64(123456789), slowQuery.GuaranteeTimestamp)
	assert.Equal(t, "100ms", slowQuery.Duration)
	assert.Equal(t, user, slowQuery.User)
	assert.Equal(t, "HybridSearch", slowQuery.Type)
	assert.NotNil(t, slowQuery.QueryParams)
	assert.Equal(t, "dsl2", slowQuery.QueryParams.Expr)
	assert.Equal(t, "field1,field2", slowQuery.QueryParams.OutputFields)
	assert.Len(t, slowQuery.QueryParams.SearchParams, 1)
	assert.Equal(t, []string{"dsl1"}, slowQuery.QueryParams.SearchParams[0].DSL)
	assert.Equal(t, []string{"param2=value2"}, slowQuery.QueryParams.SearchParams[0].SearchParams)
	assert.Equal(t, []int64{10}, slowQuery.QueryParams.SearchParams[0].NQ)
}

func TestNewSlowQueryWithQueryRequest(t *testing.T) {
	request := &milvuspb.QueryRequest{
		DbName:                "test_db",
		CollectionName:        "test_collection",
		PartitionNames:        []string{"partition1", "partition2"},
		ConsistencyLevel:      commonpb.ConsistencyLevel_Bounded,
		UseDefaultConsistency: true,
		GuaranteeTimestamp:    123456789,
		Expr:                  "expr1",
		OutputFields:          []string{"field1", "field2"},
	}
	user := "test_user"
	cost := time.Duration(100) * time.Millisecond

	slowQuery := NewSlowQueryWithQueryRequest(request, user, cost, "")

	assert.NotNil(t, slowQuery)
	assert.Equal(t, "proxy", slowQuery.Role)
	assert.Equal(t, "test_db", slowQuery.Database)
	assert.Equal(t, "test_collection", slowQuery.Collection)
	assert.Equal(t, "partition1,partition2", slowQuery.Partitions)
	assert.Equal(t, "Bounded", slowQuery.ConsistencyLevel)
	assert.True(t, slowQuery.UseDefaultConsistency)
	assert.Equal(t, uint64(123456789), slowQuery.GuaranteeTimestamp)
	assert.Equal(t, "100ms", slowQuery.Duration)
	assert.Equal(t, user, slowQuery.User)
	assert.Equal(t, "Query", slowQuery.Type)
	assert.NotNil(t, slowQuery.QueryParams)
	assert.Equal(t, "expr1", slowQuery.QueryParams.Expr)
	assert.Equal(t, "field1,field2", slowQuery.QueryParams.OutputFields)
}
