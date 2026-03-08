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
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// FillDeployMetricsWithEnv fill deploy metrics with env.
func FillDeployMetricsWithEnv(m *DeployMetrics) {
	m.SystemVersion = os.Getenv(GitCommitEnvKey)
	m.DeployMode = os.Getenv(DeployModeEnvKey)
	m.BuildVersion = os.Getenv(GitBuildTagsEnvKey)
	m.UsedGoVersion = os.Getenv(MilvusUsedGoVersion)
	m.BuildTime = os.Getenv(MilvusBuildTimeEnvKey)
}

func MarshalGetMetricsValues[T any](metrics []T, err error) (string, error) {
	if err != nil {
		return "", err
	}

	bs, err := json.Marshal(metrics)
	if err != nil {
		log.Warn("marshal metrics value failed", zap.Any("metrics", metrics), zap.String("err", err.Error()))
		return "", nil
	}
	return string(bs), nil
}

func getSearchParamString(params []*commonpb.KeyValuePair) string {
	searchParams := ""
	for _, kv := range params {
		searchParams += kv.Key + "=" + kv.Value + ","
	}
	if len(searchParams) > 0 {
		searchParams = searchParams[:len(searchParams)-1]
	}
	return searchParams
}

func NewSlowQueryWithQueryRequest(request *milvuspb.QueryRequest, user string, cost time.Duration, traceID string) *SlowQuery {
	queryParams := &QueryParams{
		Expr:         request.GetExpr(),
		OutputFields: strings.Join(request.GetOutputFields(), ","),
	}

	return &SlowQuery{
		Role:                  typeutil.ProxyRole,
		Database:              request.GetDbName(),
		Collection:            request.GetCollectionName(),
		Partitions:            strings.Join(request.GetPartitionNames(), ","),
		ConsistencyLevel:      request.GetConsistencyLevel().String(),
		UseDefaultConsistency: request.GetUseDefaultConsistency(),
		GuaranteeTimestamp:    request.GetGuaranteeTimestamp(),
		Duration:              cost.String(),
		User:                  user,
		QueryParams:           queryParams,
		Type:                  "Query",
		TraceID:               traceID,
		Time:                  time.Now().Format(time.DateTime),
	}
}

func NewSlowQueryWithSearchRequest(request *milvuspb.SearchRequest, user string, cost time.Duration, traceID string) *SlowQuery {
	searchParams := getSearchParamString(request.GetSearchParams())

	var subReqs []*SearchParams
	for _, req := range request.GetSubReqs() {
		subReqs = append(subReqs, &SearchParams{
			DSL:          []string{req.GetDsl()},
			SearchParams: []string{getSearchParamString(req.GetSearchParams())},
			NQ:           []int64{req.GetNq()},
		})
	}

	searchType := "HybridSearch"
	if len(request.GetSubReqs()) == 0 {
		subReqs = append(subReqs, &SearchParams{
			DSL:          []string{request.GetDsl()},
			SearchParams: []string{searchParams},
			NQ:           []int64{request.GetNq()},
		})
		searchType = "Search"
	}

	queryParams := &QueryParams{
		SearchParams: subReqs,
		Expr:         request.GetDsl(),
		OutputFields: strings.Join(request.GetOutputFields(), ","),
	}

	return &SlowQuery{
		Role:                  typeutil.ProxyRole,
		Database:              request.GetDbName(),
		Collection:            request.GetCollectionName(),
		Partitions:            strings.Join(request.GetPartitionNames(), ","),
		ConsistencyLevel:      request.GetConsistencyLevel().String(),
		UseDefaultConsistency: request.GetUseDefaultConsistency(),
		GuaranteeTimestamp:    request.GetGuaranteeTimestamp(),
		Duration:              cost.String(),
		User:                  user,
		QueryParams:           queryParams,
		Type:                  searchType,
		TraceID:               traceID,
		Time:                  time.Now().Format(time.DateTime),
	}
}

func NewPartitionInfos(partitions *milvuspb.ShowPartitionsResponse) []*PartitionInfo {
	partitionInfos := make([]*PartitionInfo, len(partitions.PartitionNames))

	for i := range partitions.PartitionNames {
		partitionInfos[i] = &PartitionInfo{
			PartitionName:       partitions.PartitionNames[i],
			PartitionID:         partitions.PartitionIDs[i],
			CreatedUtcTimestamp: typeutil.TimestampToString(partitions.CreatedUtcTimestamps[i]),
		}
	}
	return partitionInfos
}

func newFields(fields []*schemapb.FieldSchema) []*Field {
	fieldInfos := make([]*Field, len(fields))
	for i, f := range fields {
		fieldInfos[i] = &Field{
			FieldID:          strconv.FormatInt(f.FieldID, 10),
			Name:             f.Name,
			IsPrimaryKey:     f.IsPrimaryKey,
			Description:      f.Description,
			DataType:         f.DataType.String(),
			TypeParams:       funcutil.KeyValuePair2Map(f.TypeParams),
			IndexParams:      funcutil.KeyValuePair2Map(f.IndexParams),
			AutoID:           f.AutoID,
			ElementType:      f.ElementType.String(),
			DefaultValue:     f.DefaultValue.String(),
			IsDynamic:        f.IsDynamic,
			IsPartitionKey:   f.IsPartitionKey,
			IsClusteringKey:  f.IsClusteringKey,
			Nullable:         f.Nullable,
			IsFunctionOutput: f.IsFunctionOutput,
		}
	}
	return fieldInfos
}

func NewFields(fields *schemapb.CollectionSchema) []*Field {
	return newFields(fields.GetFields())
}

func NewStructArrayFields(fields *schemapb.CollectionSchema) []*StructArrayField {
	fieldInfos := make([]*StructArrayField, len(fields.StructArrayFields))
	for i, f := range fields.StructArrayFields {
		fieldInfos[i] = &StructArrayField{
			FieldID:     strconv.FormatInt(f.FieldID, 10),
			Name:        f.Name,
			Description: f.Description,
			Fields:      newFields(f.Fields),
		}
	}
	return fieldInfos
}

func NewDatabase(resp *milvuspb.DescribeDatabaseResponse) *Database {
	return &Database{
		DBName:           resp.GetDbName(),
		DBID:             resp.GetDbID(),
		CreatedTimestamp: typeutil.TimestampToString(uint64(int64(resp.GetCreatedTimestamp()) / int64(time.Millisecond) / int64(time.Nanosecond))),
		Properties:       funcutil.KeyValuePair2Map(resp.GetProperties()),
	}
}

func NewDatabases(resp *milvuspb.ListDatabasesResponse) *Databases {
	createdTimestamps := make([]string, len(resp.GetCreatedTimestamp()))
	for i, ts := range resp.GetCreatedTimestamp() {
		createdTimestamps[i] = typeutil.TimestampToString(uint64(int64(ts) / int64(time.Millisecond) / int64(time.Nanosecond)))
	}
	return &Databases{
		Names:             resp.GetDbNames(),
		IDs:               lo.Map(resp.GetDbIds(), func(t int64, i int) string { return strconv.FormatInt(t, 10) }),
		CreatedTimestamps: createdTimestamps,
	}
}
