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
package featureusage

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
	"github.com/milvus-io/milvus/tests/integration"
)

// TestGeoAndTimeExpressions covers the expression features that need a field
// type the ordinary workload collection does not have: the nine geospatial
// predicates and the timestamp-with-timezone comparison.
func (s *Suite) TestGeoAndTimeExpressions() {
	ctx := context.Background()
	name := "fu_geo_" + funcutil.GenRandomStr()
	dim := 8

	schema := &schemapb.CollectionSchema{
		Name: name,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
			{FieldID: 101, Name: "geo", DataType: schemapb.DataType_Geometry},
			{FieldID: 102, Name: "ts", DataType: schemapb.DataType_Timestamptz},
			{
				FieldID: 103, Name: "vec", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: fmt.Sprint(dim)}},
			},
		},
	}
	marshaled, err := proto.Marshal(schema)
	s.Require().NoError(err)
	status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: name, Schema: marshaled, ShardsNum: 1,
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, status.GetErrorCode(), status.GetReason())

	const rowNum = 100
	wkt := make([]string, rowNum)
	stamps := make([]string, rowNum)
	for i := range wkt {
		wkt[i] = fmt.Sprintf("POINT (%d %d)", i%10, i%7)
		stamps[i] = fmt.Sprintf("2025-01-%02dT00:00:00Z", i%28+1)
	}
	insert, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: name,
		FieldsData: []*schemapb.FieldData{
			newGeometryColumn("geo", wkt),
			newTimestamptzColumn("ts", stamps),
			integration.NewFloatVectorFieldData("vec", rowNum, dim),
		},
		HashKeys: integration.GenerateHashKeys(rowNum),
		NumRows:  uint32(rowNum),
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, insert.GetStatus().GetErrorCode(), insert.GetStatus().GetReason())

	flush, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{CollectionNames: []string{name}})
	s.Require().NoError(err)
	s.WaitForFlush(ctx, flush.GetCollSegIDs()[name].GetData(), flush.GetCollFlushTs()[name], "", name)

	index, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: name, FieldName: "vec", IndexName: "_default",
		ExtraParams: integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.L2),
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, index.GetErrorCode(), index.GetReason())
	s.WaitForIndexBuilt(ctx, name, "vec")

	load, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: name})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, load.GetErrorCode(), load.GetReason())
	s.WaitForLoad(ctx, name)

	cases := []struct {
		counter string
		expr    string
	}{
		{"st_equals", `st_equals(geo, "POINT (1 1)")`},
		{"st_touches", `st_touches(geo, "LINESTRING (0 0, 1 1)")`},
		{"st_overlaps", `st_overlaps(geo, "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))")`},
		{"st_crosses", `st_crosses(geo, "LINESTRING (0 0, 5 5)")`},
		{"st_contains", `st_contains(geo, "POINT (1 1)")`},
		{"st_intersects", `st_intersects(geo, "LINESTRING (0 0, 9 9)")`},
		{"st_within", `st_within(geo, "POLYGON ((0 0, 9 0, 9 9, 0 9, 0 0))")`},
		{"st_dwithin", `st_dwithin(geo, "POINT (0 0)", 10.0)`},
		{"st_isvalid", `st_isvalid(geo)`},
	}
	for _, tc := range cases {
		s.Run(tc.counter, func() {
			before := counters(s.report(ctx), typeutil.ProxyRole)
			s.queryOn(ctx, name, tc.expr)
			after := counters(s.report(ctx), typeutil.ProxyRole)
			requireOnlyDelta(s.T(), before, after, map[string]int64{tc.counter: 1})
		})
	}

	// A comparison against an interval on a timestamptz column is the form
	// that produces the arithmetic-compare node the walk counts.
	s.Run("timestamptz_compare", func() {
		before := counters(s.report(ctx), typeutil.ProxyRole)
		s.queryOn(ctx, name, `ts + INTERVAL 'P1D' > ISO '2025-01-01T00:00:00Z'`)
		after := counters(s.report(ctx), typeutil.ProxyRole)
		requireOnlyDelta(s.T(), before, after, map[string]int64{"timestamptz_compare": 1})
	})
}

// TestStructArrayExpressions covers the two predicates that only exist for
// struct array fields.
func (s *Suite) TestStructArrayExpressions() {
	ctx := context.Background()
	name := "fu_struct_" + funcutil.GenRandomStr()
	dim := 8

	schema := integration.ConstructSchemaOfVecDataTypeWithStruct(name, dim, true)
	schema.Name = name
	marshaled, err := proto.Marshal(schema)
	s.Require().NoError(err)
	status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: name, Schema: marshaled, ShardsNum: 1,
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, status.GetErrorCode(), status.GetReason())

	const rowNum = 100
	insert, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: name,
		FieldsData: []*schemapb.FieldData{
			integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim),
			integration.NewStructArrayFieldData(schema.GetStructArrayFields()[0], integration.StructArrayField, rowNum, dim),
		},
		HashKeys: integration.GenerateHashKeys(rowNum),
		NumRows:  uint32(rowNum),
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, insert.GetStatus().GetErrorCode(), insert.GetStatus().GetReason())

	flush, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{CollectionNames: []string{name}})
	s.Require().NoError(err)
	s.WaitForFlush(ctx, flush.GetCollSegIDs()[name].GetData(), flush.GetCollFlushTs()[name], "", name)

	// Both vector fields need an index: the struct's vector sub-field counts as
	// one, and loading refuses until every vector field has one. Its name is
	// the concatenated form the proxy rewrote it to at create time.
	subVec := typeutil.ConcatStructFieldName(integration.StructArrayField, integration.StructSubFloatVecField)
	for _, field := range []string{integration.FloatVecField, subVec} {
		index, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
			CollectionName: name, FieldName: field, IndexName: field + "_idx",
			ExtraParams: integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.L2),
		})
		s.Require().NoError(err)
		s.Require().Equal(commonpb.ErrorCode_Success, index.GetErrorCode(), index.GetReason())
		s.WaitForIndexBuilt(ctx, name, field)
	}

	load, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: name})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, load.GetErrorCode(), load.GetReason())
	s.WaitForLoad(ctx, name)

	sub := integration.StructSubInt32Field
	for _, tc := range []struct {
		counter string
		expr    string
	}{
		{"element_filter", fmt.Sprintf(`element_filter(%s, $[%s] > 1)`, integration.StructArrayField, sub)},
		{"struct_match", fmt.Sprintf(`MATCH_ANY(%s, $[%s] > 1)`, integration.StructArrayField, sub)},
	} {
		s.Run(tc.counter, func() {
			before := counters(s.report(ctx), typeutil.ProxyRole)
			s.queryOn(ctx, name, tc.expr)
			after := counters(s.report(ctx), typeutil.ProxyRole)
			requireOnlyDelta(s.T(), before, after, map[string]int64{tc.counter: 1})
		})
	}
}

// queryOn issues one query with the given expression against the named
// collection. A predicate that matches nothing is fine: the counters are
// recorded from the parsed plan, not from the result.
func (s *Suite) queryOn(ctx context.Context, collection, expr string) {
	_, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName:        collection,
		Expr:                  expr,
		OutputFields:          []string{"count(*)"},
		UseDefaultConsistency: true,
	})
	s.Require().NoError(err, "transport error")
}

func newGeometryColumn(name string, wkt []string) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Geometry,
		FieldName: name,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_GeometryWktData{GeometryWktData: &schemapb.GeometryWktArray{Data: wkt}},
		}},
	}
}

// newTimestamptzColumn sends ISO-8601 strings; the proxy converts them to the
// internal microsecond form.
func newTimestamptzColumn(name string, iso []string) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Timestamptz,
		FieldName: name,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: iso}},
		}},
	}
}
