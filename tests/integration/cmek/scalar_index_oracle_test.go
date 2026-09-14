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

package cmek

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type scalarCell struct {
	name        string
	indexType   string
	dataType    schemapb.DataType
	indexParams []*commonpb.KeyValuePair
	textLog     bool
	data        func(string, int) *schemapb.FieldData
	expr        func(string) string
	match       func(int) bool
}

func (s *scalarIndexSuite) assertOracle(cell scalarCell, collectionName string) {
	ctx := s.Cluster.GetContext()
	expr := cell.expr(fixtureFieldName)
	response, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		DbName: s.dbName, CollectionName: collectionName, Expr: expr,
		OutputFields:     []string{fixturePrimaryKey},
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(merr.CheckRPCCall(response, err))

	actual := make(map[int64]struct{})
	for _, field := range response.GetFieldsData() {
		if field.GetFieldName() != fixturePrimaryKey {
			continue
		}
		for _, id := range field.GetScalars().GetLongData().GetData() {
			actual[id] = struct{}{}
		}
	}
	expected := make(map[int64]struct{})
	for i := 0; i < fixtureRowCount; i++ {
		if cell.match(i) {
			expected[int64(i)] = struct{}{}
		}
	}
	s.Require().Equal(expected, actual, "%s query %q returned an unexpected primary-key set", cell.name, expr)
}

func int64RangeCell(name, indexType string) scalarCell {
	return scalarCell{
		name: name, indexType: indexType, dataType: schemapb.DataType_Int64,
		data: func(field string, rows int) *schemapb.FieldData {
			values := make([]int64, rows)
			for i := range values {
				values[i] = int64(i % 16)
			}
			return newInt64FieldData(field, values)
		},
		expr:  func(field string) string { return fmt.Sprintf("%s >= 4 && %s < 8", field, field) },
		match: func(i int) bool { return i%16 >= 4 && i%16 < 8 },
	}
}

func bitmapCell() scalarCell {
	return scalarCell{
		name: "bitmap", indexType: "BITMAP", dataType: schemapb.DataType_Bool,
		data: func(field string, rows int) *schemapb.FieldData {
			values := make([]bool, rows)
			for i := range values {
				values[i] = i%3 == 0
			}
			return &schemapb.FieldData{
				Type: schemapb.DataType_Bool, FieldName: field,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: values}},
				}},
			}
		},
		expr:  func(field string) string { return field + " == true" },
		match: func(i int) bool { return i%3 == 0 },
	}
}

func trieCell() scalarCell {
	return scalarCell{
		name: "trie", indexType: "TRIE", dataType: schemapb.DataType_VarChar,
		data: func(field string, rows int) *schemapb.FieldData {
			values := make([]string, rows)
			for i := range values {
				values[i] = fmt.Sprintf("trie-%c", 'a'+rune(i%3))
			}
			return stringFieldData(field, values)
		},
		expr:  func(field string) string { return field + " in ['trie-a', 'trie-c']" },
		match: func(i int) bool { return i%3 == 0 || i%3 == 2 },
	}
}

func likeCell(name, indexType string) scalarCell {
	return scalarCell{
		name: name, indexType: indexType, dataType: schemapb.DataType_VarChar,
		data: func(field string, rows int) *schemapb.FieldData {
			values := make([]string, rows)
			for i := range values {
				if i%8 == 0 {
					values[i] = fmt.Sprintf("prefix-needle-%03d", i)
				} else {
					values[i] = fmt.Sprintf("ordinary-%03d", i)
				}
			}
			return stringFieldData(field, values)
		},
		expr:  func(field string) string { return field + " like '%needle%'" },
		match: func(i int) bool { return i%8 == 0 },
	}
}

func ngramCell() scalarCell {
	cell := likeCell("ngram", "NGRAM")
	cell.indexParams = []*commonpb.KeyValuePair{
		{Key: common.ParamsKey, Value: `{"min_gram":"2","max_gram":"3"}`},
	}
	return cell
}

func textMatchCell() scalarCell {
	cell := likeCell("textmatch", "TEXT_MATCH")
	cell.textLog = true
	cell.expr = func(field string) string { return fmt.Sprintf("TEXT_MATCH(%s, 'needle')", field) }
	return cell
}

func geometryCell() scalarCell {
	return scalarCell{
		name: "rtree", indexType: "RTREE", dataType: schemapb.DataType_Geometry,
		data: func(field string, rows int) *schemapb.FieldData {
			values := make([]string, rows)
			for i := range values {
				if i%2 == 0 {
					values[i] = "POINT (0 0)"
				} else {
					values[i] = "POINT (10 10)"
				}
			}
			return &schemapb.FieldData{
				Type: schemapb.DataType_Geometry, FieldName: field,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_GeometryWktData{GeometryWktData: &schemapb.GeometryWktArray{Data: values}},
				}},
			}
		},
		expr: func(field string) string {
			return fmt.Sprintf("ST_INTERSECTS(%s, 'POLYGON ((-1 -1, 1 -1, 1 1, -1 1, -1 -1))')", field)
		},
		match: func(i int) bool { return i%2 == 0 },
	}
}

func stringFieldData(field string, values []string) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type: schemapb.DataType_VarChar, FieldName: field,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: values}},
		}},
	}
}
