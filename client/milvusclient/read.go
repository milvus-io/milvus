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

package milvusclient

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func (c *Client) Search(ctx context.Context, option SearchOption, callOptions ...grpc.CallOption) ([]ResultSet, error) {
	req, err := option.Request()
	if err != nil {
		return nil, err
	}
	collection, err := c.getCollection(ctx, req.GetCollectionName())
	if err != nil {
		return nil, err
	}

	var resultSets []ResultSet

	err = c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.Search(ctx, req, callOptions...)
		err = merr.CheckRPCCall(resp, err)
		if err != nil {
			return err
		}
		resultSets, err = c.handleSearchResult(collection.Schema, req.GetOutputFields(), int(resp.GetResults().GetNumQueries()), resp)

		return err
	})

	return resultSets, err
}

func (c *Client) handleSearchResult(schema *entity.Schema, outputFields []string, nq int, resp *milvuspb.SearchResults) ([]ResultSet, error) {
	sr := make([]ResultSet, 0, nq)
	results := resp.GetResults()
	offset := 0
	fieldDataList := results.GetFieldsData()
	gb := results.GetGroupByFieldValue()
	for i := 0; i < int(results.GetNumQueries()); i++ {
		func() {
			var rc int
			entry := ResultSet{
				sch: schema,
			}
			defer func() {
				offset += rc
				sr = append(sr, entry)
			}()

			if i >= len(results.Topks) {
				entry.Err = errors.Newf("topk not returned for nq %d", i)
				return
			}
			rc = int(results.GetTopks()[i]) // result entry count for current query
			entry.ResultCount = rc
			entry.Scores = results.GetScores()[offset : offset+rc]

			// set recall if returned
			if i < len(results.Recalls) {
				entry.Recall = results.Recalls[i]
			}

			entry.IDs, entry.Err = column.IDColumns(schema, results.GetIds(), offset, offset+rc)
			if entry.Err != nil {
				return
			}
			// parse group-by values
			if gb != nil {
				entry.GroupByValue, entry.Err = column.FieldDataColumn(gb, offset, offset+rc)
				if entry.Err != nil {
					return
				}
			}
			entry.Fields, entry.Err = c.parseSearchResult(schema, outputFields, fieldDataList, i, offset, offset+rc)
		}()
	}
	return sr, nil
}

func (c *Client) parseSearchResult(sch *entity.Schema, outputFields []string, fieldDataList []*schemapb.FieldData, _, from, to int) ([]column.Column, error) {
	var wildcard bool
	// serveral cases shall be handled here
	// 1. output fields contains "*" wildcard => the schema shall be checked
	// 2. dynamic schema $meta column, with field name not exist in schema
	// 3. explicitly specified json column name
	// 4. partial load field

	// translate "*" into possible field names
	// if partial load enabled, result set could miss some column
	outputFields, wildcard = expandWildcard(sch, outputFields)
	// duplicated field name will be merged into one column
	outputSet := typeutil.NewSet(outputFields...)

	// setup schema valid field name to get possible dynamic field name
	schemaFieldSet := typeutil.NewSet(lo.Map(sch.Fields, func(f *entity.Field, _ int) string {
		return f.Name
	})...)
	dynamicNames := outputSet.Complement(schemaFieldSet)

	columns := make([]column.Column, 0, len(outputFields))
	var dynamicColumn *column.ColumnJSONBytes
	for _, fieldData := range fieldDataList {
		col, err := column.FieldDataColumn(fieldData, from, to)
		if err != nil {
			return nil, err
		}

		// if output data contains dynamic json, setup dynamicColumn
		if fieldData.GetIsDynamic() {
			var ok bool
			dynamicColumn, ok = col.(*column.ColumnJSONBytes)
			if !ok {
				return nil, errors.New("dynamic field not json")
			}

			// return json column only explicitly specified in output fields and not in wildcard mode
			if _, ok := outputSet[fieldData.GetFieldName()]; !ok && !wildcard {
				continue
			}
		}

		// remove processed field, remove from possible dynamic set
		delete(dynamicNames, fieldData.GetFieldName())

		columns = append(columns, col)
	}

	// extra name found and not json output
	if len(dynamicNames) > 0 && dynamicColumn == nil {
		var extraFields []string
		for output := range dynamicNames {
			extraFields = append(extraFields, output)
		}
		return nil, errors.Newf("extra output fields %v found and result does not contain dynamic field", extraFields)
	}
	// add dynamic column for extra fields
	for outputField := range dynamicNames {
		column := column.NewColumnDynamic(dynamicColumn, outputField)
		columns = append(columns, column)
	}

	return columns, nil
}

func (c *Client) Query(ctx context.Context, option QueryOption, callOptions ...grpc.CallOption) (ResultSet, error) {
	var resultSet ResultSet
	req, err := option.Request()
	if err != nil {
		return resultSet, err
	}

	collection, err := c.getCollection(ctx, req.GetCollectionName())
	if err != nil {
		return resultSet, err
	}

	err = c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.Query(ctx, req, callOptions...)
		err = merr.CheckRPCCall(resp, err)
		if err != nil {
			return err
		}

		columns, err := c.parseSearchResult(collection.Schema, resp.GetOutputFields(), resp.GetFieldsData(), 0, 0, -1)
		if err != nil {
			return err
		}
		resultSet = ResultSet{
			sch:    collection.Schema,
			Fields: columns,
		}
		if len(columns) > 0 {
			resultSet.ResultCount = columns[0].Len()
		}

		return nil
	})
	return resultSet, err
}

func (c *Client) Get(ctx context.Context, option QueryOption, callOptions ...grpc.CallOption) (ResultSet, error) {
	return c.Query(ctx, option, callOptions...)
}

func (c *Client) HybridSearch(ctx context.Context, option HybridSearchOption, callOptions ...grpc.CallOption) ([]ResultSet, error) {
	req, err := option.HybridRequest()
	if err != nil {
		return nil, err
	}

	collection, err := c.getCollection(ctx, req.GetCollectionName())
	if err != nil {
		return nil, err
	}

	var resultSets []ResultSet

	err = c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.HybridSearch(ctx, req, callOptions...)
		err = merr.CheckRPCCall(resp, err)
		if err != nil {
			return err
		}

		resultSets, err = c.handleSearchResult(collection.Schema, req.GetOutputFields(), int(resp.GetResults().GetNumQueries()), resp)

		return err
	})
	return resultSets, err
}

func (c *Client) RunAnalyzer(ctx context.Context, option RunAnalyzerOption, callOptions ...grpc.CallOption) ([]*entity.AnalyzerResult, error) {
	req, err := option.Request()
	if err != nil {
		return nil, err
	}

	var result []*entity.AnalyzerResult
	err = c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.RunAnalyzer(ctx, req, callOptions...)
		err = merr.CheckRPCCall(resp, err)
		if err != nil {
			return err
		}

		result = lo.Map(resp.Results, func(result *milvuspb.AnalyzerResult, _ int) *entity.AnalyzerResult {
			return &entity.AnalyzerResult{
				Tokens: lo.Map(result.Tokens, func(token *milvuspb.AnalyzerToken, _ int) *entity.Token {
					return &entity.Token{
						Text:           token.GetToken(),
						StartOffset:    token.GetStartOffset(),
						EndOffset:      token.GetEndOffset(),
						Position:       token.GetPosition(),
						PositionLength: token.GetPositionLength(),
						Hash:           token.GetHash(),
					}
				}),
			}
		})
		return err
	})

	return result, err
}

func expandWildcard(schema *entity.Schema, outputFields []string) ([]string, bool) {
	wildcard := false
	for _, outputField := range outputFields {
		if outputField == "*" {
			wildcard = true
		}
	}
	if !wildcard {
		return outputFields, false
	}

	set := make(map[string]struct{})
	result := make([]string, 0, len(schema.Fields))
	for _, field := range schema.Fields {
		result = append(result, field.Name)
		set[field.Name] = struct{}{}
	}

	// add dynamic fields output
	for _, output := range outputFields {
		if output == "*" {
			continue
		}
		_, ok := set[output]
		if !ok {
			result = append(result, output)
		}
	}
	return result, true
}
