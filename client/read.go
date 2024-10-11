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

package client

import (
	"context"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

func (c *Client) Search(ctx context.Context, option SearchOption, callOptions ...grpc.CallOption) ([]ResultSet, error) {
	req := option.Request()
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
		resultSets, err = c.handleSearchResult(collection.Schema, req.GetOutputFields(), int(req.GetNq()), resp)

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
		rc := int(results.GetTopks()[i]) // result entry count for current query
		entry := ResultSet{
			ResultCount: rc,
			Scores:      results.GetScores()[offset : offset+rc],
		}

		entry.IDs, entry.Err = column.IDColumns(schema, results.GetIds(), offset, offset+rc)
		if entry.Err != nil {
			offset += rc
			continue
		}
		// parse group-by values
		if gb != nil {
			entry.GroupByValue, entry.Err = column.FieldDataColumn(gb, offset, offset+rc)
			if entry.Err != nil {
				offset += rc
				continue
			}
		}
		entry.Fields, entry.Err = c.parseSearchResult(schema, outputFields, fieldDataList, i, offset, offset+rc)
		sr = append(sr, entry)

		offset += rc
	}
	return sr, nil
}

func (c *Client) parseSearchResult(sch *entity.Schema, outputFields []string, fieldDataList []*schemapb.FieldData, _, from, to int) ([]column.Column, error) {
	var wildcard bool
	outputFields, wildcard = expandWildcard(sch, outputFields)
	// duplicated name will have only one column now
	outputSet := make(map[string]struct{})
	for _, output := range outputFields {
		outputSet[output] = struct{}{}
	}
	// fields := make(map[string]*schemapb.FieldData)
	columns := make([]column.Column, 0, len(outputFields))
	var dynamicColumn *column.ColumnJSONBytes
	for _, fieldData := range fieldDataList {
		col, err := column.FieldDataColumn(fieldData, from, to)
		if err != nil {
			return nil, err
		}
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

		// remove processed field
		delete(outputSet, fieldData.GetFieldName())

		columns = append(columns, col)
	}

	if len(outputSet) > 0 && dynamicColumn == nil {
		var extraFields []string
		for output := range outputSet {
			extraFields = append(extraFields, output)
		}
		return nil, errors.Newf("extra output fields %v found and result does not dynamic field", extraFields)
	}
	// add dynamic column for extra fields
	for outputField := range outputSet {
		column := column.NewColumnDynamic(dynamicColumn, outputField)
		columns = append(columns, column)
	}

	return columns, nil
}

func (c *Client) Query(ctx context.Context, option QueryOption, callOptions ...grpc.CallOption) (ResultSet, error) {
	req := option.Request()
	var resultSet ResultSet

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
			Fields: columns,
		}
		if len(columns) > 0 {
			resultSet.ResultCount = columns[0].Len()
		}

		return nil
	})
	return resultSet, err
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
