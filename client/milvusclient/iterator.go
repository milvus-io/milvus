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
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

const (
	// IteratorKey is the const search param key in indicating enabling iterator.
	IteratorKey                = "iterator"
	IteratorSessionTsKey       = "iterator_session_ts"
	IteratorSearchV2Key        = "search_iter_v2"
	IteratorSearchBatchSizeKey = "search_iter_batch_size"
	IteratorSearchLastBoundKey = "search_iter_last_bound"
	IteratorSearchIDKey        = "search_iter_id"
	CollectionIDKey            = `collection_id`

	// Unlimited
	Unlimited int64 = -1
)

var ErrServerVersionIncompatible = errors.New("server version incompatible")

// SearchIterator is the interface for search iterator.
type SearchIterator interface {
	// Next returns next batch of iterator
	// when iterator reaches the end, return `io.EOF`.
	Next(ctx context.Context) (ResultSet, error)
}

type searchIteratorV2 struct {
	client *Client
	option SearchIteratorOption
	schema *entity.Schema
	limit  int64
}

func (it *searchIteratorV2) Next(ctx context.Context) (ResultSet, error) {
	// limit reached, return EOF
	if it.limit == 0 {
		return ResultSet{}, io.EOF
	}

	rs, err := it.next(ctx)
	if err != nil {
		return rs, err
	}

	if it.limit == Unlimited {
		return rs, err
	}

	if int64(rs.Len()) > it.limit {
		rs = rs.Slice(0, int(it.limit))
	}
	it.limit -= int64(rs.Len())
	return rs, nil
}

func (it *searchIteratorV2) next(ctx context.Context) (ResultSet, error) {
	opt := it.option.SearchOption()
	req, err := opt.Request()
	if err != nil {
		return ResultSet{}, err
	}

	var rs ResultSet

	err = it.client.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.Search(ctx, req)
		err = merr.CheckRPCCall(resp, err)
		if err != nil {
			return err
		}

		iteratorInfo := resp.GetResults().GetSearchIteratorV2Results()
		opt.annRequest.WithSearchParam(IteratorSearchIDKey, iteratorInfo.GetToken())
		opt.annRequest.WithSearchParam(IteratorSearchLastBoundKey, fmt.Sprintf("%v", iteratorInfo.GetLastBound()))

		resultSets, err := it.client.handleSearchResult(it.schema, req.GetOutputFields(), int(resp.GetResults().GetNumQueries()), resp)
		if err != nil {
			return err
		}
		rs = resultSets[0]

		if rs.IDs.Len() == 0 {
			return io.EOF
		}

		return nil
	})
	return rs, err
}

func (it *searchIteratorV2) setupCollectionID(ctx context.Context) error {
	opt := it.option.SearchOption()

	return it.client.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
			CollectionName: opt.collectionName,
		})
		if merr.CheckRPCCall(resp, err) != nil {
			return err
		}

		opt.WithSearchParam(CollectionIDKey, fmt.Sprintf("%d", resp.GetCollectionID()))
		schema := &entity.Schema{}
		it.schema = schema.ReadProto(resp.GetSchema())
		return nil
	})
}

// probeCompatiblity checks if the server support SearchIteratorV2.
// It checks if the search result contains search iterator v2 results info and token.
func (it *searchIteratorV2) probeCompatiblity(ctx context.Context) error {
	opt := it.option.SearchOption()
	opt.annRequest.topK = 1 // ok to leave it here, will be overwritten in next iteration
	opt.annRequest.WithSearchParam(IteratorSearchBatchSizeKey, "1")
	req, err := opt.Request()
	if err != nil {
		return err
	}
	return it.client.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.Search(ctx, req)
		err = merr.CheckRPCCall(resp, err)
		if err != nil {
			return err
		}

		if resp.GetResults().GetSearchIteratorV2Results() == nil || resp.GetResults().GetSearchIteratorV2Results().GetToken() == "" {
			return ErrServerVersionIncompatible
		}
		return nil
	})
}

// newSearchIteratorV2 creates a new search iterator V2.
//
// It sets up the collection ID and checks if the server supports search iterator V2.
// If the server does not support search iterator V2, it returns an error.
func newSearchIteratorV2(ctx context.Context, client *Client, option SearchIteratorOption) (*searchIteratorV2, error) {
	iter := &searchIteratorV2{
		client: client,
		option: option,
		limit:  option.Limit(),
	}
	if err := iter.setupCollectionID(ctx); err != nil {
		return nil, err
	}

	if err := iter.probeCompatiblity(ctx); err != nil {
		return nil, err
	}

	return iter, nil
}

type searchIteratorV1 struct {
	client *Client
}

func (s *searchIteratorV1) Next(_ context.Context) (ResultSet, error) {
	return ResultSet{}, errors.New("not implemented")
}

func newSearchIteratorV1(_ *Client) (*searchIteratorV1, error) {
	// search iterator v1 is not supported
	return nil, ErrServerVersionIncompatible
}

// SearchIterator creates a search iterator from a collection.
//
// If the server supports search iterator V2, it creates a search iterator V2.
func (c *Client) SearchIterator(ctx context.Context, option SearchIteratorOption, callOptions ...grpc.CallOption) (SearchIterator, error) {
	if err := option.ValidateParams(); err != nil {
		return nil, err
	}

	iter, err := newSearchIteratorV2(ctx, c, option)
	if err == nil {
		return iter, nil
	}

	if !errors.Is(err, ErrServerVersionIncompatible) {
		return nil, err
	}

	return newSearchIteratorV1(c)
}

// QueryIterator is the interface for query iterator.
type QueryIterator interface {
	// Next returns next batch of iterator
	// when iterator reaches the end, return `io.EOF`.
	Next(ctx context.Context) (ResultSet, error)
}

type queryIterator struct {
	client *Client
	option QueryIteratorOption
	schema *entity.Schema

	// pagination state
	expr         string   // base expression from option
	outputFields []string // override output fields(force include pk field)
	pkField      *entity.Field
	lastPK       any
	batchSize    int
	limit        int64

	// cached results
	cached ResultSet
}

// composeIteratorExpr builds the filter expression for pagination.
// It combines the user's original expression with a PK range filter.
func (it *queryIterator) composeIteratorExpr() string {
	if it.lastPK == nil {
		return it.expr
	}

	expr := strings.TrimSpace(it.expr)
	pkName := it.pkField.Name

	switch it.pkField.DataType {
	case entity.FieldTypeInt64:
		pkFilter := fmt.Sprintf("%s > %d", pkName, it.lastPK)
		if len(expr) == 0 {
			return pkFilter
		}
		return fmt.Sprintf("(%s) and %s", expr, pkFilter)
	case entity.FieldTypeVarChar:
		pkFilter := fmt.Sprintf(`%s > "%s"`, pkName, it.lastPK)
		if len(expr) == 0 {
			return pkFilter
		}
		return fmt.Sprintf(`(%s) and %s`, expr, pkFilter)
	default:
		return it.expr
	}
}

// fetchNextBatch fetches the next batch of data from the server.
func (it *queryIterator) fetchNextBatch(ctx context.Context) (ResultSet, error) {
	req, err := it.option.Request()
	if err != nil {
		return ResultSet{}, err
	}

	// override expression and limit for pagination
	req.Expr = it.composeIteratorExpr()
	req.OutputFields = it.outputFields
	req.QueryParams = append(req.QueryParams,
		&commonpb.KeyValuePair{Key: spLimit, Value: strconv.Itoa(it.batchSize)},
	)

	var resultSet ResultSet
	err = it.client.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.Query(ctx, req)
		err = merr.CheckRPCCall(resp, err)
		if err != nil {
			return err
		}

		columns, err := it.client.parseSearchResult(it.schema, resp.GetOutputFields(), resp.GetFieldsData(), 0, 0, -1)
		if err != nil {
			return err
		}
		resultSet = ResultSet{
			sch:    it.schema,
			Fields: columns,
		}
		if len(columns) > 0 {
			resultSet.ResultCount = columns[0].Len()
		}

		return nil
	})

	return resultSet, err
}

// cacheNextBatch returns the next batch and updates the cache.
func (it *queryIterator) cacheNextBatch(rs ResultSet) (ResultSet, error) {
	var result ResultSet
	if rs.ResultCount > it.batchSize {
		result = rs.Slice(0, it.batchSize)
		it.cached = rs.Slice(it.batchSize, rs.ResultCount)
	} else {
		result = rs
		it.cached = ResultSet{}
	}

	if result.ResultCount == 0 {
		return ResultSet{}, io.EOF
	}

	// extract and update the last PK for pagination
	pkColumn := result.GetColumn(it.pkField.Name)
	if pkColumn == nil {
		// try to find PK in Fields
		for _, col := range result.Fields {
			if col.Name() == it.pkField.Name {
				pkColumn = col
				break
			}
		}
	}

	if pkColumn != nil && pkColumn.Len() > 0 {
		pk, err := pkColumn.Get(pkColumn.Len() - 1)
		if err != nil {
			return ResultSet{}, errors.Wrapf(err, "failed to get last pk value")
		}
		it.lastPK = pk
	}

	return result, nil
}

// Next returns the next batch of results.
func (it *queryIterator) Next(ctx context.Context) (ResultSet, error) {
	// limit reached, return EOF
	if it.limit == 0 {
		return ResultSet{}, io.EOF
	}

	// if cache is empty, fetch new data
	if it.cached.ResultCount == 0 {
		rs, err := it.fetchNextBatch(ctx)
		if err != nil {
			return ResultSet{}, err
		}
		it.cached = rs
	}

	// if still no data, return EOF
	if it.cached.ResultCount == 0 {
		return ResultSet{}, io.EOF
	}

	result, err := it.cacheNextBatch(it.cached)
	if err != nil {
		return ResultSet{}, err
	}

	// handle overall limit
	if it.limit != Unlimited {
		if int64(result.ResultCount) > it.limit {
			result = result.Slice(0, int(it.limit))
		}
		it.limit -= int64(result.ResultCount)
	}

	return result, nil
}

// newQueryIterator creates a new query iterator.
func newQueryIterator(ctx context.Context, client *Client, option QueryIteratorOption) (*queryIterator, error) {
	req, err := option.Request()
	if err != nil {
		return nil, err
	}

	collection, err := client.getCollection(ctx, req.GetCollectionName())
	if err != nil {
		return nil, err
	}

	pkField := collection.Schema.PKField()
	if pkField == nil {
		return nil, errors.New("primary key field not found in schema")
	}

	// ensure PK field is included in output fields for pagination
	outputFields := req.GetOutputFields()
	hasPK := false
	for _, f := range outputFields {
		if f == pkField.Name {
			hasPK = true
			break
		}
	}
	if !hasPK && len(outputFields) > 0 {
		// modify the underlying option to include PK field
		outputFields = append(outputFields, pkField.Name)
	}

	iter := &queryIterator{
		client:       client,
		option:       option,
		schema:       collection.Schema,
		expr:         req.GetExpr(),
		outputFields: outputFields,
		pkField:      pkField,
		batchSize:    option.BatchSize(),
		limit:        option.Limit(),
	}

	// init: fetch the first batch to validate parameters
	rs, err := iter.fetchNextBatch(ctx)
	if err != nil {
		return nil, err
	}
	iter.cached = rs

	return iter, nil
}

// QueryIterator creates a query iterator from a collection.
func (c *Client) QueryIterator(ctx context.Context, option QueryIteratorOption, callOptions ...grpc.CallOption) (QueryIterator, error) {
	if err := option.ValidateParams(); err != nil {
		return nil, err
	}

	return newQueryIterator(ctx, c, option)
}
