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
	"maps"
	"math"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/internal/merr"
)

const (
	// IteratorKey is the const search param key in indicating enabling iterator.
	IteratorKey                    = "iterator"
	IteratorSessionTsKey           = "iterator_session_ts"
	IteratorSearchV2Key            = "search_iter_v2"
	IteratorSearchBatchSizeKey     = "search_iter_batch_size"
	IteratorSearchLastBoundKey     = "search_iter_last_bound"
	IteratorSearchIDKey            = "search_iter_id"
	CollectionIDKey                = `collection_id`
	IteratorSearchCursorVersionKey = "search_iter_cursor_version"
	IteratorSearchLastPKTypeKey    = "search_iter_last_pk_type"
	IteratorSearchLastPKKey        = "search_iter_last_pk"

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
	client      *Client
	request     *milvuspb.SearchRequest
	callOptions []grpc.CallOption
	schema      *entity.Schema
	limit       int64
	cached      *ResultSet
	finished    bool
	cursorMode  string
	seenPKs     map[any]struct{}
}

func (it *searchIteratorV2) Next(ctx context.Context) (ResultSet, error) {
	if it.finished || it.limit == 0 {
		return ResultSet{}, io.EOF
	}
	var rs ResultSet
	for {
		if it.cached != nil {
			rs = *it.cached
			it.cached = nil
		} else {
			var err error
			rs, err = it.next(ctx)
			if err != nil {
				return ResultSet{}, err
			}
		}
		if rs.Len() != 0 || it.cursorMode != "2" {
			break
		}
	}
	if it.limit != Unlimited {
		if int64(rs.Len()) > it.limit {
			rs = rs.Slice(0, int(it.limit))
		}
		it.limit -= int64(rs.Len())
	}
	return rs, nil
}

// Every page uses a private request. Commit its cursor only after the entire
// response has been decoded and validated, so retrying a failed page cannot skip it.
func (it *searchIteratorV2) next(ctx context.Context) (ResultSet, error) {
	req := proto.Clone(it.request).(*milvuspb.SearchRequest)
	var resp *milvuspb.SearchResults
	err := it.client.callService(func(service milvuspb.MilvusServiceClient) error {
		var err error
		resp, err = service.Search(ctx, req, it.callOptions...)
		return merr.CheckRPCCall(resp, err)
	})
	if err != nil {
		return ResultSet{}, err
	}
	data := resp.GetResults()
	version := resp.GetStatus().GetExtraInfo()[IteratorSearchCursorVersionKey]
	if version != "" && version != "2" {
		return ResultSet{}, merr.WrapErrServiceInternal("unsupported search iterator cursor version %q", version)
	}
	info := data.GetSearchIteratorV2Results()
	if info == nil || info.GetToken() == "" {
		if version == "2" {
			return ResultSet{}, merr.WrapErrServiceInternal("PK cursor response has no Search Iterator V2 token")
		}
		return ResultSet{}, errors.Wrap(ErrServerVersionIncompatible, "server does not return Search Iterator V2 metadata")
	}
	if version == "2" && searchIteratorParam(req, IteratorSearchCursorVersionKey) != "2" {
		return ResultSet{}, merr.WrapErrServiceInternal("server activated PK cursor mode without client opt-in")
	}
	mode := version
	if mode == "" {
		mode = "legacy"
	}
	if it.cursorMode != "" && mode != it.cursorMode {
		return ResultSet{}, merr.WrapErrServiceInternal("search iterator cursor mode changed between pages")
	}
	if previousToken := searchIteratorParam(req, IteratorSearchIDKey); previousToken != "" && previousToken != info.GetToken() {
		return ResultSet{}, merr.WrapErrServiceInternal("search iterator token changed between pages")
	}
	// Keep legacy result decoding compatible; strict raw cursor validation is
	// required only after primary-key pagination is negotiated.
	if len(data.GetTopks()) == 0 || data.GetTopks()[0] < 0 {
		return ResultSet{}, merr.WrapErrServiceInternal("invalid search iterator result shape")
	}
	count := int(data.GetTopks()[0])
	if len(data.GetScores()) < count {
		return ResultSet{}, merr.WrapErrServiceInternal("invalid search iterator scores or bound")
	}
	pkType := ""
	if version == "2" {
		if data.GetNumQueries() != 1 || len(data.GetTopks()) != 1 || data.GetTopks()[0] > iteratorSearchBatchSize(req) {
			return ResultSet{}, merr.WrapErrServiceInternal("invalid search iterator result shape")
		}
		if len(data.GetScores()) != count || math.IsNaN(float64(info.GetLastBound())) || math.IsInf(float64(info.GetLastBound()), 0) {
			return ResultSet{}, merr.WrapErrServiceInternal("invalid search iterator scores or bound")
		}
		for _, score := range data.GetScores() {
			if math.IsNaN(float64(score)) || math.IsInf(float64(score), 0) {
				return ResultSet{}, merr.WrapErrServiceInternal("non-finite search iterator score")
			}
		}
		pk := it.schema.PKField()
		if pk == nil {
			return ResultSet{}, merr.WrapErrServiceInternal("search iterator collection has no primary key")
		}
		switch pk.DataType {
		case entity.FieldTypeInt64:
			pkType = "int64"
			if (count > 0 && data.GetIds().GetIntId() == nil) || len(data.GetIds().GetIntId().GetData()) != count {
				return ResultSet{}, merr.WrapErrServiceInternal("invalid search iterator int64 primary keys")
			}
		case entity.FieldTypeVarChar:
			pkType = "varchar"
			if (count > 0 && data.GetIds().GetStrId() == nil) || len(data.GetIds().GetStrId().GetData()) != count {
				return ResultSet{}, merr.WrapErrServiceInternal("invalid search iterator varchar primary keys")
			}
		default:
			return ResultSet{}, merr.WrapErrServiceInternal("unsupported search iterator primary key type")
		}
	}
	var rs ResultSet
	if count > 0 {
		// The shared decoder slices scores for each declared query. Check its
		// bounds without imposing PK-mode shape constraints on legacy replies.
		remainingScores := int64(len(data.GetScores()))
		for i, topk := range data.GetTopks() {
			if int64(i) >= data.GetNumQueries() {
				break
			}
			if topk < 0 || topk > remainingScores {
				return ResultSet{}, merr.WrapErrServiceInternal("invalid search iterator score bounds")
			}
			remainingScores -= topk
		}
		sets, err := it.client.handleSearchResult(it.schema, req.GetOutputFields(), 1, resp)
		if err != nil {
			return ResultSet{}, err
		}
		if len(sets) != 1 {
			return ResultSet{}, merr.WrapErrServiceInternal("invalid search iterator result count")
		}
		rs = sets[0]
		if rs.Err != nil {
			return ResultSet{}, rs.Err
		}
	}
	if version == "2" {
		if count > 0 && resp.GetStatus().GetExtraInfo()[IteratorSearchLastPKTypeKey] != pkType {
			return ResultSet{}, merr.WrapErrServiceInternal("search iterator cursor primary key type does not match schema")
		}
		if count > 0 {
			lastPK := ""
			if pkType == "int64" {
				lastPK = strconv.FormatInt(data.GetIds().GetIntId().GetData()[count-1], 10)
			} else {
				lastPK = data.GetIds().GetStrId().GetData()[count-1]
			}
			returnedPK, hasPK := resp.GetStatus().GetExtraInfo()[IteratorSearchLastPKKey]
			if !hasPK || returnedPK != lastPK || info.GetLastBound() != data.GetScores()[count-1] {
				return ResultSet{}, merr.WrapErrServiceInternal("search iterator cursor does not match last result")
			}
			setSearchIteratorParam(req, IteratorSearchLastPKTypeKey, pkType)
			setSearchIteratorParam(req, IteratorSearchLastPKKey, lastPK)
		}
	} else {
		deleteSearchIteratorParam(req, IteratorSearchCursorVersionKey)
		deleteSearchIteratorParam(req, IteratorSearchLastPKTypeKey)
		deleteSearchIteratorParam(req, IteratorSearchLastPKKey)
	}
	var acceptedPKs map[any]struct{}
	if version == "2" && count > 0 {
		var err error
		rs, acceptedPKs, err = distinctIteratorResults(rs, it.seenPKs)
		if err != nil {
			return ResultSet{}, err
		}
	}
	if req.GetGuaranteeTimestamp() == 0 {
		timestamp := resp.GetSessionTs()
		if timestamp == 0 && version == "2" {
			return ResultSet{}, merr.WrapErrServiceInternal("search iterator PK cursor response has no snapshot timestamp")
		}
		// Legacy V2 servers that omit session_ts retain their previous live-read semantics.
		req.GuaranteeTimestamp = timestamp
	}
	setSearchIteratorParam(req, IteratorSearchIDKey, info.GetToken())
	setSearchIteratorParam(req, IteratorSearchLastBoundKey, strconv.FormatFloat(float64(info.GetLastBound()), 'g', -1, 32))
	it.request, it.cursorMode = req, mode
	if len(acceptedPKs) > 0 {
		if it.seenPKs == nil {
			it.seenPKs = make(map[any]struct{}, len(acceptedPKs))
		}
		maps.Copy(it.seenPKs, acceptedPKs)
	}
	if count == 0 {
		it.finished = true
		return ResultSet{}, io.EOF
	}
	return rs, nil
}

// Select complete rows before committing either the raw cursor or accepted PKs.
// The raw cursor advances even when all rows have already been returned.
func distinctIteratorResults(rs ResultSet, seen map[any]struct{}) (ResultSet, map[any]struct{}, error) {
	keys := make(map[any]struct{}, rs.Len())
	indices := make([]int, 0, rs.Len())
	for i := 0; i < rs.Len(); i++ {
		pk, err := rs.IDs.Get(i)
		if err != nil {
			return ResultSet{}, nil, err
		}
		if _, exists := seen[pk]; exists {
			continue
		}
		if _, exists := keys[pk]; !exists {
			keys[pk] = struct{}{}
			indices = append(indices, i)
		}
	}
	if len(indices) == rs.Len() {
		return rs, keys, nil
	}
	selectColumn := func(source column.Column) (column.Column, error) {
		if source == nil {
			return nil, nil
		}
		result := source.Slice(0, 0)
		for _, i := range indices {
			null, err := source.IsNull(i)
			if err != nil {
				return nil, err
			}
			if null {
				err = result.AppendNull()
			} else {
				reader := source
				if dynamic, ok := source.(*column.ColumnDynamic); ok {
					reader = dynamic.ColumnJSONBytes
				}
				var value any
				value, err = reader.Get(i)
				if err == nil {
					err = result.AppendValue(value)
				}
			}
			if err != nil {
				return nil, err
			}
		}
		return result, nil
	}
	result := rs
	var err error
	result.IDs, err = selectColumn(rs.IDs)
	if err != nil {
		return ResultSet{}, nil, err
	}
	result.Fields = make(DataSet, len(rs.Fields))
	for i, field := range rs.Fields {
		result.Fields[i], err = selectColumn(field)
		if err != nil {
			return ResultSet{}, nil, err
		}
	}
	result.GroupByValue, err = selectColumn(rs.GroupByValue)
	if err != nil {
		return ResultSet{}, nil, err
	}
	result.Scores = make([]float32, len(indices))
	for i, index := range indices {
		result.Scores[i] = rs.Scores[index]
	}
	result.ResultCount = len(indices)
	return result, keys, nil
}

func iteratorSearchBatchSize(req *milvuspb.SearchRequest) int64 {
	batchSize, _ := strconv.ParseInt(searchIteratorParam(req, IteratorSearchBatchSizeKey), 10, 64)
	return batchSize
}

func searchIteratorParam(req *milvuspb.SearchRequest, key string) string {
	for _, pair := range req.GetSearchParams() {
		if pair.GetKey() == key {
			return pair.GetValue()
		}
	}
	return ""
}

func setSearchIteratorParam(req *milvuspb.SearchRequest, key, value string) {
	deleteSearchIteratorParam(req, key)
	req.SearchParams = append(req.SearchParams, &commonpb.KeyValuePair{Key: key, Value: value})
}

func deleteSearchIteratorParam(req *milvuspb.SearchRequest, key string) {
	params := req.SearchParams[:0]
	for _, pair := range req.SearchParams {
		if pair.GetKey() != key {
			params = append(params, pair)
		}
	}
	req.SearchParams = params
}

func (it *searchIteratorV2) setupCollectionID(ctx context.Context) error {
	return it.client.callService(func(service milvuspb.MilvusServiceClient) error {
		resp, err := service.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
			CollectionName: it.request.GetCollectionName(),
		}, it.callOptions...)
		if err := merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		setSearchIteratorParam(it.request, CollectionIDKey, strconv.FormatInt(resp.GetCollectionID(), 10))
		it.schema = (&entity.Schema{}).ReadProto(resp.GetSchema())
		return nil
	})
}

// Constructor fetches the real first page and caches it for Next. Negotiation
// and parameter checking therefore share the RPC that produces useful results.
func newSearchIteratorV2(ctx context.Context, client *Client, option SearchIteratorOption, callOptions ...grpc.CallOption) (*searchIteratorV2, error) {
	// SearchOption resets batch parameters; perform that operation on a copy,
	// so constructing or advancing an iterator never changes caller options.
	var opt *searchOption
	if original, ok := option.(*searchIteratorOption); ok {
		copy := *original
		search := *original.searchOption
		ann := *search.annRequest
		ann.searchParam = maps.Clone(ann.searchParam)
		search.annRequest = &ann
		copy.searchOption = &search
		opt = copy.SearchOption()
	} else {
		opt = option.SearchOption()
	}
	req, err := opt.Request()
	if err != nil {
		return nil, err
	}
	iter := &searchIteratorV2{
		client: client, request: proto.Clone(req).(*milvuspb.SearchRequest),
		limit: option.Limit(), callOptions: append([]grpc.CallOption(nil), callOptions...),
	}
	requestedVersion := searchIteratorParam(iter.request, IteratorSearchCursorVersionKey)
	if requestedVersion != "" && requestedVersion != "2" {
		return nil, merr.WrapErrParameterInvalidMsg("unsupported search iterator cursor version %q", requestedVersion)
	}
	legacyCursor := searchIteratorParam(iter.request, IteratorSearchIDKey) != "" || searchIteratorParam(iter.request, IteratorSearchLastBoundKey) != ""
	if requestedVersion == "2" && legacyCursor {
		return nil, merr.WrapErrParameterInvalidMsg("PK cursor mode requires a complete typed cursor; legacy token/bound continuation must omit cursor version 2")
	}
	if requestedVersion != "2" || legacyCursor {
		iter.cursorMode = "legacy"
		deleteSearchIteratorParam(iter.request, IteratorSearchCursorVersionKey)
		deleteSearchIteratorParam(iter.request, IteratorSearchLastPKTypeKey)
		deleteSearchIteratorParam(iter.request, IteratorSearchLastPKKey)
	} else {
		setSearchIteratorParam(iter.request, IteratorSearchCursorVersionKey, "2")
	}
	if iter.limit == 0 {
		iter.finished = true
		return iter, nil
	}
	if err := iter.setupCollectionID(ctx); err != nil {
		return nil, err
	}
	rs, err := iter.next(ctx)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if err == nil {
		iter.cached = &rs
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

// SearchIterator creates a search iterator from a collection. Distance-only
// pagination is the default. Set search_iter_cursor_version to "2" with
// WithSearchParam to opt into score/primary-key pagination on supporting servers.
//
// If the server supports search iterator V2, it creates a search iterator V2.
func (c *Client) SearchIterator(ctx context.Context, option SearchIteratorOption, callOptions ...grpc.CallOption) (SearchIterator, error) {
	if err := option.ValidateParams(); err != nil {
		return nil, err
	}

	iter, err := newSearchIteratorV2(ctx, c, option, callOptions...)
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
