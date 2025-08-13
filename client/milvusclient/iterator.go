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

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"

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
