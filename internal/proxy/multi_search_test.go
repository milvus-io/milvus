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

package proxy

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func newMultiSearchTestProxy(t *testing.T, queueCapacity, workers int) *Proxy {
	t.Helper()
	s := newMultiSearchScheduler(t.Context(), queueCapacity)
	s.start(workers)
	t.Cleanup(s.stop)
	p := &Proxy{multiSearch: s}
	p.UpdateStateCode(commonpb.StateCode_Healthy)
	return p
}

func multiSearchTestPrepare(collection string) MultiSearchPrepare {
	return func(context.Context) (*milvuspb.SearchRequest, error) {
		return &milvuspb.SearchRequest{DbName: "default", CollectionName: collection}, nil
	}
}

func TestMultiSearchKeepsInputOrder(t *testing.T) {
	p := newMultiSearchTestProxy(t, 1, 1)
	ctx, cancel := context.WithTimeout(metrics.WrapRestfulContext(t.Context(), 123), 5*time.Second)
	defer cancel()
	secondFinished := make(chan struct{})
	seen := make(chan *metrics.RPCStats, 2)
	prepare := []MultiSearchPrepare{
		multiSearchTestPrepare("slow"),
		multiSearchTestPrepare("denied"),
		func(context.Context) (*milvuspb.SearchRequest, error) {
			return nil, merr.WrapErrParameterMissingMsg("collection")
		},
	}
	results, err := p.RunMultiSearch(ctx, 2, prepare, func(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
		seen <- metrics.GetStats(ctx)
		if req.GetCollectionName() == "denied" {
			close(secondFinished)
			return nil, status.Error(codes.PermissionDenied, "search permission denied")
		}
		select {
		case <-secondFinished:
			return &milvuspb.SearchResults{Status: merr.Success(), CollectionName: "slow", Results: &schemapb.SearchResultData{
				Topks: []int64{0},
			}}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	require.NoError(t, err)
	require.Len(t, results, 3)
	require.Equal(t, "slow", results[0].GetCollectionName())
	require.Equal(t, []int64{0}, results[0].GetResults().GetTopks())
	require.ErrorIs(t, merr.Error(results[1].GetStatus()), merr.ErrPrivilegeNotPermitted)
	require.Nil(t, results[1].GetResults())
	require.ErrorIs(t, merr.Error(results[2].GetStatus()), merr.ErrParameterMissing)
	firstStats, secondStats := <-seen, <-seen
	require.NotSame(t, firstStats, secondStats)
	require.NotSame(t, metrics.GetStats(ctx), firstStats)
	require.NotSame(t, metrics.GetStats(ctx), secondStats)
}

func TestMultiSearchBoundsBatchAdmission(t *testing.T) {
	p := newMultiSearchTestProxy(t, 1, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	started := make(chan struct{})
	release := make(chan struct{})
	execute := func(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
		if req.GetCollectionName() == "first" {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return &milvuspb.SearchResults{Status: merr.Success()}, nil
	}
	done := make(chan error, 2)
	go func() {
		_, err := p.RunMultiSearch(ctx, 1, []MultiSearchPrepare{multiSearchTestPrepare("first")}, execute)
		done <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("first batch did not start")
	}
	go func() {
		_, err := p.RunMultiSearch(ctx, 1, []MultiSearchPrepare{multiSearchTestPrepare("second")}, execute)
		done <- err
	}()
	require.Eventually(t, func() bool { return p.CheckMultiSearchAdmission() != nil }, time.Second, time.Millisecond)

	ran := make(chan struct{}, 1)
	prepare := func(context.Context) (*milvuspb.SearchRequest, error) {
		ran <- struct{}{}
		return &milvuspb.SearchRequest{CollectionName: "third"}, nil
	}
	results, err := p.RunMultiSearch(ctx, 1, []MultiSearchPrepare{prepare}, execute)
	require.ErrorIs(t, err, merr.ErrServiceTooManyRequests)
	require.Empty(t, results)
	close(release)
	for range 2 {
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-ctx.Done():
			t.Fatal("accepted batch did not finish")
		}
	}
	require.Empty(t, ran)

	results, err = p.RunMultiSearch(ctx, 1, []MultiSearchPrepare{prepare}, execute)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.True(t, merr.Ok(results[0].GetStatus()))
}
