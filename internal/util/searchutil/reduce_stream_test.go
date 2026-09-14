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

package searchutil

import (
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	commonpb "github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type fakeStreamRecv struct {
	chunk *internalpb.SearchResults
	err   error
}

type fakeReduceStream struct {
	mu         sync.Mutex
	recv       []fakeStreamRecv
	closeErr   error
	recvCalls  int
	closeCalls int
}

func (s *fakeReduceStream) Recv() (*internalpb.SearchResults, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recvCalls++
	if len(s.recv) == 0 {
		return nil, io.EOF
	}
	next := s.recv[0]
	s.recv = s.recv[1:]
	return next.chunk, next.err
}

func (s *fakeReduceStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeCalls++
	return s.closeErr
}

func (s *fakeReduceStream) Interrupt() (*internalpb.SearchResults, error) {
	return nil, errors.New("not implemented")
}

func (s *fakeReduceStream) calls() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.recvCalls, s.closeCalls
}

type blockingReduceStream struct {
	mu         sync.Mutex
	started    chan struct{}
	release    <-chan struct{}
	chunk      *internalpb.SearchResults
	recvCalled bool
	closeCalls int
}

func (s *blockingReduceStream) Recv() (*internalpb.SearchResults, error) {
	s.mu.Lock()
	if s.recvCalled {
		s.mu.Unlock()
		return nil, io.EOF
	}
	s.recvCalled = true
	close(s.started)
	s.mu.Unlock()

	<-s.release
	return s.chunk, nil
}

func (s *blockingReduceStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeCalls++
	return nil
}

func (s *blockingReduceStream) Interrupt() (*internalpb.SearchResults, error) {
	return nil, errors.New("not implemented")
}

type testHit struct {
	id    int64
	score float32
}

type retainedInputStats struct {
	chunks        int
	bufferedUnits int
	unreadUnits   int
	protobufBytes int
}

func assertRetainedInputBound(t *testing.T, stream *OrderedReduceStream) retainedInputStats {
	t.Helper()

	stats := retainedInputStats{}
	for i := range stream.childBuffers {
		buffer := &stream.childBuffers[i]
		require.LessOrEqual(t, len(buffer.units), stream.chunkSize)
		if !buffer.hasUnit() {
			continue
		}

		chunk := buffer.front().result
		for _, unit := range buffer.units {
			require.Same(t, chunk, unit.result)
		}
		stats.chunks++
		stats.bufferedUnits += len(buffer.units)
		stats.unreadUnits += len(buffer.units) - buffer.cursor
		stats.protobufBytes += proto.Size(chunk)
	}

	require.LessOrEqual(t, stats.chunks, len(stream.childStreams))
	require.LessOrEqual(t, stats.bufferedUnits, stream.chunkSize*len(stream.childStreams))
	require.LessOrEqual(t, stats.unreadUnits, stream.chunkSize*len(stream.childStreams))
	return stats
}

func TestOrderedReduceStreamMergesANNHits(t *testing.T) {
	left := &fakeReduceStream{recv: []fakeStreamRecv{
		{chunk: newSearchChunk(1, 4, []testHit{{id: 1, score: 0.95}, {id: 4, score: 0.70}})},
		{chunk: newSearchChunk(1, 4, []testHit{{id: 7, score: 0.60}})},
	}}
	right := &fakeReduceStream{recv: []fakeStreamRecv{
		{chunk: newSearchChunk(1, 4, []testHit{{id: 2, score: 0.90}, {id: 3, score: 0.80}, {id: 8, score: 0.50}})},
	}}

	stream, err := NewReduceStream(
		&internalpb.SearchRequest{Nq: 1, Topk: 4, MetricType: "IP", IsIterator: true},
		[]ReduceStream{left, right},
		2,
	)
	require.NoError(t, err)

	assertSearchChunk(t, recvChunk(t, stream), []int64{1, 2}, []float32{0.95, 0.90}, []int64{2})
	assertSearchChunk(t, recvChunk(t, stream), []int64{3, 4}, []float32{0.80, 0.70}, []int64{2})

	chunk, err := stream.Recv()
	require.Nil(t, chunk)
	require.ErrorIs(t, err, io.EOF)

	leftRecv, leftClose := left.calls()
	rightRecv, rightClose := right.calls()
	require.Equal(t, 1, leftRecv, "final topK should cancel the unread child tail")
	require.Equal(t, 1, rightRecv)
	require.Equal(t, 1, leftClose)
	require.Equal(t, 1, rightClose)
}

func TestOrderedReduceStreamReturnsPartialFinalChunk(t *testing.T) {
	left := &fakeReduceStream{recv: []fakeStreamRecv{{chunk: newSearchChunk(1, 3,
		[]testHit{{id: 1, score: 0.9}, {id: 3, score: 0.7}})}}}
	right := &fakeReduceStream{recv: []fakeStreamRecv{{chunk: newSearchChunk(1, 3,
		[]testHit{{id: 2, score: 0.8}})}}}

	stream, err := NewReduceStream(
		&internalpb.SearchRequest{Nq: 1, Topk: 3, MetricType: "IP", IsIterator: true},
		[]ReduceStream{left, right},
		2,
	)
	require.NoError(t, err)

	assertSearchChunk(t, recvChunk(t, stream), []int64{1, 2}, []float32{0.9, 0.8}, []int64{2})
	assertSearchChunk(t, recvChunk(t, stream), []int64{3}, []float32{0.7}, []int64{1})
}

func TestOrderedReduceStreamAllocBufferUsesRemainingUnits(t *testing.T) {
	stream := &OrderedReduceStream{
		nq:              2,
		topK:            5,
		chunkSize:       1024,
		emittedPerQuery: []int64{4, 3},
	}
	require.Equal(t, 3, cap(stream.allocBuffer().units))

	stream.chunkSize = 2
	require.Equal(t, 2, cap(stream.allocBuffer().units))
}

func TestOrderedReduceStreamMergesMultipleQueries(t *testing.T) {
	left := &fakeReduceStream{recv: []fakeStreamRecv{{chunk: newSearchChunk(2, 2,
		[]testHit{{id: 1, score: 0.90}},
		[]testHit{{id: 10, score: 0.80}},
	)}}}
	right := &fakeReduceStream{recv: []fakeStreamRecv{
		{chunk: newSearchChunk(2, 2,
			[]testHit{{id: 2, score: 0.85}, {id: 3, score: 0.70}},
			nil,
		)},
		{chunk: newSearchChunk(2, 2,
			nil,
			[]testHit{{id: 11, score: 0.95}, {id: 12, score: 0.60}},
		)},
	}}

	stream, err := NewReduceStream(
		&internalpb.SearchRequest{Nq: 2, Topk: 2, MetricType: "IP", IsIterator: true},
		[]ReduceStream{left, right},
		3,
	)
	require.NoError(t, err)

	assertSearchChunk(t, recvChunk(t, stream), []int64{1, 2, 11}, []float32{0.90, 0.85, 0.95}, []int64{2, 1})
	assertSearchChunk(t, recvChunk(t, stream), []int64{10}, []float32{0.80}, []int64{0, 1})
}

func TestOrderedReduceStreamBreaksScoreTiesByPK(t *testing.T) {
	left := &fakeReduceStream{recv: []fakeStreamRecv{{chunk: newSearchChunk(1, 2,
		[]testHit{{id: 5, score: 0.9}})}}}
	right := &fakeReduceStream{recv: []fakeStreamRecv{{chunk: newSearchChunk(1, 2,
		[]testHit{{id: 2, score: 0.9}})}}}

	stream, err := NewReduceStream(
		&internalpb.SearchRequest{Nq: 1, Topk: 2, MetricType: "IP", IsIterator: true},
		[]ReduceStream{left, right},
		2,
	)
	require.NoError(t, err)

	assertSearchChunk(t, recvChunk(t, stream), []int64{2, 5}, []float32{0.9, 0.9}, []int64{2})
}

func TestOrderedReduceStreamComposesReducedChildStreams(t *testing.T) {
	request := &internalpb.SearchRequest{Nq: 1, Topk: 3, MetricType: "IP", IsIterator: true}
	left, err := NewReduceStream(request, []ReduceStream{
		&fakeReduceStream{recv: []fakeStreamRecv{{chunk: newSearchChunk(1, 3,
			[]testHit{{id: 1, score: 0.95}, {id: 5, score: 0.50}})}}},
		&fakeReduceStream{recv: []fakeStreamRecv{{chunk: newSearchChunk(1, 3,
			[]testHit{{id: 3, score: 0.75}})}}},
	}, 2)
	require.NoError(t, err)

	right, err := NewReduceStream(request, []ReduceStream{
		&fakeReduceStream{recv: []fakeStreamRecv{{chunk: newSearchChunk(1, 3,
			[]testHit{{id: 2, score: 0.90}})}}},
		&fakeReduceStream{recv: []fakeStreamRecv{{chunk: newSearchChunk(1, 3,
			[]testHit{{id: 4, score: 0.70}})}}},
	}, 2)
	require.NoError(t, err)

	stream, err := NewReduceStream(request, []ReduceStream{left, right}, 2)
	require.NoError(t, err)

	assertSearchChunk(t, recvChunk(t, stream), []int64{1, 2}, []float32{0.95, 0.90}, []int64{2})
	assertSearchChunk(t, recvChunk(t, stream), []int64{3}, []float32{0.75}, []int64{1})
}

func TestOrderedReduceStreamRetainsOneChunkPerChild(t *testing.T) {
	request := &internalpb.SearchRequest{Nq: 1, Topk: 8, MetricType: "IP", IsIterator: true}
	leftHighFirst := newSearchChunk(1, 8, []testHit{{id: 1, score: 0.99}, {id: 2, score: 0.98}})
	leftHighSecond := newSearchChunk(1, 8, []testHit{{id: 3, score: 0.95}, {id: 4, score: 0.94}})
	leftOtherChunk := newSearchChunk(1, 8, []testHit{{id: 5, score: 0.80}, {id: 6, score: 0.79}})
	rightHighChunk := newSearchChunk(1, 8, []testHit{{id: 7, score: 0.70}, {id: 8, score: 0.69}})
	rightOtherChunk := newSearchChunk(1, 8, []testHit{{id: 9, score: 0.60}, {id: 10, score: 0.59}})

	leftHigh := &fakeReduceStream{recv: []fakeStreamRecv{{chunk: leftHighFirst}, {chunk: leftHighSecond}}}
	leftOther := &fakeReduceStream{recv: []fakeStreamRecv{{chunk: leftOtherChunk}}}
	rightHigh := &fakeReduceStream{recv: []fakeStreamRecv{{chunk: rightHighChunk}}}
	rightOther := &fakeReduceStream{recv: []fakeStreamRecv{{chunk: rightOtherChunk}}}

	leftStream, err := NewReduceStream(request, []ReduceStream{leftHigh, leftOther}, 2)
	require.NoError(t, err)
	left := leftStream.(*OrderedReduceStream)
	rightStream, err := NewReduceStream(request, []ReduceStream{rightHigh, rightOther}, 2)
	require.NoError(t, err)
	right := rightStream.(*OrderedReduceStream)

	leftReady, err := left.getReadyBuffers()
	require.NoError(t, err)
	require.Len(t, leftReady, 2)
	leftStats := assertRetainedInputBound(t, left)
	require.Equal(t, retainedInputStats{
		chunks:        2,
		bufferedUnits: 4,
		unreadUnits:   4,
		protobufBytes: proto.Size(leftHighFirst) + proto.Size(leftOtherChunk),
	}, leftStats)

	rightReady, err := right.getReadyBuffers()
	require.NoError(t, err)
	require.Len(t, rightReady, 2)
	rightStats := assertRetainedInputBound(t, right)
	require.Equal(t, retainedInputStats{
		chunks:        2,
		bufferedUnits: 4,
		unreadUnits:   4,
		protobufBytes: proto.Size(rightHighChunk) + proto.Size(rightOtherChunk),
	}, rightStats)

	finalStream, err := NewReduceStream(request, []ReduceStream{left, right}, 2)
	require.NoError(t, err)
	final := finalStream.(*OrderedReduceStream)
	readyBuffers, err := final.getReadyBuffers()
	require.NoError(t, err)
	require.Len(t, readyBuffers, 2)
	require.Equal(t, retainedInputStats{chunks: 1, bufferedUnits: 2, unreadUnits: 2, protobufBytes: proto.Size(leftOtherChunk)}, assertRetainedInputBound(t, left))
	require.Equal(t, retainedInputStats{chunks: 1, bufferedUnits: 2, unreadUnits: 2, protobufBytes: proto.Size(rightOtherChunk)}, assertRetainedInputBound(t, right))
	finalStats := assertRetainedInputBound(t, final)
	require.Equal(t, 2, finalStats.chunks)
	require.Equal(t, 4, finalStats.bufferedUnits)
	require.Equal(t, 4, finalStats.unreadUnits)
	require.Positive(t, finalStats.protobufBytes)

	outputBuffer := final.allocBuffer()
	for range 2 {
		unit, err := final.produceNextUnits(readyBuffers)
		require.NoError(t, err)
		require.NotNil(t, unit)
		final.merge(outputBuffer, unit)
	}
	require.True(t, final.isChunkReady(outputBuffer))
	finalStats = assertRetainedInputBound(t, final)
	require.Equal(t, 1, finalStats.chunks)
	require.Equal(t, 2, finalStats.bufferedUnits)
	require.Equal(t, 2, finalStats.unreadUnits)

	readyBuffers, err = final.getReadyBuffers()
	require.NoError(t, err)
	require.Len(t, readyBuffers, 2)
	finalStats = assertRetainedInputBound(t, final)
	require.Equal(t, 2, finalStats.chunks)
	require.Equal(t, 4, finalStats.bufferedUnits)
	require.Equal(t, 4, finalStats.unreadUnits)
	leftHighRecv, _ := leftHigh.calls()
	leftOtherRecv, _ := leftOther.calls()
	rightHighRecv, _ := rightHigh.calls()
	rightOtherRecv, _ := rightOther.calls()
	require.Equal(t, 2, leftHighRecv)
	require.Equal(t, 1, leftOtherRecv)
	require.Equal(t, 1, rightHighRecv)
	require.Equal(t, 1, rightOtherRecv)

	require.NoError(t, final.Close())
	require.Equal(t, retainedInputStats{}, assertRetainedInputBound(t, final))
	require.Equal(t, retainedInputStats{}, assertRetainedInputBound(t, left))
	require.Equal(t, retainedInputStats{}, assertRetainedInputBound(t, right))
	for _, child := range []*fakeReduceStream{leftHigh, leftOther, rightHigh, rightOther} {
		_, closeCalls := child.calls()
		require.Equal(t, 1, closeCalls)
	}
}

func TestOrderedReduceStreamComposesMetadata(t *testing.T) {
	request := &internalpb.SearchRequest{Nq: 1, Topk: 4, MetricType: "IP", IsIterator: true}
	leftFirst := newSearchChunk(1, 4, []testHit{{id: 1, score: 0.95}})
	leftFirst.CostAggregation = &internalpb.CostAggregation{ResponseTime: 10, TotalRelatedDataSize: 10}
	leftFirst.ChannelsMvcc = map[string]uint64{"channel-a": 100}
	leftFirst.ScannedRemoteBytes = 1
	leftFirst.ScannedTotalBytes = 2
	leftFirst.ResultData.AllSearchCount = 3
	leftSecond := newSearchChunk(1, 4, []testHit{{id: 3, score: 0.75}})
	leftSecond.CostAggregation = &internalpb.CostAggregation{ResponseTime: 20, TotalRelatedDataSize: 20}
	leftSecond.ChannelsMvcc = map[string]uint64{"channel-b": 200}
	leftSecond.ScannedRemoteBytes = 4
	leftSecond.ScannedTotalBytes = 5
	leftSecond.ResultData.AllSearchCount = 6
	left, err := NewReduceStream(request, []ReduceStream{
		&fakeReduceStream{recv: []fakeStreamRecv{{chunk: leftFirst}}},
		&fakeReduceStream{recv: []fakeStreamRecv{{chunk: leftSecond}}},
	}, 2)
	require.NoError(t, err)

	rightFirst := newSearchChunk(1, 4, []testHit{{id: 2, score: 0.90}})
	rightFirst.CostAggregation = &internalpb.CostAggregation{ResponseTime: 30, TotalRelatedDataSize: 30}
	rightFirst.ChannelsMvcc = map[string]uint64{"channel-c": 300}
	rightFirst.ScannedRemoteBytes = 7
	rightFirst.ScannedTotalBytes = 8
	rightFirst.ResultData.AllSearchCount = 9
	rightSecond := newSearchChunk(1, 4, []testHit{{id: 4, score: 0.70}})
	rightSecond.CostAggregation = &internalpb.CostAggregation{ResponseTime: 40, TotalRelatedDataSize: 40}
	rightSecond.ChannelsMvcc = map[string]uint64{"channel-d": 400}
	rightSecond.ScannedRemoteBytes = 10
	rightSecond.ScannedTotalBytes = 11
	rightSecond.ResultData.AllSearchCount = 12
	right, err := NewReduceStream(request, []ReduceStream{
		&fakeReduceStream{recv: []fakeStreamRecv{{chunk: rightFirst}}},
		&fakeReduceStream{recv: []fakeStreamRecv{{chunk: rightSecond}}},
	}, 2)
	require.NoError(t, err)

	stream, err := NewReduceStream(request, []ReduceStream{left, right}, 4)
	require.NoError(t, err)
	chunk := recvChunk(t, stream)
	assertSearchChunk(t, chunk, []int64{1, 2, 3, 4}, []float32{0.95, 0.90, 0.75, 0.70}, []int64{4})
	require.Equal(t, int64(40), chunk.GetCostAggregation().GetResponseTime())
	require.Equal(t, int64(100), chunk.GetCostAggregation().GetTotalRelatedDataSize())
	require.Equal(t, map[string]uint64{
		"channel-a": 100,
		"channel-b": 200,
		"channel-c": 300,
		"channel-d": 400,
	}, chunk.GetChannelsMvcc())
	require.Equal(t, int64(22), chunk.GetScannedRemoteBytes())
	require.Equal(t, int64(26), chunk.GetScannedTotalBytes())
	require.Equal(t, int64(30), chunk.GetResultData().GetAllSearchCount())
}

func TestOrderedReduceStreamUsesInternalScoreOrderForL2(t *testing.T) {
	leftChunk := newSearchChunk(1, 2, []testHit{{id: 1, score: -0.10}})
	leftChunk.MetricType = "L2"
	rightChunk := newSearchChunk(1, 2, []testHit{{id: 2, score: -0.20}})
	rightChunk.MetricType = "L2"

	stream, err := NewReduceStream(
		&internalpb.SearchRequest{Nq: 1, Topk: 2, MetricType: "L2", IsIterator: true},
		[]ReduceStream{
			&fakeReduceStream{recv: []fakeStreamRecv{{chunk: leftChunk}}},
			&fakeReduceStream{recv: []fakeStreamRecv{{chunk: rightChunk}}},
		},
		2,
	)
	require.NoError(t, err)

	assertSearchChunk(t, recvChunk(t, stream), []int64{1, 2}, []float32{-0.10, -0.20}, []int64{2})
}

func TestOrderedReduceStreamAcceptsEncodedChunk(t *testing.T) {
	chunk := newSearchChunk(1, 1, []testHit{{id: 1, score: 0.9}})
	blob, err := proto.Marshal(chunk.GetResultData())
	require.NoError(t, err)
	chunk.ResultData = nil
	chunk.SlicedBlob = blob

	stream, err := NewReduceStream(
		&internalpb.SearchRequest{Nq: 1, Topk: 1, MetricType: "IP", IsIterator: true},
		[]ReduceStream{&fakeReduceStream{recv: []fakeStreamRecv{{chunk: chunk}}}},
		1,
	)
	require.NoError(t, err)
	assertSearchChunk(t, recvChunk(t, stream), []int64{1}, []float32{0.9}, []int64{1})
}

func TestSplitSearchResultPreservesQueryBoundaries(t *testing.T) {
	result := newSearchChunk(2, 3,
		[]testHit{{id: 1, score: 0.9}, {id: 2, score: 0.8}, {id: 3, score: 0.7}},
		[]testHit{{id: 10, score: 0.95}, {id: 11, score: 0.85}},
	)

	chunks, err := SplitSearchResult(result, 2)
	require.NoError(t, err)
	require.Len(t, chunks, 3)
	assertSearchChunk(t, chunks[0], []int64{1, 2}, []float32{0.9, 0.8}, []int64{2, 0})
	assertSearchChunk(t, chunks[1], []int64{3, 10}, []float32{0.7, 0.95}, []int64{1, 1})
	assertSearchChunk(t, chunks[2], []int64{11}, []float32{0.85}, []int64{0, 1})
}

func TestSplitSearchResultEmitsMetadataOnce(t *testing.T) {
	result := newSearchChunk(1, 3,
		[]testHit{{id: 1, score: 0.9}, {id: 2, score: 0.8}, {id: 3, score: 0.7}},
	)
	result.Base = &commonpb.MsgBase{SourceID: 100}
	result.ReqID = 200
	result.SealedSegmentIDsSearched = []int64{11, 12}
	result.ChannelIDsSearched = []string{"channel-a"}
	result.GlobalSealedSegmentIDs = []int64{21}
	result.CostAggregation = &internalpb.CostAggregation{
		ResponseTime:         10,
		ServiceTime:          8,
		TotalNQ:              4,
		TotalRelatedDataSize: 100,
	}
	result.ChannelsMvcc = map[string]uint64{"channel-a": 1000}
	result.ScannedRemoteBytes = 30
	result.ScannedTotalBytes = 40
	result.FilterValidCounts = []int64{5, 6}
	result.ResultData.AllSearchCount = 50

	chunks, err := SplitSearchResult(result, 1)
	require.NoError(t, err)
	require.Len(t, chunks, 3)

	require.Equal(t, int64(100), chunks[0].GetBase().GetSourceID())
	require.Equal(t, int64(200), chunks[0].GetReqID())
	require.Equal(t, []int64{11, 12}, chunks[0].GetSealedSegmentIDsSearched())
	require.Equal(t, []string{"channel-a"}, chunks[0].GetChannelIDsSearched())
	require.Equal(t, []int64{21}, chunks[0].GetGlobalSealedSegmentIDs())
	require.True(t, proto.Equal(result.GetCostAggregation(), chunks[0].GetCostAggregation()))
	require.Equal(t, map[string]uint64{"channel-a": 1000}, chunks[0].GetChannelsMvcc())
	require.Equal(t, int64(30), chunks[0].GetScannedRemoteBytes())
	require.Equal(t, int64(40), chunks[0].GetScannedTotalBytes())
	require.Equal(t, []int64{5, 6}, chunks[0].GetFilterValidCounts())
	require.Equal(t, int64(50), chunks[0].GetResultData().GetAllSearchCount())

	for _, chunk := range chunks[1:] {
		require.Empty(t, chunk.GetSealedSegmentIDsSearched())
		require.Empty(t, chunk.GetChannelIDsSearched())
		require.Empty(t, chunk.GetGlobalSealedSegmentIDs())
		require.Nil(t, chunk.GetCostAggregation())
		require.Empty(t, chunk.GetChannelsMvcc())
		require.Zero(t, chunk.GetScannedRemoteBytes())
		require.Zero(t, chunk.GetScannedTotalBytes())
		require.Empty(t, chunk.GetFilterValidCounts())
		require.Zero(t, chunk.GetResultData().GetAllSearchCount())
	}
}

func TestSplitSearchResultEmitsEmptyChunkWithMetadata(t *testing.T) {
	result := newSearchChunk(1, 2)
	result.CostAggregation = &internalpb.CostAggregation{TotalRelatedDataSize: 100}
	result.ChannelsMvcc = map[string]uint64{"channel-a": 1000}
	result.ScannedRemoteBytes = 30
	result.ScannedTotalBytes = 40
	result.ResultData.AllSearchCount = 50

	chunks, err := SplitSearchResult(result, 2)
	require.NoError(t, err)
	require.Len(t, chunks, 1)
	require.Empty(t, chunks[0].GetResultData().GetIds().GetIntId().GetData())
	require.Equal(t, []int64{0}, chunks[0].GetResultData().GetTopks())
	require.Equal(t, int64(100), chunks[0].GetCostAggregation().GetTotalRelatedDataSize())
	require.Equal(t, map[string]uint64{"channel-a": 1000}, chunks[0].GetChannelsMvcc())
	require.Equal(t, int64(30), chunks[0].GetScannedRemoteBytes())
	require.Equal(t, int64(40), chunks[0].GetScannedTotalBytes())
	require.Equal(t, int64(50), chunks[0].GetResultData().GetAllSearchCount())
}

func TestOrderedReduceStreamAggregatesMetadataOnce(t *testing.T) {
	leftChunk := newSearchChunk(1, 3,
		[]testHit{{id: 1, score: 0.9}, {id: 3, score: 0.7}},
	)
	leftChunk.SealedSegmentIDsSearched = []int64{11}
	leftChunk.ChannelIDsSearched = []string{"channel-a"}
	leftChunk.GlobalSealedSegmentIDs = []int64{21}
	leftChunk.CostAggregation = &internalpb.CostAggregation{
		ResponseTime:         10,
		ServiceTime:          8,
		TotalNQ:              4,
		TotalRelatedDataSize: 100,
	}
	leftChunk.ChannelsMvcc = map[string]uint64{"channel-a": 1000}
	leftChunk.ScannedRemoteBytes = 30
	leftChunk.ScannedTotalBytes = 40
	leftChunk.FilterValidCounts = []int64{5}
	leftChunk.ResultData.AllSearchCount = 50

	rightChunk := newSearchChunk(1, 3,
		[]testHit{{id: 2, score: 0.8}},
	)
	rightChunk.SealedSegmentIDsSearched = []int64{12}
	rightChunk.ChannelIDsSearched = []string{"channel-b"}
	rightChunk.GlobalSealedSegmentIDs = []int64{22}
	rightChunk.CostAggregation = &internalpb.CostAggregation{
		ResponseTime:         20,
		ServiceTime:          18,
		TotalNQ:              6,
		TotalRelatedDataSize: 200,
	}
	rightChunk.ChannelsMvcc = map[string]uint64{"channel-b": 2000}
	rightChunk.ScannedRemoteBytes = 50
	rightChunk.ScannedTotalBytes = 60
	rightChunk.FilterValidCounts = []int64{6}
	rightChunk.ResultData.AllSearchCount = 70

	stream, err := NewReduceStream(
		&internalpb.SearchRequest{Nq: 1, Topk: 3, MetricType: "IP", IsIterator: true},
		[]ReduceStream{
			&fakeReduceStream{recv: []fakeStreamRecv{{chunk: leftChunk}}},
			&fakeReduceStream{recv: []fakeStreamRecv{{chunk: rightChunk}}},
		},
		2,
	)
	require.NoError(t, err)

	first := recvChunk(t, stream)
	assertSearchChunk(t, first, []int64{1, 2}, []float32{0.9, 0.8}, []int64{2})
	require.ElementsMatch(t, []int64{11, 12}, first.GetSealedSegmentIDsSearched())
	require.ElementsMatch(t, []string{"channel-a", "channel-b"}, first.GetChannelIDsSearched())
	require.ElementsMatch(t, []int64{21, 22}, first.GetGlobalSealedSegmentIDs())
	require.Equal(t, int64(20), first.GetCostAggregation().GetResponseTime())
	require.Equal(t, int64(18), first.GetCostAggregation().GetServiceTime())
	require.Equal(t, int64(6), first.GetCostAggregation().GetTotalNQ())
	require.Equal(t, int64(300), first.GetCostAggregation().GetTotalRelatedDataSize())
	require.Equal(t, map[string]uint64{"channel-a": 1000, "channel-b": 2000}, first.GetChannelsMvcc())
	require.Equal(t, int64(80), first.GetScannedRemoteBytes())
	require.Equal(t, int64(100), first.GetScannedTotalBytes())
	require.ElementsMatch(t, []int64{5, 6}, first.GetFilterValidCounts())
	require.Equal(t, int64(120), first.GetResultData().GetAllSearchCount())

	second := recvChunk(t, stream)
	assertSearchChunk(t, second, []int64{3}, []float32{0.7}, []int64{1})
	require.Empty(t, second.GetSealedSegmentIDsSearched())
	require.Empty(t, second.GetChannelIDsSearched())
	require.Empty(t, second.GetGlobalSealedSegmentIDs())
	require.Nil(t, second.GetCostAggregation())
	require.Empty(t, second.GetChannelsMvcc())
	require.Zero(t, second.GetScannedRemoteBytes())
	require.Zero(t, second.GetScannedTotalBytes())
	require.Empty(t, second.GetFilterValidCounts())
	require.Zero(t, second.GetResultData().GetAllSearchCount())
}

func TestOrderedReduceStreamEmitsMetadataForEmptyResults(t *testing.T) {
	leftChunk := newSearchChunk(1, 1)
	leftChunk.CostAggregation = &internalpb.CostAggregation{
		ResponseTime:         10,
		TotalRelatedDataSize: 100,
	}
	leftChunk.ChannelsMvcc = map[string]uint64{"channel-a": 1000}
	leftChunk.ScannedRemoteBytes = 30
	leftChunk.ScannedTotalBytes = 40
	leftChunk.ResultData.AllSearchCount = 50

	rightChunk := newSearchChunk(1, 1)
	rightChunk.CostAggregation = &internalpb.CostAggregation{
		ResponseTime:         20,
		TotalRelatedDataSize: 200,
	}
	rightChunk.ChannelsMvcc = map[string]uint64{"channel-b": 2000}
	rightChunk.ScannedRemoteBytes = 50
	rightChunk.ScannedTotalBytes = 60
	rightChunk.ResultData.AllSearchCount = 70

	stream, err := NewReduceStream(
		&internalpb.SearchRequest{Nq: 1, Topk: 1, MetricType: "IP", IsIterator: true},
		[]ReduceStream{
			&fakeReduceStream{recv: []fakeStreamRecv{{chunk: leftChunk}}},
			&fakeReduceStream{recv: []fakeStreamRecv{{chunk: rightChunk}}},
		},
		1,
	)
	require.NoError(t, err)

	chunk := recvChunk(t, stream)
	require.Empty(t, chunk.GetResultData().GetIds().GetIntId().GetData())
	require.Equal(t, []int64{0}, chunk.GetResultData().GetTopks())
	require.Equal(t, int64(20), chunk.GetCostAggregation().GetResponseTime())
	require.Equal(t, int64(300), chunk.GetCostAggregation().GetTotalRelatedDataSize())
	require.Equal(t, map[string]uint64{"channel-a": 1000, "channel-b": 2000}, chunk.GetChannelsMvcc())
	require.Equal(t, int64(80), chunk.GetScannedRemoteBytes())
	require.Equal(t, int64(100), chunk.GetScannedTotalBytes())
	require.Equal(t, int64(120), chunk.GetResultData().GetAllSearchCount())

	chunk, err = stream.Recv()
	require.Nil(t, chunk)
	require.ErrorIs(t, err, io.EOF)
}

func TestOrderedReduceStreamStartsMissingChildReceivesConcurrently(t *testing.T) {
	release := make(chan struct{})
	left := &blockingReduceStream{
		started: make(chan struct{}),
		release: release,
		chunk:   newSearchChunk(1, 2, []testHit{{id: 1, score: 0.9}}),
	}
	right := &blockingReduceStream{
		started: make(chan struct{}),
		release: release,
		chunk:   newSearchChunk(1, 2, []testHit{{id: 2, score: 0.8}}),
	}
	stream, err := NewReduceStream(
		&internalpb.SearchRequest{Nq: 1, Topk: 2, MetricType: "IP", IsIterator: true},
		[]ReduceStream{left, right},
		2,
	)
	require.NoError(t, err)

	type recvResult struct {
		chunk *internalpb.SearchResults
		err   error
	}
	received := make(chan recvResult, 1)
	go func() {
		chunk, err := stream.Recv()
		received <- recvResult{chunk: chunk, err: err}
	}()

	for _, started := range []chan struct{}{left.started, right.started} {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("child Recv did not start concurrently")
		}
	}
	close(release)

	result := <-received
	require.NoError(t, result.err)
	assertSearchChunk(t, result.chunk, []int64{1, 2}, []float32{0.9, 0.8}, []int64{2})
	require.NoError(t, stream.Close())
}

func TestOrderedReduceStreamClosesChildrenOnRecvError(t *testing.T) {
	recvErr := errors.New("recv failed")
	left := &fakeReduceStream{recv: []fakeStreamRecv{{err: recvErr}}}
	right := &fakeReduceStream{recv: []fakeStreamRecv{{chunk: newSearchChunk(1, 2,
		[]testHit{{id: 2, score: 0.8}})}}}
	stream, err := NewReduceStream(
		&internalpb.SearchRequest{Nq: 1, Topk: 2, MetricType: "IP", IsIterator: true},
		[]ReduceStream{left, right},
		2,
	)
	require.NoError(t, err)

	chunk, err := stream.Recv()
	require.Nil(t, chunk)
	require.ErrorIs(t, err, recvErr)
	_, leftClose := left.calls()
	_, rightClose := right.calls()
	require.Equal(t, 1, leftClose)
	require.Equal(t, 1, rightClose)
}

func TestOrderedReduceStreamCloseIsIdempotent(t *testing.T) {
	closeErr := errors.New("close failed")
	child := &fakeReduceStream{closeErr: closeErr}
	stream, err := NewReduceStream(
		&internalpb.SearchRequest{Nq: 1, Topk: 1, IsIterator: true},
		[]ReduceStream{child},
		1,
	)
	require.NoError(t, err)

	require.ErrorIs(t, stream.Close(), closeErr)
	require.ErrorIs(t, stream.Close(), closeErr)
	_, closeCalls := child.calls()
	require.Equal(t, 1, closeCalls)

	chunk, err := stream.Recv()
	require.Nil(t, chunk)
	require.ErrorIs(t, err, io.ErrClosedPipe)
}

func TestNewReduceStreamValidatesPlainANNSearch(t *testing.T) {
	child := &fakeReduceStream{}

	_, err := NewReduceStream(nil, []ReduceStream{child}, 1)
	require.ErrorContains(t, err, "requires a Search request")

	_, err = NewReduceStream(&internalpb.SearchRequest{Topk: 1, IsIterator: true}, []ReduceStream{child}, 1)
	require.ErrorContains(t, err, "positive nq")

	_, err = NewReduceStream(&internalpb.SearchRequest{Nq: 1, IsIterator: true}, []ReduceStream{child}, 1)
	require.ErrorContains(t, err, "positive topK")

	_, err = NewReduceStream(&internalpb.SearchRequest{Nq: 1, Topk: 1, IsIterator: true, IsAdvanced: true}, []ReduceStream{child}, 1)
	require.ErrorContains(t, err, "Plain ANN Search only")

	_, err = NewReduceStream(&internalpb.SearchRequest{Nq: 1, Topk: 1, IsIterator: true, GroupByFieldIds: []int64{101}}, []ReduceStream{child}, 1)
	require.ErrorContains(t, err, "Plain ANN Search only")

	stream, err := NewReduceStream(&internalpb.SearchRequest{Nq: 1, Topk: 1}, []ReduceStream{child}, 1)
	require.NoError(t, err)
	require.NoError(t, stream.Close())

	_, err = NewReduceStream(&internalpb.SearchRequest{Nq: 1, Topk: 1, IsIterator: true}, []ReduceStream{child}, 0)
	require.ErrorContains(t, err, "positive Chunk size")

	_, err = NewReduceStream(&internalpb.SearchRequest{Nq: 1, Topk: 1, IsIterator: true}, []ReduceStream{nil}, 1)
	require.ErrorContains(t, err, "child stream 0 is nil")
}

func TestOrderedReduceStreamInterruptIsUnimplemented(t *testing.T) {
	stream, err := NewReduceStream(&internalpb.SearchRequest{Nq: 1, Topk: 1, IsIterator: true}, nil, 1)
	require.NoError(t, err)

	metadata, err := stream.Interrupt()
	require.Nil(t, metadata)
	require.ErrorContains(t, err, "Interrupt is not implemented")
}

func recvChunk(t *testing.T, stream ReduceStream) *internalpb.SearchResults {
	t.Helper()
	chunk, err := stream.Recv()
	require.NoError(t, err)
	return chunk
}

func assertSearchChunk(t *testing.T, chunk *internalpb.SearchResults, ids []int64, scores []float32, topks []int64) {
	t.Helper()
	require.True(t, merr.Ok(chunk.GetStatus()))
	require.Equal(t, ids, chunk.GetResultData().GetIds().GetIntId().GetData())
	require.Equal(t, scores, chunk.GetResultData().GetScores())
	require.Equal(t, topks, chunk.GetResultData().GetTopks())
	values := make([]int64, len(ids))
	for i, id := range ids {
		values[i] = id * 10
	}
	require.Equal(t, values, chunk.GetResultData().GetFieldsData()[0].GetScalars().GetLongData().GetData())
}

func newSearchChunk(nq, topK int64, hitsByQuery ...[]testHit) *internalpb.SearchResults {
	topks := make([]int64, nq)
	ids := make([]int64, 0)
	scores := make([]float32, 0)
	values := make([]int64, 0)
	for queryIndex := int64(0); queryIndex < nq; queryIndex++ {
		var hits []testHit
		if int(queryIndex) < len(hitsByQuery) {
			hits = hitsByQuery[queryIndex]
		}
		topks[queryIndex] = int64(len(hits))
		for _, hit := range hits {
			ids = append(ids, hit.id)
			scores = append(scores, hit.score)
			values = append(values, hit.id*10)
		}
	}

	return &internalpb.SearchResults{
		Status:     merr.Success(),
		MetricType: "IP",
		NumQueries: nq,
		TopK:       topK,
		ResultData: &schemapb.SearchResultData{
			NumQueries: nq,
			TopK:       topK,
			Topks:      topks,
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: ids},
				},
			},
			Scores: scores,
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Int64,
					FieldName: "value",
					FieldId:   101,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: values},
							},
						},
					},
				},
			},
		},
	}
}
