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
	"fmt"
	"io"

	"google.golang.org/protobuf/proto"

	commonpb "github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/fastpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ReduceStream is the common parent-side contract for reduced streams and
// transport-backed child streams. Exactly one caller must drive Recv, Close,
// and Interrupt serially.
type ReduceStream interface {
	Recv() (*internalpb.SearchResults, error)
	Close() error
	Interrupt() (*internalpb.SearchResults, error)
}

type childRecvCompletion struct {
	childIndex int
	chunk      *internalpb.SearchResults
	err        error
}

type orderedUnit struct {
	result     *internalpb.SearchResults
	data       *schemapb.SearchResultData
	queryIndex int64
	rowIndex   int64
}

func (u orderedUnit) score() float32 {
	return u.data.GetScores()[u.rowIndex]
}

func (u orderedUnit) pk() any {
	return typeutil.GetPK(u.data.GetIds(), u.rowIndex)
}

type orderedOutputBuffer struct {
	units []orderedUnit
}

func (b *orderedOutputBuffer) isEmpty() bool {
	return len(b.units) == 0
}

type orderedChildBuffer struct {
	units  []orderedUnit
	cursor int
}

func (b *orderedChildBuffer) hasUnit() bool {
	return b.cursor < len(b.units)
}

func (b *orderedChildBuffer) front() orderedUnit {
	return b.units[b.cursor]
}

func (b *orderedChildBuffer) pop() orderedUnit {
	unit := b.front()
	b.cursor++
	if !b.hasUnit() {
		b.units = nil
		b.cursor = 0
	}
	return unit
}

func (b *orderedChildBuffer) accept(chunk *internalpb.SearchResults, nq, topK int64) (*schemapb.SearchResultData, error) {
	data, err := decodeChunk(chunk, nq, topK)
	if err != nil {
		return nil, err
	}

	hitCount := typeutil.GetSizeOfIDs(data.GetIds())
	if len(data.GetScores()) != hitCount {
		return nil, fmt.Errorf("Search Chunk score count %d does not match hit count %d", len(data.GetScores()), hitCount)
	}

	totalTopK := int64(0)
	for _, count := range data.GetTopks() {
		if count < 0 {
			return nil, fmt.Errorf("Search Chunk contains a negative per-query hit count %d", count)
		}
		totalTopK += count
	}
	if totalTopK != int64(hitCount) {
		return nil, fmt.Errorf("Search Chunk Topks total %d does not match hit count %d", totalTopK, hitCount)
	}

	b.units = make([]orderedUnit, 0, hitCount)
	b.cursor = 0
	rowIndex := int64(0)
	for queryIndex, count := range data.GetTopks() {
		for i := int64(0); i < count; i++ {
			b.units = append(b.units, orderedUnit{
				result:     chunk,
				data:       data,
				queryIndex: int64(queryIndex),
				rowIndex:   rowIndex,
			})
			rowIndex++
		}
	}
	return data, nil
}

func decodeChunk(chunk *internalpb.SearchResults, nq, topK int64) (*schemapb.SearchResultData, error) {
	if chunk.GetNumQueries() != 0 && chunk.GetNumQueries() != nq {
		return nil, fmt.Errorf("Search Chunk nq %d does not match request nq %d", chunk.GetNumQueries(), nq)
	}
	if chunk.GetTopK() != 0 && chunk.GetTopK() != topK {
		return nil, fmt.Errorf("Search Chunk topK %d does not match request topK %d", chunk.GetTopK(), topK)
	}

	data := chunk.GetResultData()
	if data == nil && len(chunk.GetSlicedBlob()) > 0 {
		data = &schemapb.SearchResultData{}
		if err := fastpb.UnmarshalSearchResultData(chunk.GetSlicedBlob(), data); err != nil {
			return nil, fmt.Errorf("decode Search Chunk: %w", err)
		}
	}
	if data == nil {
		data = &schemapb.SearchResultData{
			NumQueries: nq,
			TopK:       topK,
			Topks:      make([]int64, nq),
		}
	}

	if data.GetNumQueries() != nq {
		return nil, fmt.Errorf("Search Chunk data nq %d does not match request nq %d", data.GetNumQueries(), nq)
	}
	if data.GetTopK() != topK {
		return nil, fmt.Errorf("Search Chunk data topK %d does not match request topK %d", data.GetTopK(), topK)
	}
	if len(data.GetTopks()) != int(nq) {
		return nil, fmt.Errorf("Search Chunk has %d per-query hit counts, expected %d", len(data.GetTopks()), nq)
	}
	return data, nil
}

// SplitSearchResult splits one complete Search result into query-major Chunks.
func SplitSearchResult(result *internalpb.SearchResults, chunkSize int) ([]*internalpb.SearchResults, error) {
	if result == nil {
		return nil, errors.New("SplitSearchResult requires a Search result")
	}
	if !merr.Ok(result.GetStatus()) {
		return nil, merr.Error(result.GetStatus())
	}
	if result.GetNumQueries() <= 0 {
		return nil, fmt.Errorf("SplitSearchResult requires a positive nq, got %d", result.GetNumQueries())
	}
	if result.GetTopK() <= 0 {
		return nil, fmt.Errorf("SplitSearchResult requires a positive topK, got %d", result.GetTopK())
	}
	if chunkSize <= 0 {
		return nil, fmt.Errorf("SplitSearchResult requires a positive Chunk size, got %d", chunkSize)
	}

	buffer := &orderedChildBuffer{}
	data, err := buffer.accept(result, result.GetNumQueries(), result.GetTopK())
	if err != nil {
		return nil, err
	}
	if !buffer.hasUnit() {
		chunk := proto.Clone(result).(*internalpb.SearchResults)
		chunk.SlicedBlob = nil
		chunk.SlicedNumCount = 0
		chunk.SlicedOffset = 0
		chunk.ResultData = proto.Clone(data).(*schemapb.SearchResultData)
		return []*internalpb.SearchResults{chunk}, nil
	}

	chunks := make([]*internalpb.SearchResults, 0, (len(buffer.units)+chunkSize-1)/chunkSize)
	for start := 0; start < len(buffer.units); start += chunkSize {
		end := min(start+chunkSize, len(buffer.units))
		chunk, err := buildSearchChunk(
			buffer.units[start:end],
			result.GetNumQueries(),
			result.GetTopK(),
			result.GetMetricType(),
			result.GetIsTopkReduce(),
			result.GetIsRecallEvaluation(),
		)
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, chunk)
	}

	first := chunks[0]
	first.SealedSegmentIDsSearched = append([]int64(nil), result.GetSealedSegmentIDsSearched()...)
	first.ChannelIDsSearched = append([]string(nil), result.GetChannelIDsSearched()...)
	first.GlobalSealedSegmentIDs = append([]int64(nil), result.GetGlobalSealedSegmentIDs()...)
	if result.GetCostAggregation() != nil {
		first.CostAggregation = proto.Clone(result.GetCostAggregation()).(*internalpb.CostAggregation)
	}
	if len(result.GetChannelsMvcc()) > 0 {
		first.ChannelsMvcc = make(map[string]uint64, len(result.GetChannelsMvcc()))
		for channel, timestamp := range result.GetChannelsMvcc() {
			first.ChannelsMvcc[channel] = timestamp
		}
	}
	first.ScannedRemoteBytes = result.GetScannedRemoteBytes()
	first.ScannedTotalBytes = result.GetScannedTotalBytes()
	first.FilterValidCounts = append([]int64(nil), result.GetFilterValidCounts()...)
	first.ResultData.AllSearchCount = data.GetAllSearchCount()
	return chunks, nil
}

// OrderedReduceStream merges Plain ANN Search child Chunks by score.
type OrderedReduceStream struct {
	childStreams         []ReduceStream
	childBuffers         []orderedChildBuffer
	childRecvTasks       []bool
	childDrained         []bool
	childRecvCompletions chan childRecvCompletion

	nq              int64
	topK            int64
	chunkSize       int
	metricType      string
	currentQuery    int64
	emittedPerQuery []int64
	isTopKReduce    bool
	isRecallEval    bool
	metadata        *internalpb.SearchResults
	metadataEmitted bool

	closed         bool
	finished       bool
	childrenClosed bool
	closeErr       error
}

// NewReduceStream creates the Plain ANN Search iterator OrderedReduceStream implementation.
func NewReduceStream(request *internalpb.SearchRequest, childStreams []ReduceStream, chunkSize int) (ReduceStream, error) {
	if request == nil {
		return nil, errors.New("NewReduceStream requires a Search request")
	}
	if request.GetNq() <= 0 {
		return nil, fmt.Errorf("NewReduceStream requires a positive nq, got %d", request.GetNq())
	}
	if request.GetTopk() <= 0 {
		return nil, fmt.Errorf("NewReduceStream requires a positive topK, got %d", request.GetTopk())
	}
	if request.GetIsAdvanced() || len(request.GetSubReqs()) > 0 || request.GetGroupByFieldId() > 0 || len(request.GetGroupByFieldIds()) > 0 {
		return nil, errors.New("NewReduceStream currently supports Plain ANN Search only")
	}
	if chunkSize <= 0 {
		return nil, fmt.Errorf("NewReduceStream requires a positive Chunk size, got %d", chunkSize)
	}
	for i, childStream := range childStreams {
		if childStream == nil {
			return nil, fmt.Errorf("NewReduceStream child stream %d is nil", i)
		}
	}

	return &OrderedReduceStream{
		childStreams:         append([]ReduceStream(nil), childStreams...),
		childBuffers:         make([]orderedChildBuffer, len(childStreams)),
		childRecvTasks:       make([]bool, len(childStreams)),
		childDrained:         make([]bool, len(childStreams)),
		childRecvCompletions: make(chan childRecvCompletion, max(1, len(childStreams))),
		nq:                   request.GetNq(),
		topK:                 request.GetTopk(),
		chunkSize:            chunkSize,
		metricType:           request.GetMetricType(),
		emittedPerQuery:      make([]int64, request.GetNq()),
	}, nil
}

// Recv returns the next ordered Search Chunk.
func (s *OrderedReduceStream) Recv() (*internalpb.SearchResults, error) {
	if s.finished {
		return nil, io.EOF
	}
	if s.closed {
		return nil, io.ErrClosedPipe
	}

	outputBuffer := s.allocBuffer()

	for !s.isChunkReady(outputBuffer) {
		readyBuffers, err := s.getReadyBuffers()
		if err != nil {
			return nil, s.fail(err)
		}

		oneReduceResult, err := s.produceNextUnits(readyBuffers)
		if err != nil {
			return nil, s.fail(err)
		}

		s.merge(outputBuffer, oneReduceResult)
	}

	if outputBuffer.isEmpty() {
		s.finished = true
		if s.metadata != nil && !s.metadataEmitted {
			chunk, err := s.createOutputChunk(outputBuffer)
			if err != nil {
				return nil, s.fail(err)
			}
			s.attachMetadata(chunk)
			if err := s.closeChildren(); err != nil {
				return nil, err
			}
			return chunk, nil
		}
		if err := s.closeChildren(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}

	chunk, err := s.createOutputChunk(outputBuffer)
	if err != nil {
		return nil, s.fail(err)
	}
	s.attachMetadata(chunk)
	return chunk, nil
}

func (s *OrderedReduceStream) allocBuffer() *orderedOutputBuffer {
	capacity := 0
	for queryIndex := s.currentQuery; queryIndex < s.nq && capacity < s.chunkSize; queryIndex++ {
		remaining := max(int64(0), s.topK-s.emittedPerQuery[queryIndex])
		capacity += min(s.chunkSize-capacity, int(remaining))
	}
	return &orderedOutputBuffer{
		units: make([]orderedUnit, 0, capacity),
	}
}

func (s *OrderedReduceStream) isChunkReady(outputBuffer *orderedOutputBuffer) bool {
	return len(outputBuffer.units) >= s.chunkSize || s.currentQuery >= s.nq
}

func (s *OrderedReduceStream) getReadyBuffers() ([]*orderedChildBuffer, error) {
	for {
		allReady := true
		for i, childStream := range s.childStreams {
			if s.childDrained[i] || s.childBuffers[i].hasUnit() {
				continue
			}

			allReady = false
			if s.childRecvTasks[i] {
				continue
			}

			s.childRecvTasks[i] = true
			completionChannel := s.childRecvCompletions
			go func(childIndex int) {
				chunk, err := childStream.Recv()
				completionChannel <- childRecvCompletion{
					childIndex: childIndex,
					chunk:      chunk,
					err:        err,
				}
			}(i)
		}

		if allReady {
			readyBuffers := make([]*orderedChildBuffer, 0, len(s.childBuffers))
			for i := range s.childBuffers {
				if s.childBuffers[i].hasUnit() {
					readyBuffers = append(readyBuffers, &s.childBuffers[i])
				}
			}
			return readyBuffers, nil
		}

		received := <-s.childRecvCompletions
		s.childRecvTasks[received.childIndex] = false

		if errors.Is(received.err, io.EOF) {
			s.childDrained[received.childIndex] = true
			continue
		}
		if received.err != nil {
			return nil, fmt.Errorf("child stream %d Recv failed: %w", received.childIndex, received.err)
		}
		if received.chunk == nil {
			return nil, fmt.Errorf("child stream %d returned a nil Chunk", received.childIndex)
		}
		if !merr.Ok(received.chunk.GetStatus()) {
			return nil, fmt.Errorf("child stream %d returned a failed Chunk: %w", received.childIndex, merr.Error(received.chunk.GetStatus()))
		}
		if s.metricType == "" {
			s.metricType = received.chunk.GetMetricType()
		}
		if received.chunk.GetMetricType() != "" && s.metricType != received.chunk.GetMetricType() {
			return nil, fmt.Errorf("child stream %d metric %q does not match %q", received.childIndex, received.chunk.GetMetricType(), s.metricType)
		}
		s.isTopKReduce = s.isTopKReduce || received.chunk.GetIsTopkReduce()
		s.isRecallEval = s.isRecallEval || received.chunk.GetIsRecallEvaluation()
		data, err := s.childBuffers[received.childIndex].accept(received.chunk, s.nq, s.topK)
		if err != nil {
			return nil, fmt.Errorf("child stream %d returned an invalid Chunk: %w", received.childIndex, err)
		}
		s.acceptMetadata(received.chunk, data)
	}
}

func (s *OrderedReduceStream) acceptMetadata(chunk *internalpb.SearchResults, data *schemapb.SearchResultData) {
	if s.metadataEmitted {
		return
	}
	if s.metadata == nil {
		emptyData := proto.Clone(data).(*schemapb.SearchResultData)
		emptyData.Scores = nil
		emptyData.Topks = make([]int64, s.nq)
		emptyData.FieldsData = typeutil.PrepareResultFieldData(data.GetFieldsData(), 0)
		emptyData.AllSearchCount = 0
		emptyData.Distances = nil
		emptyData.SearchIteratorV2Results = nil
		emptyData.Recalls = nil
		emptyData.HighlightResults = nil
		emptyData.GroupByFieldValue = nil
		emptyData.GroupByFieldValues = nil
		emptyData.AggBuckets = nil
		emptyData.AggTopks = nil
		if data.GetElementIndices() != nil {
			emptyData.ElementIndices = &schemapb.LongArray{}
		}
		switch data.GetIds().GetIdField().(type) {
		case *schemapb.IDs_IntId:
			emptyData.Ids = &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{}}}
		case *schemapb.IDs_StrId:
			emptyData.Ids = &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{}}}
		default:
			emptyData.Ids = &schemapb.IDs{}
		}

		s.metadata = &internalpb.SearchResults{
			Status:     merr.Success(),
			ReqID:      chunk.GetReqID(),
			MetricType: s.metricType,
			NumQueries: s.nq,
			TopK:       s.topK,
			ResultData: emptyData,
		}
		if chunk.GetBase() != nil {
			s.metadata.Base = proto.Clone(chunk.GetBase()).(*commonpb.MsgBase)
		}
	}

	s.metadata.SealedSegmentIDsSearched = append(s.metadata.SealedSegmentIDsSearched, chunk.GetSealedSegmentIDsSearched()...)
	s.metadata.ChannelIDsSearched = append(s.metadata.ChannelIDsSearched, chunk.GetChannelIDsSearched()...)
	s.metadata.GlobalSealedSegmentIDs = append(s.metadata.GlobalSealedSegmentIDs, chunk.GetGlobalSealedSegmentIDs()...)
	s.metadata.FilterValidCounts = append(s.metadata.FilterValidCounts, chunk.GetFilterValidCounts()...)

	if cost := chunk.GetCostAggregation(); cost != nil {
		totalRelatedDataSize := s.metadata.GetCostAggregation().GetTotalRelatedDataSize() + cost.GetTotalRelatedDataSize()
		if s.metadata.GetCostAggregation() == nil || s.metadata.GetCostAggregation().GetResponseTime() < cost.GetResponseTime() {
			s.metadata.CostAggregation = proto.Clone(cost).(*internalpb.CostAggregation)
		}
		s.metadata.CostAggregation.TotalRelatedDataSize = totalRelatedDataSize
	}

	if len(chunk.GetChannelsMvcc()) > 0 {
		if s.metadata.ChannelsMvcc == nil {
			s.metadata.ChannelsMvcc = make(map[string]uint64)
		}
		for channel, timestamp := range chunk.GetChannelsMvcc() {
			s.metadata.ChannelsMvcc[channel] = timestamp
		}
	}
	s.metadata.ScannedRemoteBytes += chunk.GetScannedRemoteBytes()
	s.metadata.ScannedTotalBytes += chunk.GetScannedTotalBytes()
	s.metadata.ResultData.AllSearchCount += data.GetAllSearchCount()
}

func (s *OrderedReduceStream) produceNextUnits(readyBuffers []*orderedChildBuffer) (*orderedUnit, error) {
	for s.currentQuery < s.nq {
		if s.emittedPerQuery[s.currentQuery] >= s.topK {
			if s.currentQuery == s.nq-1 {
				s.currentQuery++
				return nil, nil
			}

			for _, buffer := range readyBuffers {
				s.discardQuery(buffer, s.currentQuery)
			}
			for i := range s.childBuffers {
				if !s.childDrained[i] && !s.childBuffers[i].hasUnit() {
					return nil, nil
				}
			}
			s.currentQuery++
			continue
		}

		winner := -1
		for i, buffer := range readyBuffers {
			if !buffer.hasUnit() {
				continue
			}
			candidate := buffer.front()
			if candidate.queryIndex < s.currentQuery {
				return nil, fmt.Errorf("child buffer returned query %d after query %d started", candidate.queryIndex, s.currentQuery)
			}
			if candidate.queryIndex > s.currentQuery {
				continue
			}
			if winner == -1 {
				winner = i
				continue
			}

			selected := readyBuffers[winner].front()
			if candidate.score() > selected.score() ||
				(candidate.score() == selected.score() && typeutil.ComparePK(candidate.pk(), selected.pk())) {
				winner = i
			}
		}

		if winner == -1 {
			s.currentQuery++
			continue
		}

		oneReduceResult := s.pop(readyBuffers[winner])
		s.emittedPerQuery[s.currentQuery]++
		if s.emittedPerQuery[s.currentQuery] == s.topK && s.currentQuery == s.nq-1 {
			s.currentQuery++
		}
		return &oneReduceResult, nil
	}
	return nil, nil
}

func (s *OrderedReduceStream) merge(outputBuffer *orderedOutputBuffer, oneReduceResult *orderedUnit) {
	if oneReduceResult != nil {
		outputBuffer.units = append(outputBuffer.units, *oneReduceResult)
	}
}

func (s *OrderedReduceStream) pop(buffer *orderedChildBuffer) orderedUnit {
	return buffer.pop()
}

func (s *OrderedReduceStream) discardQuery(buffer *orderedChildBuffer, queryIndex int64) {
	for buffer.hasUnit() && buffer.front().queryIndex == queryIndex {
		s.pop(buffer)
	}
}

func (s *OrderedReduceStream) createOutputChunk(outputBuffer *orderedOutputBuffer) (*internalpb.SearchResults, error) {
	if outputBuffer.isEmpty() {
		data := &schemapb.SearchResultData{
			NumQueries: s.nq,
			TopK:       s.topK,
			Topks:      make([]int64, s.nq),
			Ids:        &schemapb.IDs{},
		}
		if s.metadata != nil && s.metadata.GetResultData() != nil {
			data = proto.Clone(s.metadata.GetResultData()).(*schemapb.SearchResultData)
		}
		return &internalpb.SearchResults{
			Status:             merr.Success(),
			MetricType:         s.metricType,
			NumQueries:         s.nq,
			TopK:               s.topK,
			ResultData:         data,
			IsTopkReduce:       s.isTopKReduce,
			IsRecallEvaluation: s.isRecallEval,
		}, nil
	}
	return buildSearchChunk(
		outputBuffer.units,
		s.nq,
		s.topK,
		s.metricType,
		s.isTopKReduce,
		s.isRecallEval,
	)
}

func (s *OrderedReduceStream) attachMetadata(chunk *internalpb.SearchResults) {
	if s.metadata == nil || s.metadataEmitted {
		return
	}

	if s.metadata.GetBase() != nil {
		chunk.Base = proto.Clone(s.metadata.GetBase()).(*commonpb.MsgBase)
	}
	chunk.ReqID = s.metadata.GetReqID()
	chunk.SealedSegmentIDsSearched = append([]int64(nil), s.metadata.GetSealedSegmentIDsSearched()...)
	chunk.ChannelIDsSearched = append([]string(nil), s.metadata.GetChannelIDsSearched()...)
	chunk.GlobalSealedSegmentIDs = append([]int64(nil), s.metadata.GetGlobalSealedSegmentIDs()...)
	if s.metadata.GetCostAggregation() != nil {
		chunk.CostAggregation = proto.Clone(s.metadata.GetCostAggregation()).(*internalpb.CostAggregation)
	}
	if len(s.metadata.GetChannelsMvcc()) > 0 {
		chunk.ChannelsMvcc = make(map[string]uint64, len(s.metadata.GetChannelsMvcc()))
		for channel, timestamp := range s.metadata.GetChannelsMvcc() {
			chunk.ChannelsMvcc[channel] = timestamp
		}
	}
	chunk.ScannedRemoteBytes = s.metadata.GetScannedRemoteBytes()
	chunk.ScannedTotalBytes = s.metadata.GetScannedTotalBytes()
	chunk.FilterValidCounts = append([]int64(nil), s.metadata.GetFilterValidCounts()...)
	chunk.ResultData.AllSearchCount = s.metadata.GetResultData().GetAllSearchCount()
	s.metadataEmitted = true
	s.metadata = nil
}

func buildSearchChunk(
	units []orderedUnit,
	nq int64,
	topK int64,
	metricType string,
	isTopKReduce bool,
	isRecallEvaluation bool,
) (*internalpb.SearchResults, error) {
	first := units[0]
	templateFields := first.data.GetFieldsData()
	for _, unit := range units {
		if len(unit.data.GetFieldsData()) > 0 {
			templateFields = unit.data.GetFieldsData()
			break
		}
	}

	data := &schemapb.SearchResultData{
		NumQueries:       nq,
		TopK:             topK,
		FieldsData:       typeutil.PrepareResultFieldData(templateFields, int64(len(units))),
		Scores:           make([]float32, 0, len(units)),
		Ids:              &schemapb.IDs{},
		Topks:            make([]int64, nq),
		OutputFields:     append([]string(nil), first.data.GetOutputFields()...),
		PrimaryFieldName: first.data.GetPrimaryFieldName(),
	}
	fieldIndexComputers := make(map[*schemapb.SearchResultData]*typeutil.FieldDataIdxComputer)

	for _, unit := range units {
		if len(data.FieldsData) > 0 {
			if len(unit.data.GetFieldsData()) != len(data.FieldsData) {
				return nil, fmt.Errorf("Search Chunk field count %d does not match output field count %d", len(unit.data.GetFieldsData()), len(data.FieldsData))
			}
			computer := fieldIndexComputers[unit.data]
			if computer == nil {
				computer = typeutil.NewFieldDataIdxComputer(unit.data.GetFieldsData())
				fieldIndexComputers[unit.data] = computer
			}
			fieldIndexes := computer.Compute(unit.rowIndex)
			typeutil.AppendFieldData(data.FieldsData, unit.data.GetFieldsData(), unit.rowIndex, fieldIndexes...)
		}

		typeutil.AppendPKs(data.Ids, unit.pk())
		data.Scores = append(data.Scores, unit.score())
		data.Topks[unit.queryIndex]++
		if unit.data.GetElementIndices() != nil {
			if data.ElementIndices == nil {
				data.ElementIndices = &schemapb.LongArray{Data: make([]int64, 0, len(units))}
			}
			data.ElementIndices.Data = append(data.ElementIndices.Data, unit.data.GetElementIndices().GetData()[unit.rowIndex])
		}
	}

	return &internalpb.SearchResults{
		Base:               first.result.GetBase(),
		Status:             merr.Success(),
		ReqID:              first.result.GetReqID(),
		MetricType:         metricType,
		NumQueries:         nq,
		TopK:               topK,
		ResultData:         data,
		IsTopkReduce:       isTopKReduce,
		IsRecallEvaluation: isRecallEvaluation,
	}, nil
}

func (s *OrderedReduceStream) fail(err error) error {
	return errors.Join(err, s.Close())
}

// Close idempotently closes every child stream and releases retained Chunks.
func (s *OrderedReduceStream) Close() error {
	if s.closed {
		return s.closeErr
	}
	s.closed = true
	return s.closeChildren()
}

func (s *OrderedReduceStream) closeChildren() error {
	if s.childrenClosed {
		return s.closeErr
	}
	s.childrenClosed = true

	closeErrors := make([]error, 0, len(s.childStreams))
	for i := range s.childStreams {
		if err := s.childStreams[i].Close(); err != nil {
			closeErrors = append(closeErrors, fmt.Errorf("close child stream %d: %w", i, err))
		}
		s.childBuffers[i].units = nil
		s.childBuffers[i].cursor = 0
		s.childRecvTasks[i] = false
	}
	s.metadata = nil
	s.closeErr = errors.Join(closeErrors...)
	return s.closeErr
}

// Interrupt is reserved for the bidirectional gRPC integration.
func (s *OrderedReduceStream) Interrupt() (*internalpb.SearchResults, error) {
	return nil, errors.New("OrderedReduceStream Interrupt is not implemented")
}
