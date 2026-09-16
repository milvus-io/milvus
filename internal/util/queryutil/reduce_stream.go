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

package queryutil

import (
	"errors"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	commonpb "github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ReduceStream is the Query counterpart of the Search ReduceStream contract.
type ReduceStream interface {
	Recv() (*internalpb.RetrieveResults, error)
	Close() error
	Interrupt() (*internalpb.RetrieveResults, error)
}

type childRecvCompletion struct {
	childIndex int
	chunk      *internalpb.RetrieveResults
	err        error
}

type retrieveUnit struct {
	result   *internalpb.RetrieveResults
	rowIndex int64
}

func (u retrieveUnit) pk() any {
	return typeutil.GetPK(u.result.GetIds(), u.rowIndex)
}

type orderedChildBuffer struct {
	chunk  *internalpb.RetrieveResults
	cursor int64
}

func (b *orderedChildBuffer) hasUnit() bool {
	return b.chunk != nil && int(b.cursor) < typeutil.GetSizeOfIDs(b.chunk.GetIds())
}

func (b *orderedChildBuffer) front() retrieveUnit {
	return retrieveUnit{result: b.chunk, rowIndex: b.cursor}
}

func (b *orderedChildBuffer) pop() retrieveUnit {
	unit := b.front()
	b.cursor++
	if !b.hasUnit() {
		b.chunk = nil
		b.cursor = 0
	}
	return unit
}

func (b *orderedChildBuffer) accept(chunk *internalpb.RetrieveResults) error {
	if b.hasUnit() {
		return merr.WrapErrServiceInternalMsg("Query child Buffer already contains Units")
	}
	if chunk.GetElementLevel() {
		return merr.WrapErrServiceUnimplemented(status.Error(codes.Unimplemented, "element-level Query streaming is not implemented"))
	}
	rowCount := typeutil.GetSizeOfIDs(chunk.GetIds())
	if rowCount > 0 && len(chunk.GetFieldsData()) == 0 {
		return merr.WrapErrServiceInternalMsg("Query Chunk contains %d IDs without field data", rowCount)
	}
	if rowCount == 0 {
		return nil
	}
	b.chunk = chunk
	b.cursor = 0
	return nil
}

// OrderedReduceStream performs the Plain Query PK-ordered merge.
type OrderedReduceStream struct {
	childStreams         []ReduceStream
	childBuffers         []orderedChildBuffer
	childRecvTasks       []bool
	childDrained         []bool
	childRecvCompletions chan childRecvCompletion

	limit     int64
	chunkSize int
	emitted   int64
	hasLastPK bool
	lastPK    any

	metadata        *internalpb.RetrieveResults
	metadataEmitted bool
	closed          bool
	finished        bool
	childrenClosed  bool
	closeErr        error
}

// NewReduceStream creates the bounded Plain Query OrderedReduceStream.
func NewReduceStream(request *internalpb.RetrieveRequest, childStreams []ReduceStream, chunkSize int) (ReduceStream, error) {
	if request == nil {
		return nil, merr.WrapErrServiceInternalMsg("NewReduceStream requires a Query request")
	}
	if request.GetIsCount() || len(request.GetGroupByFieldIds()) > 0 ||
		len(request.GetAggregates()) > 0 || len(request.GetOrderByFields()) > 0 {
		return nil, merr.WrapErrServiceUnimplemented(status.Error(codes.Unimplemented, "Query ReduceStream supports bounded Plain Query only"))
	}
	if request.GetLimit() <= 0 {
		return nil, merr.WrapErrServiceInternalMsg("Query ReduceStream requires a positive limit, got %d", request.GetLimit())
	}
	if chunkSize <= 0 {
		return nil, merr.WrapErrServiceInternalMsg("Query ReduceStream requires a positive Chunk size, got %d", chunkSize)
	}
	for i, childStream := range childStreams {
		if childStream == nil {
			return nil, merr.WrapErrServiceInternalMsg("Query ReduceStream child stream %d is nil", i)
		}
	}

	return &OrderedReduceStream{
		childStreams:         childStreams,
		childBuffers:         make([]orderedChildBuffer, len(childStreams)),
		childRecvTasks:       make([]bool, len(childStreams)),
		childDrained:         make([]bool, len(childStreams)),
		childRecvCompletions: make(chan childRecvCompletion, max(1, len(childStreams))),
		limit:                request.GetLimit(),
		chunkSize:            chunkSize,
	}, nil
}

func (s *OrderedReduceStream) Recv() (*internalpb.RetrieveResults, error) {
	if s.finished {
		return nil, io.EOF
	}
	if s.closed {
		return nil, io.ErrClosedPipe
	}

	capacity := min(s.chunkSize, int(s.limit-s.emitted))
	outputBuffer := make([]retrieveUnit, 0, capacity)
	for len(outputBuffer) < capacity {
		readyBuffers, err := s.getReadyBuffers()
		if err != nil {
			return nil, s.fail(err)
		}
		if len(readyBuffers) == 0 {
			break
		}

		winner := readyBuffers[0]
		for _, candidate := range readyBuffers[1:] {
			if comparePK(candidate.front().pk(), winner.front().pk()) < 0 {
				winner = candidate
			}
		}

		unit := winner.pop()
		if s.hasLastPK {
			comparison := comparePK(unit.pk(), s.lastPK)
			if comparison == 0 {
				return nil, s.fail(merr.WrapErrDataIntegrityMsg("duplicate PK %v found across Query child streams", unit.pk()))
			}
			if comparison < 0 {
				return nil, s.fail(merr.WrapErrServiceInternalMsg("Query child streams are not ordered by primary key"))
			}
		}
		s.lastPK = unit.pk()
		s.hasLastPK = true
		s.emitted++
		outputBuffer = append(outputBuffer, unit)
	}

	if len(outputBuffer) == 0 {
		s.finished = true
		if s.metadata != nil && !s.metadataEmitted {
			chunk := &internalpb.RetrieveResults{Status: merr.Success()}
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

	chunk, err := buildRetrieveChunk(outputBuffer)
	if err != nil {
		return nil, s.fail(err)
	}
	s.attachMetadata(chunk)
	return chunk, nil
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
			go func(childIndex int, stream ReduceStream) {
				chunk, err := stream.Recv()
				s.childRecvCompletions <- childRecvCompletion{childIndex: childIndex, chunk: chunk, err: err}
			}(i, childStream)
		}

		if allReady {
			ready := make([]*orderedChildBuffer, 0, len(s.childBuffers))
			for i := range s.childBuffers {
				if s.childBuffers[i].hasUnit() {
					ready = append(ready, &s.childBuffers[i])
				}
			}
			return ready, nil
		}

		received := <-s.childRecvCompletions
		s.childRecvTasks[received.childIndex] = false
		if errors.Is(received.err, io.EOF) {
			s.childDrained[received.childIndex] = true
			continue
		}
		if received.err != nil {
			return nil, merr.Wrapf(received.err, "Query child stream %d Recv failed", received.childIndex)
		}
		if received.chunk == nil {
			return nil, merr.WrapErrServiceInternalMsg("Query child stream %d returned a nil Chunk", received.childIndex)
		}
		if !merr.Ok(received.chunk.GetStatus()) {
			return nil, merr.Wrapf(merr.Error(received.chunk.GetStatus()), "Query child stream %d returned a failed Chunk", received.childIndex)
		}
		s.acceptMetadata(received.chunk)
		if err := s.childBuffers[received.childIndex].accept(received.chunk); err != nil {
			return nil, merr.Wrapf(err, "Query child stream %d returned an invalid Chunk", received.childIndex)
		}
	}
}

func (s *OrderedReduceStream) acceptMetadata(chunk *internalpb.RetrieveResults) {
	if s.metadataEmitted {
		return
	}
	if s.metadata == nil {
		s.metadata = &internalpb.RetrieveResults{Status: merr.Success(), ReqID: chunk.GetReqID()}
		if chunk.GetBase() != nil {
			s.metadata.Base = proto.Clone(chunk.GetBase()).(*commonpb.MsgBase)
		}
	}
	s.metadata.SealedSegmentIDsRetrieved = append(s.metadata.SealedSegmentIDsRetrieved, chunk.GetSealedSegmentIDsRetrieved()...)
	s.metadata.ChannelIDsRetrieved = append(s.metadata.ChannelIDsRetrieved, chunk.GetChannelIDsRetrieved()...)
	s.metadata.GlobalSealedSegmentIDs = append(s.metadata.GlobalSealedSegmentIDs, chunk.GetGlobalSealedSegmentIDs()...)
	s.metadata.AllRetrieveCount += chunk.GetAllRetrieveCount()
	s.metadata.HasMoreResult = s.metadata.GetHasMoreResult() || chunk.GetHasMoreResult()
	s.metadata.ScannedRemoteBytes += chunk.GetScannedRemoteBytes()
	s.metadata.ScannedTotalBytes += chunk.GetScannedTotalBytes()
	s.metadata.MvccTimestamp = max(s.metadata.GetMvccTimestamp(), chunk.GetMvccTimestamp())
	s.metadata.CostAggregation = mergeQueryCost(s.metadata.GetCostAggregation(), chunk.GetCostAggregation())
}

func (s *OrderedReduceStream) attachMetadata(chunk *internalpb.RetrieveResults) {
	if s.metadata == nil || s.metadataEmitted {
		return
	}
	metadata := s.metadata
	chunk.Base = metadata.GetBase()
	chunk.ReqID = metadata.GetReqID()
	chunk.SealedSegmentIDsRetrieved = append([]int64(nil), metadata.GetSealedSegmentIDsRetrieved()...)
	chunk.ChannelIDsRetrieved = append([]string(nil), metadata.GetChannelIDsRetrieved()...)
	chunk.GlobalSealedSegmentIDs = append([]int64(nil), metadata.GetGlobalSealedSegmentIDs()...)
	chunk.CostAggregation = metadata.GetCostAggregation()
	chunk.AllRetrieveCount = metadata.GetAllRetrieveCount()
	chunk.HasMoreResult = metadata.GetHasMoreResult()
	chunk.ScannedRemoteBytes = metadata.GetScannedRemoteBytes()
	chunk.ScannedTotalBytes = metadata.GetScannedTotalBytes()
	chunk.MvccTimestamp = metadata.GetMvccTimestamp()
	s.metadataEmitted = true
	s.metadata = nil
}

func buildRetrieveChunk(units []retrieveUnit) (*internalpb.RetrieveResults, error) {
	results := make([]*internalpb.RetrieveResults, len(units))
	rows := make([]rowRef, len(units))
	for i, unit := range units {
		results[i] = unit.result
		rows[i] = rowRef{resultIdx: i, rowIdx: unit.rowIndex}
	}
	chunk, err := buildMergedRetrieveResults(results, rows, nil)
	if err != nil {
		return nil, err
	}
	chunk.Status = merr.Success()
	return chunk, nil
}

// SplitRetrieveResult splits one materialized Query result into Unit-count Chunks.
func SplitRetrieveResult(result *internalpb.RetrieveResults, chunkSize int) ([]*internalpb.RetrieveResults, error) {
	if result == nil {
		return nil, merr.WrapErrServiceInternalMsg("cannot split a nil Query result")
	}
	if chunkSize <= 0 {
		return nil, merr.WrapErrServiceInternalMsg("Query Chunk size must be positive, got %d", chunkSize)
	}
	rowCount := typeutil.GetSizeOfIDs(result.GetIds())
	if rowCount == 0 {
		return []*internalpb.RetrieveResults{proto.Clone(result).(*internalpb.RetrieveResults)}, nil
	}

	chunks := make([]*internalpb.RetrieveResults, 0, (rowCount+chunkSize-1)/chunkSize)
	for start := 0; start < rowCount; start += chunkSize {
		end := min(start+chunkSize, rowCount)
		units := make([]retrieveUnit, 0, end-start)
		for row := start; row < end; row++ {
			units = append(units, retrieveUnit{result: result, rowIndex: int64(row)})
		}
		chunk, err := buildRetrieveChunk(units)
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, chunk)
	}

	first := chunks[0]
	first.Base = result.GetBase()
	first.ReqID = result.GetReqID()
	first.SealedSegmentIDsRetrieved = append([]int64(nil), result.GetSealedSegmentIDsRetrieved()...)
	first.ChannelIDsRetrieved = append([]string(nil), result.GetChannelIDsRetrieved()...)
	first.GlobalSealedSegmentIDs = append([]int64(nil), result.GetGlobalSealedSegmentIDs()...)
	first.CostAggregation = result.GetCostAggregation()
	first.AllRetrieveCount = result.GetAllRetrieveCount()
	first.HasMoreResult = result.GetHasMoreResult()
	first.ScannedRemoteBytes = result.GetScannedRemoteBytes()
	first.ScannedTotalBytes = result.GetScannedTotalBytes()
	first.MvccTimestamp = result.GetMvccTimestamp()
	return chunks, nil
}

func mergeQueryCost(current, incoming *internalpb.CostAggregation) *internalpb.CostAggregation {
	if incoming == nil {
		return current
	}
	if current == nil {
		return proto.Clone(incoming).(*internalpb.CostAggregation)
	}
	totalRelatedDataSize := current.GetTotalRelatedDataSize() + incoming.GetTotalRelatedDataSize()
	if current.GetResponseTime() < incoming.GetResponseTime() {
		current = proto.Clone(incoming).(*internalpb.CostAggregation)
	}
	current.TotalRelatedDataSize = totalRelatedDataSize
	return current
}

func (s *OrderedReduceStream) fail(err error) error {
	return errors.Join(err, s.Close())
}

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
	for i, childStream := range s.childStreams {
		if err := childStream.Close(); err != nil {
			closeErrors = append(closeErrors, merr.Wrapf(err, "close Query child stream %d", i))
		}
		s.childBuffers[i] = orderedChildBuffer{}
		s.childRecvTasks[i] = false
	}
	s.metadata = nil
	s.closeErr = errors.Join(closeErrors...)
	return s.closeErr
}

func (s *OrderedReduceStream) Interrupt() (*internalpb.RetrieveResults, error) {
	return nil, merr.WrapErrServiceUnimplemented(status.Error(codes.Unimplemented, "Query ReduceStream Interrupt is not implemented"))
}
