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
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	commonpb "github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/agg"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ReduceStream is the Query counterpart of the Search ReduceStream contract.
// Recv calls must be serial; Close may run concurrently with Recv to cancel it.
// The first Chunk carries HasMoreResult: whether matching rows exist beyond
// this stream's current result, not whether more Chunks will arrive. It is
// known before any rows are emitted and may be omitted from later Chunks.
type ReduceStream interface {
	Recv() (*internalpb.RetrieveResults, error)
	Close() error
	Interrupt() (*internalpb.RetrieveResults, error)
}

type retrieveUnit struct {
	result         *internalpb.RetrieveResults
	rowIndex       int64
	byteSize       int
	sizeCalculator *rowSizeCalculator
}

func (u retrieveUnit) pk() any {
	return typeutil.GetPK(u.result.GetIds(), u.rowIndex)
}

func (u retrieveUnit) timestamp() int64 {
	timestamps := extractTimestamps(u.result)
	if u.rowIndex >= int64(len(timestamps)) {
		return 0
	}
	return timestamps[u.rowIndex]
}

func (u retrieveUnit) fieldData(fieldID int64) (*schemapb.FieldData, error) {
	for _, field := range u.result.GetFieldsData() {
		if field.GetFieldId() == fieldID {
			return field, nil
		}
	}
	return nil, merr.WrapErrServiceInternalMsg("Query ORDER BY field %d is missing from a child Chunk", fieldID)
}

func compareRetrieveUnits(left, right retrieveUnit, orderByFields []*planpb.OrderByField) (int, error) {
	for _, orderByField := range orderByFields {
		leftField, err := left.fieldData(orderByField.GetFieldId())
		if err != nil {
			return 0, err
		}
		rightField, err := right.fieldData(orderByField.GetFieldId())
		if err != nil {
			return 0, err
		}
		if leftField.GetType() != rightField.GetType() {
			return 0, merr.WrapErrServiceInternalMsg(
				"Query ORDER BY field %d has inconsistent child types %s and %s",
				orderByField.GetFieldId(), leftField.GetType(), rightField.GetType())
		}
		comparison := compareFieldDataRows(
			leftField,
			int(left.rowIndex),
			rightField,
			int(right.rowIndex),
			leftField.GetType(),
			orderByField.GetAscending(),
			orderByField.GetNullsFirst(),
		)
		if comparison != 0 {
			return comparison, nil
		}
	}
	return comparePK(left.pk(), right.pk()), nil
}

// reducibleByteSize uses Query's existing row payload accounting without copying a row.
func (u retrieveUnit) reducibleByteSize() (int, error) {
	if u.rowIndex < 0 || u.rowIndex >= int64(GetRowCount(u.result)) {
		return 0, merr.WrapErrServiceInternalMsg("Query Unit row index %d is out of range", u.rowIndex)
	}
	calculator := u.sizeCalculator
	if calculator == nil {
		calculator = newRowSizeCalculator(u.result)
	}
	size := int(calculator.rowSize(u.rowIndex))
	switch ids := u.result.GetIds().GetIdField().(type) {
	case *schemapb.IDs_IntId:
		size += 8
	case *schemapb.IDs_StrId:
		size += len(ids.StrId.GetData()[u.rowIndex])
	}
	if indices := u.result.GetElementIndices(); len(indices) > 0 {
		size += 4 * len(indices[u.rowIndex].GetIndices())
	}
	return size, nil
}

type orderedChildBuffer struct {
	chunk          *internalpb.RetrieveResults
	cursor         int64
	hasMoreResult  bool // Retained from the first Chunk after its rows are consumed.
	sizeCalculator *rowSizeCalculator
}

func (b *orderedChildBuffer) hasUnit() bool {
	return b.chunk != nil && int(b.cursor) < typeutil.GetSizeOfIDs(b.chunk.GetIds())
}

func (b *orderedChildBuffer) front() retrieveUnit {
	return retrieveUnit{result: b.chunk, rowIndex: b.cursor, sizeCalculator: b.sizeCalculator}
}

func (b *orderedChildBuffer) pop() retrieveUnit {
	unit := b.front()
	b.cursor++
	if !b.hasUnit() {
		b.chunk = nil
		b.cursor = 0
		b.sizeCalculator = nil
	}
	return unit
}

func (b *orderedChildBuffer) accept(chunk *internalpb.RetrieveResults) error {
	if b.hasUnit() {
		return merr.WrapErrServiceInternalMsg("Query child Buffer already contains Units")
	}
	if err := validateElementLevelConsistency([]*internalpb.RetrieveResults{chunk}, nil); err != nil {
		return err
	}
	b.hasMoreResult = b.hasMoreResult || chunk.GetHasMoreResult()
	rowCount := typeutil.GetSizeOfIDs(chunk.GetIds())
	if rowCount > 0 && len(chunk.GetFieldsData()) == 0 {
		return merr.WrapErrServiceInternalMsg("Query Chunk contains %d IDs without field data", rowCount)
	}
	if rowCount == 0 {
		return nil
	}
	b.chunk = chunk
	b.cursor = 0
	b.sizeCalculator = newRowSizeCalculator(chunk)
	return nil
}

// OrderedReduceStream performs the bounded Query ordered merge.
type OrderedReduceStream struct {
	recvMu       sync.Mutex
	childStreams *reduce.ChildStreams[*internalpb.RetrieveResults]
	childBuffers []orderedChildBuffer

	limit          int64
	chunkBytes     int
	reduceType     reduce.IReduceType
	emitted        int64
	orderByFields  []*planpb.OrderByField
	hasLastPK      bool
	lastPK         any
	deduplicatePKs bool
	outputBuffer   []retrieveUnit

	elementLevel      bool
	elementLevelKnown bool

	metadata        *internalpb.RetrieveResults
	metadataEmitted bool
	closed          bool
	finished        bool
	closeErr        error
}

// NewReduceStream creates the Query ReduceStream selected by request semantics.
func NewReduceStream(
	request *internalpb.RetrieveRequest,
	schema *schemapb.CollectionSchema,
	childStreams []ReduceStream,
	chunkBytes int,
) (ReduceStream, error) {
	if request == nil {
		return nil, merr.WrapErrServiceInternalMsg("NewReduceStream requires a Query request")
	}
	if len(request.GetGroupByFieldIds()) > 0 {
		if schema == nil {
			return nil, merr.WrapErrServiceInternalMsg("Query Group By ReduceStream requires a collection schema")
		}
		return newUnOrderedReduceStream(request, schema, childStreams, chunkBytes)
	}
	if request.GetIsCount() || len(request.GetAggregates()) > 0 {
		return nil, merr.WrapErrServiceUnimplemented(status.Error(codes.Unimplemented, "Query ReduceStream does not support global aggregation"))
	}
	if request.GetLimit() <= 0 {
		return nil, merr.WrapErrServiceInternalMsg("Query ReduceStream requires a positive limit, got %d", request.GetLimit())
	}
	if chunkBytes <= 0 {
		return nil, merr.WrapErrServiceInternalMsg("Query ReduceStream requires positive Chunk bytes, got %d", chunkBytes)
	}
	for i, childStream := range childStreams {
		if childStream == nil {
			return nil, merr.WrapErrServiceInternalMsg("Query ReduceStream child stream %d is nil", i)
		}
	}

	return &OrderedReduceStream{
		childStreams:  reduce.NewChildStreams[*internalpb.RetrieveResults](childStreams),
		childBuffers:  make([]orderedChildBuffer, len(childStreams)),
		limit:         request.GetLimit(),
		chunkBytes:    chunkBytes,
		reduceType:    reduce.ToReduceType(request.GetReduceType()),
		orderByFields: request.GetOrderByFields(),
	}, nil
}

// NewShardReduceStream resolves duplicate PKs by timestamp for PK-ordered worker results.
// ORDER BY and Group By retain their existing reduction behavior.
func NewShardReduceStream(request *internalpb.RetrieveRequest, schema *schemapb.CollectionSchema, childStreams []ReduceStream, chunkBytes int) (ReduceStream, error) {
	stream, err := NewReduceStream(request, schema, childStreams, chunkBytes)
	if err != nil {
		return nil, err
	}
	if ordered, ok := stream.(*OrderedReduceStream); ok && len(ordered.orderByFields) == 0 {
		ordered.deduplicatePKs = true
	}
	return stream, nil
}

func (s *OrderedReduceStream) Recv() (*internalpb.RetrieveResults, error) {
	s.recvMu.Lock()
	defer s.recvMu.Unlock()

	if s.finished {
		return nil, io.EOF
	}
	if s.closed {
		return nil, io.ErrClosedPipe
	}

	outputBuffer := s.outputBuffer
	s.outputBuffer = nil
	outputBytes := 0
	for _, unit := range outputBuffer {
		outputBytes += unit.byteSize
	}
	for s.deduplicatePKs || (outputBytes < s.chunkBytes && s.emitted < s.limit) {
		if len(outputBuffer) == 0 && s.emitted >= s.limit {
			break
		}
		readyBuffers, err := s.getReadyBuffers()
		if err != nil {
			return nil, s.fail(err)
		}
		if len(readyBuffers) == 0 {
			break
		}

		winner := readyBuffers[0]
		for _, candidate := range readyBuffers[1:] {
			comparison, err := compareRetrieveUnits(candidate.front(), winner.front(), s.orderByFields)
			if err != nil {
				return nil, s.fail(err)
			}
			if comparison < 0 {
				winner = candidate
			}
		}

		selected := winner.front()
		// Resolve the trailing PK even after the byte threshold or row limit is reached.
		// Returning its Chunk first would make a newer row in the next input Chunk too late.
		if s.deduplicatePKs && len(outputBuffer) > 0 && comparePK(selected.pk(), s.lastPK) == 0 {
			unit := winner.pop()
			last := len(outputBuffer) - 1
			if timestamp := unit.timestamp(); timestamp != 0 && timestamp > outputBuffer[last].timestamp() {
				unit.byteSize, err = unit.reducibleByteSize()
				if err != nil {
					return nil, s.fail(err)
				}
				outputBytes += unit.byteSize - outputBuffer[last].byteSize
				if unit.result.GetElementLevel() {
					// The winning document version also determines its matched-element count.
					s.emitted += int64(len(unit.result.GetElementIndices()[unit.rowIndex].GetIndices())) -
						int64(len(outputBuffer[last].result.GetElementIndices()[outputBuffer[last].rowIndex].GetIndices()))
				}
				outputBuffer[last] = unit
			}
			continue
		}
		if outputBytes >= s.chunkBytes || s.emitted >= s.limit {
			break
		}
		unitBytes, err := selected.reducibleByteSize()
		if err != nil {
			return nil, s.fail(err)
		}
		if unitBytes > s.chunkBytes && outputBytes > 0 {
			break
		}

		unit := winner.pop()
		unit.byteSize = unitBytes
		if s.hasLastPK {
			comparison := comparePK(unit.pk(), s.lastPK)
			if comparison == 0 {
				return nil, s.fail(merr.WrapErrDataIntegrityMsg("duplicate PK %v found across Query child streams", unit.pk()))
			}
			if len(s.orderByFields) == 0 && comparison < 0 {
				return nil, s.fail(merr.WrapErrServiceInternalMsg("Query child streams are not ordered by primary key"))
			}
		}
		s.lastPK = unit.pk()
		s.hasLastPK = true
		// A Unit remains a complete row, but element-level limits count its matches.
		if unit.result.GetElementLevel() {
			s.emitted += int64(len(unit.result.GetElementIndices()[unit.rowIndex].GetIndices()))
		} else {
			s.emitted++
		}
		outputBuffer = append(outputBuffer, unit)
		outputBytes += unitBytes
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

	last := len(outputBuffer) - 1
	if s.deduplicatePKs && last > 0 && outputBuffer[last].byteSize > s.chunkBytes {
		// A newer version can turn the trailing Unit into an oversized Unit.
		// Keep it for the next Recv so it is still emitted alone.
		s.outputBuffer = []retrieveUnit{outputBuffer[last]}
		outputBuffer = outputBuffer[:last]
	}
	chunk, err := buildRetrieveChunk(outputBuffer)
	if err != nil {
		return nil, s.fail(err)
	}
	s.attachMetadata(chunk)
	return chunk, nil
}

func (s *OrderedReduceStream) getReadyBuffers() ([]*orderedChildBuffer, error) {
	// Every active child supplies its first Chunk before our first output, so
	// HasMoreResult can be forwarded then, even if that child reaches EOF later.
	err := s.childStreams.ReceiveUntilReady(
		func(childIndex int) bool {
			return s.childBuffers[childIndex].hasUnit()
		},
		func(childIndex int, chunk *internalpb.RetrieveResults) error {
			if chunk == nil {
				return merr.WrapErrServiceInternalMsg("Query child stream %d returned a nil Chunk", childIndex)
			}
			if !merr.Ok(chunk.GetStatus()) {
				return merr.Wrapf(merr.Error(chunk.GetStatus()), "Query child stream %d returned a failed Chunk", childIndex)
			}
			s.acceptMetadata(chunk)
			if err := s.childBuffers[childIndex].accept(chunk); err != nil {
				return merr.Wrapf(err, "Query child stream %d returned an invalid Chunk", childIndex)
			}
			if s.childBuffers[childIndex].hasUnit() || chunk.GetElementLevel() {
				if s.elementLevelKnown && s.elementLevel != chunk.GetElementLevel() {
					return merr.WrapErrServiceInternalMsg("Query child stream %d returned an inconsistent element-level flag", childIndex)
				}
				s.elementLevel, s.elementLevelKnown = chunk.GetElementLevel(), true
			}
			return nil
		},
	)
	if err != nil {
		return nil, err
	}

	ready := make([]*orderedChildBuffer, 0, len(s.childBuffers))
	for i := range s.childBuffers {
		// EOF ends only this child's current result. Iterator reduction must
		// stop if matching rows remain outside it, rather than skip those rows.
		if reduce.ShouldStopWhenDrained(s.reduceType) &&
			s.childStreams.IsDrained(i) && s.childBuffers[i].hasMoreResult {
			return nil, nil
		}
		if s.childBuffers[i].hasUnit() {
			ready = append(ready, &s.childBuffers[i])
		}
	}
	return ready, nil
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
	chunk.ElementLevel = s.elementLevel
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

// UnOrderedReduceStream merges Query Group By results without depending on child arrival order.
type UnOrderedReduceStream struct {
	recvMu       sync.Mutex
	childStreams *reduce.ChildStreams[*internalpb.RetrieveResults]
	childBuffers []*internalpb.RetrieveResults
	reducer      *agg.GroupAggReducer
	chunkBytes   int

	outputChunks []*internalpb.RetrieveResults
	outputIndex  int
	metadata     *internalpb.RetrieveResults
	closed       bool
	finished     bool
	closeErr     error
}

func newUnOrderedReduceStream(
	request *internalpb.RetrieveRequest,
	schema *schemapb.CollectionSchema,
	childStreams []ReduceStream,
	chunkBytes int,
) (ReduceStream, error) {
	if chunkBytes <= 0 {
		return nil, merr.WrapErrServiceInternalMsg("Query ReduceStream requires positive Chunk bytes, got %d", chunkBytes)
	}
	for i, childStream := range childStreams {
		if childStream == nil {
			return nil, merr.WrapErrServiceInternalMsg("Query ReduceStream child stream %d is nil", i)
		}
	}
	return &UnOrderedReduceStream{
		childStreams: reduce.NewChildStreams[*internalpb.RetrieveResults](childStreams),
		childBuffers: make([]*internalpb.RetrieveResults, len(childStreams)),
		reducer:      agg.NewGroupAggReducer(request.GetGroupByFieldIds(), request.GetAggregates(), -1, schema),
		chunkBytes:   chunkBytes,
	}, nil
}

func (s *UnOrderedReduceStream) Recv() (*internalpb.RetrieveResults, error) {
	s.recvMu.Lock()
	defer s.recvMu.Unlock()

	if s.finished {
		return nil, io.EOF
	}
	if s.closed {
		return nil, io.ErrClosedPipe
	}

	if s.outputChunks == nil {
		for {
			readyBuffers, err := s.getReadyBuffers()
			if err != nil {
				return nil, s.fail(err)
			}
			if len(readyBuffers) == 0 {
				break
			}

			partialResults := make([]*agg.AggregationResult, len(readyBuffers))
			for i, readyBuffer := range readyBuffers {
				partialResults[i] = agg.NewAggregationResult(readyBuffer.GetFieldsData(), 0)
			}
			if err := s.reducer.Merge(partialResults); err != nil {
				return nil, s.fail(err)
			}
			for i := range s.childBuffers {
				s.childBuffers[i] = nil
			}
		}

		reduced, err := s.reducer.Result()
		if err != nil {
			return nil, s.fail(err)
		}
		result := agg.AggResult2internalResult(reduced)
		result.Status = merr.Success()
		s.attachMetadata(result)
		s.outputChunks, err = SplitRetrieveResult(result, s.chunkBytes)
		if err != nil {
			return nil, s.fail(err)
		}
		s.reducer = nil
	}

	if s.outputIndex < len(s.outputChunks) {
		chunk := s.outputChunks[s.outputIndex]
		s.outputChunks[s.outputIndex] = nil
		s.outputIndex++
		return chunk, nil
	}

	s.finished = true
	if err := s.closeChildren(); err != nil {
		return nil, err
	}
	return nil, io.EOF
}

func (s *UnOrderedReduceStream) getReadyBuffers() ([]*internalpb.RetrieveResults, error) {
	err := s.childStreams.ReceiveAnyReady(
		func(childIndex int) bool {
			return s.childBuffers[childIndex] != nil
		},
		func(childIndex int, chunk *internalpb.RetrieveResults) error {
			if chunk == nil {
				return merr.WrapErrServiceInternalMsg("Query child stream %d returned a nil Chunk", childIndex)
			}
			if !merr.Ok(chunk.GetStatus()) {
				return merr.Wrapf(merr.Error(chunk.GetStatus()), "Query child stream %d returned a failed Chunk", childIndex)
			}
			s.acceptMetadata(chunk)
			s.childBuffers[childIndex] = chunk
			return nil
		},
	)
	if err != nil {
		return nil, err
	}

	ready := make([]*internalpb.RetrieveResults, 0, len(s.childBuffers))
	for _, childBuffer := range s.childBuffers {
		if childBuffer != nil {
			ready = append(ready, childBuffer)
		}
	}
	return ready, nil
}

func (s *UnOrderedReduceStream) acceptMetadata(chunk *internalpb.RetrieveResults) {
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

func (s *UnOrderedReduceStream) attachMetadata(chunk *internalpb.RetrieveResults) {
	if s.metadata == nil {
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
	s.metadata = nil
}

func (s *UnOrderedReduceStream) fail(err error) error {
	return errors.Join(err, s.closeChildren())
}

func (s *UnOrderedReduceStream) Close() error {
	s.childStreams.Cancel()
	s.recvMu.Lock()
	defer s.recvMu.Unlock()
	return s.closeChildren()
}

// closeChildren runs with recvMu held, including on the Recv error path.
func (s *UnOrderedReduceStream) closeChildren() error {
	if s.closed {
		return s.closeErr
	}
	s.closed = true
	s.closeErr = s.childStreams.Close()
	for i := range s.childBuffers {
		s.childBuffers[i] = nil
	}
	for i := range s.outputChunks {
		s.outputChunks[i] = nil
	}
	s.reducer = nil
	s.metadata = nil
	return s.closeErr
}

func (s *UnOrderedReduceStream) Interrupt() (*internalpb.RetrieveResults, error) {
	return nil, merr.WrapErrServiceUnimplemented(status.Error(codes.Unimplemented, "Query ReduceStream Interrupt is not implemented"))
}

// SplitRetrieveResult splits one materialized Query result into byte-threshold Chunks.
func SplitRetrieveResult(result *internalpb.RetrieveResults, chunkBytes int) ([]*internalpb.RetrieveResults, error) {
	if result == nil {
		return nil, merr.WrapErrServiceInternalMsg("cannot split a nil Query result")
	}
	if chunkBytes <= 0 {
		return nil, merr.WrapErrServiceInternalMsg("Query Chunk bytes must be positive, got %d", chunkBytes)
	}
	rowCount := GetRowCount(result)
	if rowCount == 0 {
		return []*internalpb.RetrieveResults{proto.Clone(result).(*internalpb.RetrieveResults)}, nil
	}

	chunks := make([]*internalpb.RetrieveResults, 0)
	units := make([]retrieveUnit, 0)
	accumulatedBytes := 0
	sizeCalculator := newRowSizeCalculator(result)
	for row := 0; row < rowCount; row++ {
		unit := retrieveUnit{result: result, rowIndex: int64(row), sizeCalculator: sizeCalculator}
		unitBytes, err := unit.reducibleByteSize()
		if err != nil {
			return nil, err
		}
		if unitBytes > chunkBytes && accumulatedBytes > 0 {
			chunk, err := buildRetrieveChunk(units)
			if err != nil {
				return nil, err
			}
			chunks = append(chunks, chunk)
			units = nil
			accumulatedBytes = 0
		}
		units = append(units, unit)
		accumulatedBytes += unitBytes
		if accumulatedBytes < chunkBytes {
			continue
		}
		chunk, err := buildRetrieveChunk(units)
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, chunk)
		units = nil
		accumulatedBytes = 0
	}
	if len(units) > 0 {
		chunk, err := buildRetrieveChunk(units)
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, chunk)
	}

	// Workers materialize the result before splitting it, so HasMoreResult is
	// already known. Carry it in the first Chunk, including an empty result.
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
	return errors.Join(err, s.closeChildren())
}

func (s *OrderedReduceStream) Close() error {
	s.childStreams.Cancel()
	s.recvMu.Lock()
	defer s.recvMu.Unlock()
	return s.closeChildren()
}

// closeChildren runs with recvMu held, including on the Recv error path.
func (s *OrderedReduceStream) closeChildren() error {
	if s.closed {
		return s.closeErr
	}
	s.closed = true
	s.closeErr = s.childStreams.Close()
	for i := range s.childBuffers {
		s.childBuffers[i] = orderedChildBuffer{}
	}
	s.outputBuffer = nil
	s.metadata = nil
	return s.closeErr
}

func (s *OrderedReduceStream) Interrupt() (*internalpb.RetrieveResults, error) {
	return nil, merr.WrapErrServiceUnimplemented(status.Error(codes.Unimplemented, "Query ReduceStream Interrupt is not implemented"))
}
