package writebuffer

import (
	"math"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	noLimit int64 = -1
)

type BufferBase struct {
	rows      int64
	rowLimit  int64
	size      int64
	sizeLimit int64

	TimestampFrom typeutil.Timestamp
	TimestampTo   typeutil.Timestamp

	startPos *msgpb.MsgPosition
	endPos   *msgpb.MsgPosition
}

func (b *BufferBase) UpdateStatistics(entryNum, size int64, tr TimeRange, startPos, endPos *msgpb.MsgPosition) {
	b.rows += entryNum
	b.size += size

	if tr.timestampMin < b.TimestampFrom {
		b.TimestampFrom = tr.timestampMin
	}
	if tr.timestampMax > b.TimestampTo {
		b.TimestampTo = tr.timestampMax
	}

	if b.startPos == nil || startPos.Timestamp < b.startPos.Timestamp {
		b.startPos = startPos
	}
	if b.endPos == nil || endPos.Timestamp > b.endPos.Timestamp {
		b.endPos = endPos
	}
}

func (b *BufferBase) IsFull() bool {
	return (b.rowLimit != noLimit && b.rows >= b.rowLimit) ||
		(b.sizeLimit != noLimit && b.size >= b.sizeLimit)
}

func (b *BufferBase) IsEmpty() bool {
	return b.rows == 0 && b.size == 0
}

func (b *BufferBase) MinTimestamp() typeutil.Timestamp {
	if b.startPos == nil {
		return math.MaxUint64
	}
	return b.startPos.GetTimestamp()
}

func (b *BufferBase) GetTimeRange() *TimeRange {
	return NewTimeRange(b.TimestampFrom, b.TimestampTo)
}

type InsertBuffer struct {
	BufferBase
	collSchema *schemapb.CollectionSchema

	estRow    int64
	chunkSize int64
	// buffer *storage.InsertData
	buffers []*InsertBufferChunk
}

// InsertBufferChunk resembles pre-allocated insert data and statistic.
type InsertBufferChunk struct {
	BufferBase

	buffer *storage.InsertData
}

func NewInsertBuffer(sch *schemapb.CollectionSchema) (*InsertBuffer, error) {
	estSize, err := typeutil.EstimateSizePerRecord(sch)
	if err != nil {
		log.Warn("failed to estimate size per record", zap.Error(err))
		return nil, err
	}

	if estSize == 0 {
		return nil, errors.New("Invalid schema")
	}

	sizeLimit := paramtable.Get().DataNodeCfg.FlushInsertBufferSize.GetAsInt64()
	chunkSize := paramtable.Get().DataNodeCfg.InsertBufferChunkSize.GetAsInt64()
	// use size Limit when chunkSize not valid
	if chunkSize <= 0 || chunkSize > sizeLimit {
		log.Warn("invalidate chunk size, use insert buffer size", zap.Int64("chunkSize", chunkSize), zap.Int64("insertBufferSize", sizeLimit))
		chunkSize = sizeLimit
	}

	estRow := chunkSize / int64(estSize)
	ib := &InsertBuffer{
		BufferBase: BufferBase{
			rowLimit:      noLimit,
			sizeLimit:     sizeLimit,
			TimestampFrom: math.MaxUint64,
			TimestampTo:   0,
		},
		collSchema: sch,
		estRow:     estRow,
		chunkSize:  chunkSize,
		buffers:    make([]*InsertBufferChunk, 0, sizeLimit/chunkSize),
	}
	err = ib.nextBatch()
	if err != nil {
		return nil, err
	}

	return ib, nil
}

func (ib *InsertBuffer) nextBatch() error {
	buffer, err := storage.NewInsertDataWithCap(ib.collSchema, int(ib.estRow))
	if err != nil {
		return err
	}
	ib.buffers = append(ib.buffers, &InsertBufferChunk{
		BufferBase: BufferBase{
			rowLimit:      ib.estRow,
			sizeLimit:     ib.chunkSize,
			TimestampFrom: math.MaxUint64,
			TimestampTo:   0,
		},
		buffer: buffer,
	})
	return nil
}

func (ib *InsertBuffer) currentBuffer() *InsertBufferChunk {
	idx := len(ib.buffers) - 1
	if idx < 0 || ib.buffers[idx].IsFull() {
		ib.nextBatch()
		idx++
	}
	return ib.buffers[idx]
}

func (ib *InsertBuffer) buffer(inData *storage.InsertData, tr TimeRange, startPos, endPos *msgpb.MsgPosition) {
	buffer := ib.currentBuffer()
	storage.MergeInsertData(buffer.buffer, inData)
	buffer.UpdateStatistics(int64(inData.GetRowNum()), int64(inData.GetMemorySize()), tr, startPos, endPos)
}

func (ib *InsertBuffer) Yield() *storage.InsertData {
	if ib.IsEmpty() {
		return nil
	}

	// avoid copy when there is only one buffer
	if len(ib.buffers) == 1 {
		return ib.buffers[0].buffer
	}
	// no error assumed, buffer created before
	result, _ := storage.NewInsertDataWithCap(ib.collSchema, int(ib.rows))
	for _, chunk := range ib.buffers {
		storage.MergeInsertData(result, chunk.buffer)
	}
	// set buffer nil to so that fragmented buffer could get GCed
	ib.buffers = nil
	return result
}

func (ib *InsertBuffer) Buffer(inData *inData, startPos, endPos *msgpb.MsgPosition) int64 {
	bufferedSize := int64(0)
	for idx, data := range inData.data {
		tsData := inData.tsField[idx]
		tr := ib.getTimestampRange(tsData)
		ib.buffer(data, tr, startPos, endPos)

		// update buffer size
		ib.UpdateStatistics(int64(data.GetRowNum()), int64(data.GetMemorySize()), tr, startPos, endPos)
		bufferedSize += int64(data.GetMemorySize())
	}
	return bufferedSize
}

func (ib *InsertBuffer) getTimestampRange(tsData *storage.Int64FieldData) TimeRange {
	tr := TimeRange{
		timestampMin: math.MaxUint64,
		timestampMax: 0,
	}

	for _, data := range tsData.Data {
		if uint64(data) < tr.timestampMin {
			tr.timestampMin = typeutil.Timestamp(data)
		}
		if uint64(data) > tr.timestampMax {
			tr.timestampMax = typeutil.Timestamp(data)
		}
	}
	return tr
}
