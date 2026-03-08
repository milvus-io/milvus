package writebuffer

import (
	"math"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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

	buffers     []*storage.InsertData
	statsBuffer *statsBuffer
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

	ib := &InsertBuffer{
		BufferBase: BufferBase{
			rowLimit:      noLimit,
			sizeLimit:     sizeLimit,
			TimestampFrom: math.MaxUint64,
			TimestampTo:   0,
		},
		collSchema: sch,
	}

	if len(sch.GetFunctions()) > 0 {
		ib.statsBuffer = newStatsBuffer()
	}
	return ib, nil
}

func (ib *InsertBuffer) buffer(inData *storage.InsertData, tr TimeRange, startPos, endPos *msgpb.MsgPosition) {
	// buffer := ib.currentBuffer()
	// storage.MergeInsertData(buffer.buffer, inData)
	ib.buffers = append(ib.buffers, inData)
}

func (ib *InsertBuffer) Yield() []*storage.InsertData {
	result := ib.buffers
	// set buffer nil to so that fragmented buffer could get GCed
	ib.buffers = nil
	return result
}

func (ib *InsertBuffer) YieldStats() map[int64]*storage.BM25Stats {
	if ib.statsBuffer == nil {
		return nil
	}
	return ib.statsBuffer.yieldBuffer()
}

func (ib *InsertBuffer) Buffer(inData *InsertData, startPos, endPos *msgpb.MsgPosition) int64 {
	bufferedSize := int64(0)
	for idx, data := range inData.data {
		tsData := inData.tsField[idx]

		tr := ib.getTimestampRange(tsData)
		ib.buffer(data, tr, startPos, endPos)
		// update buffer size
		ib.UpdateStatistics(int64(data.GetRowNum()), int64(data.GetMemorySize()), tr, startPos, endPos)
		bufferedSize += int64(data.GetMemorySize())
	}
	if inData.bm25Stats != nil {
		ib.statsBuffer.Buffer(inData.bm25Stats)
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
