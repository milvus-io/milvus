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
	buffer, err := storage.NewInsertData(sch)
	if err != nil {
		return nil, err
	}
	sizeLimit := paramtable.Get().DataNodeCfg.FlushInsertBufferSize.GetAsInt64()

	return &InsertBuffer{
		BufferBase: BufferBase{
			rowLimit:      noLimit,
			sizeLimit:     sizeLimit,
			TimestampFrom: math.MaxUint64,
			TimestampTo:   0,
		},
		collSchema: sch,
		buffer:     buffer,
	}, nil
}

func (ib *InsertBuffer) Yield() *storage.InsertData {
	if ib.IsEmpty() {
		return nil
	}

	return ib.buffer
}

func (ib *InsertBuffer) Buffer(inData *inData, startPos, endPos *msgpb.MsgPosition) int64 {
	bufferedSize := int64(0)
	for idx, data := range inData.data {
		storage.MergeInsertData(ib.buffer, data)
		tsData := inData.tsField[idx]

		// update buffer size
		ib.UpdateStatistics(int64(data.GetRowNum()), int64(data.GetMemorySize()), ib.getTimestampRange(tsData), startPos, endPos)
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
