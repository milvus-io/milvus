package writebuffer

import (
	"math"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type DeltaBuffer struct {
	BufferBase

	buffer *storage.DeleteData
}

func NewDeltaBuffer() *DeltaBuffer {
	return &DeltaBuffer{
		BufferBase: BufferBase{
			rowLimit:      noLimit,
			sizeLimit:     paramtable.Get().DataNodeCfg.FlushDeleteBufferBytes.GetAsInt64(),
			TimestampFrom: math.MaxUint64,
			TimestampTo:   0,
		},
		buffer: &storage.DeleteData{},
	}
}

func (ib *DeltaBuffer) getTimestampRange(tss []typeutil.Timestamp) TimeRange {
	tr := TimeRange{
		timestampMin: math.MaxUint64,
		timestampMax: 0,
	}

	for _, data := range tss {
		if data < tr.timestampMin {
			tr.timestampMin = data
		}
		if data > tr.timestampMax {
			tr.timestampMax = data
		}
	}
	return tr
}

func (db *DeltaBuffer) Yield() *storage.DeleteData {
	if db.IsEmpty() {
		return nil
	}

	return db.buffer
}

func (db *DeltaBuffer) Buffer(pks []storage.PrimaryKey, tss []typeutil.Timestamp, startPos, endPos *msgpb.MsgPosition) (bufSize int64) {
	beforeSize := db.buffer.Size()
	rowCount := len(pks)

	for i := 0; i < rowCount; i++ {
		db.buffer.Append(pks[i], tss[i])
	}

	bufSize = db.buffer.Size() - beforeSize
	db.UpdateStatistics(int64(rowCount), bufSize, db.getTimestampRange(tss), startPos, endPos)

	return bufSize
}
