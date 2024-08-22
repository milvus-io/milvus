package storage

import (
	"context"
	"io"

	"github.com/samber/lo"
	"go.uber.org/zap"

	binlogIO "github.com/milvus-io/milvus/internal/storage/io"
	"github.com/milvus-io/milvus/pkg/log"
)

type SegmentDeserializeReader struct {
	ctx      context.Context
	binlogIO binlogIO.BinlogIO
	reader   *DeserializeReader[*Value]

	pos           int
	PKFieldID     int64
	binlogPaths   [][]string
	binlogPathPos int
}

func NewSegmentDeserializeReader(binlogPaths [][]string, binlogIO binlogIO.BinlogIO, PKFieldID int64) *SegmentDeserializeReader {
	return &SegmentDeserializeReader{
		binlogIO:      binlogIO,
		reader:        nil,
		pos:           0,
		PKFieldID:     PKFieldID,
		binlogPaths:   binlogPaths,
		binlogPathPos: 0,
	}
}

func (r *SegmentDeserializeReader) initDeserializeReader() error {
	if r.binlogPathPos >= len(r.binlogPaths) {
		return io.EOF
	}
	allValues, err := r.binlogIO.Download(r.ctx, r.binlogPaths[r.binlogPathPos])
	if err != nil {
		log.Warn("compact wrong, fail to download insertLogs", zap.Error(err))
		return err
	}

	blobs := lo.Map(allValues, func(v []byte, i int) *Blob {
		return &Blob{Key: r.binlogPaths[r.binlogPathPos][i], Value: v}
	})

	r.reader, err = NewBinlogDeserializeReader(blobs, r.PKFieldID)
	if err != nil {
		log.Warn("compact wrong, failed to new insert binlogs reader", zap.Error(err))
		return err
	}
	r.binlogPathPos++
	return nil
}

func (r *SegmentDeserializeReader) Next() error {
	if r.reader == nil {
		if err := r.initDeserializeReader(); err != nil {
			return err
		}
	}
	if err := r.reader.Next(); err != nil {
		if err == io.EOF {
			r.reader.Close()
			if err := r.initDeserializeReader(); err != nil {
				return err
			}
			return r.reader.Next()
		}
		return err
	}
	return nil
}

func (r *SegmentDeserializeReader) Close() {
	r.reader.Close()
}

func (r *SegmentDeserializeReader) Value() *Value {
	return r.reader.Value()
}
