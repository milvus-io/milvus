package compaction

import (
	"context"
	"io"

	"github.com/samber/lo"
	"go.uber.org/zap"

	binlogIO "github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
)

type SegmentDeserializeReader struct {
	ctx      context.Context
	binlogIO binlogIO.BinlogIO
	reader   *storage.DeserializeReader[*storage.Value]

	pos           int
	PKFieldID     int64
	binlogPaths   [][]string
	binlogPathPos int
}

func NewSegmentDeserializeReader(ctx context.Context, binlogPaths [][]string, binlogIO binlogIO.BinlogIO, PKFieldID int64) *SegmentDeserializeReader {
	return &SegmentDeserializeReader{
		ctx:           ctx,
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

	blobs := lo.Map(allValues, func(v []byte, i int) *storage.Blob {
		return &storage.Blob{Key: r.binlogPaths[r.binlogPathPos][i], Value: v}
	})

	r.reader, err = storage.NewBinlogDeserializeReader(blobs, r.PKFieldID)
	if err != nil {
		log.Warn("compact wrong, failed to new insert binlogs reader", zap.Error(err))
		return err
	}
	r.binlogPathPos++
	return nil
}

func (r *SegmentDeserializeReader) Next() (*storage.Value, error) {
	if r.reader == nil {
		if err := r.initDeserializeReader(); err != nil {
			return nil, err
		}
	}
	if err := r.reader.Next(); err != nil {
		if err == io.EOF {
			r.reader.Close()
			if err := r.initDeserializeReader(); err != nil {
				return nil, err
			}
			err = r.reader.Next()
			return r.reader.Value(), err
		}
		return nil, err
	}
	return r.reader.Value(), nil
}

func (r *SegmentDeserializeReader) Close() {
	r.reader.Close()
}
