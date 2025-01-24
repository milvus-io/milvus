package compaction

import (
	"context"
	"io"

	"github.com/samber/lo"

	binlogIO "github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/storage"
)

func NewSegmentRecordReader(ctx context.Context, binlogPaths [][]string, binlogIO binlogIO.BinlogIO) storage.RecordReader {
	pos := 0
	return &storage.CompositeBinlogRecordReader{
		BlobsReader: func() ([]*storage.Blob, error) {
			if pos >= len(binlogPaths) {
				return nil, io.EOF
			}
			bytesArr, err := binlogIO.Download(ctx, binlogPaths[pos])
			if err != nil {
				return nil, err
			}
			pos++
			blobs := lo.Map(bytesArr, func(v []byte, i int) *storage.Blob {
				return &storage.Blob{Key: binlogPaths[pos][i], Value: v}
			})
			return blobs, nil
		},
	}
}
