package csv

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type Row = map[storage.FieldID]any

type reader struct {
	ctx    context.Context
	cm     storage.ChunkManager
	schema *schemapb.CollectionSchema

	cr     *csv.Reader
	parser RowParser

	fileSize   *atomic.Int64
	bufferSize int
	count      int64
	filePath   string
}

func NewReader(ctx context.Context, cm storage.ChunkManager, schema *schemapb.CollectionSchema, path string, bufferSize int, sep rune) (*reader, error) {
	cmReader, err := cm.Reader(ctx, path)
	if err != nil {
		return nil, merr.WrapErrImportFailed(fmt.Sprintf("read csv file failed, path=%s, err=%s", path, err.Error()))
	}
	// count, err := estimateReadCountPerBatch(bufferSize, schema)
	// if err != nil {
	// 	return nil, err
	// }

	// set the interval for determining if the buffer is exceeded
	var count int64 = 1000

	csvReader := csv.NewReader(cmReader)
	csvReader.Comma = sep

	header, err := csvReader.Read()
	log.Info("csv header parsed", zap.Strings("header", header))
	if err != nil {
		return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to read csv header, error: %v", err))
	}

	rowParser, err := NewRowParser(schema, header)
	if err != nil {
		return nil, err
	}
	return &reader{
		ctx:        ctx,
		cm:         cm,
		schema:     schema,
		cr:         csvReader,
		parser:     rowParser,
		fileSize:   atomic.NewInt64(0),
		filePath:   path,
		bufferSize: bufferSize,
		count:      count,
	}, nil
}

func (r *reader) Read() (*storage.InsertData, error) {
	insertData, err := storage.NewInsertData(r.schema)
	if err != nil {
		return nil, err
	}
	var cnt int64 = 0
	for {
		value, err := r.cr.Read()
		if err == io.EOF || len(value) == 0 {
			break
		}
		row, err := r.parser.Parse(value)
		if err != nil {
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to parse row, error: %v", err))
		}
		err = insertData.Append(row)
		if err != nil {
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to append row, error: %v", err))
		}
		cnt++
		if cnt >= r.count {
			cnt = 0
			if insertData.GetMemorySize() >= r.bufferSize {
				break
			}
		}
	}

	// finish reading
	if insertData.GetRowNum() == 0 {
		return nil, io.EOF
	}

	return insertData, nil
}

func (r *reader) Close() {}

func (r *reader) Size() (int64, error) {
	if size := r.fileSize.Load(); size != 0 {
		return size, nil
	}
	size, err := r.cm.Size(r.ctx, r.filePath)
	if err != nil {
		return 0, err
	}
	r.fileSize.Store(size)
	return size, nil
}

// func estimateReadCountPerBatch(bufferSize int, schema *schemapb.CollectionSchema) (int64, error) {
// 	sizePerRecord, err := typeutil.EstimateMaxSizePerRecord(schema)
// 	if err != nil {
// 		return 0, err
// 	}
// 	if 1000*sizePerRecord <= bufferSize {
// 		return 1000, nil
// 	}
// 	return int64(bufferSize) / int64(sizePerRecord), nil
// }
