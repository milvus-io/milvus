// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package importv3

// The Import V3 count-only preimport worker task and its file-counting
// helpers: it computes the exact per-file row counts and sizes so DataCoord
// can allocate exact per-file ID ranges before Reshard.

import (
	"context"
	"io"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/datanode/importv2"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/internal/util/importutilv2/binlog"
	"github.com/milvus-io/milvus/internal/util/importutilv2/numpy"
	"github.com/milvus-io/milvus/internal/util/importutilv2/parquet"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// PreImportV3RunID is the run id every count-only preimport task uses: the
// kind has no run fencing of its own (DataCoord re-creates it with the same id
// after a retry, and the manager replaces the finished attempt).
const PreImportV3RunID = 1

// preImportTask is the Import V3 count-only preimport: it computes the exact
// per-file row count and decoded size so DataCoord can allocate exact per-file
// ID ranges before Reshard. A backup job carries its own PK/RowID and needs no
// rows, so it runs a size-only mode that expands each source's object list and
// sums its bytes without reading any content. The task never hashes, so it never
// builds the hashed bucket stats only the V2 regrouping consumes.
type preImportTask struct {
	workerTaskBase
	req *datapb.PreImportRequest
	cm  storage.ChunkManager
}

// NewPreImportTask builds the count-only preimport task. The Scheduler admits
// it once the node has free slots.
func NewPreImportTask(req *datapb.PreImportRequest, cm storage.ChunkManager) Task {
	return &preImportTask{
		workerTaskBase: workerTaskBase{
			taskID: req.GetTaskID(),
			runID:  PreImportV3RunID,
			slot:   req.GetTaskSlot(),
		},
		req: req,
		cm:  cm,
	}
}

func (t *preImportTask) Kind() string { return "preimport" }

func (t *preImportTask) Execute(ctx context.Context) (any, error) {
	// A single file failure short-circuits AwaitAll; cancel the shared ctx so
	// the remaining per-file count tasks stop instead of scanning on after this
	// task has reported failure and freed its slot.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	bufferSize := int(paramtable.Get().DataNodeCfg.ImportBaseBufferSize.GetAsInt64())
	files := t.req.GetImportFiles()
	// A backup job needs no row counts, so it runs the size-only mode: it only
	// sizes each source's expanded object list for DataCoord's reshard BFD.
	backup := importutilv2.IsBackup(t.req.GetOptions())
	mlog.Info(ctx, "start to preimport v3 (count-only)",
		mlog.FieldTaskID(t.taskID),
		mlog.FieldJobID(t.req.GetJobID()),
		mlog.FieldCollectionID(t.req.GetCollectionID()),
		mlog.Int("bufferSize", bufferSize),
		mlog.Int64("taskSlot", t.slot),
		mlog.Bool("backup", backup),
		mlog.Any("files", files))
	stats := make([]*datapb.ImportV3FileStats, len(files))
	futures := make([]*conc.Future[any], 0, len(files))
	for i, file := range files {
		f := importv2.GetExecPool().Submit(func() (any, error) {
			var stat *datapb.ImportV3FileStats
			var err error
			if backup {
				stat, err = t.sizeOnlyFile(ctx, file)
			} else {
				stat, err = t.countFile(ctx, file, bufferSize)
			}
			if err != nil {
				return nil, err
			}
			stats[i] = stat
			return nil, nil
		})
		futures = append(futures, f)
	}
	if err := conc.AwaitAll(futures...); err != nil {
		return nil, err
	}
	return stats, nil
}

// sizeOnlyFile is the backup preimport mode: it expands one ImportFile's object
// list (the insert prefix plus the optional delta prefix) and sums the object
// bytes, reading no content. The row count is left 0 and total_memory_size is
// left unset, because DataCoord only needs the per-file size for reshard BFD.
func (t *preImportTask) sizeOnlyFile(ctx context.Context, file *internalpb.ImportFile) (*datapb.ImportV3FileStats, error) {
	maxSize := paramtable.Get().DataNodeCfg.MaxImportFileSizeInGB.GetAsFloat() * 1024 * 1024 * 1024

	start := time.Now()
	insertObjects, deltaObjects, err := binlog.ExpandObjects(ctx, t.cm, file.GetPaths())
	if err != nil {
		return nil, err
	}
	paths := make([]string, 0, len(insertObjects)+len(deltaObjects))
	for _, fieldPaths := range insertObjects {
		paths = append(paths, fieldPaths...)
	}
	paths = append(paths, deltaObjects...)
	size, err := storage.GetFilesSize(ctx, paths, t.cm)
	if err != nil {
		return nil, err
	}
	if size > int64(maxSize) {
		return nil, merr.WrapErrParameterInvalidMsg(
			"The import file size has reached the maximum limit allowed for importing, "+
				"fileSize=%d, maxSize=%d", size, int64(maxSize))
	}
	mlog.Info(ctx, "size file stat done (backup)",
		mlog.FieldTaskID(t.taskID), mlog.Strings("files", file.GetPaths()),
		mlog.Duration("dur", time.Since(start)))
	return &datapb.ImportV3FileStats{
		FileId:   file.GetId(),
		FileSize: size,
	}, nil
}

// countFile computes the exact row count and decoded (logical) size of one
// source file. Parquet and numpy record the exact row count in metadata, so
// only the footer/header is read; JSON/CSV have no such metadata and fall
// through to the row scan.
func (t *preImportTask) countFile(ctx context.Context, file *internalpb.ImportFile, bufferSize int) (*datapb.ImportV3FileStats, error) {
	maxSize := paramtable.Get().DataNodeCfg.MaxImportFileSizeInGB.GetAsFloat() * 1024 * 1024 * 1024

	start := time.Now()
	if rows, logicalBytes, fileSize, cheap, err := t.cheapRowCount(ctx, file); err != nil {
		return nil, err
	} else if cheap {
		if fileSize > int64(maxSize) {
			return nil, merr.WrapErrParameterInvalidMsg(
				"The import file size has reached the maximum limit allowed for importing, "+
					"fileSize=%d, maxSize=%d", fileSize, int64(maxSize))
		}
		mlog.Info(ctx, "count file stat done (metadata)",
			mlog.FieldTaskID(t.taskID), mlog.Strings("files", file.GetPaths()),
			mlog.Duration("dur", time.Since(start)))
		return &datapb.ImportV3FileStats{
			FileId:          file.GetId(),
			FileSize:        fileSize,
			TotalRows:       rows,
			TotalMemorySize: logicalBytes,
		}, nil
	}

	reader, err := importutilv2.NewReader(ctx, t.cm, t.req.GetSchema(), file, t.req.GetOptions(), bufferSize, t.req.GetStorageConfig())
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	fileSize, err := reader.Size()
	if err != nil {
		return nil, err
	}
	if fileSize > int64(maxSize) {
		return nil, merr.WrapErrParameterInvalidMsg(
			"The import file size has reached the maximum limit allowed for importing, "+
				"fileSize=%d, maxSize=%d", fileSize, int64(maxSize))
	}

	totalRows := 0
	totalSize := 0
	for {
		data, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		if err := importv2.CheckRowsEqual(t.req.GetSchema(), data); err != nil {
			return nil, err
		}
		if err := importv2.CheckStructArrayConsistency(t.req.GetSchema(), data); err != nil {
			return nil, err
		}
		rows := data.GetRowNum()
		size := data.GetMemorySize()
		totalRows += rows
		totalSize += size
	}
	mlog.Info(ctx, "count file stat done (scan)",
		mlog.FieldTaskID(t.taskID), mlog.Strings("files", file.GetPaths()),
		mlog.Duration("dur", time.Since(start)))
	return &datapb.ImportV3FileStats{
		FileId:          file.GetId(),
		FileSize:        fileSize,
		TotalRows:       int64(totalRows),
		TotalMemorySize: int64(totalSize),
	}, nil
}

// cheapRowCount returns the exact row count, decoded (logical) size and physical
// size of a columnar file from its metadata, without decoding any data: the parquet
// footer or the npy header. The fourth return value is false for formats (JSON/CSV)
// whose exact count requires a scan; the caller then reads the file. Callers on the
// metadata path use the physical size to avoid a second object-store stat.
func (t *preImportTask) cheapRowCount(ctx context.Context, file *internalpb.ImportFile) (int64, int64, int64, bool, error) {
	fileType, err := importutilv2.GetFileType(file)
	if err != nil {
		return 0, 0, 0, false, err
	}
	switch fileType {
	case importutilv2.FileTypeParquet:
		rows, bytes, size, err := parquet.NumRowsAndBytes(ctx, t.cm, file.GetPaths()[0])
		return rows, bytes, size, true, err
	case importutilv2.FileTypeNumpy:
		rows, bytes, size, err := numpy.NumRowsAndBytes(ctx, t.cm, t.req.GetSchema(), file.GetPaths())
		return rows, bytes, size, true, err
	default:
		return 0, 0, 0, false, nil
	}
}
