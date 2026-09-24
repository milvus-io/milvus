// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package importv3

// The import V3-family worker tasks. Each kind is a small, passive struct
// implementing Task: it carries its request and knows only how to Execute. It
// holds no manager reference; the Scheduler admits it and the TaskManager runs
// it. Nothing here is shared with the legacy importv2 task model.

import (
	"context"
	"io"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/datanode/importv2"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/internal/util/importutilv2/numpy"
	"github.com/milvus-io/milvus/internal/util/importutilv2/parquet"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// PreImportV2RunID is the run id every count-only preimport task uses: the
// kind has no run fencing of its own (DataCoord re-creates it with the same id
// after a retry, and the manager replaces the finished attempt).
const PreImportV2RunID = 1

// workerTaskBase carries what every import V3 worker task shares: its fence
// identity and slot cost. It deliberately holds no manager reference.
type workerTaskBase struct {
	taskID int64
	runID  int64
	slot   int64
}

func (t *workerTaskBase) TaskID() int64 { return t.taskID }
func (t *workerTaskBase) RunID() int64  { return t.runID }
func (t *workerTaskBase) Slot() int64   { return t.slot }

// reshardTask executes one ReshardTask run: route the source files into buckets
// and write the sorted fragments plus the manifest.
type reshardTask struct {
	workerTaskBase
	req           *datapb.ReshardTaskRequest
	cm            storage.ChunkManager
	pluginContext *indexcgopb.StoragePluginContext
	metrics       *Metrics
	progress      *ReshardProgress
}

// NewReshardTask builds one ReshardTask run. The Scheduler admits it once the
// node has free slots.
func NewReshardTask(req *datapb.ReshardTaskRequest, cm storage.ChunkManager, pluginContext *indexcgopb.StoragePluginContext, metrics *Metrics) Task {
	return &reshardTask{
		workerTaskBase: workerTaskBase{
			taskID: req.GetTaskId(),
			runID:  req.GetRunId(),
			slot:   req.GetSlot(),
		},
		req:           req,
		cm:            cm,
		pluginContext: pluginContext,
		metrics:       metrics,
		progress:      NewReshardProgress(),
	}
}

func (t *reshardTask) Kind() string { return "reshard" }

// Progress reports the run's per-source hashed rows; the query path asserts the
// concrete *ReshardProgress.
func (t *reshardTask) Progress() TaskProgress {
	return t.progress
}

func (t *reshardTask) Execute(ctx context.Context) (any, error) {
	return nil, executeReshardPlan(ctx, t.cm, t.req, t.req.GetPlan(), t.pluginContext, t.metrics, t.progress)
}

// importTask executes one ImportTaskV3 run: merge the plan's fragments into the
// formal segment and return its result.
type importTask struct {
	workerTaskBase
	req           *datapb.ImportTaskV3Request
	cm            storage.ChunkManager
	pluginContext *indexcgopb.StoragePluginContext
}

// NewImportTask builds one ImportTaskV3 run. The Scheduler admits it once the
// node has free slots.
func NewImportTask(req *datapb.ImportTaskV3Request, cm storage.ChunkManager, pluginContext *indexcgopb.StoragePluginContext) Task {
	return &importTask{
		workerTaskBase: workerTaskBase{
			taskID: req.GetTaskId(),
			runID:  req.GetRunId(),
			slot:   req.GetSlot(),
		},
		req:           req,
		cm:            cm,
		pluginContext: pluginContext,
	}
}

func (t *importTask) Kind() string { return "import" }

func (t *importTask) Execute(ctx context.Context) (any, error) {
	return executeImportPlan(ctx, t.cm, t.req, t.req.GetPlan(), t.pluginContext)
}

// preImportTask is the Import V3 count-only preimport: it computes the exact
// per-file row count and decoded size so DataCoord can allocate exact per-file
// ID ranges before Reshard. It never hashes, so it never builds the hashed
// bucket stats only the V2 regrouping consumes.
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
			runID:  PreImportV2RunID,
			slot:   req.GetTaskSlot(),
		},
		req: req,
		cm:  cm,
	}
}

func (t *preImportTask) Kind() string { return "preimport" }

func (t *preImportTask) Execute(ctx context.Context) (any, error) {
	bufferSize := int(paramtable.Get().DataNodeCfg.ImportBaseBufferSize.GetAsInt64())
	files := t.req.GetImportFiles()
	mlog.Info(ctx, "start to preimport v2 (count-only)",
		mlog.FieldTaskID(t.taskID),
		mlog.FieldJobID(t.req.GetJobID()),
		mlog.FieldCollectionID(t.req.GetCollectionID()),
		mlog.Int("bufferSize", bufferSize),
		mlog.Int64("taskSlot", t.slot),
		mlog.Any("files", files))
	stats := make([]*datapb.ImportFileStats, len(files))
	futures := make([]*conc.Future[any], 0, len(files))
	for i, file := range files {
		f := importv2.GetExecPool().Submit(func() (any, error) {
			stat, err := t.countFile(ctx, file, bufferSize)
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

// countFile computes the exact row count and decoded (logical) size of one
// source file. Parquet and numpy record the exact row count in metadata, so
// only the footer/header is read; JSON/CSV have no such metadata and fall
// through to the row scan.
func (t *preImportTask) countFile(ctx context.Context, file *internalpb.ImportFile, bufferSize int) (*datapb.ImportFileStats, error) {
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
		return &datapb.ImportFileStats{
			ImportFile:      file,
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
	return &datapb.ImportFileStats{
		ImportFile:      file,
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
	case datapb.ImportFileType_Parquet:
		rows, bytes, size, err := parquet.NumRowsAndBytes(ctx, t.cm, file.GetPaths()[0])
		return rows, bytes, size, true, err
	case datapb.ImportFileType_Numpy:
		rows, bytes, size, err := numpy.NumRowsAndBytes(ctx, t.cm, t.req.GetSchema(), file.GetPaths())
		return rows, bytes, size, true, err
	default:
		return 0, 0, 0, false, nil
	}
}
