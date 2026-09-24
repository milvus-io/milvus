// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package importv3

// The import V3-family worker tasks. Each kind is a small, passive struct
// implementing Task: it carries its request and knows only how to Execute. It
// holds no pool reference; the Scheduler admits it and runs it. Nothing here is
// shared with the legacy importv2 task model.

import (
	"context"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
)

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
