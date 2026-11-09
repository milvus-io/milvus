// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package importv3

import (
	"context"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
)

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
