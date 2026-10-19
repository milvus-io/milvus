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
