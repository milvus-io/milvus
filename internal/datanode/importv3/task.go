// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package importv3

// The import V3-family worker tasks. Each kind is a small, passive struct
// implementing Task: it carries its request and knows only how to Execute. It
// holds no pool reference; the Scheduler admits it and runs it. Nothing here is
// shared with the legacy importv2 task model.

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
