// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0.

package importv3

import (
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// TaskProgress is the optional, task-kind-specific progress a worker reports
// through the task manager. The manager carries it opaquely; only the query
// path of the matching kind knows the concrete type (e.g. *ReshardProgress).
type TaskProgress any

// ReshardProgress tracks rows hashed per source file of one reshard run. The
// routing side counts a batch once HashDataBySchema has split it into buckets,
// so a file's rows count as imported at hash time rather than after the merge
// flush. The routing side writes while a Query may read, so the map is
// mutex-guarded.
type ReshardProgress struct {
	mu      sync.Mutex
	perFile map[int64]int64
}

func NewReshardProgress() *ReshardProgress {
	return &ReshardProgress{perFile: make(map[int64]int64)}
}

// AddHashed adds the rows of one routed batch to its source file's counter.
func (p *ReshardProgress) AddHashed(fileID, rows int64) {
	if p == nil || rows <= 0 {
		return
	}
	p.mu.Lock()
	p.perFile[fileID] += rows
	p.mu.Unlock()
}

// SourceProgresses returns the per-file hashed rows observed so far.
func (p *ReshardProgress) SourceProgresses() []*datapb.ReshardSourceProgress {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]*datapb.ReshardSourceProgress, 0, len(p.perFile))
	for fileID, rows := range p.perFile {
		out = append(out, &datapb.ReshardSourceProgress{FileId: fileID, HashedRows: rows})
	}
	return out
}
