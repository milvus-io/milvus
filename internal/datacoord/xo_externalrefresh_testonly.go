// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build test

package datacoord

import (
	"context"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// drainBuildIndexCh empties the build-acceleration channel and returns what it
// held, so a test can assert on exactly what one call pushed.
func drainBuildIndexCh() []int64 {
	ch := getBuildIndexChSingleton()
	out := make([]int64, 0)
	for {
		select {
		case id := <-ch:
			out = append(out, id)
		default:
			return out
		}
	}
}

// stubCatalog is a simple stub implementation of DataCoordCatalog for testing
type stubCatalog struct {
	metastore.DataCoordCatalog
	jobs            []*datapb.ExternalCollectionRefreshJob
	tasks           []*datapb.ExternalCollectionRefreshTask
	alterSegmentErr error
	alteredSegments []*datapb.SegmentInfo

	updateErr     error
	updateActions [][]metastore.UpdateAction
}

// Update records the actions passed to it (so tests can assert on the
// composite write a caller issued) and returns updateErr.
func (s *stubCatalog) Update(ctx context.Context, actions ...metastore.UpdateAction) error {
	s.updateActions = append(s.updateActions, actions)
	return s.updateErr
}

func (s *stubCatalog) ListExternalCollectionRefreshJobs(ctx context.Context) ([]*datapb.ExternalCollectionRefreshJob, error) {
	return s.jobs, nil
}

func (s *stubCatalog) ListExternalCollectionRefreshTasks(ctx context.Context) ([]*datapb.ExternalCollectionRefreshTask, error) {
	return s.tasks, nil
}

func (s *stubCatalog) SaveExternalCollectionRefreshJob(ctx context.Context, job *datapb.ExternalCollectionRefreshJob) error {
	// Append gives the body enough instructions for mockey to patch (a bare
	// `return nil` compiles to a single ret and cannot be patched).
	s.jobs = append(s.jobs, job)
	return nil
}

func (s *stubCatalog) SaveExternalCollectionRefreshTask(ctx context.Context, task *datapb.ExternalCollectionRefreshTask) error {
	// Append gives the body enough instructions for mockey to patch (a bare
	// `return nil` compiles to a single ret and cannot be patched).
	s.tasks = append(s.tasks, task)
	return nil
}

func (s *stubCatalog) AlterSegments(ctx context.Context, newSegments []*datapb.SegmentInfo, binlogs ...metastore.BinlogsIncrement) error {
	s.alteredSegments = append([]*datapb.SegmentInfo(nil), newSegments...)
	return s.alterSegmentErr
}
