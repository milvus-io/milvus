// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package importv3

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/datanode/taskcost"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestMemoryAdmissionPreImportFailureDrainsSiblingReaders(t *testing.T) {
	budget := taskcost.NewMemoryAdmission(func() int64 { return 8 * memoryGiB }, nil)
	s := memoryScheduler(t, 64, budget)
	gate, release := memoryGate(t)
	readerStarted := make(chan struct{})
	failed := make(chan struct{})
	patch := mockey.Mock((*preImportTask).countFile).To(
		func(_ *preImportTask, _ context.Context, file *internalpb.ImportFile, _ int) (*datapb.ImportV3FileStats, error) {
			if file.GetId() == 1 {
				<-readerStarted
				close(failed)
				return nil, merr.ErrIoTooManyRequests
			}
			close(readerStarted)
			<-gate // emulate a reader whose native IO ignores cancellation
			return &datapb.ImportV3FileStats{}, nil
		}).Build()
	defer patch.UnPatch()
	task := NewPreImportTask(&datapb.PreImportRequest{
		TaskID: 1, TaskSlot: 128,
		ImportFiles: []*internalpb.ImportFile{{Id: 1}, {Id: 2}},
	}, nil, taskcommon.Resource{CPU: 1, Memory: 6 * memoryGiB})
	submitAdmit(t, s, task)
	select {
	case <-failed:
	case <-time.After(5 * time.Second):
		t.Fatal("file failure was not reached")
	}
	started := make(chan int64, 1)
	submitAdmit(t, s, memoryTask(2, 1, 4*memoryGiB, nil, started))
	assertMemoryWaiting(t, started)
	require.EqualValues(t, 10*memoryGiB, s.Resource().Memory)
	release()
	require.Eventually(t, func() bool { return s.Slots() == 0 }, time.Second, time.Millisecond)
	result, ok := s.Query(1, PreImportV3RunID)
	require.True(t, ok)
	require.Equal(t, datapb.ImportTaskStateV2_Retry, result.State)
	s.admit()
	require.EqualValues(t, 2, waitMemoryTask(t, started))
	require.Eventually(t, func() bool { return s.Resource().Memory == 0 }, time.Second, time.Millisecond)
}

func TestMemoryAdmissionFragmentCleanupDrainsAfterFutureError(t *testing.T) {
	gate, release := memoryGate(t)
	started := make(chan struct{})
	writer := &fragmentWriter{
		cancel: func() {}, pool: conc.NewPool[any](1),
		futures: []*conc.Future[any]{
			conc.Go(func() (any, error) { return nil, merr.ErrIoTooManyRequests }),
			conc.Go(func() (any, error) { close(started); <-gate; return nil, nil }),
		},
	}
	finished := make(chan struct{})
	go func() { writer.close(); close(finished) }()
	<-started
	select {
	case <-finished:
		t.Fatal("cleanup returned while a fragment writer still owned its inputs")
	case <-time.After(100 * time.Millisecond):
	}
	release()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("fragment cleanup did not finish")
	}
}
