// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestManifestCommitExecutorIndependentFromRecovery(t *testing.T) {
	readLimit := Params.DataCoordCfg.SegmentIndexManifestLoadConcurrency.SwapTempValue("1")
	defer Params.DataCoordCfg.SegmentIndexManifestLoadConcurrency.SwapTempValue(readLimit)
	commitLimit := Params.DataCoordCfg.ManifestCommitConcurrency.SwapTempValue("1")
	defer Params.DataCoordCfg.ManifestCommitConcurrency.SwapTempValue(commitLimit)
	m := &meta{manifestCommitExecutor: newManifestCommitExecutor(3)}
	t.Cleanup(m.closeManifestCommitExecutor)
	executor := m.manifestCommitExecutor
	io, release, err := executor.acquire(context.Background())
	require.NoError(t, err)
	defer release()
	assertManifestCommitCapacity(t, io, 3)
	reader := m.newManifestIndexReader(context.Background(), []*SegmentInfo{NewSegmentInfo(&datapb.SegmentInfo{ID: 1}), NewSegmentInfo(&datapb.SegmentInfo{ID: 2})}, 2, nil)
	require.NotSame(t, io, reader.io)
	require.Equal(t, 2, reader.limit, "use the supplied concurrency, not the global read setting")
	reader.close()
	err = packed.SubmitManifestIndexInfos(context.Background(), reader.io, packed.MarshalManifestPath("/tmp/closed-reader", 1), nil, func([]packed.ManifestIndexInfo, error) {
		t.Error("closed reader must not accept a callback")
	})
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	release()
	Params.DataCoordCfg.SegmentIndexManifestLoadConcurrency.SwapTempValue("64")
	reused, releaseAgain, err := executor.acquire(context.Background())
	require.NoError(t, err)
	defer releaseAgain()
	require.Same(t, io, reused, "recovery closure and read settings must not replace the commit executor")
	assertManifestCommitCapacity(t, reused, 3)
}

func TestManifestCommitExecutorCloseDrainsLeases(t *testing.T) {
	executor := newManifestCommitExecutor(2)
	defer executor.close()
	io, release, err := executor.acquire(context.Background())
	require.NoError(t, err)
	defer release()
	closed := make(chan struct{})
	go func() { executor.close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("closed executor before its lease finished")
	case <-time.After(20 * time.Millisecond):
	}
	release()
	<-closed
	_, _, err = executor.acquire(context.Background())
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	err = packed.SubmitManifestIndexInfos(context.Background(), io, packed.MarshalManifestPath("/tmp/closed-executor", 1), nil, func([]packed.ManifestIndexInfo, error) {
		t.Error("closed executor must not accept a callback")
	})
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
}

func TestManifestCommitExecutorInitializedWithMeta(t *testing.T) {
	previous := Params.DataCoordCfg.ManifestCommitConcurrency.SwapTempValue("2")
	defer Params.DataCoordCfg.ManifestCommitConcurrency.SwapTempValue(previous)
	const obsoleteKey = "dataCoord.compaction.levelzero.manifestUpdatePoolSize"
	require.NoError(t, Params.Save(obsoleteKey, "1"))
	defer Params.Reset(obsoleteKey)
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	executor := m.manifestCommitExecutor
	io, release, err := executor.acquire(context.Background())
	require.NoError(t, err)
	assertManifestCommitCapacity(t, io, 2)
	release()
	Params.DataCoordCfg.ManifestCommitConcurrency.SwapTempValue("4")
	reused, releaseAgain, err := executor.acquire(context.Background())
	require.NoError(t, err)
	defer releaseAgain()
	require.Same(t, io, reused)
	assertManifestCommitCapacity(t, reused, 2)
}

// Hold terminal callbacks to keep real native operations admitted. A further
// submission must time out until those callbacks return; inspecting a saved
// configuration value would not prove the executor actually enforces it.
func assertManifestCommitCapacity(t *testing.T, io *packed.ManifestIOContext, capacity int) {
	t.Helper()
	root := t.TempDir()
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: root}
	manifest := packed.MarshalManifestPath(root, 0)
	gate := make(chan struct{})
	entered := make(chan error, capacity)
	var callbacks sync.WaitGroup
	defer func() { close(gate); callbacks.Wait() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for range capacity {
		callbacks.Add(1)
		err := packed.SubmitManifestIndexInfos(ctx, io, manifest, cfg, func(_ []packed.ManifestIndexInfo, err error) {
			defer callbacks.Done()
			entered <- err
			<-gate // Test-only barrier: production callbacks do not block.
		})
		if err != nil {
			callbacks.Done()
		}
		require.NoError(t, err)
	}
	for range capacity {
		select {
		case err := <-entered:
			require.NoError(t, err)
		case <-ctx.Done():
			t.Fatal("configured manifest capacity was not available")
		}
	}
	blockedCtx, cancelBlocked := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelBlocked()
	err := packed.SubmitManifestIndexInfos(blockedCtx, io, manifest, cfg, func([]packed.ManifestIndexInfo, error) {
		t.Error("submission exceeded configured capacity")
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
