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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestManifestCommitExecutorIndependentFromRecovery(t *testing.T) {
	readLimit := Params.DataCoordCfg.SegmentIndexManifestLoadConcurrency.SwapTempValue("1")
	defer Params.DataCoordCfg.SegmentIndexManifestLoadConcurrency.SwapTempValue(readLimit)
	commitLimit := Params.DataCoordCfg.L0ManifestUpdatePoolSize.SwapTempValue("1")
	defer Params.DataCoordCfg.L0ManifestUpdatePoolSize.SwapTempValue(commitLimit)
	m := &meta{manifestCommitExecutor: newManifestCommitExecutor(3)}
	t.Cleanup(m.closeManifestCommitExecutor)
	executor := m.manifestCommitExecutor
	io, release, err := executor.acquire(context.Background())
	require.NoError(t, err)
	defer release()
	require.Equal(t, 3, executor.concurrency)
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
	require.Equal(t, 3, executor.concurrency)
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
	previous := Params.DataCoordCfg.L0ManifestUpdatePoolSize.SwapTempValue("2")
	defer Params.DataCoordCfg.L0ManifestUpdatePoolSize.SwapTempValue(previous)
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	executor := m.manifestCommitExecutor
	require.Equal(t, 2, executor.concurrency)
	io, release, err := executor.acquire(context.Background())
	require.NoError(t, err)
	release()
	Params.DataCoordCfg.L0ManifestUpdatePoolSize.SwapTempValue("4")
	reused, releaseAgain, err := executor.acquire(context.Background())
	require.NoError(t, err)
	defer releaseAgain()
	require.Same(t, io, reused)
	require.Equal(t, 2, executor.concurrency, "capacity stays fixed until the component is recreated")
}
