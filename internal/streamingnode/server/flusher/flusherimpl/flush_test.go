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

package flusherimpl

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/stretchr/testify/mock"
	"sync"
	"testing"

	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestFlush(t *testing.T) {
	paramtable.Init()
	const (
		vchannel = "mock_vchannel_v0"
	)

	rootcoord := mocks.NewMockRootCoordClient(t)
	datacoord := mocks.NewMockDataCoordClient(t)
	chunkManager := mocks.NewChunkManager(t)
	syncMgr := syncmgr.NewSyncManager(chunkManager)
	wbMgr := writebuffer.NewManager(syncMgr)

	resource.Init(
		resource.OptSyncManager(syncMgr),
		resource.OptBufferManager(wbMgr),
		resource.OptRootCoordClient(rootcoord),
		resource.OptDataCoordClient(datacoord),
	)

	GetPipelineParams()

	flusher := NewFlusher()
	flusher.Start()
	defer flusher.Stop()

	var handler wal.MessageHandler
	scanner := mock_wal.NewMockScanner(t)

	w := mock_wal.NewMockWAL(t)
	w.EXPECT().WALName().Return("rocksmq")
	w.EXPECT().Read(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, option wal.ReadOption) (wal.Scanner, error) {
			handlers = append(handlers, option.MesasgeHandler)
			return scanner, nil
		})

	once := sync.Once{}
	scanner.EXPECT().Close().RunAndReturn(func() error {
		once.Do(func() {
			for _, handler := range handlers {
				handler.Close()
			}
		})
		return nil
	})

	flusher.RegisterVChannel(vchannel, w)
}