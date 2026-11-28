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

package meta

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

func TestManifestReadBudgetSharedAndCancellable(t *testing.T) {
	old := Params.DataCoordCfg.SegmentIndexManifestLoadConcurrency.SwapTempValue("2")
	defer Params.DataCoordCfg.SegmentIndexManifestLoadConcurrency.SwapTempValue(old)
	m := &Meta{}
	io := packed.NewManifestIOContext(8)
	defer io.Close()
	entered := make(chan struct{}, 8)
	release := make(chan struct{})
	reader := mockey.Mock(packed.GetManifestIndexInfosAsync).To(func(context.Context, *packed.ManifestIOContext, string, *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
		entered <- struct{}{}
		<-release
		return nil, nil
	}).Build()
	defer reader.UnPatch()
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := m.ReadManifestIndexesWithIO(context.Background(), io, "manifest", nil)
			require.NoError(t, err)
		}()
	}
	for range 2 {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("read admission stalled")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := m.ReadManifestIndexesWithIO(ctx, io, "canceled", nil)
	require.ErrorIs(t, err, context.Canceled)
	select {
	case <-entered:
		t.Fatal("read exceeded process budget")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	wg.Wait()
	require.Len(t, entered, 6)
}

// A delayed first read must not impose a barrier on subsequent segments.
