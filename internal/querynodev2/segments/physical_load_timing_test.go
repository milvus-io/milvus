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

package segments

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestLocalSegmentLoadRecordsEnteredPhases(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().CommonCfg.EnabledJSONKeyStats.Key
	old := paramtable.Get().CommonCfg.EnabledJSONKeyStats.GetValue()
	require.NoError(t, paramtable.Get().Save(key, "true"))
	t.Cleanup(func() { require.NoError(t, paramtable.Get().Save(key, old)) })
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "success"},
		{name: "native failure", err: merr.ErrServiceUnavailable},
		{name: "cancellation", err: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			native := mock_segcore.NewMockCSegment(t)
			native.EXPECT().Load(mock.Anything).Return(tc.err).Once()
			segment := &LocalSegment{
				baseSegment: baseSegment{loadInfo: atomic.NewPointer(&querypb.SegmentLoadInfo{})},
				csegment:    native,
			}
			timing := &PhysicalLoadTiming{}
			err := segment.Load(WithPhysicalLoadTiming(context.Background(), timing))
			require.ErrorIs(t, err, tc.err)
			require.Positive(t, timing.CSegmentLoad)
			if tc.err == nil {
				require.Positive(t, timing.SyncJSONStats)
			} else {
				require.Zero(t, timing.SyncJSONStats, "native failure must not run post-load stats sync")
			}
		})
	}
}
