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

package coordinator

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	qcmeta "github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
)

func TestTargetVersionReaderAdapter(t *testing.T) {
	tm := qcmeta.NewMockTargetManager(t)
	tm.EXPECT().GetCollectionTargetVersion(mock.Anything, int64(1), qcmeta.CurrentTarget).Return(int64(42))
	reader := NewTargetVersionReader(tm)
	assert.Equal(t, int64(42), reader.CurrentTargetVersion(context.Background(), 1))
}

func TestMixCoordPushGatedFields(t *testing.T) {
	// mixCoordImpl implements ProxyGatePusher by fanning the gated set out through the
	// proxy meta-cache invalidation with the defence payload.
	var got *proxypb.InvalidateCollMetaCacheRequest
	m := mockey.Mock((*mixCoordImpl).InvalidateCollectionMetaCache).To(
		func(s *mixCoordImpl, ctx context.Context, req *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
			got = req
			return &commonpb.Status{}, nil
		}).Build()
	defer m.UnPatch()

	s := &mixCoordImpl{}
	require.NoError(t, s.PushGatedFields(context.Background(), 100, []int64{10, 11}))
	require.NotNil(t, got)
	assert.Equal(t, int64(100), got.GetCollectionID())
	assert.True(t, got.GetDefenceUpdate())
	assert.Equal(t, []int64{10, 11}, got.GetDefenceGatedFields())
}
