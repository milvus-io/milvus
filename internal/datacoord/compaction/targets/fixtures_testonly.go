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

package targets

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	metastoremocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func NewCompactionTargetTestCatalog(
	t *testing.T,
	records ...*datapb.CompactionTarget,
) (*metastoremocks.DataCoordCatalog, map[int64]*datapb.CompactionTarget) {
	catalog := metastoremocks.NewDataCoordCatalog(t)
	stored := make(map[int64]*datapb.CompactionTarget)
	for _, record := range records {
		stored[record.GetTargetID()] = proto.CloneOf(record)
	}
	var mu sync.Mutex
	catalog.EXPECT().ListCompactionTargets(mock.Anything).RunAndReturn(
		func(context.Context) ([]*datapb.CompactionTarget, error) {
			mu.Lock()
			defer mu.Unlock()
			result := make([]*datapb.CompactionTarget, 0, len(stored))
			for _, record := range stored {
				result = append(result, proto.CloneOf(record))
			}
			return result, nil
		}).Maybe()
	catalog.EXPECT().CreateCompactionTarget(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, record *datapb.CompactionTarget, _ *metastore.CompactionTargetCollectionVersion) (bool, error) {
			mu.Lock()
			defer mu.Unlock()
			if stored[record.GetTargetID()] != nil {
				return false, merr.ErrCompactionPlanConflict
			}
			stored[record.GetTargetID()] = proto.CloneOf(record)
			return false, nil
		}).Maybe()
	catalog.EXPECT().LoadCompactionTarget(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, id int64) (*datapb.CompactionTarget, error) {
			mu.Lock()
			defer mu.Unlock()
			return proto.CloneOf(stored[id]), nil
		}).Maybe()
	catalog.EXPECT().StopCompactionTarget(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, record *datapb.CompactionTarget) (*datapb.CompactionTarget, bool, error) {
			mu.Lock()
			defer mu.Unlock()
			current := stored[record.GetTargetID()]
			if current == nil || current.GetState() == datapb.TargetState_TARGET_STATE_INACTIVE {
				return proto.CloneOf(current), false, nil
			}
			stored[record.GetTargetID()] = proto.CloneOf(record)
			return proto.CloneOf(record), true, nil
		}).Maybe()
	return catalog, stored
}

func NewLoadedCompactionTargetMeta(t *testing.T, ctx context.Context, records ...*datapb.CompactionTarget) *CompactionTargetMeta {
	t.Helper()
	for _, record := range records {
		if record.GetIntent() == datapb.TargetIntent_INTENT_REWRITE {
			if record.GetRewrite() == nil {
				record.Rewrite = &datapb.CompactionTargetRewrite{}
			}
			record.ActivatedAtTS = record.GetExpectedTS()
		}
	}
	catalog, _ := NewCompactionTargetTestCatalog(t, records...)
	targetMeta, err := NewCompactionTargetMeta(ctx, catalog)
	require.NoError(t, err)
	return targetMeta
}
