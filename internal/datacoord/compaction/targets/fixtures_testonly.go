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
	"testing"

	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"

	metastoremocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

type compactionTargetCatalogUpdate struct {
	targetID        int64
	state           datapb.TargetState
	inactivatedAtTS uint64
}

func NewCompactionTargetTestCatalog(
	t *testing.T,
	records ...*datapb.CompactionTarget,
) (*metastoremocks.DataCoordCatalog, map[int64]*datapb.CompactionTarget, *[]compactionTargetCatalogUpdate, *[]int64) {
	catalog := metastoremocks.NewDataCoordCatalog(t)
	stored := make(map[int64]*datapb.CompactionTarget)
	for _, record := range records {
		stored[record.GetTargetID()] = proto.Clone(record).(*datapb.CompactionTarget)
	}
	updates := make([]compactionTargetCatalogUpdate, 0)
	dropped := make([]int64, 0)

	catalog.EXPECT().ListCompactionTargets(mock.Anything).RunAndReturn(
		func(context.Context) ([]*datapb.CompactionTarget, error) {
			return cloneCompactionTargetSlice(records), nil
		}).Maybe()
	catalog.EXPECT().SaveCompactionTarget(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, record *datapb.CompactionTarget) error {
			stored[record.GetTargetID()] = proto.Clone(record).(*datapb.CompactionTarget)
			return nil
		}).Maybe()
	catalog.EXPECT().UpdateCompactionTargetState(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, targetID int64, state datapb.TargetState, inactivatedAtTS uint64) error {
			updates = append(updates, compactionTargetCatalogUpdate{
				targetID:        targetID,
				state:           state,
				inactivatedAtTS: inactivatedAtTS,
			})
			record := proto.Clone(stored[targetID]).(*datapb.CompactionTarget)
			record.State = state
			record.InactivatedAtTS = inactivatedAtTS
			stored[targetID] = record
			return nil
		}).Maybe()
	catalog.EXPECT().DropCompactionTarget(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, record *datapb.CompactionTarget) error {
			dropped = append(dropped, record.GetTargetID())
			delete(stored, record.GetTargetID())
			return nil
		}).Maybe()

	return catalog, stored, &updates, &dropped
}

func cloneCompactionTargetSlice(records []*datapb.CompactionTarget) []*datapb.CompactionTarget {
	cloned := make([]*datapb.CompactionTarget, 0, len(records))
	for _, record := range records {
		cloned = append(cloned, proto.Clone(record).(*datapb.CompactionTarget))
	}
	return cloned
}
