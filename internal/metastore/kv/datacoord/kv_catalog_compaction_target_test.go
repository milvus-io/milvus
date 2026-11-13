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

package datacoord

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestCatalogCompactionTargetSuite(t *testing.T) {
	suite.Run(t, new(CatalogCompactionTargetSuite))
}

type CatalogCompactionTargetSuite struct {
	suite.Suite

	ctx     context.Context
	metakv  *catalogCompactionTargetMemoryKV
	catalog *Catalog
}

func (s *CatalogCompactionTargetSuite) SetupTest() {
	s.ctx = context.Background()
	s.metakv = newCatalogCompactionTargetMemoryKV()
	s.catalog = NewCatalog(s.metakv, rootPath, "")
}

func (s *CatalogCompactionTargetSuite) TestCreateListStopAndRemove() {
	record := &datapb.CompactionTarget{
		TargetID:      10,
		CollectionID:  100,
		Intent:        datapb.TargetIntent_INTENT_REWRITE,
		Properties:    map[string]string{"segment_ids": "[1,2]"},
		ExpectedTS:    300,
		TailLimit:     0,
		State:         datapb.TargetState_TARGET_STATE_ACTIVE,
		ActivatedAtTS: 301,
	}

	_, err := s.catalog.CreateCompactionTarget(s.ctx, record)
	s.Require().NoError(err)
	s.True(proto.Equal(record, s.loadRecord(10)))

	unknownTarget := proto.Clone(record).(*datapb.CompactionTarget)
	unknownTarget.TargetID = 11
	unknownTarget.Intent = datapb.TargetIntent(999)
	_, err = s.catalog.CreateCompactionTarget(s.ctx, unknownTarget)
	s.Require().NoError(err)

	records, err := s.catalog.ListCompactionTargets(s.ctx)
	s.Require().NoError(err)
	s.Require().Len(records, 2)
	s.Equal(datapb.TargetIntent_INTENT_REWRITE, records[0].GetIntent())
	s.Equal(datapb.TargetIntent(999), records[1].GetIntent())

	stop := &datapb.CompactionTarget{TargetID: 10, State: datapb.TargetState_TARGET_STATE_INACTIVE,
		InactivatedAtTS: 500, StopReason: "operator_dropped"}
	updated, changed, err := s.catalog.StopCompactionTarget(s.ctx, stop)
	s.Require().NoError(err)
	s.True(changed)
	stop.InactivatedAtTS = 900
	stop.StopReason = "satisfied"
	repeated, changed, err := s.catalog.StopCompactionTarget(s.ctx, stop)
	s.Require().NoError(err)
	s.False(changed)
	s.True(proto.Equal(updated, repeated))
	s.Equal(int64(10), updated.GetTargetID())
	s.Equal(int64(100), updated.GetCollectionID())
	s.Equal(map[string]string{"segment_ids": "[1,2]"}, updated.GetProperties())
	s.Equal(datapb.TargetState_TARGET_STATE_INACTIVE, updated.GetState())
	s.Equal(uint64(500), updated.GetInactivatedAtTS())

	s.Require().NoError(s.catalog.DropCompactionTarget(s.ctx, record))
	_, err = s.metakv.Load(s.ctx, CompactionTargetPrefix+"/10")
	s.Error(err)
}

func (s *CatalogCompactionTargetSuite) TestListReturnsUnmarshalError() {
	s.Require().NoError(s.metakv.Save(s.ctx, CompactionTargetPrefix+"/10", "not-protobuf"))

	records, err := s.catalog.ListCompactionTargets(s.ctx)
	s.Error(err)
	s.Nil(records)
}

func (s *CatalogCompactionTargetSuite) loadRecord(targetID int64) *datapb.CompactionTarget {
	value, err := s.metakv.Load(s.ctx, buildCompactionTargetPath(targetID))
	s.Require().NoError(err)
	record := &datapb.CompactionTarget{}
	s.Require().NoError(proto.Unmarshal([]byte(value), record))
	return record
}

type catalogCompactionTargetMemoryKV struct {
	memkv.MemoryKV
	writes sync.Mutex
}

var _ kv.MetaKv = (*catalogCompactionTargetMemoryKV)(nil)

func newCatalogCompactionTargetMemoryKV() *catalogCompactionTargetMemoryKV {
	return &catalogCompactionTargetMemoryKV{MemoryKV: *memkv.NewMemoryKV()}
}

func (m *catalogCompactionTargetMemoryKV) WalkWithPrefix(ctx context.Context, prefix string, paginationSize int, fn func([]byte, []byte) error) error {
	keys, values, err := m.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return err
	}
	for i, key := range keys {
		if err := fn([]byte(key), []byte(values[i])); err != nil {
			return err
		}
	}
	return nil
}

func (m *catalogCompactionTargetMemoryKV) GetPath(key string) string {
	return key
}

func (m *catalogCompactionTargetMemoryKV) CompareVersionAndSwap(context.Context, string, int64, string) (bool, error) {
	panic("unexpected CompareVersionAndSwap")
}

func (m *catalogCompactionTargetMemoryKV) MultiSaveAndRemove(ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error {
	m.writes.Lock()
	defer m.writes.Unlock()
	for _, pred := range preds {
		value, err := m.MemoryKV.Load(ctx, pred.Key())
		switch pred.Type() {
		case predicates.PredTypeNotExist:
			if err == nil {
				return merr.ErrCompactionPlanConflict
			}
		case predicates.PredTypeEqual:
			if err != nil || !pred.IsTrue(value) {
				return merr.ErrCompactionPlanConflict
			}
		}
	}
	return m.MemoryKV.MultiSaveAndRemove(ctx, saves, removals)
}

func TestCompactionTargetCreateClassifiesBackendWriteOutcome(t *testing.T) {
	for _, test := range []struct {
		name      string
		err       error
		ambiguous bool
	}{
		{"local deadline", context.DeadlineExceeded, true},
		{"lost server acknowledgment", rpctypes.ErrTimeoutDueToConnectionLost, true},
		{"leader timeout", rpctypes.ErrTimeoutDueToLeaderFail, true},
		{"transport unavailable", status.Error(codes.Unavailable, "connection closed"), true},
		{"server permission denied", rpctypes.ErrPermissionDenied, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend := &targetWriteErrorKV{
				catalogCompactionTargetMemoryKV: newCatalogCompactionTargetMemoryKV(),
				err:                             merr.WrapErrIoFailedErr(test.err, "execute etcd txn"),
			}
			catalog := NewCatalog(backend, rootPath, "")
			unknown, err := catalog.CreateCompactionTarget(t.Context(), &datapb.CompactionTarget{TargetID: 100})
			require.ErrorIs(t, err, merr.ErrIoFailed)
			require.Equal(t, test.ambiguous, unknown)
		})
	}
}

type targetWriteErrorKV struct {
	*catalogCompactionTargetMemoryKV
	err error
}

func (m *targetWriteErrorKV) MultiSaveAndRemove(context.Context, map[string]string, []string, ...predicates.Predicate) error {
	return m.err
}
