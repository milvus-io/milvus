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
	"net/url"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/server/v3/embed"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/metastore"
	rootcatalog "github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	streamingcatalog "github.com/milvus-io/milvus/internal/metastore/kv/streamingcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
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
	epoch   uint64
}

func (s *CatalogCompactionTargetSuite) SetupTest() {
	s.ctx = context.Background()
	s.metakv = newCatalogCompactionTargetMemoryKV()
	s.catalog = NewCatalog(s.metakv, rootPath, "")
	var err error
	s.epoch, err = s.catalog.FenceCompactionTargetWrites(s.ctx)
	s.Require().NoError(err)
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

	require.NoError(s.T(), rootcatalog.NewCatalog(s.metakv).CreateCollection(s.ctx, &model.Collection{
		CollectionID: record.GetCollectionID(), State: etcdpb.CollectionState_CollectionCreated,
	}, 0))
	scope := &metastore.CompactionTargetCollectionVersion{}
	_, err := s.catalog.CreateCompactionTarget(s.ctx, record, scope, s.epoch)
	s.Require().NoError(err)
	s.True(proto.Equal(record, s.loadRecord(10)))

	unknownTarget := proto.Clone(record).(*datapb.CompactionTarget)
	unknownTarget.TargetID = 11
	unknownTarget.Intent = datapb.TargetIntent(999)
	_, err = s.catalog.CreateCompactionTarget(s.ctx, unknownTarget, scope, s.epoch)
	s.Require().NoError(err)

	records, err := s.catalog.ListCompactionTargets(s.ctx)
	s.Require().NoError(err)
	s.Require().Len(records, 2)
	s.Equal(datapb.TargetIntent_INTENT_REWRITE, records[0].GetIntent())
	s.Equal(datapb.TargetIntent(999), records[1].GetIntent())

	stop := &datapb.CompactionTarget{TargetID: 10, State: datapb.TargetState_TARGET_STATE_INACTIVE,
		InactivatedAtTS: 500, StopReason: "operator_dropped"}
	updated, changed, err := s.catalog.StopCompactionTarget(s.ctx, stop, s.epoch)
	s.Require().NoError(err)
	s.True(changed)
	stop.InactivatedAtTS = 900
	stop.StopReason = "satisfied"
	repeated, changed, err := s.catalog.StopCompactionTarget(s.ctx, stop, s.epoch)
	s.Require().NoError(err)
	s.False(changed)
	s.True(proto.Equal(updated, repeated))
	s.Equal(int64(10), updated.GetTargetID())
	s.Equal(int64(100), updated.GetCollectionID())
	s.Equal(map[string]string{"segment_ids": "[1,2]"}, updated.GetProperties())
	s.Equal(datapb.TargetState_TARGET_STATE_INACTIVE, updated.GetState())
	s.Equal(uint64(500), updated.GetInactivatedAtTS())

	s.Require().ErrorIs(s.catalog.DropCompactionTarget(s.ctx, record, s.epoch), merr.ErrCompactionPlanConflict)
	s.Require().NoError(s.catalog.DropCompactionTarget(s.ctx, updated, s.epoch))
	_, err = s.metakv.Load(s.ctx, CompactionTargetPrefix+"/10")
	s.Error(err)
}

func (s *CatalogCompactionTargetSuite) TestListReturnsUnmarshalError() {
	s.Require().NoError(s.metakv.Save(s.ctx, CompactionTargetPrefix+"/10", "not-protobuf"))

	records, err := s.catalog.ListCompactionTargets(s.ctx)
	s.ErrorIs(err, merr.ErrDataIntegrity)
	s.Nil(records)
	_, err = s.catalog.LoadCompactionTarget(s.ctx, 10)
	s.ErrorIs(err, merr.ErrDataIntegrity)
}

func (s *CatalogCompactionTargetSuite) TestCreateRejectsCorruptCollectionMetadata() {
	const collectionID = 100
	s.Require().NoError(s.metakv.Save(s.ctx, rootcatalog.BuildCollectionKey(0, collectionID), "not-protobuf"))

	unknown, err := s.catalog.CreateCompactionTarget(s.ctx, &datapb.CompactionTarget{
		TargetID: 10, CollectionID: collectionID,
	}, &metastore.CompactionTargetCollectionVersion{}, s.epoch)
	s.Require().ErrorIs(err, merr.ErrDataIntegrity)
	s.False(unknown)
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
			epoch, err := NewCatalog(backend.catalogCompactionTargetMemoryKV, rootPath, "").FenceCompactionTargetWrites(t.Context())
			require.NoError(t, err)
			catalog := NewCatalog(backend, rootPath, "")
			unknown, err := catalog.CreateCompactionTarget(t.Context(), &datapb.CompactionTarget{TargetID: 100}, nil, epoch)
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

func TestLateCompactionTargetCreateCannotOutliveCollectionDrop(t *testing.T) {
	backend := newCompactionTargetEtcd(t)
	ctx := t.Context()
	root := rootcatalog.NewCatalog(backend)
	collection := &model.Collection{CollectionID: 42, DBID: 2, State: etcdpb.CollectionState_CollectionCreated, UpdateTimestamp: 10}
	require.NoError(t, root.CreateCollection(ctx, collection, collection.UpdateTimestamp))
	delayed := &delayedTargetCreateKV{MetaKv: backend}
	catalog := NewCatalog(delayed, "", "")
	current := NewCatalog(backend, "", "")
	epoch, err := current.FenceCompactionTargetWrites(ctx)
	require.NoError(t, err)
	record := &datapb.CompactionTarget{TargetID: 123, CollectionID: collection.CollectionID}
	scope := &metastore.CompactionTargetCollectionVersion{DatabaseID: collection.DBID, Revision: collection.UpdateTimestamp}
	unknown, err := catalog.CreateCompactionTarget(ctx, record, scope, epoch)
	require.Error(t, err)
	require.True(t, unknown)
	stored, err := catalog.LoadCompactionTarget(ctx, record.GetTargetID())
	require.NoError(t, err)
	require.Nil(t, stored)

	dropped := collection.Clone()
	dropped.State = etcdpb.CollectionState_CollectionDropping
	dropped.UpdateTimestamp = 20
	require.NoError(t, root.AlterCollection(ctx, collection, dropped, metastore.MODIFY, dropped.UpdateTimestamp, false))
	require.NoError(t, current.DropCompactionTargetCreateFence(ctx, collection.CollectionID, epoch))
	require.Error(t, delayed.commit())
	stored, err = catalog.ResolveCompactionTargetCreate(ctx, record, *scope, epoch)
	require.NoError(t, err)
	require.Nil(t, stored)
}

func newCompactionTargetEtcd(t *testing.T) kv.MetaKv {
	t.Helper()
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.LogLevel = "error"
	cfg.ListenClientUrls = []url.URL{{Scheme: "http", Host: "127.0.0.1:0"}}
	cfg.AdvertiseClientUrls = cfg.ListenClientUrls
	cfg.ListenPeerUrls = []url.URL{{Scheme: "http", Host: "127.0.0.1:0"}}
	cfg.AdvertisePeerUrls = cfg.ListenPeerUrls
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
	backend, err := etcdkv.NewEmbededEtcdKV(cfg, "test/meta")
	require.NoError(t, err)
	t.Cleanup(backend.Close)
	return backend
}

func TestOptimizeSlotSerializesCoordinatorsAndReleasesAtStop(t *testing.T) {
	backend := newCompactionTargetEtcd(t)
	ctx := t.Context()
	collection := &model.Collection{CollectionID: 42, DBID: 2, State: etcdpb.CollectionState_CollectionCreated, UpdateTimestamp: 10}
	require.NoError(t, rootcatalog.NewCatalog(backend).CreateCollection(ctx, collection, collection.UpdateTimestamp))
	scope := &metastore.CompactionTargetCollectionVersion{DatabaseID: collection.DBID, Revision: collection.UpdateTimestamp}
	newCandidate := func(id int64) *datapb.CompactionTarget {
		return &datapb.CompactionTarget{TargetID: id, CollectionID: collection.CollectionID,
			Intent: datapb.TargetIntent_INTENT_OPTIMIZE, Optimize: &datapb.CompactionTargetOptimize{InputTargetSizeBytes: 1000, ActualTargetSizeBytes: 1000}}
	}
	delayed := &delayedTargetCreateKV{MetaKv: backend}
	first := NewCatalog(delayed, "", "")
	second := NewCatalog(backend, "", "")
	epoch, err := second.FenceCompactionTargetWrites(ctx)
	require.NoError(t, err)
	unknown, err := first.CreateCompactionTarget(ctx, newCandidate(100), scope, epoch)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.True(t, unknown)
	rows, err := second.ListCompactionTargets(ctx)
	require.NoError(t, err)
	require.Empty(t, rows)
	winner := newCandidate(101)
	_, err = second.CreateCompactionTarget(ctx, winner, scope, epoch)
	require.NoError(t, err)
	unknown, err = second.CreateCompactionTarget(ctx, newCandidate(102), scope, epoch)
	require.ErrorIs(t, err, merr.ErrCompactionPlanConflict)
	require.False(t, unknown)
	require.ErrorContains(t, err, "101")
	winner.State = datapb.TargetState_TARGET_STATE_INACTIVE
	winner.StopReason = "operator_dropped"
	stopped, changed, err := second.StopCompactionTarget(ctx, winner, epoch)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, winner.GetState(), stopped.GetState())
	// The empty slot must not let the older, unacknowledged Create through.
	require.Error(t, delayed.commit())
	_, err = second.CreateCompactionTarget(ctx, newCandidate(102), scope, epoch)
	require.NoError(t, err)
	rows, err = second.ListCompactionTargets(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 2)
}

func TestResolveCompactionTargetCreateConfirmsCommitOrFencesAbsentCandidate(t *testing.T) {
	ctx := t.Context()
	backend := newCompactionTargetEtcd(t)
	root := rootcatalog.NewCatalog(backend)
	catalog := NewCatalog(backend, "", "")
	epoch, err := catalog.FenceCompactionTargetWrites(ctx)
	require.NoError(t, err)
	for _, committed := range []bool{false, true} {
		name := "fence absent candidate"
		id := int64(100)
		if committed {
			name, id = "preserve a late commit", 200
		}
		t.Run(name, func(t *testing.T) {
			collection := &model.Collection{CollectionID: id, DBID: 2, State: etcdpb.CollectionState_CollectionCreated}
			require.NoError(t, root.CreateCollection(ctx, collection, 0))
			scope := &metastore.CompactionTargetCollectionVersion{DatabaseID: collection.DBID}
			candidate := &datapb.CompactionTarget{TargetID: id + 1, CollectionID: id, Intent: datapb.TargetIntent_INTENT_OPTIMIZE}
			delayed := &delayedTargetCreateKV{MetaKv: backend}
			unknown, err := NewCatalog(delayed, "", "").CreateCompactionTarget(ctx, candidate, scope, epoch)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.True(t, unknown)
			absent, err := catalog.LoadCompactionTarget(ctx, candidate.GetTargetID())
			require.NoError(t, err)
			require.Nil(t, absent)
			if committed {
				require.NoError(t, delayed.commit())
			}
			record, err := catalog.ResolveCompactionTargetCreate(ctx, candidate, *scope, epoch)
			require.NoError(t, err)
			if committed {
				require.True(t, proto.Equal(candidate, record))
				return
			}
			require.Nil(t, record)
			replacement := proto.CloneOf(candidate)
			replacement.TargetID++
			_, err = catalog.CreateCompactionTarget(ctx, replacement, scope, epoch)
			require.NoError(t, err)
			replacement.State = datapb.TargetState_TARGET_STATE_INACTIVE
			_, _, err = catalog.StopCompactionTarget(ctx, replacement, epoch)
			require.NoError(t, err)
			require.Error(t, delayed.commit())
			record, err = catalog.LoadCompactionTarget(ctx, candidate.GetTargetID())
			require.NoError(t, err)
			require.Nil(t, record)
		})
	}
}

func TestCompactionTargetCreateEpochOrdersRecoverySnapshot(t *testing.T) {
	ctx := t.Context()
	backend := newCompactionTargetEtcd(t)
	catalog := NewCatalog(backend, "", "")
	epoch, err := catalog.FenceCompactionTargetWrites(ctx)
	require.NoError(t, err)
	collection := &model.Collection{CollectionID: 42, DBID: 2, State: etcdpb.CollectionState_CollectionCreated}
	require.NoError(t, rootcatalog.NewCatalog(backend).CreateCollection(ctx, collection, 0))
	scope := &metastore.CompactionTargetCollectionVersion{DatabaseID: collection.DBID}
	durable := &datapb.CompactionTarget{TargetID: 100, CollectionID: collection.CollectionID}
	late := &datapb.CompactionTarget{TargetID: 101, CollectionID: collection.CollectionID}

	delayed := &delayedTargetCreateKV{MetaKv: backend}
	prior := NewCatalog(delayed, "", "")
	_, err = prior.CreateCompactionTarget(ctx, durable, scope, epoch)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NoError(t, delayed.commit())
	_, err = prior.CreateCompactionTarget(ctx, late, scope, epoch)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	nextEpoch, err := catalog.FenceCompactionTargetWrites(ctx)
	require.NoError(t, err)
	require.Greater(t, nextEpoch, epoch)
	require.Error(t, delayed.commit())
	rows, err := catalog.ListCompactionTargets(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.True(t, proto.Equal(durable, rows[0]))

	_, err = catalog.CreateCompactionTarget(ctx, late, scope, epoch)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	_, err = catalog.CreateCompactionTarget(ctx, late, scope, nextEpoch)
	require.NoError(t, err)
}

func TestCompactionTargetWritesAreOrderedWithRecovery(t *testing.T) {
	standing := &datapb.CompactionTarget{TargetID: 100, Intent: datapb.TargetIntent_INTENT_SORT,
		TailLimit: -1, ActivatedAtTS: 10, Enabled: proto.Bool(true), ConditionRevision: 1}
	updatedStanding := proto.CloneOf(standing)
	updatedStanding.Enabled = proto.Bool(false)
	updatedStanding.ConditionRevision++
	optimize := &datapb.CompactionTarget{TargetID: 200, CollectionID: 42,
		Intent: datapb.TargetIntent_INTENT_OPTIMIZE, ActivatedAtTS: 10}
	stoppedOptimize := proto.CloneOf(optimize)
	stoppedOptimize.State = datapb.TargetState_TARGET_STATE_INACTIVE
	stoppedOptimize.InactivatedAtTS = 20
	stoppedOptimize.StopReason = "operator_dropped"
	inactive := &datapb.CompactionTarget{TargetID: 300, ActivatedAtTS: 10}
	inactive.State = datapb.TargetState_TARGET_STATE_INACTIVE
	inactive.InactivatedAtTS = 20
	inactive.StopReason = "operator_dropped"

	for _, test := range []struct {
		name   string
		before *datapb.CompactionTarget
		after  *datapb.CompactionTarget
		write  func(context.Context, *Catalog, uint64) error
	}{
		{"Standing create", nil, standing, func(ctx context.Context, catalog *Catalog, epoch uint64) error {
			_, err := catalog.CreateCompactionTarget(ctx, standing, nil, epoch)
			return err
		}},
		{"Standing update", standing, updatedStanding, func(ctx context.Context, catalog *Catalog, epoch uint64) error {
			return catalog.UpdateStandingCompactionTarget(ctx, standing, updatedStanding, epoch)
		}},
		{"OPTIMIZE Stop", optimize, stoppedOptimize, func(ctx context.Context, catalog *Catalog, epoch uint64) error {
			_, _, err := catalog.StopCompactionTarget(ctx, stoppedOptimize, epoch)
			return err
		}},
		{"GC", inactive, nil, func(ctx context.Context, catalog *Catalog, epoch uint64) error {
			return catalog.DropCompactionTarget(ctx, inactive, epoch)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, commitBeforeRecovery := range []bool{true, false} {
				name := "attempted after recovery"
				if commitBeforeRecovery {
					name = "committed before recovery"
				}
				t.Run(name, func(t *testing.T) {
					ctx := t.Context()
					backend := newCompactionTargetEtcd(t)
					catalog := NewCatalog(backend, "", "")
					epoch, err := catalog.FenceCompactionTargetWrites(ctx)
					require.NoError(t, err)
					var scope *metastore.CompactionTargetCollectionVersion
					if test.before == optimize {
						collection := &model.Collection{CollectionID: optimize.GetCollectionID(), DBID: 2,
							State: etcdpb.CollectionState_CollectionCreated}
						require.NoError(t, rootcatalog.NewCatalog(backend).CreateCollection(ctx, collection, 0))
						scope = &metastore.CompactionTargetCollectionVersion{DatabaseID: collection.DBID}
					}
					if test.before != nil {
						_, err := catalog.CreateCompactionTarget(ctx, test.before, scope, epoch)
						require.NoError(t, err)
					}

					delayed := &delayedTargetCreateKV{MetaKv: backend}
					require.ErrorIs(t, test.write(ctx, NewCatalog(delayed, "", ""), epoch), context.DeadlineExceeded)
					want := test.before
					if commitBeforeRecovery {
						require.NoError(t, delayed.commit())
						want = test.after
					}

					current := NewCatalog(backend, "", "")
					nextEpoch, err := current.FenceCompactionTargetWrites(ctx)
					require.NoError(t, err)
					require.Greater(t, nextEpoch, epoch)
					snapshot, err := current.ListCompactionTargets(ctx)
					require.NoError(t, err)
					if want == nil {
						require.Empty(t, snapshot)
					} else {
						require.Len(t, snapshot, 1)
						require.True(t, proto.Equal(want, snapshot[0]))
					}
					if commitBeforeRecovery {
						if test.before == optimize {
							next := proto.CloneOf(optimize)
							next.TargetID++
							_, err := current.CreateCompactionTarget(ctx, next, scope, nextEpoch)
							require.NoError(t, err)
						}
						return
					}

					// A new coordinator can initialize a missing Standing
					// family before the previous coordinator's write arrives.
					if test.before == nil {
						want = proto.CloneOf(standing)
						want.TargetID++
						_, err := current.CreateCompactionTarget(ctx, want, nil, nextEpoch)
						require.NoError(t, err)
					}
					require.Error(t, delayed.commit())
					rows, err := current.ListCompactionTargets(ctx)
					require.NoError(t, err)
					require.Len(t, rows, 1)
					require.True(t, proto.Equal(want, rows[0]))
					if test.before == optimize {
						next := proto.CloneOf(optimize)
						next.TargetID++
						_, err := current.CreateCompactionTarget(ctx, next, scope, nextEpoch)
						require.ErrorIs(t, err, merr.ErrCompactionPlanConflict)
					}
					if test.before != nil {
						require.NoError(t, test.write(ctx, current, nextEpoch))
						stored, err := current.LoadCompactionTarget(ctx, test.before.GetTargetID())
						require.NoError(t, err)
						require.True(t, proto.Equal(test.after, stored))
					}
				})
			}
		})
	}
}

func TestLateCompactionTargetCreateCannotCrossReplicationTopologyChange(t *testing.T) {
	ctx := t.Context()
	backend := newCompactionTargetEtcd(t)
	collection := &model.Collection{CollectionID: 42, DBID: 2, State: etcdpb.CollectionState_CollectionCreated}
	require.NoError(t, rootcatalog.NewCatalog(backend).CreateCollection(ctx, collection, 0))
	scope := &metastore.CompactionTargetCollectionVersion{DatabaseID: collection.DBID}
	delayed := &delayedTargetCreateKV{MetaKv: backend}
	catalog := NewCatalog(delayed, "", "")
	epoch, err := NewCatalog(backend, "", "").FenceCompactionTargetWrites(ctx)
	require.NoError(t, err)
	record := &datapb.CompactionTarget{TargetID: 123, CollectionID: collection.CollectionID}
	unknown, err := catalog.CreateCompactionTarget(ctx, record, scope, epoch)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.True(t, unknown)

	require.NoError(t, streamingcatalog.NewCataLog(backend).SaveReplicateConfiguration(ctx,
		&streamingpb.ReplicateConfigurationMeta{ReplicateConfiguration: &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{{ClusterId: "primary"}, {ClusterId: "secondary"}},
			CrossClusterTopology: []*commonpb.CrossClusterTopology{
				{SourceClusterId: "primary", TargetClusterId: "secondary"},
			},
		}}, nil))
	require.Error(t, delayed.commit())
	stored, err := catalog.LoadCompactionTarget(ctx, record.GetTargetID())
	require.NoError(t, err)
	require.Nil(t, stored)
	unknown, err = catalog.CreateCompactionTarget(ctx, record, scope, epoch)
	require.ErrorIs(t, err, merr.ErrOperationNotSupported)
	require.False(t, unknown)
}

// Hold the storage request past its timeout and a subsequent read-back, as a
// server may do after the caller has already lost its response.
type delayedTargetCreateKV struct {
	kv.MetaKv
	commit func() error
}

func (m *delayedTargetCreateKV) MultiSaveAndRemove(ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error {
	m.commit = func() error {
		return m.MetaKv.MultiSaveAndRemove(ctx, saves, removals, preds...)
	}
	return context.DeadlineExceeded
}
