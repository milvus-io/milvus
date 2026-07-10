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

package meta

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/samber/lo"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ReplicaManagerInterface defines core operations for replica management
type ReplicaManagerInterface interface {
	// Basic operations
	Recover(ctx context.Context, collections []int64) error
	Get(ctx context.Context, id typeutil.UniqueID) *Replica
	Spawn(ctx context.Context, collection int64,
		replicaNumInRG map[string]int, channels []string, loadPriority commonpb.LoadPriority, opts ...SpawnOption) ([]*Replica, error)

	// Replica manipulation
	TransferReplica(ctx context.Context, collectionID typeutil.UniqueID, srcRGName string, dstRGName string, replicaNum int) error
	MoveReplica(ctx context.Context, collectionID typeutil.UniqueID, dstRGName string, toMove []*Replica) error
	RemoveCollection(ctx context.Context, collectionID typeutil.UniqueID) error
	RemoveReplicas(ctx context.Context, collectionID typeutil.UniqueID, replicas ...typeutil.UniqueID) error

	// Query operations
	GetByCollection(ctx context.Context, collectionID typeutil.UniqueID) []*Replica
	GetByCollectionAndNode(ctx context.Context, collectionID, nodeID typeutil.UniqueID) *Replica
	GetByNode(ctx context.Context, nodeID typeutil.UniqueID) []*Replica
	GetByResourceGroup(ctx context.Context, rgName string) []*Replica

	// Node management
	RecoverNodesInCollection(ctx context.Context, collectionID typeutil.UniqueID, rgs map[string]*ResourceGroup) error
	RemoveNode(ctx context.Context, collectionID typeutil.UniqueID, replicaID typeutil.UniqueID, nodes ...typeutil.UniqueID) error
	RemoveSQNode(ctx context.Context, collectionID typeutil.UniqueID, replicaID typeutil.UniqueID, nodes ...typeutil.UniqueID) error

	// Metadata access
	GetResourceGroupByCollection(ctx context.Context, collection typeutil.UniqueID) typeutil.Set[string]
	GetReplicasJSON(ctx context.Context, meta *Meta) string
}

// Add the interface implementation assertion
var _ ReplicaManagerInterface = (*ReplicaManager)(nil)

type ReplicaManager struct {
	// per-collection write lock, auto-created/recycled by KeyLock
	collLock *lock.KeyLock[int64]

	// flat index: replicaID -> *Replica, lock-free read for Get()
	flatReplicas *typeutil.ConcurrentMap[int64, *Replica]

	// per-collection replica list, lock-free read via ConcurrentMap
	coll2Replicas *typeutil.ConcurrentMap[int64, []*Replica]

	// queryInvisibleReplicas is a secondary index over Replica.queryInvisible.
	// Replica.queryInvisible remains the source of truth; this set only avoids
	// scanning all replicas in the load-config promotion loop.
	queryInvisibleReplicas *typeutil.ConcurrentSet[int64]

	idAllocator func() (int64, error)
	catalog     metastore.QueryCoordCatalog
}

func NewReplicaManager(idAllocator func() (int64, error), catalog metastore.QueryCoordCatalog) *ReplicaManager {
	return &ReplicaManager{
		collLock:               lock.NewKeyLock[int64](),
		flatReplicas:           typeutil.NewConcurrentMap[int64, *Replica](),
		coll2Replicas:          typeutil.NewConcurrentMap[int64, []*Replica](),
		queryInvisibleReplicas: typeutil.NewConcurrentSet[int64](),
		idAllocator:            idAllocator,
		catalog:                catalog,
	}
}

// Recover recovers the replicas for given collections from meta store
func (m *ReplicaManager) Recover(ctx context.Context, collections []int64) error {
	replicas, err := m.catalog.GetReplicas(ctx)
	if err != nil {
		return merr.Wrap(err, "failed to recover replicas")
	}

	collectionSet := typeutil.NewUniqueSet(collections...)
	grouped := make(map[int64][]*Replica)
	for _, replica := range replicas {
		if len(replica.GetResourceGroup()) == 0 {
			replica.ResourceGroup = DefaultResourceGroupName
		}

		if collectionSet.Contain(replica.GetCollectionID()) {
			rep := NewReplicaWithPriority(replica, commonpb.LoadPriority_HIGH)
			grouped[rep.GetCollectionID()] = append(grouped[rep.GetCollectionID()], rep)
			mlog.Info(ctx, "recover replica",
				mlog.FieldCollectionID(replica.GetCollectionID()),
				mlog.Int64("replicaID", replica.GetID()),
				mlog.Int64s("rwNodes", replica.GetNodes()),
				mlog.Int64s("roNodes", replica.GetRoNodes()),
				mlog.Int64s("rwSQNodes", replica.GetRwSqNodes()),
				mlog.Int64s("roSQNodes", replica.GetRoNodes()),
			)
		} else {
			err := m.catalog.ReleaseReplica(ctx, replica.GetCollectionID(), replica.GetID())
			if err != nil {
				return err
			}
			mlog.Info(ctx, "clear stale replica",
				mlog.FieldCollectionID(replica.GetCollectionID()),
				mlog.Int64("replicaID", replica.GetID()),
				mlog.Int64s("nodes", replica.GetNodes()),
			)
		}
	}
	for collID, reps := range grouped {
		m.collLock.Lock(collID)
		m.putReplicasInMemory(collID, reps...)
		m.collLock.Unlock(collID)
	}
	return nil
}

// Get returns the replica by id.
// Replica should be read-only, do not modify it.
// Uses lock-free flat index for fast lookup; safe because Replica is COW-immutable.
func (m *ReplicaManager) Get(ctx context.Context, id typeutil.UniqueID) *Replica {
	replica, _ := m.flatReplicas.Get(id)
	return replica
}

func (m *ReplicaManager) GetQueryInvisibleReplicas(ctx context.Context) []*Replica {
	replicaIDs := m.queryInvisibleReplicas.Collect()
	replicas := make([]*Replica, 0, len(replicaIDs))
	for _, replicaID := range replicaIDs {
		replica, ok := m.flatReplicas.Get(replicaID)
		if !ok || replica.IsQueryVisible() {
			continue
		}
		replicas = append(replicas, replica)
	}
	return replicas
}

func (m *ReplicaManager) SetReplicasQueryVisible(ctx context.Context, replicaIDs ...typeutil.UniqueID) []typeutil.UniqueID {
	grouped := make(map[typeutil.UniqueID][]typeutil.UniqueID)
	for _, replicaID := range replicaIDs {
		replica, ok := m.flatReplicas.Get(replicaID)
		if !ok || replica.IsQueryVisible() {
			continue
		}
		grouped[replica.GetCollectionID()] = append(grouped[replica.GetCollectionID()], replicaID)
	}

	collections := typeutil.NewUniqueSet()
	for collectionID, ids := range grouped {
		m.collLock.Lock(collectionID)

		modifiedReplicas := make([]*Replica, 0, len(ids))
		for _, replicaID := range ids {
			replica, ok := m.flatReplicas.Get(replicaID)
			if !ok || replica.GetCollectionID() != collectionID || replica.IsQueryVisible() {
				continue
			}
			mutableReplica := replica.CopyForWrite()
			mutableReplica.SetQueryInvisible(false)
			modifiedReplicas = append(modifiedReplicas, mutableReplica.IntoReplica())
		}
		if len(modifiedReplicas) > 0 {
			m.putReplicasInMemory(collectionID, modifiedReplicas...)
			collections.Insert(collectionID)
		}

		m.collLock.Unlock(collectionID)
	}
	return collections.Collect()
}

type SpawnWithReplicaConfigParams struct {
	CollectionID int64
	Channels     []string
	Configs      []*messagespb.LoadReplicaConfig
}

// SpawnWithReplicaConfig spawns replicas with replica config.
func (m *ReplicaManager) SpawnWithReplicaConfig(ctx context.Context, params SpawnWithReplicaConfigParams) ([]*Replica, error) {
	m.collLock.Lock(params.CollectionID)
	defer m.collLock.Unlock(params.CollectionID)

	balancePolicy := paramtable.Get().QueryCoordCfg.Balancer.GetValue()
	enableChannelExclusiveMode := balancePolicy == ChannelLevelScoreBalancerName
	replicas := make([]*Replica, 0)
	for _, config := range params.Configs {
		if existedReplica, ok := m.flatReplicas.Get(config.GetReplicaId()); ok &&
			existedReplica.GetCollectionID() == params.CollectionID {
			// if the replica is already existed, just update the resource group
			mutableReplica := existedReplica.CopyForWrite()
			mutableReplica.SetResourceGroup(config.GetResourceGroupName())
			replicas = append(replicas, mutableReplica.IntoReplica())
			continue
		}
		replica := NewReplicaWithPriority(&querypb.Replica{
			ID:            config.GetReplicaId(),
			CollectionID:  params.CollectionID,
			ResourceGroup: config.ResourceGroupName,
		}, config.GetPriority())
		if enableChannelExclusiveMode {
			mutableReplica := replica.CopyForWrite()
			mutableReplica.TryEnableChannelExclusiveMode(params.Channels...)
			replica = mutableReplica.IntoReplica()
		}
		replicas = append(replicas, replica)
		mlog.Info(ctx, "spawn replica for collection",
			mlog.FieldCollectionID(params.CollectionID),
			mlog.Int64("replicaID", config.GetReplicaId()),
			mlog.String("resourceGroup", config.GetResourceGroupName()),
		)
	}
	// Pre-compute the redundant replicas that are not in the new replica config.
	// All newly spawned replica IDs come from params.Configs, so computing this
	// before persistence is equivalent to computing it after.
	toRemove := make([]int64, 0)
	if existedReplicas, ok := m.coll2Replicas.Get(params.CollectionID); ok {
		configIDs := typeutil.NewUniqueSet()
		for _, config := range params.Configs {
			configIDs.Insert(config.GetReplicaId())
		}
		for _, replica := range existedReplicas {
			if !configIDs.Contain(replica.GetID()) {
				toRemove = append(toRemove, replica.GetID())
			}
		}
	}

	// Persist the spawned replicas and release the redundant ones in one
	// compound catalog call; on failure the in-memory state is left untouched
	// and the error propagates to the caller for retry.
	replicaPBs := make([]*querypb.Replica, 0, len(replicas))
	for _, replica := range replicas {
		replicaPBs = append(replicaPBs, replica.replicaPB)
	}
	if err := m.catalog.SaveAndReleaseReplicas(ctx, params.CollectionID, replicaPBs, toRemove); err != nil {
		return nil, merr.Wrap(err, "failed to save and release replicas")
	}

	m.putReplicasInMemory(params.CollectionID, replicas...)
	if len(toRemove) > 0 {
		for _, replicaID := range toRemove {
			if replica, ok := m.flatReplicas.Get(replicaID); ok {
				metrics.QueryCoordResourceGroupReplicaTotal.WithLabelValues(replica.GetResourceGroup()).Dec()
				metrics.QueryCoordReplicaRONodeTotal.Add(float64(-replica.RONodesCount()))
			}
		}
		m.removeReplicasInMemory(params.CollectionID, toRemove...)
	}
	return replicas, nil
}

// AllocateReplicaID allocates a replica ID.
func (m *ReplicaManager) AllocateReplicaID(ctx context.Context) (int64, error) {
	return m.idAllocator()
}

// SpawnOption is a functional option for Spawn.
type SpawnOption func(*spawnConfig)

type spawnConfig struct {
	waitRGReady    bool
	queryInvisible bool
}

// WithNeedWaitRGReady returns a SpawnOption that enables waiting for resource group readiness.
// When enabled, the first node assignment for these replicas will be deferred
// until their resource groups have all requested nodes ready (MissingNumOfNodes == 0),
// or until the configured timeout (queryCoord.waitRGReadyTimeout) has elapsed.
// This prevents unbalanced segment loading during replica scale-up.
func WithNeedWaitRGReady() SpawnOption {
	return func(cfg *spawnConfig) {
		cfg.waitRGReady = true
	}
}

func WithQueryInvisible() SpawnOption {
	return func(cfg *spawnConfig) {
		cfg.queryInvisible = true
	}
}

// Spawn spawns N replicas at resource group for given collection in ReplicaManager.
func (m *ReplicaManager) Spawn(ctx context.Context, collection int64, replicaNumInRG map[string]int,
	channels []string, loadPriority commonpb.LoadPriority, opts ...SpawnOption,
) ([]*Replica, error) {
	cfg := &spawnConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	m.collLock.Lock(collection)
	defer m.collLock.Unlock(collection)

	balancePolicy := paramtable.Get().QueryCoordCfg.Balancer.GetValue()
	enableChannelExclusiveMode := balancePolicy == ChannelLevelScoreBalancerName

	replicas := make([]*Replica, 0)
	for rgName, replicaNum := range replicaNumInRG {
		for ; replicaNum > 0; replicaNum-- {
			id, err := m.idAllocator()
			if err != nil {
				return nil, err
			}

			replica := NewReplicaWithPriority(&querypb.Replica{
				ID:            id,
				CollectionID:  collection,
				ResourceGroup: rgName,
			}, loadPriority)
			mutableReplica := replica.CopyForWrite()
			if cfg.waitRGReady {
				mutableReplica.SetWaitRGReadyAt(time.Now())
			}
			if cfg.queryInvisible {
				mutableReplica.SetQueryInvisible(true)
			}
			if enableChannelExclusiveMode {
				mutableReplica.TryEnableChannelExclusiveMode(channels...)
			}
			replica = mutableReplica.IntoReplica()
			replicas = append(replicas, replica)
		}
	}
	if err := m.put(ctx, collection, replicas...); err != nil {
		return nil, err
	}
	return replicas, nil
}

// Deprecated: Warning, break the consistency of ReplicaManager,
// never use it in non-test code, use Spawn instead.
func (m *ReplicaManager) Put(ctx context.Context, replicas ...*Replica) error {
	if len(replicas) == 0 {
		return nil
	}
	grouped := make(map[int64][]*Replica)
	collectionIDs := make([]int64, 0)
	for _, r := range replicas {
		if _, ok := grouped[r.GetCollectionID()]; !ok {
			collectionIDs = append(collectionIDs, r.GetCollectionID())
		}
		grouped[r.GetCollectionID()] = append(grouped[r.GetCollectionID()], r)
	}
	sort.Slice(collectionIDs, func(i, j int) bool {
		return collectionIDs[i] < collectionIDs[j]
	})

	for _, collID := range collectionIDs {
		m.collLock.Lock(collID)
	}
	defer func() {
		for i := len(collectionIDs) - 1; i >= 0; i-- {
			m.collLock.Unlock(collectionIDs[i])
		}
	}()

	replicaPBs := make([]*querypb.Replica, 0, len(replicas))
	for _, replica := range replicas {
		replicaPBs = append(replicaPBs, replica.replicaPB)
	}
	if err := m.catalog.SaveReplica(ctx, replicaPBs...); err != nil {
		return err
	}
	for _, collID := range collectionIDs {
		m.putReplicasInMemory(collID, grouped[collID]...)
	}
	return nil
}

func (m *ReplicaManager) put(ctx context.Context, collectionID typeutil.UniqueID, replicas ...*Replica) error {
	if len(replicas) == 0 {
		return nil
	}
	// Persist replicas into KV.
	replicaPBs := make([]*querypb.Replica, 0, len(replicas))
	for _, replica := range replicas {
		replicaPBs = append(replicaPBs, replica.replicaPB)
	}
	if err := m.catalog.SaveReplica(ctx, replicaPBs...); err != nil {
		return err
	}

	m.putReplicasInMemory(collectionID, replicas...)
	return nil
}

// putReplicasInMemory puts replicas into in-memory indexes.
// Caller must hold collLock for the collection.
func (m *ReplicaManager) putReplicasInMemory(collectionID typeutil.UniqueID, replicas ...*Replica) {
	if len(replicas) == 0 {
		return
	}

	old, _ := m.coll2Replicas.Get(collectionID)
	newSlice := append([]*Replica(nil), old...)

	indexes := make(map[int64]int, len(newSlice))
	for i, r := range newSlice {
		indexes[r.GetID()] = i
	}

	for _, r := range replicas {
		if i, ok := indexes[r.GetID()]; ok {
			newSlice[i] = r
		} else {
			newSlice = append(newSlice, r)
		}
	}

	for _, r := range replicas {
		if oldReplica, ok := m.flatReplicas.Get(r.GetID()); ok {
			metrics.QueryCoordResourceGroupReplicaTotal.WithLabelValues(oldReplica.GetResourceGroup()).Dec()
			metrics.QueryCoordReplicaRONodeTotal.Add(-float64(oldReplica.RONodesCount()))
			m.queryInvisibleReplicas.Remove(oldReplica.GetID())
		}
		m.flatReplicas.Insert(r.GetID(), r)
		if !r.IsQueryVisible() {
			m.queryInvisibleReplicas.Insert(r.GetID())
		}
		metrics.QueryCoordResourceGroupReplicaTotal.WithLabelValues(r.GetResourceGroup()).Inc()
		metrics.QueryCoordReplicaRONodeTotal.Add(float64(r.RONodesCount()))
	}
	m.coll2Replicas.Insert(collectionID, newSlice)
}

// TransferReplica transfers N replicas from srcRGName to dstRGName.
func (m *ReplicaManager) TransferReplica(ctx context.Context, collectionID typeutil.UniqueID, srcRGName string, dstRGName string, replicaNum int) error {
	if srcRGName == dstRGName {
		return merr.WrapErrParameterInvalidMsg("source resource group and target resource group should not be the same, resource group: %s", srcRGName)
	}
	if replicaNum <= 0 {
		return merr.WrapErrParameterInvalid("NumReplica > 0", fmt.Sprintf("invalid NumReplica %d", replicaNum))
	}

	m.collLock.Lock(collectionID)
	defer m.collLock.Unlock(collectionID)

	if _, ok := m.coll2Replicas.Get(collectionID); !ok {
		return merr.WrapErrParameterInvalid(
			"Collection not loaded",
			fmt.Sprintf("collectionID %d", collectionID),
		)
	}

	// Check if replica can be transfer.
	srcReplicas, err := m.getSrcReplicasAndCheckIfTransferable(collectionID, srcRGName, replicaNum)
	if err != nil {
		return err
	}

	// Transfer N replicas from srcRGName to dstRGName.
	// Node Change will be executed by replica_observer in background.
	replicas := make([]*Replica, 0, replicaNum)
	for i := 0; i < replicaNum; i++ {
		mutableReplica := srcReplicas[i].CopyForWrite()
		mutableReplica.SetResourceGroup(dstRGName)
		replicas = append(replicas, mutableReplica.IntoReplica())
	}

	return m.put(ctx, collectionID, replicas...)
}

func (m *ReplicaManager) MoveReplica(ctx context.Context, collectionID typeutil.UniqueID, dstRGName string, toMove []*Replica) error {
	if len(toMove) == 0 {
		return nil
	}

	m.collLock.Lock(collectionID)
	defer m.collLock.Unlock(collectionID)

	if _, ok := m.coll2Replicas.Get(collectionID); !ok {
		return nil
	}

	replicas := make([]*Replica, 0, len(toMove))
	replicaIDs := make([]int64, 0, len(toMove))
	for _, replica := range toMove {
		mutableReplica := replica.CopyForWrite()
		mutableReplica.SetResourceGroup(dstRGName)
		replicas = append(replicas, mutableReplica.IntoReplica())
		replicaIDs = append(replicaIDs, replica.GetID())
	}
	mlog.Info(ctx, "move replicas to resource group", mlog.String("dstRGName", dstRGName), mlog.Int64s("replicas", replicaIDs))
	return m.put(ctx, collectionID, replicas...)
}

// getSrcReplicasAndCheckIfTransferable checks if the collection can be transferred.
// Caller must hold collLock for this collection.
func (m *ReplicaManager) getSrcReplicasAndCheckIfTransferable(collectionID typeutil.UniqueID, srcRGName string, replicaNum int) ([]*Replica, error) {
	replicas, _ := m.coll2Replicas.Get(collectionID)
	srcReplicas := lo.Filter(replicas, func(replica *Replica, _ int) bool {
		return replica.GetResourceGroup() == srcRGName
	})
	if len(srcReplicas) < replicaNum {
		err := merr.WrapErrParameterInvalid(
			"NumReplica not greater than the number of replica in source resource group", fmt.Sprintf("only found [%d] replicas of collection [%d] in source resource group [%s], but %d require",
				len(srcReplicas),
				collectionID,
				srcRGName,
				replicaNum))
		return nil, err
	}
	return srcReplicas, nil
}

// RemoveCollection removes replicas of given collection,
// returns error if failed to remove replica from KV
func (m *ReplicaManager) RemoveCollection(ctx context.Context, collectionID typeutil.UniqueID) error {
	m.collLock.Lock(collectionID)
	defer m.collLock.Unlock(collectionID)

	err := m.catalog.ReleaseReplicas(ctx, collectionID)
	if err != nil {
		return err
	}

	if replicas, ok := m.coll2Replicas.Get(collectionID); ok {
		// Remove all replica of collection and remove collection from coll2Replicas.
		// coll2Replicas is updated before flatReplicas so the invariant
		// "visible via GetByCollection => visible via Get" holds during deletion.
		m.coll2Replicas.Remove(collectionID)
		for _, replica := range replicas {
			metrics.QueryCoordResourceGroupReplicaTotal.WithLabelValues(replica.GetResourceGroup()).Dec()
			metrics.QueryCoordReplicaRONodeTotal.Add(-float64(replica.RONodesCount()))
			m.flatReplicas.Remove(replica.GetID())
			m.queryInvisibleReplicas.Remove(replica.GetID())
		}
	}
	return nil
}

func (m *ReplicaManager) RemoveReplicas(ctx context.Context, collectionID typeutil.UniqueID, replicaIDs ...typeutil.UniqueID) error {
	m.collLock.Lock(collectionID)
	defer m.collLock.Unlock(collectionID)

	if _, ok := m.coll2Replicas.Get(collectionID); !ok {
		return nil
	}

	mlog.Info(ctx, "release replicas", mlog.FieldCollectionID(collectionID), mlog.Int64s("replicas", replicaIDs))
	return m.removeReplicas(ctx, collectionID, replicaIDs...)
}

// removeReplicas removes specific replicas while holding collLock.Lock.
// coll2Replicas is updated before flatReplicas so lock-free readers never observe
// a replica that is visible via GetByCollection but missing from Get(id).
func (m *ReplicaManager) removeReplicas(ctx context.Context, collectionID int64, replicaIDs ...int64) error {
	if len(replicaIDs) == 0 {
		return nil
	}
	if err := m.catalog.ReleaseReplica(ctx, collectionID, replicaIDs...); err != nil {
		return err
	}
	for _, replicaID := range replicaIDs {
		if replica, ok := m.flatReplicas.Get(replicaID); ok {
			metrics.QueryCoordResourceGroupReplicaTotal.WithLabelValues(replica.GetResourceGroup()).Dec()
			metrics.QueryCoordReplicaRONodeTotal.Add(float64(-replica.RONodesCount()))
		}
	}
	m.removeReplicasInMemory(collectionID, replicaIDs...)
	return nil
}

// removeReplicasInMemory removes replicas from both collection and flat indexes.
// coll2Replicas is updated before flatReplicas so lock-free readers never observe
// a replica that is visible via GetByCollection but missing from Get(id).
// Caller must hold collLock for the collection.
func (m *ReplicaManager) removeReplicasInMemory(collectionID typeutil.UniqueID, replicaIDs ...int64) {
	old, ok := m.coll2Replicas.Get(collectionID)
	if ok {
		removeSet := typeutil.NewSet(replicaIDs...)
		newSlice := make([]*Replica, 0, len(old))
		for _, r := range old {
			if !removeSet.Contain(r.GetID()) {
				newSlice = append(newSlice, r)
			}
		}
		if len(newSlice) == 0 {
			m.coll2Replicas.Remove(collectionID)
		} else {
			m.coll2Replicas.Insert(collectionID, newSlice)
		}
	}

	for _, replicaID := range replicaIDs {
		m.flatReplicas.Remove(replicaID)
		m.queryInvisibleReplicas.Remove(replicaID)
	}
}

func (m *ReplicaManager) GetByCollection(ctx context.Context, collectionID typeutil.UniqueID) []*Replica {
	replicas, _ := m.coll2Replicas.Get(collectionID)
	return replicas
}

func (m *ReplicaManager) GetByCollectionAndNode(ctx context.Context, collectionID, nodeID typeutil.UniqueID) *Replica {
	replicas, ok := m.coll2Replicas.Get(collectionID)
	if !ok {
		return nil
	}
	for _, replica := range replicas {
		if replica.Contains(nodeID) {
			return replica
		}
	}
	return nil
}

func (m *ReplicaManager) GetByNode(ctx context.Context, nodeID typeutil.UniqueID) []*Replica {
	replicas := make([]*Replica, 0)
	m.coll2Replicas.Range(func(_ int64, collReplicas []*Replica) bool {
		for _, replica := range collReplicas {
			if replica.Contains(nodeID) {
				replicas = append(replicas, replica)
			}
		}
		return true
	})
	return replicas
}

func (m *ReplicaManager) GetByResourceGroup(ctx context.Context, rgName string) []*Replica {
	ret := make([]*Replica, 0)
	m.coll2Replicas.Range(func(_ int64, collReplicas []*Replica) bool {
		for _, replica := range collReplicas {
			if replica.GetResourceGroup() == rgName {
				ret = append(ret, replica)
			}
		}
		return true
	})
	return ret
}

// RecoverNodesInCollection recovers all nodes in collection with latest resource group.
// Promise a node will be only assigned to one replica in same collection at same time.
// 1. Move the rw nodes to ro nodes if they are not in related resource group.
// 2. Add new incoming nodes into the replica if they are not in-used by other replicas of same collection.
// 3. replicas in same resource group will shared the nodes in resource group fairly.
func (m *ReplicaManager) RecoverNodesInCollection(ctx context.Context, collectionID typeutil.UniqueID, rgs map[string]*ResourceGroup) error {
	// Build node sets from resource groups.
	rgNodeSets := make(map[string]typeutil.UniqueSet, len(rgs))
	for rgName, rg := range rgs {
		if rg == nil {
			rgNodeSets[rgName] = typeutil.NewUniqueSet()
		} else {
			rgNodeSets[rgName] = typeutil.NewUniqueSet(rg.GetNodes()...)
		}
	}

	if err := m.validateResourceGroups(rgNodeSets); err != nil {
		return err
	}

	m.collLock.Lock(collectionID)
	defer m.collLock.Unlock(collectionID)

	if _, ok := m.coll2Replicas.Get(collectionID); !ok {
		return merr.WrapErrCollectionNotLoaded(collectionID)
	}

	// create a helper to do the recover.
	helper, err := m.getCollectionAssignmentHelper(collectionID, rgNodeSets)
	if err != nil {
		return err
	}

	modifiedReplicas := make([]*Replica, 0)
	// recover node by resource group.
	helper.RangeOverResourceGroup(func(replicaHelper *replicasInSameRGAssignmentHelper) {
		replicaHelper.RangeOverReplicas(func(assignment *replicaAssignmentInfo) {
			replica := assignment.GetReplica()
			// For replicas with needWaitRGReady flag, skip assignment if the RG still has missing nodes.
			if replica.NeedWaitRGReady() {
				rgName := replica.GetResourceGroup()
				if rg := rgs[rgName]; rg != nil && rg.MissingNumOfNodes() > 0 {
					mlog.RatedInfo(ctx, rate.Limit(10), "defer node assignment for new replica, resource group not ready",
						mlog.FieldCollectionID(collectionID),
						mlog.Int64("replicaID", replica.GetID()),
						mlog.String("rgName", rgName),
						mlog.Int("missingNodes", rg.MissingNumOfNodes()),
					)
					return
				}
			}

			roNodes := assignment.GetNewRONodes()
			recoverableNodes, incomingNodeCount := assignment.GetRecoverNodesAndIncomingNodeCount()
			// There may be not enough incoming nodes for current replica,
			// Even we filtering the nodes that are used by other replica of same collection in other resource group,
			// current replica's expected node may be still used by other replica of same collection in same resource group.
			incomingNode := replicaHelper.AllocateIncomingNodes(incomingNodeCount)
			if len(roNodes) == 0 && len(recoverableNodes) == 0 && len(incomingNode) == 0 {
				// nothing to do.
				return
			}
			mutableReplica := replica.CopyForWrite()
			mutableReplica.AddRONode(roNodes...)          // rw -> ro
			mutableReplica.AddRWNode(recoverableNodes...) // ro -> rw
			mutableReplica.AddRWNode(incomingNode...)     // unused -> rw
			// Clear waitRGReady after first successful node assignment.
			if mutableReplica.NeedWaitRGReady() {
				mutableReplica.SetWaitRGReadyAt(time.Time{})
			}
			mlog.Info(ctx, "new replica recovery found",
				mlog.FieldCollectionID(collectionID),
				mlog.Int64("replicaID", assignment.GetReplicaID()),
				mlog.Int64s("newRONodes", roNodes),
				mlog.Int64s("roToRWNodes", recoverableNodes),
				mlog.Int64s("newIncomingNodes", incomingNode),
				mlog.Bool("enableChannelExclusiveMode", mutableReplica.IsChannelExclusiveModeEnabled()),
				mlog.Any("channelNodeInfos", mutableReplica.replicaPB.GetChannelNodeInfos()),
				mlog.Int64s("rwNodes", mutableReplica.GetRWNodes()),
				mlog.Int64s("roNodes", mutableReplica.GetRONodes()),
				mlog.Int64s("rwSQNodes", mutableReplica.GetRWSQNodes()),
				mlog.Int64s("roSQNodes", mutableReplica.GetROSQNodes()),
			)
			modifiedReplicas = append(modifiedReplicas, mutableReplica.IntoReplica())
		})
	})

	if len(modifiedReplicas) == 0 {
		return nil
	}
	return m.put(ctx, collectionID, modifiedReplicas...)
}

// validateResourceGroups checks if the resource groups are valid.
func (m *ReplicaManager) validateResourceGroups(rgs map[string]typeutil.UniqueSet) error {
	// make sure that node in resource group is mutual exclusive.
	node := typeutil.NewUniqueSet()
	for _, rg := range rgs {
		for id := range rg {
			if node.Contain(id) {
				return merr.WrapErrServiceInternalMsg("node in resource group is not mutual exclusive")
			}
			node.Insert(id)
		}
	}
	return nil
}

// getCollectionAssignmentHelper builds an assignment helper from collection replicas.
// Caller must hold collLock for this collection.
func (m *ReplicaManager) getCollectionAssignmentHelper(collectionID typeutil.UniqueID, rgs map[string]typeutil.UniqueSet) (*collectionAssignmentHelper, error) {
	// check if the collection is exist.
	replicas, ok := m.coll2Replicas.Get(collectionID)
	if !ok {
		return nil, merr.WrapErrCollectionNotLoaded(collectionID)
	}

	rgToReplicas := make(map[string][]*Replica)
	for _, replica := range replicas {
		rgName := replica.GetResourceGroup()
		if _, ok := rgs[rgName]; !ok {
			return nil, merr.WrapErrServiceInternalMsg("lost resource group info, collectionID: %d, replicaID: %d, resourceGroup: %s", collectionID, replica.GetID(), rgName)
		}
		rgToReplicas[rgName] = append(rgToReplicas[rgName], replica)
	}
	return newCollectionAssignmentHelper(collectionID, rgToReplicas, rgs), nil
}

// RemoveNode removes the node from the given replica.
func (m *ReplicaManager) RemoveNode(ctx context.Context, collectionID typeutil.UniqueID, replicaID typeutil.UniqueID, nodes ...typeutil.UniqueID) error {
	m.collLock.Lock(collectionID)
	defer m.collLock.Unlock(collectionID)

	replica, ok := m.flatReplicas.Get(replicaID)
	if !ok || replica.GetCollectionID() != collectionID {
		return merr.WrapErrReplicaNotFound(replicaID)
	}

	mutableReplica := replica.CopyForWrite()
	mutableReplica.RemoveNode(nodes...) // ro -> unused
	return m.put(ctx, collectionID, mutableReplica.IntoReplica())
}

// RemoveSQNode removes the sq node from the given replica.
func (m *ReplicaManager) RemoveSQNode(ctx context.Context, collectionID typeutil.UniqueID, replicaID typeutil.UniqueID, nodes ...typeutil.UniqueID) error {
	m.collLock.Lock(collectionID)
	defer m.collLock.Unlock(collectionID)

	replica, ok := m.flatReplicas.Get(replicaID)
	if !ok || replica.GetCollectionID() != collectionID {
		return merr.WrapErrReplicaNotFound(replicaID)
	}

	mutableReplica := replica.CopyForWrite()
	mutableReplica.RemoveSQNode(nodes...) // ro -> unused
	return m.put(ctx, collectionID, mutableReplica.IntoReplica())
}

func (m *ReplicaManager) GetResourceGroupByCollection(ctx context.Context, collection typeutil.UniqueID) typeutil.Set[string] {
	replicas := m.GetByCollection(ctx, collection)
	ret := typeutil.NewSet(lo.Map(replicas, func(r *Replica, _ int) string { return r.GetResourceGroup() })...)
	return ret
}

// GetReplicasJSON returns a JSON representation of all replicas managed by the ReplicaManager.
func (m *ReplicaManager) GetReplicasJSON(ctx context.Context, meta *Meta) string {
	allReplicas := make([]*metricsinfo.Replica, 0)
	m.coll2Replicas.Range(func(_ int64, collReplicas []*Replica) bool {
		for _, r := range collReplicas {
			channelTowRWNodes := make(map[string][]int64)
			for k, v := range r.replicaPB.GetChannelNodeInfos() {
				channelTowRWNodes[k] = v.GetRwNodes()
			}

			collectionInfo := meta.GetCollection(ctx, r.GetCollectionID())
			dbID := util.InvalidDBID
			if collectionInfo == nil {
				mlog.Warn(ctx, "failed to get collection info", mlog.FieldCollectionID(r.GetCollectionID()))
			} else {
				dbID = collectionInfo.GetDbID()
			}

			allReplicas = append(allReplicas, &metricsinfo.Replica{
				ID:               r.GetID(),
				CollectionID:     r.GetCollectionID(),
				DatabaseID:       dbID,
				RWNodes:          r.GetNodes(),
				ResourceGroup:    r.GetResourceGroup(),
				RONodes:          r.GetRONodes(),
				ChannelToRWNodes: channelTowRWNodes,
			})
		}
		return true
	})
	ret, err := json.Marshal(allReplicas)
	if err != nil {
		mlog.Warn(ctx, "failed to marshal replicas", mlog.Err(err))
		return ""
	}
	return string(ret)
}

// RecoverSQNodesInCollection recovers all sq nodes in collection with latest node list.
// Promise a node will be only assigned to one replica in same collection at same time.
// 1. Move the rw nodes to ro nodes if current replica use too much sqn.
// 2. Add new incoming nodes into the replica if they are not ro node of other replicas in same collection.
// 3. replicas will shared the nodes in resource group fairly.
// When sqnNodesByRG covers all resource groups of replicas in the collection, streaming nodes will be assigned
// by resource group isolation (each replica only gets streaming nodes from its own resource group).
// Otherwise, all streaming nodes will be pooled together and assigned fairly across all replicas (fallback mode).
func (m *ReplicaManager) RecoverSQNodesInCollection(ctx context.Context, collectionID int64, sqnNodesByRG map[string]typeutil.UniqueSet) error {
	m.collLock.Lock(collectionID)
	defer m.collLock.Unlock(collectionID)

	replicas, ok := m.coll2Replicas.Get(collectionID)
	if !ok {
		return merr.WrapErrCollectionNotLoaded(collectionID)
	}

	// Build helpers based on whether we can use resource group isolation.
	helpers := m.buildSQNodeAssignmentHelpers(replicas, sqnNodesByRG)

	modifiedReplicas := make([]*Replica, 0)
	for rgName, helper := range helpers {
		helper.RangeOverReplicas(func(assignment *replicaAssignmentInfo) {
			roNodes := assignment.GetNewRONodes()
			recoverableNodes, incomingNodeCount := assignment.GetRecoverNodesAndIncomingNodeCount()
			incomingNode := helper.AllocateIncomingNodes(incomingNodeCount)
			if len(roNodes) == 0 && len(recoverableNodes) == 0 && len(incomingNode) == 0 {
				return
			}
			mutableReplica := assignment.GetReplica().CopyForWrite()
			mutableReplica.AddROSQNode(roNodes...)
			mutableReplica.AddRWSQNode(recoverableNodes...)
			mutableReplica.AddRWSQNode(incomingNode...)
			mlog.Info(ctx, "new replica recovery streaming query node found",
				mlog.FieldCollectionID(collectionID),
				mlog.Int64("replicaID", assignment.GetReplicaID()),
				mlog.String("resourceGroup", rgName),
				mlog.Int64s("newRONodes", roNodes),
				mlog.Int64s("roToRWNodes", recoverableNodes),
				mlog.Int64s("newIncomingNodes", incomingNode),
				mlog.Int64s("rwSQNodes", mutableReplica.GetRWSQNodes()),
				mlog.Int64s("roSQNodes", mutableReplica.GetROSQNodes()),
			)
			modifiedReplicas = append(modifiedReplicas, mutableReplica.IntoReplica())
		})
	}
	return m.put(ctx, collectionID, modifiedReplicas...)
}

// buildSQNodeAssignmentHelpers builds assignment helpers for streaming query node recovery.
// If streaming node resource groups cover all replica resource groups, creates one helper per RG (isolation mode).
// During rolling upgrades, old StreamingNodes may not carry RG labels and are reported in DefaultResourceGroupName.
// In that case, replicas whose RG is not covered by labeled StreamingNodes share the default legacy pool, while
// covered replicas still use RG isolation.
// Otherwise, behavior depends on streaming.strictResourceGroupIsolation.enabled config:
//   - If enabled (strict isolation mode): skip replicas without matching streaming node resource groups.
//   - If disabled and no default legacy pool exists: pool all nodes together into a single helper (flat allocation mode).
func (m *ReplicaManager) buildSQNodeAssignmentHelpers(
	replicas []*Replica,
	sqnNodesByRG map[string]typeutil.UniqueSet,
) map[string]*replicasInSameRGAssignmentHelper {
	// Group replicas by resource group and check coverage.
	rgToReplicas := make(map[string][]*Replica)
	uncoveredReplicas := make([]*Replica, 0)
	for _, replica := range replicas {
		rgName := replica.GetResourceGroup()
		if _, ok := sqnNodesByRG[rgName]; ok {
			rgToReplicas[rgName] = append(rgToReplicas[rgName], replica)
		} else {
			uncoveredReplicas = append(uncoveredReplicas, replica)
		}
	}

	helpers := make(map[string]*replicasInSameRGAssignmentHelper)
	strictIsolation := paramtable.Get().StreamingCfg.StrictResourceGroupIsolationEnabled.GetAsBool()

	if len(uncoveredReplicas) > 0 && !strictIsolation {
		if _, ok := sqnNodesByRG[DefaultResourceGroupName]; ok {
			// Compatibility mode for rolling upgrades from old StreamingNodes without RG labels:
			// uncovered replicas keep using the default legacy pool, while covered replicas keep
			// their isolated pools. If there are also replicas explicitly in the default RG, they
			// share the same default pool with uncovered legacy replicas.
			rgToReplicas[DefaultResourceGroupName] = append(rgToReplicas[DefaultResourceGroupName], uncoveredReplicas...)
			for rgName, rgReplicas := range rgToReplicas {
				helpers[rgName] = newReplicaSQNAssignmentHelper(rgName, rgReplicas, sqnNodesByRG[rgName])
			}
			return helpers
		}
	}

	// Check if we should use fallback mode (flat allocation).
	// Fallback mode is used when there are uncovered replicas and isolation is disabled.
	useFallbackMode := len(uncoveredReplicas) > 0 && !strictIsolation

	if useFallbackMode {
		// Fallback: pool all nodes together for ALL replicas.
		// When fallback is triggered, we must use flat allocation for all replicas,
		// not just the uncovered ones, to avoid assigning the same nodes twice.
		allSQNodes := typeutil.NewUniqueSet()
		for _, nodes := range sqnNodesByRG {
			for nodeID := range nodes {
				allSQNodes.Insert(nodeID)
			}
		}
		helpers[DefaultResourceGroupName] = newReplicaSQNAssignmentHelper(DefaultResourceGroupName, replicas, allSQNodes)
	} else {
		// Isolation mode: each replica gets nodes only from its own resource group.
		// Uncovered replicas (if any and isolation is enabled) simply don't get any streaming query nodes.
		for rgName, rgReplicas := range rgToReplicas {
			helpers[rgName] = newReplicaSQNAssignmentHelper(rgName, rgReplicas, sqnNodesByRG[rgName])
		}
	}
	return helpers
}
