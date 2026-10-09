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

package rootcoord

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	rlsPolicyWarmupConcurrency = 32
	rlsPolicyLoadTimeout       = 30 * time.Second
)

type rlsPolicySnapshot struct {
	generation uint64
	policies   map[string]*model.RLSPolicy
}

// loadRLSPolicies satisfies the generation captured on entry. A later mutation
// affects subsequent callers, not the snapshot already being loaded for this one.
func (mt *MetaTable) loadRLSPolicies(ctx context.Context, collectionID int64) (rlsPolicySnapshot, error) {
	if err := ctx.Err(); err != nil {
		return rlsPolicySnapshot{}, err
	}
	mt.ddLock.RLock()
	coll := mt.collID2Meta[collectionID]
	if coll == nil || !coll.Available() {
		mt.ddLock.RUnlock()
		return rlsPolicySnapshot{}, merr.WrapErrCollectionNotFound(collectionID)
	}
	generation := coll.RLSPolicyExpectedGeneration
	if coll.RLSPoliciesCurrent() {
		snapshot := rlsPolicySnapshot{coll.RLSPolicyGeneration, model.CloneRLSPolicyMap(coll.RLSPolicies)}
		mt.ddLock.RUnlock()
		return snapshot, nil
	}
	mt.ddLock.RUnlock()

	resultCh := mt.rlsPolicyLoads.DoChan(fmt.Sprintf("%d/%d", collectionID, generation), func() (any, error) {
		mt.ddLock.RLock()
		coll := mt.collID2Meta[collectionID]
		if coll == nil || !coll.Available() {
			mt.ddLock.RUnlock()
			return nil, merr.WrapErrCollectionNotFound(collectionID)
		}
		// Another flight may have populated a sufficient snapshot before we joined.
		if !coll.RLSPoliciesUnloaded && coll.RLSPolicyGeneration >= generation {
			snapshot := rlsPolicySnapshot{coll.RLSPolicyGeneration, model.CloneRLSPolicyMap(coll.RLSPolicies)}
			mt.ddLock.RUnlock()
			return snapshot, nil
		}
		loaded := &model.Collection{DBID: coll.DBID, CollectionID: collectionID}
		mt.ddLock.RUnlock()

		// The shared load outlives any one caller's cancellation, but not the Coord.
		loadCtx, cancel := context.WithTimeout(mt.ctx, rlsPolicyLoadTimeout)
		defer cancel()
		if err := mt.reloadCollectionRLSMetadata(loadCtx, loaded); err != nil {
			return nil, err
		}

		mt.ddLock.Lock()
		defer mt.ddLock.Unlock()
		coll = mt.collID2Meta[collectionID]
		if coll == nil || !coll.Available() {
			return nil, merr.WrapErrCollectionNotFound(collectionID)
		}
		for _, policy := range loaded.RLSPolicies {
			policy.DBID = coll.DBID
		}
		if coll.RLSPoliciesUnloaded || coll.RLSPolicyGeneration < generation {
			coll.RLSPolicies = loaded.RLSPolicies
			coll.RLSPoliciesUnloaded = false
			coll.RLSPolicyGeneration = generation
		}
		// Never relabel this read with an expectation advanced while I/O was in flight.
		return rlsPolicySnapshot{generation, model.CloneRLSPolicyMap(loaded.RLSPolicies)}, nil
	})
	select {
	case result := <-resultCh:
		if err := ctx.Err(); err != nil {
			return rlsPolicySnapshot{}, err
		}
		if result.Err != nil {
			return rlsPolicySnapshot{}, result.Err
		}
		snapshot := result.Val.(rlsPolicySnapshot)
		// A lone waiter already owns the detached result; shared waiters need copies.
		if result.Shared {
			snapshot.policies = model.CloneRLSPolicyMap(snapshot.policies)
		}
		return snapshot, nil
	case <-ctx.Done():
		return rlsPolicySnapshot{}, ctx.Err()
	}
}

func (mt *MetaTable) warmupRLSPolicies(ctx context.Context) {
	if !Params.RootCoordCfg.RLSPolicyWarmupEnabled.GetAsBool() {
		return
	}
	mt.ddLock.RLock()
	collectionIDs := make([]int64, 0)
	for id, coll := range mt.collID2Meta {
		enabled, err := common.IsRLSEnabled(coll.Properties...)
		if err == nil && enabled && coll.Available() {
			collectionIDs = append(collectionIDs, id)
		}
	}
	mt.ddLock.RUnlock()

	var group errgroup.Group
	group.SetLimit(rlsPolicyWarmupConcurrency)
	for _, id := range collectionIDs {
		if ctx.Err() != nil {
			break
		}
		group.Go(func() error {
			if _, err := mt.loadRLSPolicies(ctx, id); err != nil && ctx.Err() == nil {
				mlog.RatedWarn(ctx, 1, "RLS policy warmup failed; requests will load on demand",
					mlog.FieldCollectionID(id), mlog.Err(err))
			}
			return nil
		})
	}
	_ = group.Wait()
}
