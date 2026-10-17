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

package manifest

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
)

// pchannelClaim is the claim record of one pchannel. value is the record as
// it is stored in the meta store.
type pchannelClaim struct {
	pb    *pkindexpb.PChannelClaim
	value string
}

// pchannelClaimPred is the predicate that a flush writes under: the claim
// record of pchannel still holds the value that the flush term was checked
// against.
type pchannelClaimPred struct {
	pchannel string
	pred     predicates.Predicate
}

// claimTable keeps the pchannel claim records: it reads, caches and writes
// them. The cached term of a pchannel only goes up. A claim always reads the
// record from the meta store. A flush reads the cached record, and reads the
// meta store when the cache has no record or the flush term is above the
// cached term. A failed write removes the cached record.
type claimTable struct {
	meta  MetaStore
	locks *lock.KeyLock[string]

	mu     sync.Mutex
	claims map[string]*pchannelClaim // by pchannel
}

func newClaimTable(meta MetaStore) *claimTable {
	return &claimTable{
		meta:   meta,
		locks:  lock.NewKeyLock[string](),
		claims: map[string]*pchannelClaim{},
	}
}

// claim claims pchannel for term. It creates the claim record when there is
// none, and raises it when term is above the recorded term. The recorded term
// is idempotent, and a term below it returns ErrFenced. It reads the claim
// record from the meta store, not from the cache.
func (c *claimTable) claim(ctx context.Context, pchannel string, term int64) error {
	key := ClaimKey(pchannel)
	c.locks.Lock(key)
	defer c.locks.Unlock(key)

	cur, err := c.load(ctx, pchannel)
	if err != nil {
		return err
	}
	if cur != nil && term < cur.pb.GetPchannelTerm() {
		return errors.Wrapf(ErrFenced, "pchannel %s: claim term %d, request term %d", pchannel, cur.pb.GetPchannelTerm(), term)
	}
	if cur != nil && term == cur.pb.GetPchannelTerm() {
		c.remember(pchannel, cur)
		return nil
	}
	next := &pkindexpb.PChannelClaim{PchannelTerm: term}
	b, err := marshal.Marshal(next)
	if err != nil {
		return errors.Wrapf(err, "marshal claim record of %s", pchannel)
	}
	value := string(b)
	pred := predicates.KeyNotExist(key)
	if cur != nil {
		pred = predicates.ValueEqual(key, cur.value)
	}
	if err := c.meta.Save(ctx, map[string]string{key: value}, pred); err != nil {
		// the write may have taken effect: the stored record decides
		if err := decideByReadBack(ctx, c.meta, key, value, err); err != nil {
			c.forget(pchannel)
			return markUnavailable(err, "write claim record %s term %d", key, term)
		}
	}
	c.remember(pchannel, &pchannelClaim{pb: next, value: value})
	return nil
}

// checkClaimedTerm checks that term is the claimed term of pchannel, and
// returns the predicate that a write under the claim must carry: the claim
// record still holds the value that term was checked against. It returns
// ErrFenced when term is below the claimed term, and ErrNotClaimed when
// pchannel has no claim record or term is above the claimed term. It reads
// the cached record, and reads the meta store when the cache has no record
// or term is above the cached term, because a claim through another
// coordinator may have raised the stored record.
func (c *claimTable) checkClaimedTerm(ctx context.Context, pchannel string, term int64) (*pchannelClaimPred, error) {
	cur := c.cached(pchannel)
	if cur == nil || term > cur.pb.GetPchannelTerm() {
		var err error
		if cur, err = c.load(ctx, pchannel); err != nil {
			return nil, err
		}
		if cur != nil {
			c.remember(pchannel, cur)
		}
	}
	switch {
	case cur == nil:
		return nil, errors.Wrapf(ErrNotClaimed, "pchannel %s: no claim record, term %d", pchannel, term)
	case term < cur.pb.GetPchannelTerm():
		return nil, errors.Wrapf(ErrFenced, "pchannel %s: claim term %d, term %d", pchannel, cur.pb.GetPchannelTerm(), term)
	case term > cur.pb.GetPchannelTerm():
		return nil, errors.Wrapf(ErrNotClaimed, "pchannel %s: claim term %d, term %d", pchannel, cur.pb.GetPchannelTerm(), term)
	}
	return &pchannelClaimPred{
		pchannel: pchannel,
		pred:     predicates.ValueEqual(ClaimKey(pchannel), cur.value),
	}, nil
}

// load reads the claim record of pchannel from the meta store. It returns nil
// when the pchannel has no claim record.
func (c *claimTable) load(ctx context.Context, pchannel string) (*pchannelClaim, error) {
	key := ClaimKey(pchannel)
	value, found, err := c.meta.Load(ctx, key)
	if err != nil {
		return nil, markUnavailable(err, "load claim record %s", key)
	}
	if !found {
		return nil, nil
	}
	var pb pkindexpb.PChannelClaim
	if err := proto.Unmarshal([]byte(value), &pb); err != nil {
		return nil, errors.Mark(errors.Wrapf(err, "decode claim record %s", key), pkindex.ErrCorrupted)
	}
	if pb.GetPchannelTerm() <= 0 {
		return nil, errors.Mark(errors.Newf("claim record %s has term %d", key, pb.GetPchannelTerm()), pkindex.ErrCorrupted)
	}
	return &pchannelClaim{pb: &pb, value: value}, nil
}

func (c *claimTable) cached(pchannel string) *pchannelClaim {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.claims[pchannel]
}

// remember caches claim when the cache has no record of pchannel or a lower
// term, so that a concurrent read of an older record does not lower it.
func (c *claimTable) remember(pchannel string, claim *pchannelClaim) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cur := c.claims[pchannel]; cur == nil || claim.pb.GetPchannelTerm() > cur.pb.GetPchannelTerm() {
		c.claims[pchannel] = claim
	}
}

func (c *claimTable) forget(pchannel string) {
	c.mu.Lock()
	delete(c.claims, pchannel)
	c.mu.Unlock()
}
