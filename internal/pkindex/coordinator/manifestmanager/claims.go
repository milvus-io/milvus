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

package manifestmanager

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/pkindex/core/manifest"
	"github.com/milvus-io/milvus/internal/pkindex/pkerr"
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

// get returns the record of c, or nil when c is nil.
func (c *pchannelClaim) get() *pkindexpb.PChannelClaim {
	if c == nil {
		return nil
	}
	return c.pb
}

// pchannelClaimPred is the predicate that a flush writes under: the claim
// record of pchannel still holds the value that the flush term was checked
// against.
type pchannelClaimPred struct {
	pchannel string
	pred     predicates.Predicate
}

// pchannelClaims keeps the pchannel claim records: it reads, caches and
// writes them. The cached term of a pchannel only goes up. A claim always
// reads the record from the meta store. A flush reads the cached record, and
// reads the meta store when the cache has no record or the flush term is
// above the cached term. A failed write removes the cached record.
type pchannelClaims struct {
	meta  MetaStore
	locks *lock.KeyLock[string]

	mu     sync.Mutex
	claims map[string]*pchannelClaim // by pchannel
}

func newPChannelClaims(meta MetaStore) *pchannelClaims {
	return &pchannelClaims{
		meta:   meta,
		locks:  lock.NewKeyLock[string](),
		claims: map[string]*pchannelClaim{},
	}
}

// claim claims pchannel for term with manifest.ApplyPChannelClaim. It reads
// the claim record from the meta store, not from the cache.
func (c *pchannelClaims) claim(ctx context.Context, pchannel string, term int64) error {
	key := manifest.ClaimKey(pchannel)
	c.locks.Lock(key)
	defer c.locks.Unlock(key)

	cur, err := c.load(ctx, pchannel)
	if err != nil {
		return err
	}
	next, changed, err := manifest.ApplyPChannelClaim(cur.get(), pchannel, term)
	if err != nil {
		return err
	}
	if !changed {
		c.remember(pchannel, cur)
		return nil
	}
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

// checkFlush checks a flush term of pchannel against the claim record with
// manifest.CheckFlushTerm, and returns the predicate that the flush writes
// under. It reads the cached record, and reads the meta store when the cache
// has no record or term is above the cached term, because a claim through
// another coordinator may have raised the stored record.
func (c *pchannelClaims) checkFlush(ctx context.Context, pchannel string, term int64) (*pchannelClaimPred, error) {
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
	if err := manifest.CheckFlushTerm(cur.get(), pchannel, term); err != nil {
		return nil, err
	}
	return &pchannelClaimPred{
		pchannel: pchannel,
		pred:     predicates.ValueEqual(manifest.ClaimKey(pchannel), cur.value),
	}, nil
}

// load reads the claim record of pchannel from the meta store. It returns nil
// when the pchannel has no claim record.
func (c *pchannelClaims) load(ctx context.Context, pchannel string) (*pchannelClaim, error) {
	key := manifest.ClaimKey(pchannel)
	value, found, err := c.meta.Load(ctx, key)
	if err != nil {
		return nil, markUnavailable(err, "load claim record %s", key)
	}
	if !found {
		return nil, nil
	}
	var pb pkindexpb.PChannelClaim
	if err := proto.Unmarshal([]byte(value), &pb); err != nil {
		return nil, errors.Mark(errors.Wrapf(err, "decode claim record %s", key), pkerr.ErrCorrupted)
	}
	if pb.GetPchannelTerm() <= 0 {
		return nil, errors.Mark(errors.Newf("claim record %s has term %d", key, pb.GetPchannelTerm()), pkerr.ErrCorrupted)
	}
	return &pchannelClaim{pb: &pb, value: value}, nil
}

func (c *pchannelClaims) cached(pchannel string) *pchannelClaim {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.claims[pchannel]
}

// remember caches claim when the cache has no record of pchannel or a lower
// term, so that a concurrent read of an older record does not lower it.
func (c *pchannelClaims) remember(pchannel string, claim *pchannelClaim) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cur := c.claims[pchannel]; cur == nil || claim.pb.GetPchannelTerm() > cur.pb.GetPchannelTerm() {
		c.claims[pchannel] = claim
	}
}

func (c *pchannelClaims) forget(pchannel string) {
	c.mu.Lock()
	delete(c.claims, pchannel)
	c.mu.Unlock()
}
