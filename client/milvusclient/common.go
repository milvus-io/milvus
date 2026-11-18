package milvusclient

import (
	"context"
	"math"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/internal/merr"
	"github.com/milvus-io/milvus/client/v3/internal/retry"
)

// guarantee timestamp constants shared with the pymilvus client,
// see pymilvus/client/constants.py (EVENTUALLY_TS / BOUNDED_TS).
const (
	// eventuallyTs is the fallback guarantee timestamp when no cached
	// mutation timestamp is available for the collection.
	eventuallyTs = uint64(1)
	// boundedTs is the guarantee timestamp used by the bounded consistency level.
	boundedTs = uint64(2)
)

func (c *Client) getCollectionInDB(ctx context.Context, dbName, collName string) (*entity.Collection, error) {
	return c.collCache.GetCollection(ctx, newCollectionCacheKey(dbName, collName), false)
}

func (c *Client) retryIfSchemaError(ctx context.Context, dbName, collName string, work func(ctx context.Context) (uint64, error)) error {
	var lastTs uint64 = math.MaxUint64
	return retry.Handle(ctx, func() (bool, error) {
		ts, err := work(ctx)
		if err != nil {
			// if schema error
			if errors.Is(err, merr.ErrCollectionSchemaMismatch) {
				sameTs := ts == lastTs
				lastTs = ts
				if !sameTs {
					c.collCache.Evict(newCollectionCacheKey(dbName, collName))
				}
				// retry if not same ts
				return !sameTs, err
			}
			return false, err
		}
		return false, nil
	})
}

// guaranteeTimestamp computes the guarantee_timestamp for a search/query request
// according to the requested consistency level, mirroring pymilvus's
// `construct_guarantee_ts`:
//
//   - default consistency: last write ts of the collection, or eventually (1)
//   - Strong: 0 (server assigns the newest ts)
//   - Session: last write ts of the collection, or eventually (1)
//   - Bounded: 2
//   - Eventually: 1
//   - Customized: 1 (the legacy option interfaces do not expose a way to set a
//     caller-provided timestamp; pymilvus falls back to eventually (1) here too
//     when no timestamp is supplied)
func (c *Client) guaranteeTimestamp(key collectionCacheKey, consistencyLevel commonpb.ConsistencyLevel, useDefault bool) uint64 {
	ts := c.collCache.GetWriteTs(key)
	if useDefault {
		return firstNonZero(ts, eventuallyTs)
	}
	switch entity.ConsistencyLevel(consistencyLevel) {
	case entity.ClStrong:
		return 0
	case entity.ClSession:
		return firstNonZero(ts, eventuallyTs)
	case entity.ClBounded:
		return boundedTs
	case entity.ClEventually, entity.ClCustomized:
		// pymilvus does kwargs.setdefault(GUARANTEE_TIMESTAMP, EVENTUALLY_TS):
		// without a public way to supply a caller timestamp this falls back to
		// eventually (1)
		return eventuallyTs
	default:
		return 0
	}
}

func firstNonZero(vals ...uint64) uint64 {
	for _, v := range vals {
		if v != 0 {
			return v
		}
	}
	return 0
}
