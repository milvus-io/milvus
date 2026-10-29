package idf

import (
	"context"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// loadSealedContributions merges each completed read immediately. A worker
// retains its process-wide permit through the merge, bounding temporary decoded
// segment statistics even when merging is slower than object-storage reads.
func (p *Provider) loadSealedContributions(ctx context.Context, resources map[int64]*datapb.StreamingNodeBM25Resource, fields bm25Stats) (bm25Stats, error) {
	limiter := p.sealedStatsLoadLimiter
	group, groupCtx := errgroup.WithContext(ctx)
	aggregate := make(bm25Stats)
	var mergeMu sync.Mutex
	var acquireErr error
	for _, resource := range resources {
		if acquireErr = limiter.Acquire(groupCtx); acquireErr != nil {
			break
		}
		if acquireErr = groupCtx.Err(); acquireErr != nil {
			limiter.Release()
			break
		}
		group.Go(func() error {
			defer limiter.Release()
			stats, err := loadSealedSegmentStats(groupCtx, p.chunkManager, resource, fields)
			if err != nil {
				return err
			}
			mergeMu.Lock()
			defer mergeMu.Unlock()
			for field, value := range stats {
				if current := aggregate[field]; current != nil {
					current.Merge(value)
				} else {
					aggregate[field] = value
				}
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	if acquireErr != nil {
		return nil, acquireErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return aggregate, nil
}
