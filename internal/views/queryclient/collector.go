package queryclient

import (
	"sync"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type searchResultCollector interface {
	Add(shardID qviews.ShardID, response *viewpb.SearchOnViewResponse) error
	ResetShard(shardID qviews.ShardID)
}

type queryResultCollector interface {
	Add(shardID qviews.ShardID, response *viewpb.QueryOnViewResponse) error
	ResetShard(shardID qviews.ShardID)
}

type legacySearchCollector struct {
	mu      sync.Mutex
	results map[string][]*internalpb.SearchResults
}

func newLegacySearchCollector() *legacySearchCollector {
	return &legacySearchCollector{results: make(map[string][]*internalpb.SearchResults)}
}

func (c *legacySearchCollector) Add(shardID qviews.ShardID, response *viewpb.SearchOnViewResponse) error {
	result := response.GetLegacyResults()
	if result == nil {
		return merr.WrapErrServiceInternalMsg("missing legacy search result for shard %s", shardID)
	}
	if err := merr.Error(result.GetStatus()); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.results[shardID.String()] = append(c.results[shardID.String()], result)
	return nil
}

func (c *legacySearchCollector) ResetShard(shardID qviews.ShardID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.results, shardID.String())
}

func (c *legacySearchCollector) Results() []*internalpb.SearchResults {
	c.mu.Lock()
	defer c.mu.Unlock()
	results := make([]*internalpb.SearchResults, 0)
	for _, shardResults := range c.results {
		results = append(results, shardResults...)
	}
	return results
}

type legacyQueryCollector struct {
	mu      sync.Mutex
	results map[string][]*internalpb.RetrieveResults
}

func newLegacyQueryCollector() *legacyQueryCollector {
	return &legacyQueryCollector{results: make(map[string][]*internalpb.RetrieveResults)}
}

func (c *legacyQueryCollector) Add(shardID qviews.ShardID, response *viewpb.QueryOnViewResponse) error {
	result := response.GetLegacyResults()
	if result == nil {
		return merr.WrapErrServiceInternalMsg("missing legacy query result for shard %s", shardID)
	}
	if err := merr.Error(result.GetStatus()); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.results[shardID.String()] = append(c.results[shardID.String()], result)
	return nil
}

func (c *legacyQueryCollector) ResetShard(shardID qviews.ShardID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.results, shardID.String())
}

func (c *legacyQueryCollector) Results() []*internalpb.RetrieveResults {
	c.mu.Lock()
	defer c.mu.Unlock()
	results := make([]*internalpb.RetrieveResults, 0)
	for _, shardResults := range c.results {
		results = append(results, shardResults...)
	}
	return results
}
