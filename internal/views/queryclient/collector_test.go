package queryclient

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestLegacyCollectors(t *testing.T) {
	shard := testShard("v0")
	searchCollector := newLegacySearchCollector()
	require.ErrorIs(t, searchCollector.Add(shard, nil), merr.ErrServiceInternal)
	require.Error(t, searchCollector.Add(shard, &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{
		Status: merr.Status(merr.WrapErrServiceUnavailableMsg("failed")),
	}}))
	require.NoError(t, searchCollector.Add(shard, &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{
		Status: merr.Success(),
	}}))
	searchResults := searchCollector.Results()
	require.Len(t, searchResults, 1)
	require.Equal(t, shard, searchResults[0].ShardID)
	require.NotNil(t, searchResults[0].Result)
	searchCollector.ResetShard(shard)
	require.Empty(t, searchCollector.Results())

	queryCollector := newLegacyQueryCollector()
	require.ErrorIs(t, queryCollector.Add(shard, nil), merr.ErrServiceInternal)
	require.Error(t, queryCollector.Add(shard, &viewpb.QueryOnViewResponse{LegacyResults: &internalpb.RetrieveResults{
		Status: merr.Status(merr.WrapErrServiceUnavailableMsg("failed")),
	}}))
	require.NoError(t, queryCollector.Add(shard, &viewpb.QueryOnViewResponse{LegacyResults: &internalpb.RetrieveResults{
		Status: merr.Success(),
	}}))
	queryResults := queryCollector.Results()
	require.Len(t, queryResults, 1)
	require.Equal(t, shard, queryResults[0].ShardID)
	require.NotNil(t, queryResults[0].Result)
	queryCollector.ResetShard(shard)
	require.Empty(t, queryCollector.Results())
}

func TestLegacySearchCollectorConcurrentAddAndReset(t *testing.T) {
	collector := newLegacySearchCollector()
	shard := testShard("v0")
	other := testShard("v1")
	var group sync.WaitGroup
	errors := make(chan error, 100)
	for index := 0; index < 100; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			errors <- collector.Add(shard, &viewpb.SearchOnViewResponse{
				LegacyResults: &internalpb.SearchResults{Status: merr.Success()},
			})
		}()
	}
	require.NoError(t, collector.Add(other, &viewpb.SearchOnViewResponse{
		LegacyResults: &internalpb.SearchResults{Status: merr.Success()},
	}))
	group.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	collector.ResetShard(shard)
	results := collector.Results()
	require.Len(t, results, 1)
	require.Equal(t, other, results[0].ShardID)
}
