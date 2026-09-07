package resolver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

func TestShardResolverImplReturnsCollectionVChannels(t *testing.T) {
	const collectionID int64 = 100
	vchannel0 := funcutil.GetVirtualChannel("p0", collectionID, 0)
	vchannel1 := funcutil.GetVirtualChannel("p1", collectionID, 1)

	resolver := NewShardResolverImpl(&staticVChannelProvider{vchannels: map[int64][]string{
		collectionID: {vchannel0, vchannel1},
	}})
	defer resolver.Close()

	// ResolveVChannels reflects the collection's vchannels as reported by the
	// provider, without any assignment filtering.
	vchannels, err := resolver.ResolveVChannels(context.Background(), collectionID)
	require.NoError(t, err)
	assert.Equal(t, []string{vchannel0, vchannel1}, vchannels)
}

func TestShardResolverImplReturnsEmptyForNoVChannels(t *testing.T) {
	const collectionID int64 = 100
	resolver := NewShardResolverImpl(&staticVChannelProvider{vchannels: map[int64][]string{}})
	defer resolver.Close()

	// An empty provider result is returned as-is: load gating is not the
	// resolver's responsibility (the view runtime fast-fails Phase 1).
	vchannels, err := resolver.ResolveVChannels(context.Background(), collectionID)
	require.NoError(t, err)
	assert.Empty(t, vchannels)
}

func TestShardResolverImplPassesThroughProviderError(t *testing.T) {
	const collectionID int64 = 100
	providerErr := context.DeadlineExceeded
	resolver := NewShardResolverImpl(&staticVChannelProvider{err: providerErr})
	defer resolver.Close()

	_, err := resolver.ResolveVChannels(context.Background(), collectionID)
	require.ErrorIs(t, err, providerErr)
}

type staticVChannelProvider struct {
	vchannels map[int64][]string
	err       error
}

func (p *staticVChannelProvider) GetCollectionVChannels(_ context.Context, collectionID int64) ([]string, error) {
	if p.err != nil {
		return nil, p.err
	}
	return append([]string(nil), p.vchannels[collectionID]...), nil
}
