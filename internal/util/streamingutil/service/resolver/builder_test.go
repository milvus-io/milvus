package resolver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/resolver"

	"github.com/milvus-io/milvus/internal/mocks/google.golang.org/grpc/mock_resolver"
	"github.com/milvus-io/milvus/internal/mocks/util/streamingutil/service/mock_discoverer"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/discoverer"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestNewBuilder(t *testing.T) {
	d := mock_discoverer.NewMockDiscoverer(t)
	ch := make(chan discoverer.VersionedState)
	d.EXPECT().Discover(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb func(discoverer.VersionedState) error) error {
		for {
			select {
			case state := <-ch:
				if err := cb(state); err != nil {
					return err
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})
	d.EXPECT().NewVersionedState().Return(discoverer.VersionedState{
		Version: typeutil.VersionInt64(-1),
	})

	b := newBuilder("test", d, log.With())
	r := b.Resolver()
	assert.NotNil(t, r)
	assert.Equal(t, "test", b.Scheme())
	mockClientConn := mock_resolver.NewMockClientConn(t)
	mockClientConn.EXPECT().UpdateState(mock.Anything).RunAndReturn(func(args resolver.State) error {
		return nil
	})
	grpcResolver, err := b.Build(resolver.Target{}, mockClientConn, resolver.BuildOptions{})
	assert.NoError(t, err)
	assert.NotNil(t, grpcResolver)
	b.Close()
}
