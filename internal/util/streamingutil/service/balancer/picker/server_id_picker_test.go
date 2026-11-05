package picker

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/resolver"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/attributes"
	bbalancer "github.com/milvus-io/milvus/internal/util/streamingutil/service/balancer"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/util/interceptor"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type testSubConn struct {
	balancer.SubConn
}

func (sc *testSubConn) UpdateAddresses(addresses []resolver.Address) {}

func (sc *testSubConn) Connect() {}

func (sc *testSubConn) GetOrBuildProducer(balancer.ProducerBuilder) (balancer.Producer, func()) {
	return nil, nil
}

func (sc *testSubConn) RegisterHealthListener(func(balancer.SubConnState)) {}

func (sc *testSubConn) Shutdown() {}

func TestServerIDPickerBuilder(t *testing.T) {
	builder := &serverIDPickerBuilder{}
	picker := builder.Build(bbalancer.PickerBuildInfo{})
	assert.NotNil(t, picker)
	_, err := picker.Pick(balancer.PickInfo{Ctx: context.Background()})
	assert.Error(t, err)
	assert.ErrorIs(t, err, balancer.ErrNoSubConnAvailable)

	picker = builder.Build(bbalancer.PickerBuildInfo{
		ReadySCs: map[balancer.SubConn]base.SubConnInfo{
			&testSubConn{}: {
				Address: resolver.Address{
					Addr: "localhost:1",
					BalancerAttributes: attributes.WithServerID(
						new(attributes.Attributes),
						1,
					),
				},
			},
			&testSubConn{}: {
				Address: resolver.Address{
					Addr: "localhost:2",
					BalancerAttributes: attributes.WithServerID(
						new(attributes.Attributes),
						2,
					),
				},
			},
		},
		UnReadySCs: map[balancer.SubConn]base.SubConnInfo{
			&testSubConn{}: {
				Address: resolver.Address{
					Addr: "localhost:3",
					BalancerAttributes: attributes.WithServerID(
						new(attributes.Attributes),
						3,
					),
				},
			},
		},
	})
	// Test round-robin
	serverIDSet := typeutil.NewSet[string]()
	info, err := picker.Pick(balancer.PickInfo{Ctx: context.Background()})
	assert.NoError(t, err)
	serverIDSet.Insert(info.Metadata.Get(interceptor.ServerIDKey)[0])
	info, err = picker.Pick(balancer.PickInfo{Ctx: context.Background()})
	assert.NoError(t, err)
	serverIDSet.Insert(info.Metadata.Get(interceptor.ServerIDKey)[0])
	serverIDSet.Insert(info.Metadata.Get(interceptor.ServerIDKey)[0])
	assert.Equal(t, 2, serverIDSet.Len())

	// Test force address
	info, err = picker.Pick(balancer.PickInfo{
		Ctx: contextutil.WithPickServerID(context.Background(), 1),
	})
	assert.NoError(t, err)
	assert.Equal(t, "1", info.Metadata.Get(interceptor.ServerIDKey)[0])

	// Test pick not ready
	info, err = picker.Pick(balancer.PickInfo{
		Ctx: contextutil.WithPickServerID(context.Background(), 3),
	})
	assert.Error(t, err)
	assert.ErrorIs(t, err, balancer.ErrNoSubConnAvailable)
	assert.NotNil(t, info)

	// Test pick not exists
	info, err = picker.Pick(balancer.PickInfo{
		Ctx: contextutil.WithPickServerID(context.Background(), 4),
	})
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrSubConnNotExist)
	assert.NotNil(t, info)
}

func TestIsErrNoSubConnForPick(t *testing.T) {
	assert.True(t, IsErrSubConnNoExist(ErrSubConnNotExist))
	assert.False(t, IsErrSubConnNoExist(errors.New("test")))
	err := status.ConvertStreamingError("test", ErrSubConnNotExist)
	assert.True(t, IsErrSubConnNoExist(err))
}

func TestFailFastPickDistinguishesDisconnectedAndRemovedSessions(t *testing.T) {
	builder := &serverIDPickerBuilder{}
	p := builder.Build(bbalancer.PickerBuildInfo{
		Err:        ErrSubConnNotReady,
		UnReadySCs: map[balancer.SubConn]base.SubConnInfo{&testSubConn{}: {Address: resolver.Address{Addr: "localhost:1", BalancerAttributes: attributes.WithServerID(new(attributes.Attributes), 1)}}},
	})
	// Even with no ready connections the picker retains session membership.
	_, err := p.Pick(balancer.PickInfo{Ctx: contextutil.WithFailFastPick(contextutil.WithPickServerID(context.Background(), 1))})
	assert.ErrorIs(t, err, ErrSubConnNotReady)
	_, err = p.Pick(balancer.PickInfo{Ctx: contextutil.WithFailFastPick(contextutil.WithPickServerID(context.Background(), 2))})
	assert.ErrorIs(t, err, ErrSubConnNotExist)
	// Ordinary WAL callers keep their aggregate error/readiness behavior.
	_, err = p.Pick(balancer.PickInfo{Ctx: contextutil.WithPickServerID(context.Background(), 1)})
	assert.ErrorIs(t, err, ErrSubConnNotReady)
	p = builder.Build(bbalancer.PickerBuildInfo{UnReadySCs: map[balancer.SubConn]base.SubConnInfo{&testSubConn{}: {Address: resolver.Address{Addr: "localhost:1", BalancerAttributes: attributes.WithServerID(new(attributes.Attributes), 1)}}}})
	_, err = p.Pick(balancer.PickInfo{Ctx: contextutil.WithPickServerID(context.Background(), 1)})
	assert.ErrorIs(t, err, balancer.ErrNoSubConnAvailable)
}

func TestPickerRecordsSelectedServerAndPreservesRouting(t *testing.T) {
	builder := &serverIDPickerBuilder{}
	p := builder.Build(bbalancer.PickerBuildInfo{
		ReadySCs: map[balancer.SubConn]base.SubConnInfo{
			&testSubConn{}: {Address: resolver.Address{Addr: "node-7:1234", BalancerAttributes: attributes.WithServerID(new(attributes.Attributes), 7)}},
			&testSubConn{}: {Address: resolver.Address{Addr: "node-9:1234", BalancerAttributes: attributes.WithServerID(new(attributes.Attributes), 9)}},
		},
	})
	ctx := contextutil.WithPickServerID(context.Background(), contextutil.UnspecifiedServerID)
	_, err := p.Pick(balancer.PickInfo{Ctx: ctx})
	assert.NoError(t, err)
	first := contextutil.GetPickedServerID(ctx)
	id, ok := contextutil.GetPickServerID(ctx)
	assert.True(t, ok)
	assert.Equal(t, first, id)
	// Filling an unspecified ID must not turn the next attempt into forced routing.
	_, err = p.Pick(balancer.PickInfo{Ctx: ctx})
	assert.NoError(t, err)
	assert.NotEqual(t, first, contextutil.GetPickedServerID(ctx))

	forced := contextutil.WithPickServerID(context.Background(), 7)
	for range 2 {
		_, err = p.Pick(balancer.PickInfo{Ctx: forced})
		assert.NoError(t, err)
		assert.EqualValues(t, 7, contextutil.GetPickedServerID(forced))
	}
	// Session removal clears an old successful pick, but preserves the route.
	empty := builder.Build(bbalancer.PickerBuildInfo{})
	_, err = empty.Pick(balancer.PickInfo{Ctx: forced})
	assert.Error(t, err)
	assert.Equal(t, contextutil.UnspecifiedServerID, contextutil.GetPickedServerID(forced))
	id, ok = contextutil.GetPickServerID(forced)
	assert.True(t, ok)
	assert.EqualValues(t, 7, id)

	_, err = empty.Pick(balancer.PickInfo{Ctx: ctx})
	assert.Error(t, err)
	assert.Equal(t, contextutil.UnspecifiedServerID, contextutil.GetPickedServerID(ctx))
	_, ok = contextutil.GetPickServerID(ctx)
	assert.False(t, ok)
}
