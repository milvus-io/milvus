package broadcast

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_balancer"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestStartBroadcastWithSecondaryClusterResourceKey(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		paramtable.Init()
		ResetBroadcaster()
		balance.ResetBalancer()

		mb := mock_balancer.NewMockBalancer(t)
		mb.EXPECT().WaitUntilWALbasedDDLReady(mock.Anything).Return(nil).Maybe()
		balance.Register(mb)

		mba := mock_broadcaster.NewMockBroadcastAPI(t)
		mba.EXPECT().Close().Return().Maybe()

		mbc := mock_broadcaster.NewMockBroadcaster(t)
		mbc.EXPECT().WithSecondaryClusterResourceKey(mock.Anything).Return(mba, nil).Maybe()
		mbc.EXPECT().Close().Return().Maybe()
		Register(mbc)

		api, err := StartBroadcastWithSecondaryClusterResourceKey(context.Background())
		assert.NoError(t, err)
		assert.NotNil(t, api)
		api.Close()
	})

	t.Run("broadcaster_error", func(t *testing.T) {
		paramtable.Init()
		ResetBroadcaster()
		balance.ResetBalancer()

		mb := mock_balancer.NewMockBalancer(t)
		mb.EXPECT().WaitUntilWALbasedDDLReady(mock.Anything).Return(nil).Maybe()
		balance.Register(mb)

		mbc := mock_broadcaster.NewMockBroadcaster(t)
		mbc.EXPECT().WithSecondaryClusterResourceKey(mock.Anything).Return(nil, broadcaster.ErrNotSecondary).Maybe()
		mbc.EXPECT().Close().Return().Maybe()
		Register(mbc)

		api, err := StartBroadcastWithSecondaryClusterResourceKey(context.Background())
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrNotSecondary))
		assert.Nil(t, api)
	})

	t.Run("balance_error", func(t *testing.T) {
		paramtable.Init()
		ResetBroadcaster()
		balance.ResetBalancer()

		mb := mock_balancer.NewMockBalancer(t)
		mb.EXPECT().WaitUntilWALbasedDDLReady(mock.Anything).Return(errors.New("balance error")).Maybe()
		balance.Register(mb)

		mbc := mock_broadcaster.NewMockBroadcaster(t)
		mbc.EXPECT().Close().Return().Maybe()
		Register(mbc)

		api, err := StartBroadcastWithSecondaryClusterResourceKey(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to wait until WAL based DDL ready")
		assert.Nil(t, api)
	})
}

func TestStartBroadcastWithResourceKeys(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		paramtable.Init()
		ResetBroadcaster()
		balance.ResetBalancer()

		mb := mock_balancer.NewMockBalancer(t)
		mb.EXPECT().WaitUntilWALbasedDDLReady(mock.Anything).Return(nil).Maybe()
		balance.Register(mb)

		mba := mock_broadcaster.NewMockBroadcastAPI(t)
		mba.EXPECT().Close().Return().Maybe()

		mbc := mock_broadcaster.NewMockBroadcaster(t)
		mbc.EXPECT().WithResourceKeys(mock.Anything, mock.Anything).Return(mba, nil).Maybe()
		mbc.EXPECT().Close().Return().Maybe()
		Register(mbc)

		api, err := StartBroadcastWithResourceKeys(context.Background(), message.NewExclusiveClusterResourceKey())
		assert.NoError(t, err)
		assert.NotNil(t, api)
		api.Close()
	})

	t.Run("not_primary", func(t *testing.T) {
		paramtable.Init()
		ResetBroadcaster()
		balance.ResetBalancer()

		mb := mock_balancer.NewMockBalancer(t)
		mb.EXPECT().WaitUntilWALbasedDDLReady(mock.Anything).Return(nil).Maybe()
		balance.Register(mb)

		mbc := mock_broadcaster.NewMockBroadcaster(t)
		mbc.EXPECT().WithResourceKeys(mock.Anything, mock.Anything).Return(nil, broadcaster.ErrNotPrimary).Maybe()
		mbc.EXPECT().Close().Return().Maybe()
		Register(mbc)

		api, err := StartBroadcastWithResourceKeys(context.Background(), message.NewExclusiveClusterResourceKey())
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrNotPrimary))
		assert.Nil(t, api)
	})
}
