package resolver

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/resolver"

	"github.com/milvus-io/milvus/internal/mocks/google.golang.org/grpc/mock_resolver"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	m.Run()
}

func TestWatchBasedGRPCResolver(t *testing.T) {
	cc := mock_resolver.NewMockClientConn(t)
	cc.EXPECT().UpdateState(mock.Anything).Return(nil)

	r := newWatchBasedGRPCResolver(cc, log.With())
	assert.NoError(t, r.Update(VersionedState{State: resolver.State{Addresses: []resolver.Address{{Addr: "addr"}}}}))

	cc.EXPECT().UpdateState(mock.Anything).Unset()
	cc.EXPECT().UpdateState(mock.Anything).Return(errors.New("err"))
	// watch based resolver could ignore the error.
	assert.NoError(t, r.Update(VersionedState{State: resolver.State{Addresses: []resolver.Address{{Addr: "addr"}}}}))

	r.Close()
	assert.Error(t, r.Update(VersionedState{State: resolver.State{Addresses: []resolver.Address{{Addr: "addr"}}}}))
}
