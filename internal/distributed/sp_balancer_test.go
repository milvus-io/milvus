package distributed

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/resolver"
)

type mockSubConn struct {
	balancer.SubConn
}

func TestPicker(t *testing.T) {
	subconn := &mockSubConn{}
	subConnInfo := base.SubConnInfo{
		Address: resolver.Address{Addr: "127.0.0.1"},
	}
	readySCs := map[balancer.SubConn]base.SubConnInfo{subconn: subConnInfo}

	info := base.PickerBuildInfo{ReadySCs: readySCs}
	sp := &spPickerBuilder{}
	picker := sp.Build(info)

	pickInfo := balancer.PickInfo{}
	pickInfo.Ctx = context.WithValue(context.Background(), AddressKey{}, "127.0.0.1")
	result, err := picker.Pick(pickInfo)
	assert.Nil(t, err)
	assert.Equal(t, result, balancer.PickResult{SubConn: subconn})
}

func TestPickerNoCtxError(t *testing.T) {
	subconn := &mockSubConn{}
	subConnInfo := base.SubConnInfo{
		Address: resolver.Address{Addr: "127.0.0.1"},
	}
	readySCs := map[balancer.SubConn]base.SubConnInfo{subconn: subConnInfo}

	info := base.PickerBuildInfo{ReadySCs: readySCs}
	sp := &spPickerBuilder{}
	picker := sp.Build(info)

	pickInfo := balancer.PickInfo{}
	result, err := picker.Pick(pickInfo)
	assert.NotNil(t, err)
	assert.Equal(t, result, balancer.PickResult{})
}

func TestPickerNoSpecificAddressError(t *testing.T) {
	subconn := &mockSubConn{}
	subConnInfo := base.SubConnInfo{
		Address: resolver.Address{Addr: "127.0.0.1"},
	}
	readySCs := map[balancer.SubConn]base.SubConnInfo{subconn: subConnInfo}

	info := base.PickerBuildInfo{ReadySCs: readySCs}
	sp := &spPickerBuilder{}
	picker := sp.Build(info)

	pickInfo := balancer.PickInfo{Ctx: context.Background()}
	result, err := picker.Pick(pickInfo)
	assert.NotNil(t, err)
	assert.Equal(t, result, balancer.PickResult{})
}

func TestPickerNoAvailableAddressError(t *testing.T) {
	subconn := &mockSubConn{}
	subConnInfo := base.SubConnInfo{
		Address: resolver.Address{Addr: "127.0.0.1"},
	}
	readySCs := map[balancer.SubConn]base.SubConnInfo{subconn: subConnInfo}

	info := base.PickerBuildInfo{ReadySCs: readySCs}
	sp := &spPickerBuilder{}
	picker := sp.Build(info)

	pickInfo := balancer.PickInfo{}
	pickInfo.Ctx = context.WithValue(context.Background(), AddressKey{}, "127.0.0.2")
	result, err := picker.Pick(pickInfo)
	assert.NotNil(t, err)
	assert.Equal(t, result, balancer.PickResult{})
}

func TestBalancerBuildError(t *testing.T) {
	readySCs := map[balancer.SubConn]base.SubConnInfo{}
	info := base.PickerBuildInfo{ReadySCs: readySCs}
	sp := &spPickerBuilder{}
	picker := sp.Build(info)
	assert.Equal(t, picker, base.NewErrPicker(balancer.ErrNoSubConnAvailable))
}
