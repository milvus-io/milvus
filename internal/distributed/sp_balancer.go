package distributed

import (
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/status"
)

type AddressKey struct{}

// Name is the name of specific balancer.
const BalancerName = "specific"

var logger = grpclog.Component("specific")

// newBuilder creates a new specific balancer builder.
func newBuilder() balancer.Builder {
	return base.NewBalancerBuilder(BalancerName, &spPickerBuilder{}, base.Config{HealthCheck: true})
}

func init() {
	balancer.Register(newBuilder())
}

type spPickerBuilder struct{}

func (*spPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	logger.Infof("specificPicker: Build called with info: %v", info)
	if len(info.ReadySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}
	return &spPicker{
		subConns: info.ReadySCs,
	}
}

type spPicker struct {
	subConns map[balancer.SubConn]base.SubConnInfo
}

func (p *spPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	if info.Ctx == nil {
		return balancer.PickResult{}, status.Error(codes.InvalidArgument, "pick info context is is")
	}
	v := info.Ctx.Value(AddressKey{})
	target, ok := v.(string)
	if !ok {
		return balancer.PickResult{}, status.Error(codes.InvalidArgument, "no specific address error")
	}
	for sc, sci := range p.subConns {
		if target == sci.Address.Addr {
			return balancer.PickResult{SubConn: sc}, nil
		}
	}
	return balancer.PickResult{}, status.Error(codes.Unavailable, "no available server found")
}
