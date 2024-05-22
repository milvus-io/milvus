package balancer

import (
	"github.com/milvus-io/milvus/internal/util/logserviceutil/service/attributes"
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

const (
	ServerIDPickerBalancerName = "server_id_picker"
)

func init() {
	balancer.Register(base.NewBalancerBuilder(
		ServerIDPickerBalancerName,
		&serverIDPickerBuilder{},
		base.Config{HealthCheck: true}),
	)
}

// serverIDPickerBuilder is a bkproxy picker builder.
type serverIDPickerBuilder struct{}

// Build returns a picker that will be used by gRPC to pick a SubConn.
func (b *serverIDPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	if len(info.ReadySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}
	m := make(map[int64]subConnInfo, len(info.ReadySCs))
	list := make([]subConnInfo, 0, len(info.ReadySCs))
	for sc, scInfo := range info.ReadySCs {
		serverID := attributes.GetServerID(scInfo.Address.BalancerAttributes)
		if serverID == nil {
			log.Warn("no server id found in subConn", zap.String("address", scInfo.Address.Addr))
			continue
		}

		info := subConnInfo{
			serverID:    *serverID,
			subConn:     sc,
			subConnInfo: scInfo,
		}
		m[*serverID] = info
		list = append(list, info)
	}

	if len(list) == 0 {
		log.Warn("no subConn available after serverID filtering")
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}
	p := &serverIDPicker{
		next:        atomic.NewInt64(0),
		subConnsMap: m,
		subConsList: list,
	}
	return p
}
