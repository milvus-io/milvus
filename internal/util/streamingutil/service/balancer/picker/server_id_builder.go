package picker

import (
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/attributes"
	bbalancer "github.com/milvus-io/milvus/internal/util/streamingutil/service/balancer"
	"github.com/milvus-io/milvus/pkg/v2/log"
)

const (
	ServerIDPickerBalancerName = "server_id_picker"
)

func init() {
	balancer.Register(bbalancer.NewBalancerBuilder(
		ServerIDPickerBalancerName,
		&serverIDPickerBuilder{},
		base.Config{HealthCheck: true}),
	)
}

// serverIDPickerBuilder is a bkproxy picker builder.
type serverIDPickerBuilder struct{}

// Build returns a picker that will be used by gRPC to pick a SubConn.
func (b *serverIDPickerBuilder) Build(info bbalancer.PickerBuildInfo) balancer.Picker {
	if len(info.ReadySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}
	readyMap := make(map[int64]subConnInfo, len(info.ReadySCs))
	readyList := make([]subConnInfo, 0, len(info.ReadySCs))
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
		readyMap[*serverID] = info
		readyList = append(readyList, info)
	}
	unReadyMap := make(map[int64]subConnInfo, len(info.UnReadySCs))
	for sc, scInfo := range info.UnReadySCs {
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
		unReadyMap[*serverID] = info
	}

	if len(readyList) == 0 {
		log.Warn("no subConn available after serverID filtering")
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}
	p := &serverIDPicker{
		next:               atomic.NewInt64(0),
		readySubConnsMap:   readyMap,
		readySubConsList:   readyList,
		unreadySubConnsMap: unReadyMap,
	}
	return p
}
