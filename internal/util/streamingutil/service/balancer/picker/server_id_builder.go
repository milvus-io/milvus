package picker

import (
	"context"

	"go.uber.org/atomic"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/attributes"
	bbalancer "github.com/milvus-io/milvus/internal/util/streamingutil/service/balancer"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
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
	readyMap := make(map[int64]subConnInfo, len(info.ReadySCs))
	readyList := make([]subConnInfo, 0, len(info.ReadySCs))
	for sc, scInfo := range info.ReadySCs {
		serverID := attributes.GetServerID(scInfo.Address.BalancerAttributes)
		if serverID == nil {
			mlog.Warn(context.TODO(), "no server id found in subConn", mlog.String("address", scInfo.Address.Addr))
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
			mlog.Warn(context.TODO(), "no server id found in subConn", mlog.String("address", scInfo.Address.Addr))
			continue
		}
		info := subConnInfo{
			serverID:    *serverID,
			subConn:     sc,
			subConnInfo: scInfo,
		}
		unReadyMap[*serverID] = info
	}

	var fallback balancer.Picker
	if info.Err != nil {
		fallback = base.NewErrPicker(info.Err)
	} else if len(readyList) == 0 {
		fallback = base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}
	p := &serverIDPicker{
		fallback:           fallback,
		next:               atomic.NewInt64(0),
		readySubConnsMap:   readyMap,
		readySubConsList:   readyList,
		unreadySubConnsMap: unReadyMap,
	}
	return p
}
