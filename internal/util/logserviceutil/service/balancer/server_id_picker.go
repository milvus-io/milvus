package balancer

import (
	"fmt"
	"strconv"

	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/util/logserviceutil/service/contextutil"
	"github.com/milvus-io/milvus/pkg/util/interceptor"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var _ balancer.Picker = &serverIDPicker{}

type subConnInfo struct {
	serverID    int64
	subConn     balancer.SubConn
	subConnInfo base.SubConnInfo
}

// serverIDPicker is a force address picker.
type serverIDPicker struct {
	next        *atomic.Int64         // index of the next subConn to pick.
	subConsList []subConnInfo         // resolver ordered list.
	subConnsMap map[int64]subConnInfo // map the server id to subConnInfo.
}

// Pick returns the connection to use for this RPC and related information.
//
// Pick should not block.  If the balancer needs to do I/O or any blocking
// or time-consuming work to service this call, it should return
// ErrNoSubConnAvailable, and the Pick call will be repeated by gRPC when
// the Picker is updated (using ClientConn.UpdateState).
//
// If an error is returned:
//
//   - If the error is ErrNoSubConnAvailable, gRPC will block until a new
//     Picker is provided by the balancer (using ClientConn.UpdateState).
//
//   - If the error is a status error (implemented by the grpc/status
//     package), gRPC will terminate the RPC with the code and message
//     provided.
//
//   - For all other errors, wait for ready RPCs will wait, but non-wait for
//     ready RPCs will be terminated with this error's Error() string and
//     status code Unavailable.
func (p *serverIDPicker) Pick(pickInfo balancer.PickInfo) (balancer.PickResult, error) {
	var conn *subConnInfo
	var err error

	serverID, ok := contextutil.GetPickServerID(pickInfo.Ctx)
	if !ok {
		// round robin should be blocked.
		if conn, err = p.roundRobin(pickInfo); err != nil {
			return balancer.PickResult{}, err
		}
	} else {
		// force address should not be blocked.
		if conn, err = p.useGivenAddr(pickInfo, serverID); err != nil {
			return balancer.PickResult{}, err
		}
	}

	return balancer.PickResult{
		SubConn: conn.subConn,
		Done:    nil, // TODO: add a done function to handle the rpc finished.
		// Add the server id to the metadata.
		// See interceptor.ServerIDValidationUnaryServerInterceptor
		Metadata: metadata.Pairs(
			interceptor.ServerIDKey,
			strconv.FormatInt(conn.serverID, 10),
		),
	}, nil
}

// roundRobin returns the next subConn in round robin.
func (p *serverIDPicker) roundRobin(pickInfo balancer.PickInfo) (*subConnInfo, error) {
	if len(p.subConsList) == 0 {
		return nil, balancer.ErrNoSubConnAvailable
	}
	subConnsLen := len(p.subConsList)
	nextIndex := int(p.next.Inc()) % subConnsLen
	return &p.subConsList[nextIndex], nil
}

// useGivenAddr returns whether given subConn.
func (p *serverIDPicker) useGivenAddr(pickInfo balancer.PickInfo, serverID int64) (*subConnInfo, error) {
	sc, ok := p.subConnsMap[serverID]
	if !ok {
		// If the given address is not in the subConnsMap, return a unknown error to user to avoid block rpc.
		// FailPrecondition will be converted to Internal by grpc framework in function `IsRestrictedControlPlaneCode`.
		return nil, status.New(
			codes.Unknown,
			fmt.Sprintf("no subConn, method: %s, serverID: %d", pickInfo.FullMethodName, serverID),
		).Err()
	}
	return &sc, nil
}
