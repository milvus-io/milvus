package picker

import (
	"strconv"

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/pkg/v2/util/interceptor"
)

var _ balancer.Picker = &serverIDPicker{}

var ErrSubConnNotExist = status.New(codes.Unavailable, "subConn not exist").Err()

type subConnInfo struct {
	serverID    int64
	subConn     balancer.SubConn
	subConnInfo base.SubConnInfo
}

// serverIDPicker is a force address picker.
type serverIDPicker struct {
	next               *atomic.Int64         // index of the next subConn to pick.
	readySubConsList   []subConnInfo         // ready resolver ordered list.
	readySubConnsMap   map[int64]subConnInfo // map the server id to ready subConnInfo.
	unreadySubConnsMap map[int64]subConnInfo // map the server id to unready subConnInfo.
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
		if conn, err = p.roundRobin(); err != nil {
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
func (p *serverIDPicker) roundRobin() (*subConnInfo, error) {
	if len(p.readySubConsList) == 0 {
		return nil, balancer.ErrNoSubConnAvailable
	}
	subConnsLen := len(p.readySubConsList)
	nextIndex := int(p.next.Inc()) % subConnsLen
	return &p.readySubConsList[nextIndex], nil
}

// useGivenAddr returns whether given subConn.
func (p *serverIDPicker) useGivenAddr(_ balancer.PickInfo, serverID int64) (*subConnInfo, error) {
	sc, ok := p.readySubConnsMap[serverID]
	if ok {
		return &sc, nil
	}

	// subConn is not ready, return ErrNoSubConnAvailable to wait the connection ready.
	if _, ok := p.unreadySubConnsMap[serverID]; ok {
		return nil, balancer.ErrNoSubConnAvailable
	}

	// If the given address is not in the readySubConnsMap or unreadySubConnsMap, return a unavailable error to user to avoid block rpc.
	// FailPrecondition will be converted to Internal by grpc framework in function `IsRestrictedControlPlaneCode`.
	// Use Unavailable here.
	// Unavailable code is retried in many cases, so it's better to be used here to avoid when Subconn is not ready scene.
	return nil, ErrSubConnNotExist
}

// IsErrSubConnNoExist checks whether the error is ErrNoSubConnForPick.
func IsErrSubConnNoExist(err error) bool {
	if errors.Is(err, ErrSubConnNotExist) {
		return true
	}
	if se, ok := err.(interface {
		GRPCStatus() *status.Status
	}); ok {
		return errors.Is(se.GRPCStatus().Err(), ErrSubConnNotExist)
	}
	return false
}
