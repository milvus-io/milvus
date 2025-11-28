package discoverer

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc/resolver"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/attributes"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// Discoverer is the interface for the discoverer.
// Do not promise
// 1. concurrent safe.
// 2. the version of discovery may be repeated or decreasing. So user should check the version in callback.
type Discoverer interface {
	// Discover watches the service discovery on these goroutine.
	// 1. Call the callback when the discovery is changed, and block until the discovery is canceled or break down.
	// 2. Discover should always send the current state first and then block.
	Discover(ctx context.Context, cb func(VersionedState) error) error
}

// VersionedState is the state with version.
type VersionedState struct {
	Version typeutil.Version
	State   resolver.State
}

func (v VersionedState) String() string {
	as := make([]string, 0, len(v.State.Addresses))
	for _, addr := range v.State.Addresses {
		if nodeID := attributes.GetServerID(addr.Attributes); nodeID != nil {
			as = append(as, fmt.Sprintf("%d@%s", *nodeID, addr.Addr))
			continue
		}
		as = append(as, addr.Addr)
	}
	return fmt.Sprintf("Version: %s, Addrs: %s", v.Version, strings.Join(as, ","))
}
