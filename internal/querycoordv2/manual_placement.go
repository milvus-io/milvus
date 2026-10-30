package querycoordv2

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/views/coord/balancer"
	qvresource "github.com/milvus-io/milvus/internal/views/coord/resource"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func (s *Server) manualMoveSegments(ctx context.Context, collectionID, source int64, destinations, segments []int64) error {
	manager, ok := qvresource.Resource().Balancer.(balancer.Management)
	if !ok {
		return merr.Wrap(merr.ErrServiceUnimplemented, "QueryView balancer does not support manual placement")
	}
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.SegmentTaskTimeout.GetAsDuration(time.Millisecond))
	defer cancel()
	_, err := manager.MoveSegments(ctx, collectionID, source, destinations, segments)
	return err
}
