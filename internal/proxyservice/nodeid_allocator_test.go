package proxyservice

import (
	"testing"

	"github.com/zilliztech/milvus-distributed/internal/log"
	"go.uber.org/zap"
)

func TestNaiveNodeIDAllocator_AllocOne(t *testing.T) {
	allocator := newNodeIDAllocator()

	num := 10
	for i := 0; i < num; i++ {
		nodeID := allocator.AllocOne()
		log.Debug("TestNaiveNodeIDAllocator_AllocOne", zap.Any("node id", nodeID))
	}
}
