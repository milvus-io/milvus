package querycoord

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/stretchr/testify/assert"
)

func TestAddNode(t *testing.T) {
	defer removeAllSession()

	ctx := context.Background()
	coord, err := startQueryCoord(ctx)
	assert.NoError(t, err)
	defer coord.Stop()

	node1, err := startQueryNodeServer(ctx)
	assert.NoError(t, err)
	defer node1.stop()
	node2, err := startQueryNodeServer(ctx)
	assert.NoError(t, err)
	defer node2.stop()
	waitQueryNodeOnline(coord.cluster, node1.queryNodeID)
	waitQueryNodeOnline(coord.cluster, node2.queryNodeID)

	loadCollectionReq := &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadCollection,
		},
		CollectionID:  defaultCollectionID,
		Schema:        genDefaultCollectionSchema(false),
		ReplicaNumber: 1,
	}
	status, err := coord.LoadCollection(ctx, loadCollectionReq)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	waitLoadCollectionDone(ctx, coord, defaultCollectionID)

	plans, err := coord.groupBalancer.AddNode(node1.queryNodeID)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(plans))

	plans, err = coord.groupBalancer.AddNode(node2.queryNodeID)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(plans))

	newNodeID := node2.queryNodeID + 1
	plans, err = coord.groupBalancer.AddNode(newNodeID)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(plans))
}
