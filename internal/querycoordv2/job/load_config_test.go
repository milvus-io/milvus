//go:build test

package job

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

// These two tests pin the reconciliation the load-placement seam upstream of
// them depends on. The seam's whole job is to hand this function the second
// shape rather than the first, so if the reconciliation ever stopped
// distinguishing them the seam would go quietly ineffective.

func replicaIn(id int64, collectionID int64, rgName string) *meta.Replica {
	return meta.NewReplica(&querypb.Replica{
		ID:            id,
		CollectionID:  collectionID,
		ResourceGroup: rgName,
	})
}

// A request that asks for a replica in rg_1 and leaves rg_0 at zero does not
// merely drop rg_0's replica: it reuses that very replica for rg_1. The
// resulting record says the replica lives in rg_1 and never mentions rg_0,
// which is why the placement has to be completed before the record is built
// and cannot be repaired afterwards.
func TestGenerateReplicasTransfersWhenAResourceGroupIsLeftAtZero(t *testing.T) {
	req := &AlterLoadConfigRequest{
		Current: CurrentLoadConfig{
			Replicas: map[int64]*meta.Replica{1: replicaIn(1, 7, "rg_0")},
		},
		Expected: ExpectedLoadConfig{
			ExpectedReplicaNumber: map[string]int{"rg_1": 1},
		},
	}

	got, err := req.generateReplicas(context.Background())
	require.NoError(t, err)

	require.Len(t, got, 1)
	assert.Equal(t, int64(1), got[0].GetReplicaId(),
		"the existing replica is reused, not left alone")
	assert.Equal(t, "rg_1", got[0].GetResourceGroupName(),
		"and it is moved out of the resource group the request left at zero")
}

// The cumulative shape the seam produces: rg_0 keeps the replica it holds and
// rg_1 gets a newly allocated one, so the record describes a placement that
// grew rather than one that moved.
func TestGenerateReplicasAddsWhenEveryResourceGroupIsNamed(t *testing.T) {
	m := meta.NewMeta(func() (int64, error) { return 99, nil }, nil, session.NewNodeManager())
	req := &AlterLoadConfigRequest{
		Meta: m,
		Current: CurrentLoadConfig{
			Replicas: map[int64]*meta.Replica{1: replicaIn(1, 7, "rg_0")},
		},
		Expected: ExpectedLoadConfig{
			ExpectedReplicaNumber: map[string]int{"rg_0": 1, "rg_1": 1},
		},
	}

	got, err := req.generateReplicas(context.Background())
	require.NoError(t, err)

	require.Len(t, got, 2)
	placement := make(map[int64]string, len(got))
	for _, replica := range got {
		placement[replica.GetReplicaId()] = replica.GetResourceGroupName()
	}
	assert.Equal(t, "rg_0", placement[1],
		"the replica that is already serving stays where it is")
	assert.Equal(t, "rg_1", placement[99],
		"and the newly named resource group gets a freshly allocated replica")
}
