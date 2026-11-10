package milvus

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestLegacyCoordinatorRolesOf(t *testing.T) {
	owners := map[string]int64{
		typeutil.RootCoordRole: 7,
		typeutil.DataCoordRole: 9, // owned by another server, e.g. a coordinator that is still alive
	}
	lookup := func(role string) (int64, error) {
		id, ok := owners[role]
		if !ok {
			return 0, errors.New("not found value")
		}
		return id, nil
	}

	// Only the keys registered by the exited server are cleaned.
	assert.Equal(t, []string{typeutil.RootCoordRole}, legacyCoordinatorRolesOf(7, lookup))
	assert.Empty(t, legacyCoordinatorRolesOf(8, lookup))

	owners[typeutil.DataCoordRole] = 7
	owners[typeutil.QueryCoordRole] = 7
	assert.Equal(t,
		[]string{typeutil.RootCoordRole, typeutil.DataCoordRole, typeutil.QueryCoordRole},
		legacyCoordinatorRolesOf(7, lookup))
}
