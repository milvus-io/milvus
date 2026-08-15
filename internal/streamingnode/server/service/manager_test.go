package service

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

func TestValidateAssignRecoveryStorageVersion(t *testing.T) {
	assert.NoError(t, validateAssignRecoveryStorageVersion(types.PChannelInfo{
		Name:                           "rw-v2",
		AccessMode:                     types.AccessModeRW,
		RequiredRecoveryStorageVersion: types.RecoveryStorageVersionV2,
	}))
	assert.Error(t, validateAssignRecoveryStorageVersion(types.PChannelInfo{
		Name:       "rw-legacy",
		AccessMode: types.AccessModeRW,
	}))
	assert.NoError(t, validateAssignRecoveryStorageVersion(types.PChannelInfo{
		Name:       "ro-legacy",
		AccessMode: types.AccessModeRO,
	}))
}
