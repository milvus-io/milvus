package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDefaultTransferTimestampsDoNotUseWallClock(t *testing.T) {
	require.Zero(t, defaultTransferEpoch())
	require.Zero(t, defaultTransferCommitTs())
	require.Zero(t, defaultTransferCacheExpireTs())
}

func TestValidateTransferOptionsRequiresConfirmForStart(t *testing.T) {
	err := validateTransferOptions(transferOptions{
		TransferID:      "transfer-1",
		TransferEpoch:   1,
		SourceNamespace: "milvus1",
		TargetNamespace: "milvus2",
		CollectionName:  "coll",
		CommitTs:        100,
	})
	require.ErrorContains(t, err, "--confirm is required")
}

func TestValidateTransferOptionsDoesNotRequireConfirmForGet(t *testing.T) {
	err := validateTransferOptions(transferOptions{
		TransferID: "transfer-1",
		GetOnly:    true,
	})
	require.NoError(t, err)
}
