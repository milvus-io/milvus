//go:build test
// +build test

package coordclient

import (
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

// ResetRegistration resets the global local client to initial state.
// This function is only used in test.
func ResetRegistration() {
	glocalClient = &localClient{
		queryCoordClient: syncutil.NewFuture[types.QueryCoordClient](),
		dataCoordClient:  syncutil.NewFuture[types.DataCoordClient](),
		rootCoordClient:  syncutil.NewFuture[types.RootCoordClient](),
	}
}

// ResetQueryCoordRegistration resets the query coord client to initial state.
func ResetQueryCoordRegistration() {
	glocalClient.queryCoordClient = syncutil.NewFuture[types.QueryCoordClient]()
}

func ResetRootCoordRegistration() {
	glocalClient.rootCoordClient = syncutil.NewFuture[types.RootCoordClient]()
}

func ResetDataCoordRegistration() {
	glocalClient.dataCoordClient = syncutil.NewFuture[types.DataCoordClient]()
}
