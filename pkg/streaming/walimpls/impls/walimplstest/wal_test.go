package walimplstest

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
)

func TestCanInjectRandomError(t *testing.T) {
	require.False(t, canInjectRandomError(message.MessageTypeTimeTick))
	require.False(t, canInjectRandomError(message.MessageTypeRecoveryBarrier))
	require.True(t, canInjectRandomError(message.MessageTypeInsert))
}

func TestWALImplsTest(t *testing.T) {
	enableFenceError.Store(false)
	defer enableFenceError.Store(true)
	walimpls.NewWALImplsTestFramework(t, 100, &openerBuilder{}).Run()
}
