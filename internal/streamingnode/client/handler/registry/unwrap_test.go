package registry

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
)

func TestLocalWALUnwrapPreservesOptionalCapabilities(t *testing.T) {
	raw := &struct{ wal.WAL }{}
	wrapped := localWAL{WAL: localWAL{WAL: raw}}
	require.Same(t, raw, wal.Unwrap(wrapped))
}
