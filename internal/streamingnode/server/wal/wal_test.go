package wal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnwrapReturnsUnderlyingWAL(t *testing.T) {
	raw := &testWAL{}
	wrapped := testWrappedWAL{
		WAL: raw,
		raw: raw,
	}

	require.Same(t, raw, Unwrap(wrapped))
	require.Same(t, raw, Unwrap(raw))
}

type testWAL struct {
	WAL
}

type testWrappedWAL struct {
	WAL
	raw WAL
}

func (w testWrappedWAL) UnwrapWAL() WAL {
	return w.raw
}
