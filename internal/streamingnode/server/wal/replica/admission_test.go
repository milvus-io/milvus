package replica

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDrainWriterAdmission(t *testing.T) {
	var gate Admission
	require.True(t, gate.Enter())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, gate.Drain(ctx), context.Canceled)
	require.False(t, gate.Enter())
	gate.Leave()
	require.NoError(t, gate.Drain(t.Context()))
	require.False(t, gate.Enter())
	var empty Admission
	require.NoError(t, empty.Drain(t.Context()))
}
