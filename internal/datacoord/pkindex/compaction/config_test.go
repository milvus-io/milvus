package compaction

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogCorrectionsOncePerSet(t *testing.T) {
	ctx := context.Background()
	bad := []correction{{field: "L0Trigger", from: "1", to: "2"}}
	logCorrections(ctx, nil)
	require.True(t, logCorrections(ctx, bad))
	require.Equal(t, "L0Trigger:1->2", lastCorrections)
	require.False(t, logCorrections(ctx, bad))
	require.True(t, logCorrections(ctx, []correction{{field: "L0Trigger", from: "0", to: "2"}}))
	require.False(t, logCorrections(ctx, nil))
	require.Equal(t, "", lastCorrections)
}
