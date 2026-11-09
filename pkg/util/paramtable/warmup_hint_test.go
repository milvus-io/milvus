package paramtable

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadCollectionWarmupHintGateDefaultsClosed(t *testing.T) {
	base := NewBaseTable(Files([]string{}), SkipRemote(true), SkipEnv(true))
	var cfg queryCoordConfig
	cfg.init(base)
	require.Equal(t, "queryCoord.enableLoadCollectionWarmupHint", cfg.EnableLoadCollectionWarmupHint.Key)
	require.False(t, cfg.EnableLoadCollectionWarmupHint.GetAsBool())
	require.NoError(t, base.Save(cfg.EnableLoadCollectionWarmupHint.Key, "true"))
	require.True(t, cfg.EnableLoadCollectionWarmupHint.GetAsBool())
}
