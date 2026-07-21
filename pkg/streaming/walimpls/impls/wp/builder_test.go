package wp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zilliztech/woodpecker/common/config"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestSetCustomWpConfigBatchParams(t *testing.T) {
	params := paramtable.Get()
	entriesKey := params.WoodpeckerCfg.AppendMaxBatchEntries.Key
	bytesKey := params.WoodpeckerCfg.AppendMaxBatchBytes.Key

	setup := func(t *testing.T, entries, bytes string) *config.Configuration {
		require.NoError(t, params.Save(entriesKey, entries))
		require.NoError(t, params.Save(bytesKey, bytes))
		t.Cleanup(func() {
			params.Reset(entriesKey)
			params.Reset(bytesKey)
		})
		wpConfig, err := config.NewConfiguration()
		require.NoError(t, err)
		return wpConfig
	}

	t.Run("MilvusDefaults", func(t *testing.T) {
		wpConfig, err := config.NewConfiguration()
		require.NoError(t, err)
		require.NoError(t, setCustomWpConfig(wpConfig, &params.WoodpeckerCfg))
		assert.Equal(t, 1000, wpConfig.Woodpecker.Client.SegmentAppend.MaxBatchEntries)
		assert.Equal(t, config.NewByteSize(2000000), wpConfig.Woodpecker.Client.SegmentAppend.MaxBatchBytes)
	})

	t.Run("ValidValues", func(t *testing.T) {
		wpConfig := setup(t, "500", "1m")
		require.NoError(t, setCustomWpConfig(wpConfig, &params.WoodpeckerCfg))
		assert.Equal(t, 500, wpConfig.Woodpecker.Client.SegmentAppend.MaxBatchEntries)
		assert.Equal(t, config.NewByteSize(1024*1024), wpConfig.Woodpecker.Client.SegmentAppend.MaxBatchBytes)
	})

	t.Run("ExplicitZeroBytesMeansNoByteLimit", func(t *testing.T) {
		wpConfig := setup(t, "1000", "0")
		require.NoError(t, setCustomWpConfig(wpConfig, &params.WoodpeckerCfg))
		assert.Equal(t, config.NewByteSize(0), wpConfig.Woodpecker.Client.SegmentAppend.MaxBatchBytes)
	})

	t.Run("InvalidValuesKeepWoodpeckerDefaults", func(t *testing.T) {
		wpConfig := setup(t, "abc", "1,000")
		// seed sentinels to prove the invalid branches leave the fields untouched
		wpConfig.Woodpecker.Client.SegmentAppend.MaxBatchEntries = 777
		wpConfig.Woodpecker.Client.SegmentAppend.MaxBatchBytes = config.NewByteSize(888)
		require.NoError(t, setCustomWpConfig(wpConfig, &params.WoodpeckerCfg))
		assert.Equal(t, 777, wpConfig.Woodpecker.Client.SegmentAppend.MaxBatchEntries)
		assert.Equal(t, config.NewByteSize(888), wpConfig.Woodpecker.Client.SegmentAppend.MaxBatchBytes)
	})

	t.Run("ZeroEntriesKeepsWoodpeckerDefault", func(t *testing.T) {
		wpConfig := setup(t, "0", "2000000")
		require.NoError(t, setCustomWpConfig(wpConfig, &params.WoodpeckerCfg))
		assert.Equal(t, 1000, wpConfig.Woodpecker.Client.SegmentAppend.MaxBatchEntries)
	})
}
