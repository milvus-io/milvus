package wp

import (
	"strings"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zilliztech/woodpecker/common/config"

	pkgconfig "github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestQuorumBufferPoolValidationLogsRedactConfig(t *testing.T) {
	for _, phase := range []string{"startup", "refresh"} {
		t.Run(phase, func(t *testing.T) {
			base := paramtable.NewBaseTable(paramtable.SkipRemote(true), paramtable.SkipEnv(true))
			t.Cleanup(base.Manager().Close)
			var cfg paramtable.WoodpeckerConfig
			cfg.Init(base)
			wpConfig, err := config.NewConfiguration()
			assert.NoError(t, err)
			raw := `[{"name":"private-pool-canary","seeds":["private-seed-canary.invalid:1234"],"bad":!}]`
			sink := mlog.CaptureGlobalLogs(t, &mlog.Config{Level: "debug"})
			if phase == "startup" {
				require.NoError(t, base.Save(cfg.QuorumBufferPools.Key, raw))
				setQuorumConfig(wpConfig, &cfg)
				assert.Contains(t, sink.String(), "invalid quorum JSON config at startup")
			} else {
				setQuorumConfig(wpConfig, &cfg)
				require.NoError(t, base.Save(cfg.QuorumBufferPools.Key, raw))
				base.Manager().Dispatcher.Dispatch(&pkgconfig.Event{
					Key:       strings.ToLower(cfg.QuorumBufferPools.Key),
					EventType: pkgconfig.UpdateType,
					Value:     raw,
				})
				assert.Contains(t, sink.String(), "param change callback failed")
			}
			assert.Equal(t, raw, cfg.QuorumBufferPools.GetValue(), "validation still leaves the configured value intact")
			assert.NotContains(t, sink.String(), "private-pool-canary")
			assert.NotContains(t, sink.String(), "private-seed-canary")
		})
	}
}

func TestQuorumCustomPlacementRefreshLogsOmitPayload(t *testing.T) {
	for _, source := range []string{"EtcdSource", "FileSource"} {
		for _, valid := range []bool{true, false} {
			name := source + "/invalid"
			if valid {
				name = source + "/valid"
			}
			t.Run(name, func(t *testing.T) {
				base := paramtable.NewBaseTable(paramtable.SkipRemote(true), paramtable.SkipEnv(true))
				t.Cleanup(base.Manager().Close)
				oldValue := `[{"name":"old-name-canary"}]`
				key := "woodpecker.client.quorum.quorumSelectStrategy.customPlacement"
				require.NoError(t, base.Save(key, oldValue))
				var cfg paramtable.WoodpeckerConfig
				cfg.Init(base)
				require.Equal(t, key, cfg.QuorumCustomPlacement.Key)
				require.False(t, base.Manager().IsSensitive(key))
				wpConfig, err := config.NewConfiguration()
				require.NoError(t, err)
				setQuorumConfig(wpConfig, &cfg)
				value := `[{"name":"new-name-canary","ignored":"value-canary"}]`
				if !valid {
					value = `[{"name":"new-name-canary","ignored":"value-canary","bad":!}]`
				}
				sink := mlog.CaptureGlobalLogs(t, &mlog.Config{Level: "debug"})
				base.Manager().SetConfig(key, value)
				base.Manager().Dispatcher.Dispatch(&pkgconfig.Event{
					Key:         strings.ToLower(key),
					EventType:   pkgconfig.UpdateType,
					EventSource: source,
					Value:       value,
				})
				assert.Equal(t, value, cfg.QuorumCustomPlacement.GetValue())
				if valid {
					assert.Contains(t, sink.String(), "param value changed")
				} else {
					assert.Contains(t, sink.String(), "param change callback failed")
				}
				for _, canary := range []string{"old-name-canary", "new-name-canary", "value-canary"} {
					if source == "EtcdSource" {
						assert.NotContains(t, sink.String(), canary)
					} else {
						assert.Contains(t, sink.String(), canary, "file-backed public diagnostics remain available")
					}
				}
			})
		}
	}
}

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

	// maxBatchEntries=1 is the documented escape hatch that disables batching
	// (woodpecker takes the single-op path when maxBatchEntries <= 1); pin it so
	// a future guard refactor (e.g. v > 1) can't silently break it.
	t.Run("EntriesOneDisablesBatching", func(t *testing.T) {
		wpConfig := setup(t, "1", "2000000")
		require.NoError(t, setCustomWpConfig(wpConfig, &params.WoodpeckerCfg))
		assert.Equal(t, 1, wpConfig.Woodpecker.Client.SegmentAppend.MaxBatchEntries)
	})
}

func TestSetCustomWpConfigDirectReadParams(t *testing.T) {
	params := paramtable.Get()
	enabledKey := params.WoodpeckerCfg.DirectReadEnabled.Key
	batchSizeKey := params.WoodpeckerCfg.DirectReadMaxBatchSize.Key
	fetchThreadsKey := params.WoodpeckerCfg.DirectReadMaxFetchThreads.Key

	setup := func(t *testing.T, enabled, batchSize, fetchThreads string) *config.Configuration {
		require.NoError(t, params.Save(enabledKey, enabled))
		require.NoError(t, params.Save(batchSizeKey, batchSize))
		require.NoError(t, params.Save(fetchThreadsKey, fetchThreads))
		t.Cleanup(func() {
			params.Reset(enabledKey)
			params.Reset(batchSizeKey)
			params.Reset(fetchThreadsKey)
		})
		wpConfig, err := config.NewConfiguration()
		require.NoError(t, err)
		return wpConfig
	}

	t.Run("MilvusDefaults", func(t *testing.T) {
		wpConfig, err := config.NewConfiguration()
		require.NoError(t, err)
		require.NoError(t, setCustomWpConfig(wpConfig, &params.WoodpeckerCfg))
		assert.True(t, wpConfig.Woodpecker.Client.DirectRead.Enabled)
		assert.Equal(t, config.NewByteSize(16*1024*1024), wpConfig.Woodpecker.Client.DirectRead.MaxBatchSize)
		assert.Equal(t, 4, wpConfig.Woodpecker.Client.DirectRead.MaxFetchThreads)
	})

	t.Run("CustomValues", func(t *testing.T) {
		wpConfig := setup(t, "false", "32M", "8")
		require.NoError(t, setCustomWpConfig(wpConfig, &params.WoodpeckerCfg))
		assert.False(t, wpConfig.Woodpecker.Client.DirectRead.Enabled)
		assert.Equal(t, config.NewByteSize(32*1024*1024), wpConfig.Woodpecker.Client.DirectRead.MaxBatchSize)
		assert.Equal(t, 8, wpConfig.Woodpecker.Client.DirectRead.MaxFetchThreads)
	})

	for _, tc := range []struct {
		name         string
		batchSize    string
		fetchThreads string
	}{
		{name: "MalformedValues", batchSize: "bad-size", fetchThreads: "bad-threads"},
		{name: "ZeroValues", batchSize: "0", fetchThreads: "0"},
		{name: "NegativeValues", batchSize: "-1M", fetchThreads: "-1"},
	} {
		t.Run(tc.name+"KeepValidatedValues", func(t *testing.T) {
			wpConfig := setup(t, "true", tc.batchSize, tc.fetchThreads)
			// Seed non-default sentinels to prove invalid Milvus values do not
			// overwrite the already-validated Woodpecker configuration.
			wpConfig.Woodpecker.Client.DirectRead.MaxBatchSize = config.NewByteSize(24 * 1024 * 1024)
			wpConfig.Woodpecker.Client.DirectRead.MaxFetchThreads = 6
			require.NoError(t, setCustomWpConfig(wpConfig, &params.WoodpeckerCfg))
			assert.True(t, wpConfig.Woodpecker.Client.DirectRead.Enabled)
			assert.Equal(t, config.NewByteSize(24*1024*1024), wpConfig.Woodpecker.Client.DirectRead.MaxBatchSize)
			assert.Equal(t, 6, wpConfig.Woodpecker.Client.DirectRead.MaxFetchThreads)
		})
	}
}

// TestSetCustomWpConfigStorageType guards the value the WAL switch depends on: the
// configured storage type has to reach the woodpecker configuration, because it is what
// selects the service client over the embedded one.
func TestSetCustomWpConfigStorageType(t *testing.T) {
	params := paramtable.Get()
	key := params.WoodpeckerCfg.StorageType.Key
	t.Cleanup(func() { params.Reset(key) })

	for _, tc := range []struct {
		storageType string
		isService   bool
	}{
		{storageType: "service", isService: true},
		{storageType: "minio", isService: false},
		{storageType: "local", isService: false},
	} {
		t.Run(tc.storageType, func(t *testing.T) {
			require.NoError(t, params.Save(key, tc.storageType))
			wpConfig, err := config.NewConfiguration()
			require.NoError(t, err)
			require.NoError(t, setCustomWpConfig(wpConfig, &params.WoodpeckerCfg))
			assert.Equal(t, tc.storageType, wpConfig.Woodpecker.Storage.Type)
			assert.Equal(t, tc.isService, wpConfig.Woodpecker.Storage.IsStorageService())
		})
	}
}

// TestGetWpConfigFailsClosedWhenRefreshFails pins that a failed linearizable refresh aborts
// the build instead of falling back to the last polled snapshot: that snapshot may be the
// stale one the refresh exists to replace, and a build from it could select the embedded
// client for the life of the process. The opener is only cached on success, so the failure
// is retried on the next WAL open.
func TestGetWpConfigFailsClosedWhenRefreshFails(t *testing.T) {
	refreshErr := errors.New("etcd leader changed")
	mocker := mockey.Mock((*paramtable.BaseTable).RefreshRemoteConfigsLinearizable).Return(false, refreshErr).Build()
	defer mocker.UnPatch()

	_, err := (&builderImpl{}).getWpConfig()
	require.Error(t, err)
	assert.ErrorIs(t, err, refreshErr)

	// Once the refresh succeeds again the same build goes through: the failure is
	// retryable rather than a permanent verdict.
	mocker.UnPatch()
	_, err = (&builderImpl{}).getWpConfig()
	require.NoError(t, err)
}
