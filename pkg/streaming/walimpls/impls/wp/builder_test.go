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

// TestSetCustomWpConfigCompactionParams covers the six compaction and flush settings, which
// between them carry both unit conversions in this mapping -- a duration read in seconds and a
// byte size -- and three values whose bad forms switch off the bound they configure rather than
// failing loudly.
func TestSetCustomWpConfigCompactionParams(t *testing.T) {
	params := paramtable.Get()
	attemptKey := params.WoodpeckerCfg.AuditorCompactionAttemptTimeout.Key
	budgetKey := params.WoodpeckerCfg.AuditorCompactionPassBudget.Key
	timeoutKey := params.WoodpeckerCfg.CompactionTimeout.Key
	memoryKey := params.WoodpeckerCfg.CompactionMaxInflightMemory.Key
	watermarkKey := params.WoodpeckerCfg.CompactionMemoryHighWatermark.Key
	workersKey := params.WoodpeckerCfg.SyncSchedulerMaxWorkers.Key

	setup := func(t *testing.T, attempt, budget, timeout, memory, watermark, workers string) *config.Configuration {
		for k, v := range map[string]string{
			attemptKey: attempt, budgetKey: budget, timeoutKey: timeout,
			memoryKey: memory, watermarkKey: watermark, workersKey: workers,
		} {
			require.NoError(t, params.Save(k, v))
		}
		t.Cleanup(func() {
			for _, k := range []string{attemptKey, budgetKey, timeoutKey, memoryKey, watermarkKey, workersKey} {
				params.Reset(k)
			}
		})
		wpConfig, err := config.NewConfiguration()
		require.NoError(t, err)
		return wpConfig
	}

	t.Run("MilvusDefaults", func(t *testing.T) {
		wpConfig, err := config.NewConfiguration()
		require.NoError(t, err)
		require.NoError(t, setCustomWpConfig(wpConfig, &params.WoodpeckerCfg))
		assert.Equal(t, 330, wpConfig.Woodpecker.Client.Auditor.CompactionAttemptTimeout.Seconds())
		assert.Equal(t, 60, wpConfig.Woodpecker.Client.Auditor.CompactionPassBudget.Seconds())
		assert.Equal(t, 300, wpConfig.Woodpecker.Logstore.SegmentCompactionPolicy.Timeout.Seconds())
		assert.Equal(t, int64(1000000000), wpConfig.Woodpecker.Logstore.SegmentCompactionPolicy.MaxInflightMemory.Int64())
		assert.InDelta(t, 0.7, wpConfig.Woodpecker.Logstore.SegmentCompactionPolicy.MemoryHighWatermark, 1e-9)
		assert.Equal(t, 32, wpConfig.Woodpecker.Logstore.SyncScheduler.MaxWorkers)
	})

	// Distinct values in every field, so a mapping written into the wrong field cannot pass.
	t.Run("CustomValues", func(t *testing.T) {
		wpConfig := setup(t, "90s", "45s", "600s", "2G", "0.85", "12")
		require.NoError(t, setCustomWpConfig(wpConfig, &params.WoodpeckerCfg))
		assert.Equal(t, 90, wpConfig.Woodpecker.Client.Auditor.CompactionAttemptTimeout.Seconds())
		assert.Equal(t, 45, wpConfig.Woodpecker.Client.Auditor.CompactionPassBudget.Seconds())
		assert.Equal(t, 600, wpConfig.Woodpecker.Logstore.SegmentCompactionPolicy.Timeout.Seconds())
		assert.Equal(t, int64(2*1024*1024*1024), wpConfig.Woodpecker.Logstore.SegmentCompactionPolicy.MaxInflightMemory.Int64())
		assert.InDelta(t, 0.85, wpConfig.Woodpecker.Logstore.SegmentCompactionPolicy.MemoryHighWatermark, 1e-9)
		assert.Equal(t, 12, wpConfig.Woodpecker.Logstore.SyncScheduler.MaxWorkers)
	})

	// Values Woodpecker's Validate() would refuse, which it cannot here because it runs
	// before Milvus applies its overrides. Each case names what the field must end up as:
	// the sentinel when the override is skipped, a real value when it is taken.
	//
	// The two parsers differ, and the table pins that. GetAsSize and GetAsFloat return 0 on
	// malformed input, so a typo silently switches off the bound. GetAsDurationByParse falls
	// back to the item's own DefaultValue instead, so a malformed duration is already safe --
	// the only way Timeout reaches 0 is a value that parses and then truncates, which is what
	// SubSecondTimeout covers.
	const (
		sentinelTimeout   = 123
		sentinelMemory    = int64(777)
		sentinelWatermark = 0.42
	)
	for _, tc := range []struct {
		name            string
		timeout         string
		memory          string
		watermark       string
		expectTimeout   int
		expectMemory    int64
		expectWatermark float64
	}{
		{
			name: "MalformedValues", timeout: "bad-duration", memory: "bad-size", watermark: "bad-float",
			// A malformed duration falls back to this item's 300s default, which is valid.
			expectTimeout: 300, expectMemory: sentinelMemory, expectWatermark: sentinelWatermark,
		},
		{
			name: "ZeroValues", timeout: "0s", memory: "0", watermark: "0",
			expectTimeout: sentinelTimeout, expectMemory: sentinelMemory, expectWatermark: sentinelWatermark,
		},
		{
			name: "NegativeValues", timeout: "-1s", memory: "-1M", watermark: "-0.5",
			expectTimeout: sentinelTimeout, expectMemory: sentinelMemory, expectWatermark: sentinelWatermark,
		},
		{
			// Parses cleanly, then int(0.5) truncates to 0 -- every compaction would expire at once.
			name: "SubSecondTimeout", timeout: "500ms", memory: "1G", watermark: "0.7",
			expectTimeout: sentinelTimeout, expectMemory: 1024 * 1024 * 1024, expectWatermark: 0.7,
		},
		{
			// The key is a fraction, but its name invites a percentage; 70 would put the
			// pressure gate 70x above the node's limit, so it could never fire.
			name: "WatermarkAsPercentage", timeout: "300s", memory: "1G", watermark: "70",
			expectTimeout: 300, expectMemory: 1024 * 1024 * 1024, expectWatermark: sentinelWatermark,
		},
	} {
		t.Run(tc.name+"KeepValidatedValues", func(t *testing.T) {
			wpConfig := setup(t, "90s", "45s", tc.timeout, tc.memory, tc.watermark, "12")
			// Sentinels distinct from both the Milvus and the Woodpecker defaults, so a value
			// that survives proves the override was skipped rather than coincidentally equal.
			wpConfig.Woodpecker.Logstore.SegmentCompactionPolicy.Timeout = config.NewDurationSecondsFromInt(sentinelTimeout)
			wpConfig.Woodpecker.Logstore.SegmentCompactionPolicy.MaxInflightMemory = config.NewByteSize(sentinelMemory)
			wpConfig.Woodpecker.Logstore.SegmentCompactionPolicy.MemoryHighWatermark = sentinelWatermark
			require.NoError(t, setCustomWpConfig(wpConfig, &params.WoodpeckerCfg))

			policy := &wpConfig.Woodpecker.Logstore.SegmentCompactionPolicy
			assert.Equal(t, tc.expectTimeout, policy.Timeout.Seconds())
			assert.Equal(t, tc.expectMemory, policy.MaxInflightMemory.Int64())
			assert.InDelta(t, tc.expectWatermark, policy.MemoryHighWatermark, 1e-9)
			// The unguarded three are unaffected by a neighbour's bad value.
			assert.Equal(t, 90, wpConfig.Woodpecker.Client.Auditor.CompactionAttemptTimeout.Seconds())
			assert.Equal(t, 45, wpConfig.Woodpecker.Client.Auditor.CompactionPassBudget.Seconds())
			assert.Equal(t, 12, wpConfig.Woodpecker.Logstore.SyncScheduler.MaxWorkers)
		})
	}
}
