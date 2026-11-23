package recovery

import (
	"math"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	metastore "github.com/milvus-io/milvus/internal/metastore/kv/streamingnode"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestParseSummaryRetentionBytes(t *testing.T) {
	for raw, want := range map[string]uint64{
		"0": 0, "0GB": 0, "42": 42, "1k": 1024, "2KB": 2048,
		"1m": 1 << 20, "2MB": 2 << 20, "1g": 1 << 30, "4GB": 4 << 30,
		"9223372036854775807": math.MaxInt64, "8589934591GB": 8589934591 << 30,
	} {
		t.Run(raw, func(t *testing.T) {
			got, err := parseSummaryRetentionBytes(raw)
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}
	for _, raw := range []string{"", "GB", "-1", "+1", "4GiB", "4.5GB", "4 GB", " 4GB", "4GB ", "1TB", "9223372036854775808", "8589934592GB", "18446744073709551616"} {
		t.Run(raw, func(t *testing.T) {
			_, err := parseSummaryRetentionBytes(raw)
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			require.Equal(t, merr.Code(merr.ErrServiceInternal), merr.Code(err))
			require.ErrorContains(t, err, "streaming.summary.maxRetentionBytes")
		})
	}
}

func TestSummaryManagerRejectsInvalidRetentionConfig(t *testing.T) {
	paramtable.Init()
	param := &paramtable.Get().StreamingCfg.SummaryMaxRetentionBytes
	original := param.SwapTempValue("4GiB")
	t.Cleanup(func() { param.SwapTempValue(original) })
	// Validation precedes all storage/runtime access, so bad configuration
	// cannot start a Manager with a silently weakened retention floor.
	m, err := (&recoveryStorageImpl{}).newSummaryManager(moduleapi.Runtime{})
	require.Nil(t, m)
	require.ErrorContains(t, err, param.Key)
}

func TestRecoveryRejectsInvalidSummaryRetentionBeforeRestore(t *testing.T) {
	paramtable.Init()
	param := &paramtable.Get().StreamingCfg.SummaryMaxRetentionBytes
	original := param.SwapTempValue("8589934592GB")
	t.Cleanup(func() { param.SwapTempValue(original) })
	catalog := metastore.NewCataLog(nil)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))
	vchannels := mockey.Mock(mockey.GetMethod(catalog, "ListVChannel")).Return(nil, nil).Build()
	defer vchannels.UnPatch()
	segments := mockey.Mock(mockey.GetMethod(catalog, "ListSegmentAssignment")).Return(nil, nil).Build()
	defer segments.UnPatch()
	r := newTestRecoveryStorage(t, &utility.WALCheckpoint{MessageID: walimplstest.NewTestMessageID(1), TimeTick: 10})
	t.Cleanup(r.metrics.Close)
	t.Cleanup(r.backgroundTaskNotifier.Cancel)
	err := r.recoverRecoveryInfoFromMeta(t.Context(), r.channel)
	require.ErrorContains(t, err, param.Key)
	require.Nil(t, r.summaryManager, "invalid configuration must stop recovery before restoring or starting Summary")
}
