package adaptor

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	msgadaptor "github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/helper"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func TestRecoveryStreamReportsFatalScannerError(t *testing.T) {
	for _, tc := range []struct {
		name     string
		err      error
		canceled bool
	}{
		{name: "corrupted chunk", err: message.ErrCorruptedChunk},
		{name: "normal completion"},
		{name: "shutdown", err: message.ErrCorruptedChunk, canceled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := make(chan message.ImmutableMessage)
			close(upstream)
			scanner := &scannerAdaptorImpl{
				ScannerHelper: helper.NewScannerHelper(tc.name),
				readOption:    wal.ReadOption{MesasgeHandler: msgadaptor.ChanMessageHandler(upstream)},
				metrics:       metricsutil.NewScanMetrics(types.PChannelInfo{}).NewScannerMetrics(),
			}
			scanner.Finish(tc.err)
			var fatalErr error
			stream := &recoveryStreamImpl{
				notifier: syncutil.NewAsyncTaskNotifier[error](),
				scanner:  scanner,
				ch:       make(chan message.ImmutableMessage),
				onFatal:  func(err error) { fatalErr = err },
			}
			if tc.canceled {
				stream.notifier.Cancel()
			}
			_ = stream.execute()
			if tc.canceled || tc.err == nil {
				require.NoError(t, fatalErr)
			} else {
				require.ErrorIs(t, fatalErr, tc.err)
				require.ErrorIs(t, stream.Error(), tc.err)
			}
		})
	}
}
