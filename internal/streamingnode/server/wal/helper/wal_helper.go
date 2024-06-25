package helper

import (
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/streamingpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walimpls"
	"github.com/milvus-io/milvus/pkg/log"
)

// NewWALHelper creates a new WALHelper.
func NewWALHelper(opt *walimpls.OpenOption) *WALHelper {
	return &WALHelper{
		logger:  log.With(zap.Any("channel", opt.Channel)),
		channel: opt.Channel,
	}
}

// WALHelper is a helper for WAL implementation.
type WALHelper struct {
	logger  *log.MLogger
	channel *streamingpb.PChannelInfo
}

// Channel returns the channel of the WAL.
func (w *WALHelper) Channel() *streamingpb.PChannelInfo {
	return w.channel
}

// Log returns the logger of the WAL.
func (w *WALHelper) Log() *log.MLogger {
	return w.logger
}
