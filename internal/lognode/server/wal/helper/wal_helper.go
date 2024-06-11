package helper

import (
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/lognode/server/wal/walimpls"
	"github.com/milvus-io/milvus/internal/proto/logpb"
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
	channel *logpb.PChannelInfo
}

// Channel returns the channel of the WAL.
func (w *WALHelper) Channel() *logpb.PChannelInfo {
	return w.channel
}

// Log returns the logger of the WAL.
func (w *WALHelper) Log() *log.MLogger {
	return w.logger
}
