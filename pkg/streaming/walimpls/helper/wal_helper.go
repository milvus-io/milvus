package helper

import (
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
)

// NewWALHelper creates a new WALHelper.
func NewWALHelper(opt *walimpls.OpenOption) *WALHelper {
	return &WALHelper{
		logger:  mlog.With(mlog.String("channel", opt.Channel.String())),
		channel: opt.Channel,
	}
}

// WALHelper is a helper for WAL implementation.
type WALHelper struct {
	logger  *mlog.Logger
	channel types.PChannelInfo
}

// Channel returns the channel of the WAL.
func (w *WALHelper) Channel() types.PChannelInfo {
	return w.channel
}

// Log returns the logger of the WAL.
func (w *WALHelper) Log() *mlog.Logger {
	return w.logger
}
