package flusher

import (
	"github.com/milvus-io/milvus/internal/proto/datapb"
)

type watchTask struct {
	vchannel string

	watchInfo *datapb.ChannelWatchInfo
}
