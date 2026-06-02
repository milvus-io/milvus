package connection

import (
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

type clientInfo struct {
	*commonpb.ClientInfo
	identifier     int64
	lastActiveTime time.Time
}

func (c *clientInfo) GetLogger() []mlog.Field {
	fields := ZapClientInfo(c.ClientInfo)
	fields = append(fields,
		mlog.Int64("identifier", c.identifier),
		mlog.Time("last_active_time", c.lastActiveTime),
	)
	return fields
}
