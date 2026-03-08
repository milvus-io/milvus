package connection

import (
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

type clientInfo struct {
	*commonpb.ClientInfo
	identifier     int64
	lastActiveTime time.Time
}

func (c *clientInfo) GetLogger() []zap.Field {
	fields := ZapClientInfo(c.ClientInfo)
	fields = append(fields,
		zap.Int64("identifier", c.identifier),
		zap.Time("last_active_time", c.lastActiveTime),
	)
	return fields
}
