package proxy

import (
	"context"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"
)

type clientInfo struct {
	*commonpb.ClientInfo
	identifier     int64
	lastActiveTime time.Time
}

func getLoggerOfClientInfo(info *commonpb.ClientInfo) []zap.Field {
	fields := []zap.Field{
		zap.String("sdk_type", info.GetSdkType()),
		zap.String("sdk_version", info.GetSdkVersion()),
		zap.String("local_time", info.GetLocalTime()),
		zap.String("user", info.GetUser()),
		zap.String("host", info.GetHost()),
	}

	for k, v := range info.GetReserved() {
		fields = append(fields, zap.String(k, v))
	}

	return fields
}

func (c *clientInfo) getLogger() []zap.Field {
	fields := getLoggerOfClientInfo(c.ClientInfo)
	fields = append(fields,
		zap.Int64("identifier", c.identifier),
		zap.Time("last_active_time", c.lastActiveTime),
	)
	return fields
}

func (c *clientInfo) ctxLogRegister(ctx context.Context) {
	log.Ctx(ctx).Info("client register", c.getLogger()...)
}

func (c *clientInfo) logDeregister() {
	log.Info("client deregister", c.getLogger()...)
}
