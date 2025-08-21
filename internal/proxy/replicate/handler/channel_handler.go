package handler

import "context"

type ChannelHandler interface {
	GetMappedPChannel(ctx context.Context, pchannel string) (string, error)
}
