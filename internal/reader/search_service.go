package reader

import (
	"context"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
)

type searchService struct {
	ctx           context.Context
	queryNodeTime *QueryNodeTime
	msgStream     *msgstream.PulsarMsgStream
}

func (ss *searchService) Start() {}
