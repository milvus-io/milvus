package kafka

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls/helper"
)

var _ walimpls.OpenerImpls = (*openerImpl)(nil)

type openerImpl struct {
	p              *kafka.Producer
	consumerConfig kafka.ConfigMap
}

func (o *openerImpl) Open(ctx context.Context, opt *walimpls.OpenOption) (walimpls.WALImpls, error) {
	return &walImpl{
		WALHelper:      helper.NewWALHelper(opt),
		p:              o.p,
		consumerConfig: o.consumerConfig,
	}, nil
}

func (o *openerImpl) Close() {
	o.p.Close()
}
