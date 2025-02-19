package kafka

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/helper"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

var _ walimpls.OpenerImpls = (*openerImpl)(nil)

// newOpenerImpl creates a new openerImpl instance.
func newOpenerImpl(p *kafka.Producer, consumerConfig kafka.ConfigMap) *openerImpl {
	o := &openerImpl{
		n:              syncutil.NewAsyncTaskNotifier[struct{}](),
		p:              p,
		consumerConfig: consumerConfig,
	}
	go o.execute()
	return o
}

// openerImpl is the opener implementation for kafka wal.
type openerImpl struct {
	n              *syncutil.AsyncTaskNotifier[struct{}]
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

func (o *openerImpl) execute() {
	defer o.n.Finish(struct{}{})

	for {
		select {
		case <-o.n.Context().Done():
			return
		case ev, ok := <-o.p.Events():
			if !ok {
				panic("kafka producer events channel should never be closed before the execute observer exit")
			}
			switch ev := ev.(type) {
			case kafka.Error:
				log.Error("kafka producer error", zap.Error(ev))
				if ev.IsFatal() {
					panic(fmt.Sprintf("kafka producer error is fatal, %s", ev.Error()))
				}
			default:
				// ignore other events
				log.Debug("kafka producer incoming non-message, non-error event", zap.String("event", ev.String()))
			}
		}
	}
}

func (o *openerImpl) Close() {
	o.n.Cancel()
	o.n.BlockUntilFinish()
	o.p.Close()
}
