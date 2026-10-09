package pulsar

import (
	"context"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/cenkalti/backoff/v4"
	"github.com/cockroachdb/errors"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// clientFactory creates a pulsar client with its own connections and auth provider.
type clientFactory func() (pulsar.Client, error)

// walProducer is the pulsar producer of a read-write wal, with a pulsar client that serves only this producer.
// Close closes the client after the producer, so the broker drops the producer even if a reconnection
// registered it again on a new connection.
type walProducer struct {
	client   pulsar.Client
	producer pulsar.Producer
}

// newWALProducer creates the producer of topic on a new client from newClient with backoff retry,
// until the creation succeeds, ctx is done or pulsar.producerCreateTimeout elapses.
func newWALProducer(ctx context.Context, newClient clientFactory, topic string, logger *mlog.Logger) (*walProducer, error) {
	if timeout := paramtable.Get().PulsarCfg.ProducerCreateTimeout.GetAsDurationByParse(); timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	accessModeValue := paramtable.Get().PulsarCfg.ProducerAccessMode.GetValue()
	accessMode, ok := producerAccessModeFromConfig(accessModeValue)
	if !ok {
		logger.Warn(ctx, "unknown pulsar producer access mode, use exclusive", mlog.String("producerAccessMode", accessModeValue))
	}

	client, err := newClient()
	if err != nil {
		return nil, errors.Wrap(err, "create pulsar client for producer")
	}
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = 10 * time.Millisecond
	bo.MaxInterval = time.Second
	bo.MaxElapsedTime = 0
	bo.Reset()
	for {
		producer, err := client.CreateProducer(pulsar.ProducerOptions{
			Topic: topic,
			// The go pulsar client does not support ProducerAccessModeExclusiveWithFencing.
			// In ProducerAccessModeExclusive, the creation fails while another producer is connected to the topic.
			ProducerAccessMode: accessMode,
		})
		if err == nil {
			logger.Info(ctx, "pulsar create producer done", mlog.String("producerAccessMode", accessModeValue))
			return &walProducer{client: client, producer: producer}, nil
		}
		logger.RatedWarn(ctx, rate.Every(10*time.Second), "create producer failed", mlog.String("producerAccessMode", accessModeValue), mlog.Err(err))
		select {
		case <-time.After(bo.NextBackOff()):
		case <-ctx.Done():
			client.Close()
			return nil, errors.Wrap(ctx.Err(), "create pulsar producer")
		}
	}
}

// Send sends msg, and marks the error walimpls.ErrFenced if the producer can never write again.
func (p *walProducer) Send(ctx context.Context, msg *pulsar.ProducerMessage) (pulsar.MessageID, error) {
	id, err := p.producer.Send(ctx, msg)
	if err != nil && isProducerUnusable(err) {
		return nil, errors.Mark(err, walimpls.ErrFenced)
	}
	return id, err
}

// Close closes the producer, and then the client with all its broker connections.
func (p *walProducer) Close() {
	p.producer.Close()
	p.client.Close()
}

// isProducerUnusable returns true if err shows that the producer is closed or fenced, or its topic is terminated or not found.
// Such a producer can never write again.
func isProducerUnusable(err error) bool {
	for e := err; e != nil; e = errors.UnwrapOnce(e) {
		if errors.IsAny(e, pulsar.ErrProducerFenced, pulsar.ErrProducerClosed, pulsar.ErrTopicTerminated, pulsar.ErrTopicNotfound) {
			return true
		}
		// The pulsar client joins errors with the standard library errors.Join,
		// which errors.Is of github.com/cockroachdb/errors v1.9.1 cannot unwrap.
		if joined, ok := e.(interface{ Unwrap() []error }); ok {
			for _, inner := range joined.Unwrap() {
				if isProducerUnusable(inner) {
					return true
				}
			}
		}
	}
	return false
}
