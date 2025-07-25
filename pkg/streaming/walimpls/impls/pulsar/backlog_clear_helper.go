package pulsar

import (
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

const (
	backlogClearHelperName = "backlog-clear"
)

// backlogClearHelper is a helper to clear the backlog of pulsar.
type backlogClearHelper struct {
	log.Binder

	notifier       *syncutil.AsyncTaskNotifier[struct{}]
	cond           *syncutil.ContextCond
	written        int64
	threshold      int64
	channelName    types.PChannelInfo
	c              pulsar.Client
	reusedConsumer pulsar.Consumer
}

// newBacklogClearHelper creates a new backlog clear helper.
func newBacklogClearHelper(c pulsar.Client, channelName types.PChannelInfo, threshold int64) *backlogClearHelper {
	h := &backlogClearHelper{
		notifier:       syncutil.NewAsyncTaskNotifier[struct{}](),
		cond:           syncutil.NewContextCond(&sync.Mutex{}),
		written:        threshold, // trigger the backlog clear immediately.
		threshold:      threshold,
		channelName:    channelName,
		c:              c,
		reusedConsumer: nil,
	}
	h.SetLogger(log.With(zap.String("channel", channelName.String()), log.FieldComponent("backlog-clear")))
	go h.background()
	return h
}

// ObserveAppend observes the append traffic.
func (h *backlogClearHelper) ObserveAppend(size int) {
	h.cond.L.Lock()
	h.written += int64(size)
	if h.written >= h.threshold {
		h.cond.UnsafeBroadcast()
	}
	h.cond.L.Unlock()
}

// background is the background goroutine to clear the backlog.
func (h *backlogClearHelper) background() {
	defer func() {
		h.notifier.Finish(struct{}{})
		h.Logger().Info("backlog clear helper exit")
	}()

	for {
		h.cond.L.Lock()
		for h.written < h.threshold {
			if err := h.cond.Wait(h.notifier.Context()); err != nil {
				return
			}
		}
		h.written = 0
		h.cond.L.Unlock()

		if err := retry.Do(h.notifier.Context(), func() error {
			if h.notifier.Context().Err() != nil {
				return h.notifier.Context().Err()
			}
			if err := h.performBacklogClear(); err != nil {
				h.Logger().Warn("failed to perform backlog clear", zap.Error(err))
				return err
			}
			h.Logger().Debug("perform backlog clear done")
			return nil
		}, retry.AttemptAlways()); err != nil {
			return
		}
	}
}

// performBacklogClear performs the backlog clear.
func (h *backlogClearHelper) performBacklogClear() error {
	consumer, err := h.getConsumer()
	if err != nil {
		return errors.Wrap(err, "when create subscription")
	}

	if err := consumer.SeekByTime(time.Now()); err != nil {
		// close the reused consumer if seek failed.
		h.closeConsumer()
		return errors.Wrap(err, "when seek to latest message")
	}
	return nil
}

// getConsumer creates a new consumer.
func (h *backlogClearHelper) getConsumer() (pulsar.Consumer, error) {
	if h.reusedConsumer != nil {
		return h.reusedConsumer, nil
	}
	consumer, err := h.c.Subscribe(pulsar.ConsumerOptions{
		Topic:                    h.channelName.Name,
		SubscriptionName:         backlogClearHelperName,
		Type:                     pulsar.Shared, // use shared subscription to avoid the subscription is rejected because of consumer exists.
		MaxPendingChunkedMessage: 1,             // We cannot set it to 0, because the 0 means 100.
		ReceiverQueueSize:        1,             // We cannot set it to 0, because the 0 means 1000.
		StartMessageIDInclusive:  true,
	})
	if err != nil {
		return nil, errors.Wrap(err, "when create subscription")
	}
	h.reusedConsumer = consumer
	h.Logger().Info("created a new consumer")
	return h.reusedConsumer, nil
}

// closeConsumer closes the reused consumer.
func (h *backlogClearHelper) closeConsumer() {
	if h.reusedConsumer != nil {
		h.reusedConsumer.Close()
		h.reusedConsumer = nil
		h.Logger().Info("closed the reused consumer")
	}
}

// Close closes the backlog clear helper.
func (h *backlogClearHelper) Close() {
	h.notifier.Cancel()
	h.notifier.BlockUntilFinish()
	h.closeConsumer()
}
