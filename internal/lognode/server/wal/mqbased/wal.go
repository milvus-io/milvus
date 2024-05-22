package mqbased

import (
	"context"

	"github.com/milvus-io/milvus/internal/lognode/server/wal"
	"github.com/milvus-io/milvus/internal/lognode/server/wal/extends"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"go.uber.org/zap"
)

var _ wal.BasicWAL = (*walImpl)(nil)

// walImpl is implementation of basicWAL.
type walImpl struct {
	logger   *log.MLogger
	channel  *logpb.PChannelInfo
	registry *scannerRegistry
	scanners *typeutil.ConcurrentMap[string, *mqBasedScanner] // only for GetLatestMessageID api. lifetime management is managed by `extends.walExtendImpl`
	c        mqwrapper.Client
	p        mqwrapper.Producer
}

// Channel returns the channel assignment info of the writer.
func (w *walImpl) Channel() *logpb.PChannelInfo {
	return w.channel
}

// Append appends a message to the writer.
func (w *walImpl) Append(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
	msgID, err := w.p.Send(ctx, message.NewMQProducerMessageFromMutableMessage(msg))
	if err != nil {
		w.logger.RatedWarn(1, "send message to mq failed", zap.Error(err))
		return nil, err
	}
	return msgID, nil
}

// Read returns a scanner for reading records started from startMessageID.
func (sc *walImpl) Read(ctx context.Context, opt wal.ReadOption) (wal.Scanner, error) {
	scannerName, err := sc.registry.AllocateScannerName()
	if err != nil {
		return nil, status.NewInner("allocate scanner name fail: %s", err.Error())
	}

	scanner, err := sc.createNewScanner(ctx, scannerName, &opt)
	if err != nil {
		return nil, status.NewInner("create a new scanner fail: %s", err.Error())
	}

	// wrap the scanner with cleanup function.
	scannerWithCleanup := extends.ScannerWithCleanup(scanner, func() {
		sc.scanners.Remove(scannerName)
	})
	sc.scanners.Insert(scannerName, scanner)
	sc.logger.Info("new scanner created", zap.String("scannerName", scannerName))
	return scannerWithCleanup, nil
}

// GetLatestMessageID returns the latest message id of the channel.
func (a *walImpl) GetLatestMessageID(ctx context.Context) (message.MessageID, error) {
	// MQBased WAL only implement GetLatestMessageID at Consumer.
	// So if there is no consumer, GetLatestMessageID will return error.
	// At common use case, consumer always created before GetLatestMessageID.
	if a.scanners.Len() == 0 {
		return nil, status.NewInner("no scanner found for get latest message id")
	}

	// found a consumer to get the latest message id.
	var msgID message.MessageID
	var err error
	a.scanners.Range(func(name string, s *mqBasedScanner) bool {
		msgID, err = s.GetLatestMessageID()
		return false
	})
	return msgID, err
}

// Close closes the writer.
func (w *walImpl) Close() {
	w.p.Close()
	// scanners is managed by `extends.walExtendImpl`, so don't close it here.
	w.logger.Info("wal writer closed")
}

// createNewScanner creates a new scanner.
func (sc *walImpl) createNewScanner(_ context.Context, scannerName string, opt *wal.ReadOption) (*mqBasedScanner, error) {
	consumerOption := mqwrapper.ConsumerOptions{
		Topic:                       sc.channel.Name,
		SubscriptionName:            scannerName,
		SubscriptionInitialPosition: mqwrapper.SubscriptionPositionUnknown,
		BufSize:                     1024, // TODO: Configurable.
	}
	switch opt.DeliverPolicy.Policy.(type) {
	case *logpb.DeliverPolicy_All:
		consumerOption.SubscriptionInitialPosition = mqwrapper.SubscriptionPositionEarliest
	case *logpb.DeliverPolicy_Latest:
		consumerOption.SubscriptionInitialPosition = mqwrapper.SubscriptionPositionLatest
	}

	// Subscribe the MQ consumer.
	sub, err := sc.c.Subscribe(consumerOption)
	if err != nil {
		return nil, err
	}

	// Seek the MQ consumer.
	switch policy := opt.DeliverPolicy.Policy.(type) {
	case *logpb.DeliverPolicy_StartFrom:
		messageID := message.NewMessageIDFromPBMessageID(policy.StartFrom)
		// Do a inslusive seek.
		if err := sub.Seek(messageID, true); err != nil {
			return nil, err
		}
	case *logpb.DeliverPolicy_StartAfter:
		messageID := message.NewMessageIDFromPBMessageID(policy.StartAfter)
		// Do a exclude seek.
		if err := sub.Seek(messageID, false); err != nil {
			return nil, err
		}
	}
	return newMQBasedScanner(sc.channel, sub), nil
}

// dropAllExpiredScanners drop all expired scanner.
// func (a *walImpl) dropAllExpiredScanners(ctx context.Context) error {
// 	names, err := a.registry.GetAllExpiredScannerNames()
// 	if err != nil {
// 		return status.NewInner("get all expired scanner names fail: %s", err.Error())
// 	}
//
// 	g, _ := errgroup.WithContext(ctx)
// 	g.SetLimit(5)
// 	for _, n := range names {
// 		name := n
// 		g.Go(func() error {
// 			if err := a.manager.Drop(name); err != nil {
// 				return errors.Wrapf(err, "at scanner name: %s", name)
// 			}
// 			return nil
// 		})
// 	}
// 	if err := g.Wait(); err != nil {
// 		return status.NewInner("drop all expired scanner fail: %s", err.Error())
// 	}
// 	return nil
// }
