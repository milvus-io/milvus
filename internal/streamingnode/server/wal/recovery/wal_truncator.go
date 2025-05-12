package recovery

import (
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
)

// newSamplingTruncator creates a new sampling truncator.
func newSamplingTruncator(
	checkpoint *WALCheckpoint,
	truncator walimpls.WALImpls,
	recoveryMetrics *recoveryMetrics,
) *samplingTruncator {
	st := &samplingTruncator{
		notifier:                syncutil.NewAsyncTaskNotifier[struct{}](),
		cfg:                     newTruncatorConfig(),
		truncator:               truncator,
		mu:                      sync.Mutex{},
		checkpointSamples:       []*WALCheckpoint{checkpoint},
		lastTruncatedCheckpoint: nil,
		lastSampled:             time.Now(),
		metrics:                 recoveryMetrics,
	}
	go st.background()
	return st
}

// samplingTruncator is a sampling truncator that samples the incoming checkpoint and truncates the WAL.
type samplingTruncator struct {
	log.Binder
	notifier  *syncutil.AsyncTaskNotifier[struct{}]
	cfg       *truncatorConfig
	truncator walimpls.WALImpls

	mu                      sync.Mutex
	checkpointSamples       []*WALCheckpoint // the samples of checkpoints
	lastTruncatedCheckpoint *WALCheckpoint   // the last truncated checkpoint
	lastSampled             time.Time        // the last time the checkpoint is sampled
	metrics                 *recoveryMetrics
}

// SampleCheckpoint samples the incoming checkpoint and adds it to the checkpoint samples.
func (t *samplingTruncator) SampleCheckpoint(checkpoint *WALCheckpoint) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if time.Since(t.lastSampled) < t.cfg.sampleInterval {
		return
	}

	if len(t.checkpointSamples) == 0 || t.checkpointSamples[len(t.checkpointSamples)-1].MessageID.LT(checkpoint.MessageID) {
		t.checkpointSamples = append(t.checkpointSamples, checkpoint)
	}
	t.lastSampled = time.Now()
}

// background starts the background task of the sampling truncator.
func (t *samplingTruncator) background() {
	defer func() {
		t.notifier.Finish(struct{}{})
		t.Logger().Info("sampling truncator background task exit")
	}()

	ticker := time.NewTicker(t.cfg.sampleInterval / 2)
	for {
		select {
		case <-t.notifier.Context().Done():
			return
		case <-ticker.C:
			t.applyTruncate()
		}
	}
}

// consumeCheckpointSamples consumes the checkpoint samples and returns the truncate checkpoint.
func (t *samplingTruncator) consumeCheckpointSamples() *WALCheckpoint {
	t.mu.Lock()
	defer t.mu.Unlock()

	targetCheckpointIdx := -1
	for i := 0; i < len(t.checkpointSamples); i++ {
		if time.Since(tsoutil.PhysicalTime(t.checkpointSamples[i].TimeTick)) < t.cfg.retentionInterval {
			break
		}
		targetCheckpointIdx = i
	}
	if targetCheckpointIdx >= 0 {
		checkpoint := t.checkpointSamples[targetCheckpointIdx]
		t.checkpointSamples = t.checkpointSamples[targetCheckpointIdx+1:]
		return checkpoint
	}
	return nil
}

// applyTruncate applies the truncate operation.
func (t *samplingTruncator) applyTruncate() {
	truncateCheckpoint := t.consumeCheckpointSamples()
	if truncateCheckpoint == nil {
		t.Logger().Debug("no checkpoint sample can be used to truncate wal")
		return
	}
	logger := t.Logger().With(zap.String("messageID", truncateCheckpoint.MessageID.String()), zap.Uint64("timeTick", truncateCheckpoint.TimeTick))
	if t.lastTruncatedCheckpoint != nil {
		logger = logger.With(zap.String("lastMessageID", t.lastTruncatedCheckpoint.MessageID.String()), zap.Uint64("lastTimeTick", t.lastTruncatedCheckpoint.TimeTick))
		if truncateCheckpoint.MessageID.EQ(t.lastTruncatedCheckpoint.MessageID) {
			logger.Debug("checkpoint sample is the same, ignore the operation", zap.String("messageID", truncateCheckpoint.MessageID.String()))
			t.lastTruncatedCheckpoint = truncateCheckpoint
			t.metrics.ObServeTruncateMetrics(truncateCheckpoint.TimeTick)
			return
		} else if truncateCheckpoint.MessageID.LT(t.lastTruncatedCheckpoint.MessageID) {
			logger.Warn("checkpoint sample is not in order, the wal may be corrupted", zap.String("targetMessageID", truncateCheckpoint.MessageID.String()))
			return
		}
	}

	err := t.truncator.Truncate(t.notifier.Context(), truncateCheckpoint.MessageID)
	if err != nil {
		logger.Warn("failed to truncate wal, the checkpoint sample is lost", zap.Error(err))
		return
	}
	logger.Info("truncate wal")
	t.lastTruncatedCheckpoint = truncateCheckpoint
	t.metrics.ObServeTruncateMetrics(truncateCheckpoint.TimeTick)
}

// Close closes the sampling truncator.
func (t *samplingTruncator) Close() {
	t.notifier.Cancel()
	t.notifier.BlockAndGetResult()
}
