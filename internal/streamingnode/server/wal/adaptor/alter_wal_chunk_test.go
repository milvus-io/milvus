package adaptor

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"

	pulsarclient "github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/require"
	wplog "github.com/zilliztech/woodpecker/woodpecker/log"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/autowalimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/pulsar"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/wp"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestAlterWALInvalidatesChunkBudgetBeforeUnlock(t *testing.T) {
	const budget = 1024
	gate := &fenceTestGateInterceptor{
		insertEntered: make(chan struct{}), releaseInsert: make(chan struct{}),
		alterAppended: make(chan struct{}), releaseAlter: make(chan struct{}),
	}
	close(gate.releaseInsert)
	oldID := wp.NewWpID(&wplog.LogMessageId{SegmentId: 1, EntryId: 0})
	old := &recoveryPhysicalWAL{name: message.WALNameWoodpecker, WALImpls: newFirstTimeTickWALImpls(func(ctx context.Context, _ message.MutableMessage) (message.MessageID, error) {
		finishFenceTestAppend(ctx, 1)
		return oldID, nil
	})}
	var chunks, records atomic.Int64
	target := &recoveryPhysicalWAL{name: message.WALNamePulsar, WALImpls: newFirstTimeTickWALImpls(func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		if msg.MessageType() == message.MessageTypeInsert {
			if len(msg.Payload()) > budget {
				// Fail promptly instead of hanging this regression in infinite retry.
				return nil, walimpls.ErrFenced
			}
			chunks.Add(1)
		}
		finishFenceTestAppend(ctx, 2)
		return pulsar.NewPulsarID(pulsarclient.NewMessageID(1, records.Add(1), 0, 0)), nil
	})}
	auto := autowalimpls.New(old, func(context.Context, message.WALName, types.AccessMode) (walimpls.WALImpls, error) {
		return target, nil
	})
	w := newFenceTestWAL(t, auto, gate)
	params := paramtable.Get()
	for key, value := range map[string]string{
		params.StreamingCfg.SplitChunkSN.Key:    "true",
		params.PulsarCfg.MaxMessageSize.Key:     strconv.Itoa(testMinWALMessageSize),
		params.PulsarCfg.MessageReserveSize.Key: strconv.Itoa(testMinWALMessageSize - budget),
		params.WoodpeckerCfg.MaxMessageSize.Key: strconv.Itoa(2 * testMinWALMessageSize),
	} {
		require.NoError(t, params.Save(key, value))
		t.Cleanup(func() { require.NoError(t, params.Reset(key)) })
	}
	msg := newOversizedTestInsertMessage(t, budget).WithTimeTick(101).WithLastConfirmedUseMessageID()
	require.Greater(t, len(msg.Payload()), budget)
	require.Less(t, len(msg.Payload()), w.chunkPayloadSize(), "prime the larger old-backend cache")
	marker := message.NewAlterWALMessageBuilderV2().WithHeader(&message.AlterWALMessageHeader{TargetWalName: commonpb.WALName_Pulsar}).WithBody(&message.AlterWALMessageBody{}).WithAllVChannel().MustBuildMutable().WithTimeTick(100).WithLastConfirmed(oldID)
	done := make(chan error, 1)
	go func() { _, err := w.Append(context.Background(), marker); done <- err }()
	waitFenceTestSignal(t, gate.alterAppended)
	// DoAppend has released glock, but has not returned to walAdaptor.Append.
	// A waiting business append must already observe the target's budget.
	_, err := w.Append(context.Background(), msg)
	close(gate.releaseAlter)
	require.NoError(t, waitFenceTestError(t, done))
	require.NoError(t, err)
	require.Greater(t, chunks.Load(), int64(1))
}
