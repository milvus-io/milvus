package adaptor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/replica"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func TestReplicaRecoveryBarrier(t *testing.T) {
	l := newRecoveryTestWAL(t)
	barrier := appendRecoveryTestMessage(t, l, message.MessageTypeRecoveryBarrier, 2)
	next := appendRecoveryTestMessage(t, l, message.MessageTypeTimeTick, 3)
	for _, tc := range []struct {
		name    string
		capture bool
	}{
		{"handoff transaction state", true},
		{"missing transaction state", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buffer := replica.NewBuffer(1)
			defer buffer.Close()
			cursor, _, err := buffer.Pin(true)
			require.NoError(t, err)
			if tc.capture {
				buffer.ObserveRecoveryBarrier(barrier, utility.NewTxnBuffer(nil, nil))
			}
			buffer.Observe([]message.ImmutableMessage{barrier, next}, 3)
			ready := make(chan struct{})
			fatal := make(chan error, 1)
			builder := &replicaRecoveryBuilder{WALImpls: l, cursor: cursor, ready: ready, onFatal: func(err error) { fatal <- err }}
			require.Same(t, l, builder.RWWALImpls())
			s := builder.Build(recovery.BuildRecoveryStreamParam{RecoveryBarrier: barrier})
			defer s.Close()
			if !tc.capture {
				require.ErrorContains(t, s.Error(), "lost writer startup barrier")
				require.ErrorIs(t, <-fatal, s.Error())
				_, ok := <-s.Chan()
				require.False(t, ok)
				return
			}
			require.Same(t, barrier, receiveRecoveryTestMessage(t, s))
			require.NotNil(t, s.TxnBuffer())
			select {
			case <-s.Chan():
				t.Fatal("writer advanced past recovery barrier before activation")
			default:
			}
			close(ready)
			require.Same(t, next, receiveRecoveryTestMessage(t, s))
			require.ErrorIs(t, s.Close(), context.Canceled)
			require.Empty(t, fatal)
		})
	}
}
