package adaptor

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/mocks/streaming/mock_walimpls"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	m.Run()
}

func TestOpenerAdaptorFailure(t *testing.T) {
	basicOpener := mock_walimpls.NewMockOpenerImpls(t)
	errExpected := errors.New("test")
	basicOpener.EXPECT().Open(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, boo *walimpls.OpenOption) (walimpls.WALImpls, error) {
		return nil, errExpected
	})

	opener := adaptImplsToOpener(basicOpener, nil)
	l, err := opener.Open(context.Background(), &wal.OpenOption{})
	assert.ErrorIs(t, err, errExpected)
	assert.Nil(t, l)
}

func TestDetermineLastConfirmedMessageID(t *testing.T) {
	txnBuffer := utility.NewTxnBuffer(log.With(), metricsutil.NewScanMetrics(types.PChannelInfo{}).NewScannerMetrics())
	lastConfirmedMessageID := determineLastConfirmedMessageID(rmq.NewRmqID(5), txnBuffer)
	assert.Equal(t, rmq.NewRmqID(5), lastConfirmedMessageID)
	beginMsg := message.NewBeginTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.BeginTxnMessageHeader{}).
		WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable().
		WithTimeTick(1).
		WithTxnContext(message.TxnContext{
			TxnID:     1,
			Keepalive: time.Hour,
		}).
		WithLastConfirmed(rmq.NewRmqID(1)).
		IntoImmutableMessage(rmq.NewRmqID(1))
	beginMsg2 := message.NewBeginTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.BeginTxnMessageHeader{}).
		WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable().
		WithTxnContext(message.TxnContext{
			TxnID:     2,
			Keepalive: time.Hour,
		}).
		WithTimeTick(1).
		WithLastConfirmed(rmq.NewRmqID(2)).
		IntoImmutableMessage(rmq.NewRmqID(2))

	txnBuffer.HandleImmutableMessages([]message.ImmutableMessage{
		message.MustAsImmutableBeginTxnMessageV2(beginMsg2),
	}, 4)

	lastConfirmedMessageID = determineLastConfirmedMessageID(rmq.NewRmqID(5), txnBuffer)
	assert.Equal(t, rmq.NewRmqID(2), lastConfirmedMessageID)

	txnBuffer.HandleImmutableMessages([]message.ImmutableMessage{
		message.MustAsImmutableBeginTxnMessageV2(beginMsg),
	}, 4)
	lastConfirmedMessageID = determineLastConfirmedMessageID(rmq.NewRmqID(5), txnBuffer)
	assert.Equal(t, rmq.NewRmqID(1), lastConfirmedMessageID)
}
