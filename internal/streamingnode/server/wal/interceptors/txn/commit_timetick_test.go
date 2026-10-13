package txn

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func TestPreparedCommitChecksFinalTimestampExpiry(t *testing.T) {
	resource.InitForTest(t)
	manager := NewTxnManager(types.PChannelInfo{Name: "test"}, nil)
	session, err := manager.BeginNewTxn(context.Background(), newBeginTxnMessage(0, 10*time.Millisecond))
	require.NoError(t, err)
	require.NoError(t, session.RequestCommitAndWait(context.Background(), 0))
	require.NoError(t, session.ValidateCommitTimeTick(1))

	expiredTS := tsoutil.AddPhysicalDurationOnTs(0, 10*time.Millisecond)
	err = session.ValidateCommitTimeTick(expiredTS)
	require.True(t, status.AsStreamingError(err).IsTxnExpired())
	require.Equal(t, message.TxnStateOnCommit, session.State())
	session.RejectCommit()
	require.Equal(t, message.TxnStateRollbacked, session.State())
}
