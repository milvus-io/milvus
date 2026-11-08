package partialupdate

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// Inserts with and without a schema version both lock the PKs of the
// collection's schema.
func TestPartialUpdateChainNewInsertBlocksSamePKCASUntilPublication(t *testing.T) {
	for _, versionless := range []bool{false, true} {
		for _, mode := range []string{"auto_commit", "transaction", "recovered_transaction"} {
			t.Run(fmt.Sprintf("%s/versionless=%t", mode, versionless), func(t *testing.T) {
				insert := newInsertMessage([]*schemapb.FieldData{int64PKFieldData(10)})
				if versionless {
					insert = withoutSchemaVersion(insert)
				}
				var builders map[message.TxnID]*message.ImmutableTxnMessageBuilder
				if mode == "recovered_transaction" {
					builders = recoveredRowLockBuilders(t, insert)
				}
				env := newPartialUpdateChainTestEnvWithBuilders(t, builders)
				if mode == "recovered_transaction" {
					// The test allocator starts at 1, already owned by the fixture.
					_, err := resource.Resource().IDAllocator().Allocate(context.Background())
					require.NoError(t, err)
				}
				readTS := env.allocateReadTS(t)
				cas := env.prepareCASTxn(t, readTS, 10)
				write := insert
				switch mode {
				case "transaction":
					begin := message.NewBeginTxnMessageBuilderV2().WithVChannel("v1").
						WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable()
					result, err := env.append(begin, nil)
					require.NoError(t, err)
					_, err = env.append(insert.WithTxnContext(*result.TxnCtx), nil)
					require.NoError(t, err)
					write = newCommitTxnMessage("v1", result.TxnCtx.TxnID, 0)
				case "recovered_transaction":
					write = newCommitTxnMessage("v1", 1, 0)
				}
				atWAL, resume := make(chan struct{}), make(chan struct{})
				var once sync.Once
				defer once.Do(func() { close(resume) })
				writerDone := make(chan error, 1)
				go func() {
					_, err := env.append(write, func(context.Context, message.MutableMessage) (message.MessageID, error) {
						close(atWAL)
						<-resume
						return env.nextMessageID(), nil
					})
					writerDone <- err
				}()
				receiveCommitValue(t, atWAL)
				requireTestPKShared(t, env.partial.state.rowLocks, "v1", 10, "new insert must hold a shared PK lock through WAL append")
				casDone := make(chan error, 1)
				go func() {
					_, err := env.append(newChainTestCASCommit(t, cas), nil)
					casDone <- err
				}()
				waitForTestPKAwaited(t, env.partial.state.rowLocks, "v1", 10)
				once.Do(func() { close(resume) })
				require.NoError(t, receiveCommitValue(t, writerDone))
				requirePartialUpdateRetryable(t, receiveCommitValue(t, casDone))
				require.True(t, env.hasPKVersionAfter("v1", 10, readTS))
				requireTestRowLocksAvailable(t, env.partial.state.rowLocks, "v1", 10)
			})
		}
	}
}
