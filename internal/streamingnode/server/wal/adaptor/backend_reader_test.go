package adaptor

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/pkg/v3/mq/mqimpl/rocksmq/server"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	messageadaptor "github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	_ "github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/registry"
)

// The frozen payloads were produced by the 3.1 branch, rather than by this
// backport's generated builders. Only test envelope TimeTicks are rewritten.
func TestReaderReopensChunkedV2WAL(t *testing.T) {
	resource.InitForTest(t)
	fixture, err := os.ReadFile("../../../../querynodev2/segments/testdata/insert_upsert_v2_3_1.json")
	require.NoError(t, err)
	var wire []struct {
		Name       string            `json:"name"`
		Payload    []byte            `json:"payload"`
		Properties map[string]string `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(fixture, &wire))
	require.Len(t, wire, 2)
	path := filepath.Join(t.TempDir(), "wal")
	previous := server.Rmq
	t.Cleanup(func() { server.Rmq = previous })
	database, err := server.NewRocksMQ(path)
	require.NoError(t, err)
	server.Rmq = database
	builder := registry.MustGetBuilder(message.WALNameRocksmq)
	opener, err := builder.Build()
	require.NoError(t, err)
	writer, err := opener.Open(context.Background(), &walimpls.OpenOption{Channel: types.PChannelInfo{Name: "reader-backport", Term: 1, AccessMode: types.AccessModeRW}})
	require.NoError(t, err)
	closed := false
	t.Cleanup(func() {
		if !closed {
			writer.Close()
			opener.Close()
			database.Close()
		}
	})

	appendChunks := func(payload []byte, properties map[string]string) message.MessageID {
		t.Helper()
		size := (len(payload) + 1) / 2
		var head message.MessageID
		for index, start := 0, 0; start < len(payload); index, start = index+1, start+size {
			end := start + size
			if end > len(payload) {
				end = len(payload)
			}
			props := make(map[string]string, len(properties)+2)
			for key, value := range properties {
				props[key] = value
			}
			props["_ci"] = strconv.Itoa(index)
			props["_ct"] = "2"
			id, appendErr := writer.Append(context.Background(), message.NewMutableMessageBeforeAppend(payload[start:end], props))
			require.NoError(t, appendErr)
			if index == 0 {
				head = id
			}
		}
		return head
	}
	first := appendChunks(wire[0].Payload, wire[0].Properties)
	appendChunks(wire[1].Payload, wire[1].Properties)
	channel := wire[0].Properties["_vc"]
	txn := message.TxnContext{TxnID: 51, Keepalive: message.TxnKeepaliveInfinite}
	begin := message.NewBeginTxnMessageBuilderV2().WithVChannel(channel).WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable().WithTimeTick(30).WithLastConfirmed(first).WithTxnContext(txn)
	_, err = writer.Append(context.Background(), begin)
	require.NoError(t, err)
	bodyProps := make(map[string]string, len(wire[1].Properties))
	for key, value := range wire[1].Properties {
		bodyProps[key] = value
	}
	bodyProps["_tt"] = message.EncodeUint64(40)
	body := message.NewMutableMessageBeforeAppend(wire[1].Payload, bodyProps).WithTxnContext(txn)
	appendChunks(body.IntoMessageProto().Payload, body.IntoMessageProto().Properties)
	commit := message.NewCommitTxnMessageBuilderV2().WithVChannel(channel).WithHeader(&message.CommitTxnMessageHeader{}).WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable().WithTimeTick(50).WithLastConfirmed(first).WithTxnContext(txn)
	_, err = writer.Append(context.Background(), commit)
	require.NoError(t, err)
	barrier := message.NewTimeTickMessageBuilderV1().WithAllVChannel().WithHeader(&message.TimeTickMessageHeader{}).WithBody(&msgpb.TimeTickMsg{}).MustBuildMutable().WithTimeTick(60).WithLastConfirmed(first)
	_, err = writer.Append(context.Background(), barrier)
	require.NoError(t, err)
	writer.Close()
	opener.Close()
	database.Close()
	closed = true

	// Reopen the actual persistent RocksMQ store and its WAL reader, not an
	// in-memory reconstruction or a second scanner on the still-open store.
	recovered, err := server.NewRocksMQ(path)
	require.NoError(t, err)
	defer recovered.Close()
	server.Rmq = recovered
	readerOpener, err := builder.Build()
	require.NoError(t, err)
	defer readerOpener.Close()
	reader, err := readerOpener.Open(context.Background(), &walimpls.OpenOption{Channel: types.PChannelInfo{Name: "reader-backport", Term: 1, AccessMode: types.AccessModeRO}})
	require.NoError(t, err)
	defer reader.Close()
	scanner := newScannerAdaptor("reopened-v2", reader, wal.ReadOption{VChannel: channel, DeliverPolicy: options.DeliverPolicyStartFrom(first), IgnorePauseConsumption: true}, metricsutil.NewScanMetrics(types.PChannelInfo{}).NewScannerMetrics(), func() {}, false)
	defer scanner.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var logical []message.ImmutableMessage
	for {
		select {
		case msg, ok := <-scanner.Chan():
			if !ok {
				t.Fatalf("reader closed: %v", scanner.Error())
			}
			if msg.MessageType() == message.MessageTypeTimeTick {
				require.Equal(t, uint64(60), msg.TimeTick())
				goto confirmed
			}
			require.False(t, message.IsChunkedPayload(msg))
			logical = append(logical, msg)
		case <-ctx.Done():
			t.Fatal("reader did not reach persisted confirmation")
		}
	}
confirmed:
	require.Len(t, logical, 3)
	require.Equal(t, message.MessageTypeInsert, logical[0].MessageType())
	require.True(t, logical[0].MessageID().EQ(first))
	require.Equal(t, message.MessageTypeUpsert, logical[1].MessageType())
	require.Equal(t, message.MessageTypeTxn, logical[2].MessageType())
	for index, msg := range logical {
		pack, decodeErr := messageadaptor.NewMsgPackFromMessage(msg)
		require.NoError(t, decodeErr)
		expected := []uint64{10, 20, 50}[index]
		require.Equal(t, expected, pack.BeginTs)
		require.Equal(t, expected, pack.EndTs)
		expectedEffects := 1
		if index > 0 {
			expectedEffects = 2
		}
		require.Len(t, pack.Msgs, expectedEffects)
		insert := pack.Msgs[0].(*msgstream.InsertMsg)
		require.Equal(t, []uint64{expected}, insert.Timestamps)
		require.Equal(t, int64(8), insert.FieldsData[0].GetVectors().GetDim())
		require.Equal(t, int64(8), insert.FieldsData[0].GetVectors().GetSparseFloatVector().GetDim())
		if index > 0 {
			deletion := pack.Msgs[1].(*msgstream.DeleteMsg)
			require.Equal(t, []int64{101}, deletion.PrimaryKeys.GetIntId().GetData())
			require.Equal(t, []uint64{expected}, deletion.Timestamps)
		}
	}
	require.NoError(t, scanner.Close())
}
