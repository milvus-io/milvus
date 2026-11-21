package flusherimpl

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/flushcommon/pipeline"
	"github.com/milvus-io/milvus/pkg/v3/mocks/github.com/milvus-io/milvus-proto/go-api/v3/mock_hook"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

// Cipher registration is process-wide; the wrapper remains stable across -count.
var rowReaderCipher = &struct{ hook.Cipher }{}

func TestDataServiceRowWriteKMSRecoveryAndCancellation(t *testing.T) {
	message.RegisterCipher(rowReaderCipher)
	for _, scenario := range []string{"restored", "canceled", "malformed"} {
		t.Run(scenario, func(t *testing.T) {
			cipher := mock_hook.NewMockCipher(t)
			decryptor := mock_hook.NewMockDecryptor(t)
			var calls atomic.Int32
			firstLookup := make(chan struct{})
			cipher.EXPECT().GetDecryptor(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(int64, int64, []byte) (hook.Decryptor, error) {
				attempt := calls.Add(1)
				if attempt == 1 {
					close(firstLookup)
				}
				if scenario == "canceled" || scenario == "restored" && attempt == 1 {
					return nil, message.ErrKmsKeyInvalid
				}
				return decryptor, nil
			})
			decryptor.EXPECT().Decrypt(mock.Anything).RunAndReturn(func(payload []byte) ([]byte, error) { return payload, nil }).Maybe()
			rowReaderCipher.Cipher = cipher
			defer func() { rowReaderCipher.Cipher = nil }()
			mutable := message.NewUpsertMessageBuilderV2().WithVChannel("p_1v0").WithHeader(&messagespb.UpsertMessageHeader{CollectionId: 1, Partitions: []*messagespb.PartitionSegmentAssignment{{PartitionId: 10, Rows: 1, SegmentAssignment: &messagespb.SegmentAssignment{SegmentId: 100}}}}).WithBody(&messagespb.UpsertMessageBody{Rows: &messagespb.InsertRows{PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{101}}}}, RowIdsEncoding: &messagespb.InsertRows_RowIds{RowIds: &schemapb.LongArray{Data: []int64{1}}}}}).MustBuildMutable().WithTimeTick(20).WithLastConfirmed(rmq.NewRmqID(1))
			wire := mutable.IntoMessageProto()
			header, err := message.EncodeProto(&messagespb.CipherHeader{EzId: 1, CollectionId: 1})
			require.NoError(t, err)
			wire.Properties["_ch"] = header
			if scenario == "malformed" {
				wire.Payload = []byte{0xff}
			}
			msg := message.NewImmutableMesasge(rmq.NewRmqID(2), wire.Payload, wire.Properties)
			input := make(chan *msgstream.MsgPack, 1)
			service := newDataSyncServiceWrapper("p_1v0", input, &pipeline.DataSyncService{}, 0)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- service.HandleMessage(ctx, msg) }()
			if scenario == "canceled" {
				<-firstLookup
				cancel()
			}
			select {
			case err := <-result:
				switch scenario {
				case "restored":
					require.NoError(t, err)
					require.Equal(t, int32(2), calls.Load())
					require.Len(t, input, 1)
					require.Equal(t, uint64(20), (<-input).EndTs)
				case "canceled":
					require.ErrorIs(t, err, context.Canceled)
					require.Empty(t, input)
				case "malformed":
					require.Error(t, err)
					require.Equal(t, int32(1), calls.Load())
					require.Empty(t, input)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("reader did not finish")
			}
		})
	}
}
