package message

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
)

func TestReadNewRowWriteBodyReturnsKMSFailure(t *testing.T) {
	original := cipher
	t.Cleanup(func() { cipher = original })
	calls := 0
	cipher = &mockCipher{getDecryptorFunc: func(int64, int64, []byte) (hook.Decryptor, error) {
		calls++
		if calls == 1 {
			return nil, ErrKmsKeyInvalid
		}
		return &mockDecryptor{}, nil
	}}
	payload, err := proto.Marshal(&messagespb.InsertMessageBody{Rows: &messagespb.InsertRows{
		RowIdsEncoding: &messagespb.InsertRows_RowIds{RowIds: &schemapb.LongArray{Data: []int64{17}}},
	}})
	require.NoError(t, err)
	header, err := EncodeProto(&messagespb.InsertMessageHeaderV2{CollectionId: 1})
	require.NoError(t, err)
	cipherHeader, err := EncodeProto(&messagespb.CipherHeader{EzId: 1, CollectionId: 1})
	require.NoError(t, err)
	msg := NewMutableMessageBeforeAppend(payload, map[string]string{
		messageTypeKey: MessageTypeInsert.marshal(), messageVersion: "2",
		messageHeader: header, messageCipherHeader: cipherHeader,
	})
	reader := MustAsMutableInsertMessageV2(msg)
	body, err := reader.Body()
	require.ErrorIs(t, err, ErrKmsKeyInvalid)
	require.Nil(t, body)
	body, err = reader.Body()
	require.NoError(t, err)
	require.Equal(t, []int64{17}, body.Rows.GetRowIds().GetData())
	require.Equal(t, 2, calls)
}
