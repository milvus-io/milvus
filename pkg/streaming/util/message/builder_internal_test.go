package message

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const replicatePayloadValidationWALName WALName = 199

type replicatePayloadValidationMessageID string

func (id replicatePayloadValidationMessageID) WALName() WALName {
	return replicatePayloadValidationWALName
}

func (id replicatePayloadValidationMessageID) LT(other MessageID) bool {
	return id.Marshal() < other.Marshal()
}

func (id replicatePayloadValidationMessageID) LTE(other MessageID) bool {
	return id.Marshal() <= other.Marshal()
}

func (id replicatePayloadValidationMessageID) EQ(other MessageID) bool {
	return id.Marshal() == other.Marshal()
}

func (id replicatePayloadValidationMessageID) Marshal() string {
	return string(id)
}

func (id replicatePayloadValidationMessageID) IntoProto() *commonpb.MessageID {
	return &commonpb.MessageID{
		WALName: commonpb.WALName(id.WALName()),
		Id:      id.Marshal(),
	}
}

func (id replicatePayloadValidationMessageID) String() string {
	return id.Marshal()
}

type replicatePayloadValidationCipher struct {
	decryptor hook.Decryptor
	err       error
}

func (c *replicatePayloadValidationCipher) Init(map[string]string) error {
	return nil
}

func (c *replicatePayloadValidationCipher) GetEncryptor(int64, int64) (hook.Encryptor, []byte, error) {
	return nil, nil, nil
}

func (c *replicatePayloadValidationCipher) GetDecryptor(int64, int64, []byte) (hook.Decryptor, error) {
	return c.decryptor, c.err
}

func (c *replicatePayloadValidationCipher) GetUnsafeKey(int64, int64) []byte {
	return nil
}

type replicatePayloadValidationDecryptor struct {
	payload []byte
	err     error
}

func (d *replicatePayloadValidationDecryptor) Decrypt([]byte) ([]byte, error) {
	return d.payload, d.err
}

func TestNewReplicateMessageRejectsEncryptedPayloadValidationFailures(t *testing.T) {
	validPayload, err := proto.Marshal(&msgpb.InsertRequest{})
	require.NoError(t, err)

	tests := []struct {
		name     string
		cipher   hook.Cipher
		wantText string
	}{
		{
			name:     "missing cipher",
			cipher:   nil,
			wantText: "cipher not registered",
		},
		{
			name: "decryptor lookup failure",
			cipher: &replicatePayloadValidationCipher{
				err: errors.New("lookup failed"),
			},
			wantText: "lookup failed",
		},
		{
			name: "decrypt failure",
			cipher: &replicatePayloadValidationCipher{
				decryptor: &replicatePayloadValidationDecryptor{err: errors.New("decrypt failed")},
			},
			wantText: "decrypt failed",
		},
		{
			name: "decrypted payload unmarshal failure",
			cipher: &replicatePayloadValidationCipher{
				decryptor: &replicatePayloadValidationDecryptor{payload: []byte{0xff}},
			},
			wantText: "invalid message payload",
		},
		{
			name: "decryptor missing",
			cipher: &replicatePayloadValidationCipher{
				decryptor: nil,
			},
			wantText: "decryptor is nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			restoreCipher := replaceCipherForTest(tt.cipher)
			t.Cleanup(restoreCipher)

			immutableMsg := encryptedInsertImmutableMessageForTest(t, validPayload)

			replicateMsg, err := NewReplicateMessage("by-dev", immutableMsg)

			require.Error(t, err)
			assert.Nil(t, replicateMsg)
			assert.ErrorIs(t, err, merr.ErrParameterInvalid)
			assert.Contains(t, err.Error(), tt.wantText)
		})
	}
}

func replaceCipherForTest(testCipher hook.Cipher) func() {
	originalCipher := cipher
	cipher = testCipher
	return func() {
		cipher = originalCipher
	}
}

func encryptedInsertImmutableMessageForTest(t *testing.T, payload []byte) *commonpb.ImmutableMessage {
	t.Helper()

	restoreMessageIDUnmarshaler := replaceMessageIDUnmarshalerForTest()
	t.Cleanup(restoreMessageIDUnmarshaler)

	msg := NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&InsertMessageHeader{}).
		WithBody(&msgpb.InsertRequest{}).
		MustBuildMutable()
	msgID := replicatePayloadValidationMessageID("1")
	immutableMsg := msg.WithTimeTick(100).WithLastConfirmed(msgID).IntoImmutableMessage(msgID).IntoImmutableMessageProto()
	cipherHeader, err := EncodeProto(&messagespb.CipherHeader{
		EzId:         1,
		CollectionId: 2,
		SafeKey:      []byte("safe-key"),
		PayloadBytes: int64(len(payload)),
	})
	require.NoError(t, err)
	immutableMsg.Properties[messageCipherHeader] = cipherHeader
	immutableMsg.Payload = []byte("encrypted-payload")
	return immutableMsg
}

func replaceMessageIDUnmarshalerForTest() func() {
	originalUnmarshaler, loaded := messageIDUnmarshaler.Get(replicatePayloadValidationWALName)
	messageIDUnmarshaler.Insert(replicatePayloadValidationWALName, func(value string) (MessageID, error) {
		return replicatePayloadValidationMessageID(value), nil
	})
	return func() {
		if loaded {
			messageIDUnmarshaler.Insert(replicatePayloadValidationWALName, originalUnmarshaler)
			return
		}
		messageIDUnmarshaler.Remove(replicatePayloadValidationWALName)
	}
}
