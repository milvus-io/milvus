package message_test

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

func TestRegisterMessageIDUnmarshaler(t *testing.T) {
	msgID := mock_message.NewMockMessageID(t)

	id, err := message.UnmarshalMessageID(&commonpb.MessageID{WALName: commonpb.WALName(message.WALNameTest), Id: "123"})
	assert.NotNil(t, id)
	assert.NoError(t, err)

	id, err = message.UnmarshalMessageID(&commonpb.MessageID{WALName: commonpb.WALName(message.WALNameTest), Id: "123a"})
	assert.Nil(t, id)
	assert.Error(t, err)

	// An unregistered wal_name (a client can put any int32 on the wire for a
	// proto enum) must return an error instead of panicking, so it cannot crash
	// the process. 101 is not a registered WAL. MustUnmarshalMessageID keeps the
	// panic contract.
	id, err = message.UnmarshalMessageID(&commonpb.MessageID{WALName: commonpb.WALName(101), Id: "123"})
	assert.Nil(t, id)
	assert.Error(t, err)
	assert.ErrorIs(t, err, message.ErrInvalidMessageID)
	assert.Contains(t, err.Error(), "101")

	id, err = message.UnmarshalMessageID(nil)
	assert.Nil(t, id)
	assert.Error(t, err)
	assert.ErrorIs(t, err, message.ErrInvalidMessageID)

	assert.Panics(t, func() {
		message.MustUnmarshalMessageID(&commonpb.MessageID{WALName: commonpb.WALName(101), Id: "123"})
	})

	assert.Panics(t, func() {
		message.RegisterMessageIDUnmsarshaler(message.WALNameTest, func(b string) (message.MessageID, error) {
			if b == "123" {
				return msgID, nil
			}
			return nil, errors.New("invalid")
		})
	})
}

func TestCases(t *testing.T) {
	msgID := mock_message.NewMockMessageID(t)
	msgID.EXPECT().IntoProto().Return(&commonpb.MessageID{WALName: commonpb.WALName(message.WALNameTest), Id: "123"}).Maybe()
	msgID.EXPECT().Marshal().Return("123").Maybe()
	msgID.EXPECT().WALName().Return(message.WALNameTest).Maybe()
	message.CreateTestInsertMessage(t, 1, 100, 100, msgID)
	message.CreateTestCreateCollectionMessage(t, 1, 100, msgID)
	message.CreateTestEmptyInsertMesage(1, nil)
	message.CreateTestDropCollectionMessage(t, 1, 100, msgID)
	message.CreateTestTimeTickSyncMessage(t, 1, 100, msgID)
	message.CreateTestCreateSegmentMessage(t, 1, 100, msgID)
}
