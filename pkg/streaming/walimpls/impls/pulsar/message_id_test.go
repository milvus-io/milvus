package pulsar

import (
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
)

func TestMessageID(t *testing.T) {
	ids := []pulsarID{
		newMessageIDOfPulsar(0, 0, 0),
		newMessageIDOfPulsar(0, 0, 1),
		newMessageIDOfPulsar(0, 0, 1000),
		newMessageIDOfPulsar(0, 1, 0),
		newMessageIDOfPulsar(0, 1, 1000),
		newMessageIDOfPulsar(0, 1000, 0),
		newMessageIDOfPulsar(1, 0, 0),
		newMessageIDOfPulsar(1, 1000, 0),
		newMessageIDOfPulsar(2, 0, 0),
	}

	for x, idx := range ids {
		for y, idy := range ids {
			assert.Equal(t, idx.EQ(idy), x == y)
			assert.Equal(t, idy.EQ(idx), x == y)
			assert.Equal(t, idy.LT(idx), x > y)
			assert.Equal(t, idy.LTE(idx), x >= y)
			assert.Equal(t, idx.LT(idy), x < y)
			assert.Equal(t, idx.LTE(idy), x <= y)
		}
	}

	msgID, err := UnmarshalMessageID(pulsarID{newMessageIDOfPulsar(1, 2, 3)}.Marshal())
	assert.NoError(t, err)
	assert.True(t, msgID.EQ(pulsarID{newMessageIDOfPulsar(1, 2, 3)}))

	_, err = UnmarshalMessageID(string([]byte{0x01, 0x02, 0x03, 0x04}))
	assert.Error(t, err)
}

// only for pulsar id unittest.
type MessageIdData struct {
	LedgerId   *uint64 `protobuf:"varint,1,req,name=ledgerId" json:"ledgerId,omitempty"`
	EntryId    *uint64 `protobuf:"varint,2,req,name=entryId" json:"entryId,omitempty"`
	Partition  *int32  `protobuf:"varint,3,opt,name=partition,def=-1" json:"partition,omitempty"`
	BatchIndex *int32  `protobuf:"varint,4,opt,name=batch_index,json=batchIndex,def=-1" json:"batch_index,omitempty"`
}

func (m *MessageIdData) Reset()         { *m = MessageIdData{} }
func (m *MessageIdData) String() string { return proto.CompactTextString(m) }

func (*MessageIdData) ProtoMessage() {}

// newMessageIDOfPulsar only for test.
func newMessageIDOfPulsar(ledgerID uint64, entryID uint64, batchIdx int32) pulsarID {
	id := &MessageIdData{
		LedgerId:   &ledgerID,
		EntryId:    &entryID,
		BatchIndex: &batchIdx,
	}
	msg, err := proto.Marshal(id)
	if err != nil {
		panic(err)
	}
	msgID, err := pulsar.DeserializeMessageID(msg)
	if err != nil {
		panic(err)
	}

	return pulsarID{
		msgID,
	}
}
