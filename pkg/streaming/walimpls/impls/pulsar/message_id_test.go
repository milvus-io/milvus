package pulsar

import (
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
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
