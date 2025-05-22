package wp

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	woodpecker "github.com/zilliztech/woodpecker/woodpecker/log"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

func TestMessageID(t *testing.T) {
	wpId := message.MessageID(newMessageIDOfWoodpecker(1, 2)).(interface {
		WoodpeckerID() *woodpecker.LogMessageId
	}).WoodpeckerID()
	assert.Equal(t, WALName, newMessageIDOfWoodpecker(1, 2).WALName())

	assert.Equal(t, int64(1), wpId.SegmentId)
	assert.Equal(t, int64(2), wpId.EntryId)

	ids := []wpID{
		newMessageIDOfWoodpecker(0, 0),
		newMessageIDOfWoodpecker(0, 1),
		newMessageIDOfWoodpecker(0, 1000),
		newMessageIDOfWoodpecker(1, 0),
		newMessageIDOfWoodpecker(1, 1),
		newMessageIDOfWoodpecker(1, 1000),
		newMessageIDOfWoodpecker(2, 0),
		newMessageIDOfWoodpecker(2, 1),
		newMessageIDOfWoodpecker(2, 1000),
	}

	for x, idx := range ids {
		for y, idy := range ids {
			assert.Equal(t, idx.EQ(idy), x == y, fmt.Sprintf("expected %v  when idx:%v,idy:%v", idx.EQ(idy), idx, idy))
			assert.Equal(t, idy.EQ(idx), x == y, fmt.Sprintf("expected %v  when idx:%v,idy:%v", idy.EQ(idx), idx, idy))

			assert.Equal(t, idy.LT(idx), x > y, fmt.Sprintf("expected %v  when idx:%v,idy:%v", idy.LT(idx), idx, idy))
			assert.Equal(t, idy.LTE(idx), x >= y, fmt.Sprintf("expected %v  when idx:%v,idy:%v", idy.LTE(idx), idx, idy))

			assert.Equal(t, idx.LT(idy), x < y, fmt.Sprintf("expected %v  when idx:%v,idy:%v", idx.LT(idy), idx, idy))
			assert.Equal(t, idx.LTE(idy), x <= y, fmt.Sprintf("expected %v  when idx:%v,idy:%v", idx.LTE(idy), idx, idy))
		}
	}

	msgID, err := UnmarshalMessageID(newMessageIDOfWoodpecker(1, 2).Marshal())
	assert.NoError(t, err)
	assert.True(t, msgID.EQ(newMessageIDOfWoodpecker(1, 2)))

	_, err = UnmarshalMessageID(string([]byte{0x01, 0x02, 0x03, 0x04}))
	assert.Error(t, err)
}

// newMessageIDOfWoodpecker only for test.
func newMessageIDOfWoodpecker(segmentID int64, entryID int64) wpID {
	id := wpID{
		logMsgId: &woodpecker.LogMessageId{
			SegmentId: segmentID,
			EntryId:   entryID,
		},
	}
	return id
}
