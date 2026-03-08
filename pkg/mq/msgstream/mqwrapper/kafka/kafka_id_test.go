package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKafkaID_Serialize(t *testing.T) {
	rid := &KafkaID{MessageID: 8}
	bin := rid.Serialize()
	assert.NotNil(t, bin)
	assert.NotZero(t, len(bin))
}

func TestKafkaID_AtEarliestPosition(t *testing.T) {
	rid := &KafkaID{MessageID: 8}
	assert.False(t, rid.AtEarliestPosition())

	rid = &KafkaID{MessageID: 0}
	assert.True(t, rid.AtEarliestPosition())
}

func TestKafkaID_LessOrEqualThan(t *testing.T) {
	{
		rid1 := &KafkaID{MessageID: 8}
		rid2 := &KafkaID{MessageID: 0}
		ret, err := rid1.LessOrEqualThan(rid2.Serialize())
		assert.NoError(t, err)
		assert.False(t, ret)

		ret, err = rid2.LessOrEqualThan(rid1.Serialize())
		assert.NoError(t, err)
		assert.True(t, ret)
	}

	{
		rid1 := &KafkaID{MessageID: 0}
		rid2 := &KafkaID{MessageID: 0}
		ret, err := rid1.LessOrEqualThan(rid2.Serialize())
		assert.NoError(t, err)
		assert.True(t, ret)
	}
}

func TestKafkaID_Equal(t *testing.T) {
	rid1 := &KafkaID{MessageID: 0}
	rid2 := &KafkaID{MessageID: 1}

	{
		ret, err := rid1.Equal(rid1.Serialize())
		assert.NoError(t, err)
		assert.True(t, ret)
	}

	{
		ret, err := rid1.Equal(rid2.Serialize())
		assert.NoError(t, err)
		assert.False(t, ret)
	}
}

func Test_SerializeKafkaID(t *testing.T) {
	bin := SerializeKafkaID(10)
	assert.NotNil(t, bin)
	assert.NotZero(t, len(bin))
}

func Test_DeserializeKafkaID(t *testing.T) {
	bin := SerializeKafkaID(5)
	id := DeserializeKafkaID(bin)
	assert.Equal(t, id, int64(5))
}
