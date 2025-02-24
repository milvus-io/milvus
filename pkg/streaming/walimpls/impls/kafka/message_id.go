package kafka

import (
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

func UnmarshalMessageID(data string) (message.MessageID, error) {
	id, err := unmarshalMessageID(data)
	if err != nil {
		return nil, err
	}
	return id, nil
}

func unmarshalMessageID(data string) (kafkaID, error) {
	v, err := message.DecodeUint64(data)
	if err != nil {
		return 0, errors.Wrapf(message.ErrInvalidMessageID, "decode kafkaID fail with err: %s, id: %s", err.Error(), data)
	}
	return kafkaID(v), nil
}

func NewKafkaID(offset kafka.Offset) message.MessageID {
	return kafkaID(offset)
}

type kafkaID kafka.Offset

// RmqID returns the message id for conversion
// Don't delete this function until conversion logic removed.
// TODO: remove in future.
func (id kafkaID) KafkaID() kafka.Offset {
	return kafka.Offset(id)
}

// WALName returns the name of message id related wal.
func (id kafkaID) WALName() string {
	return walName
}

// LT less than.
func (id kafkaID) LT(other message.MessageID) bool {
	return id < other.(kafkaID)
}

// LTE less than or equal to.
func (id kafkaID) LTE(other message.MessageID) bool {
	return id <= other.(kafkaID)
}

// EQ Equal to.
func (id kafkaID) EQ(other message.MessageID) bool {
	return id == other.(kafkaID)
}

// Marshal marshal the message id.
func (id kafkaID) Marshal() string {
	return message.EncodeInt64(int64(id))
}

func (id kafkaID) String() string {
	return strconv.FormatInt(int64(id), 10)
}
