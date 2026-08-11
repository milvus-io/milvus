package kafka

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestConvertKafkaReadError(t *testing.T) {
	t.Run("topic not found", func(t *testing.T) {
		err := convertKafkaReadError(
			kafka.NewError(kafka.ErrUnknownTopicOrPart, "unknown topic", false),
			"missing-topic",
		)
		require.ErrorIs(t, err, merr.ErrMqTopicNotFound)
	})

	t.Run("unrelated error", func(t *testing.T) {
		expected := errors.New("unrelated error")
		require.Same(t, expected, convertKafkaReadError(expected, "topic"))
	})
}
