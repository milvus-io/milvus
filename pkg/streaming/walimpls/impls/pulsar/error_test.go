package pulsar

import (
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestConvertPulsarReadError(t *testing.T) {
	t.Run("topic not found", func(t *testing.T) {
		err := convertPulsarReadError(pulsar.ErrTopicNotfound, "missing-topic")
		require.ErrorIs(t, err, merr.ErrMqTopicNotFound)
	})

	t.Run("unrelated error", func(t *testing.T) {
		expected := errors.New("unrelated error")
		require.Same(t, expected, convertPulsarReadError(expected, "topic"))
	})
}
