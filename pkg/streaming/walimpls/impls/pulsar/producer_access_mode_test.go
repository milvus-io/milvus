package pulsar

import (
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"
)

func TestProducerAccessModeFromConfig(t *testing.T) {
	tests := []struct {
		value    string
		expected pulsar.ProducerAccessMode
		ok       bool
	}{
		{value: "shared", expected: pulsar.ProducerAccessModeShared, ok: true},
		{value: "exclusive", expected: pulsar.ProducerAccessModeExclusive, ok: true},
		{value: "waitForExclusive", expected: pulsar.ProducerAccessModeExclusive, ok: false},
		{value: "", expected: pulsar.ProducerAccessModeExclusive, ok: false},
	}
	for _, test := range tests {
		t.Run(test.value, func(t *testing.T) {
			mode, ok := producerAccessModeFromConfig(test.value)
			assert.Equal(t, test.expected, mode)
			assert.Equal(t, test.ok, ok)
		})
	}
}
