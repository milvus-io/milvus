package pulsar

import (
	"github.com/apache/pulsar-client-go/pulsar"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const (
	producerAccessModeShared               = "shared"
	producerAccessModeExclusive            = "exclusive"
	producerAccessModeExclusiveWithFencing = paramtable.PulsarProducerAccessModeExclusiveWithFencing
)

// producerAccessModeFromConfig maps the value of pulsar.producerAccessMode to the pulsar producer access mode.
// ok is false if the value is unknown, and ProducerAccessModeExclusive is returned.
func producerAccessModeFromConfig(value string) (mode pulsar.ProducerAccessMode, ok bool) {
	switch value {
	case producerAccessModeShared:
		return pulsar.ProducerAccessModeShared, true
	case producerAccessModeExclusive:
		return pulsar.ProducerAccessModeExclusive, true
	case producerAccessModeExclusiveWithFencing:
		return pulsar.ProducerAccessModeExclusiveWithFencing, true
	default:
		return pulsar.ProducerAccessModeExclusive, false
	}
}
