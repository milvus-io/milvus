package pulsar

import "github.com/apache/pulsar-client-go/pulsar"

const (
	producerAccessModeShared    = "shared"
	producerAccessModeExclusive = "exclusive"
)

// producerAccessModeFromConfig maps the value of pulsar.producerAccessMode to the pulsar producer access mode.
// ok is false if the value is unknown, and ProducerAccessModeExclusive is returned.
func producerAccessModeFromConfig(value string) (mode pulsar.ProducerAccessMode, ok bool) {
	switch value {
	case producerAccessModeShared:
		return pulsar.ProducerAccessModeShared, true
	case producerAccessModeExclusive:
		return pulsar.ProducerAccessModeExclusive, true
	default:
		return pulsar.ProducerAccessModeExclusive, false
	}
}
