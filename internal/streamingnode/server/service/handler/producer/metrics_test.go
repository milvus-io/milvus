package producer

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

func TestNewProducerMetrics(t *testing.T) {
	pchannel := types.PChannelInfo{
		Name: "test-channel",
		Term: 1,
	}

	m := newProducerMetrics(pchannel)
	assert.NotNil(t, m)
	assert.NotNil(t, m.produceTotal)
	assert.NotNil(t, m.inflightTotal)
}

func TestProducerMetrics_StartProduce(t *testing.T) {
	pchannel := types.PChannelInfo{
		Name: "test-channel",
		Term: 1,
	}

	m := newProducerMetrics(pchannel)

	// Start produce should return a guard
	guard := m.StartProduce()
	assert.NotNil(t, guard)
	assert.NotNil(t, guard.metrics)
	assert.Equal(t, m, guard.metrics)
}

func TestProducerMetrics_Close(t *testing.T) {
	pchannel := types.PChannelInfo{
		Name: "test-channel",
		Term: 1,
	}

	m := newProducerMetrics(pchannel)

	// Should not panic
	m.Close()
}

func TestProduceMetricsGuard_Finish(t *testing.T) {
	pchannel := types.PChannelInfo{
		Name: "test-channel",
		Term: 1,
	}

	m := newProducerMetrics(pchannel)
	guard := m.StartProduce()

	// Test finish with no error
	guard.Finish(nil)

	// Test finish with error
	guard2 := m.StartProduce()
	guard2.Finish(errors.New("test error"))
}

func TestProducerMetrics_Lifecycle(t *testing.T) {
	pchannel := types.PChannelInfo{
		Name: "test-channel",
		Term: 1,
	}

	m := newProducerMetrics(pchannel)

	// Simulate multiple produce operations
	guard1 := m.StartProduce()
	guard2 := m.StartProduce()

	// Finish them
	guard1.Finish(nil)
	guard2.Finish(errors.New("test error"))

	// Close the producer
	m.Close()
}
