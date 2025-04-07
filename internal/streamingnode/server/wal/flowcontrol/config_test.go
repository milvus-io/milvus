package flowcontrol

import (
	"os"
	"testing"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/stretchr/testify/assert"
)

func TestNewConfig_ValidConfig(t *testing.T) {
	paramtable.Init()
	_, err := newConfig()
	assert.NoError(t, err)
}

func TestNewConfig_InvalidThrottlingRate(t *testing.T) {
	params := paramtable.Get()
	old := params.StreamingCfg.FlowControlThrottlingWriteRate.SwapTempValue("0")
	defer params.StreamingCfg.FlowControlThrottlingWriteRate.SwapTempValue(old)

	_, err := newConfig()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "throttling rate must be greater than 0")
}

func TestNewConfig_InvalidHwmThreshold(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	old := params.StreamingCfg.FlowControlThrottlingHwmMemoryThreshold.SwapTempValue("1.2")
	defer params.StreamingCfg.FlowControlThrottlingHwmMemoryThreshold.SwapTempValue(old)

	_, err := newConfig()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "hwm threshold must be between 0 and 1")
}

func TestNewConfig_InvalidLwmThreshold(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	old := params.StreamingCfg.FlowControlThrottlingLwmMemoryThreshold.SwapTempValue("1.5")
	defer params.StreamingCfg.FlowControlThrottlingLwmMemoryThreshold.SwapTempValue(old)

	_, err := newConfig()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "lwm threshold must be between 0 and 1")
}

func TestNewConfig_InvalidDenyThreshold(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	old := params.StreamingCfg.FlowControlDenyMemoryThreshold.SwapTempValue("1.5")
	defer params.StreamingCfg.FlowControlDenyMemoryThreshold.SwapTempValue(old)

	_, err := newConfig()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "deny threshold must be between 0 and 1")
}

func TestNewConfig_ThresholdOrder(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	old := params.StreamingCfg.FlowControlDenyMemoryThreshold.SwapTempValue("0.4")
	defer params.StreamingCfg.FlowControlDenyMemoryThreshold.SwapTempValue(old)

	_, err := newConfig()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "lwm threshold must be less than hwm threshold and hwm threshold must be less than deny threshold")
}

func TestMain(m *testing.M) {
	// Initialize the paramtable package
	paramtable.Init()

	// Run the tests
	code := m.Run()
	if code != 0 {
		// Exit with the code returned by the tests
		os.Exit(code)
	}
}
