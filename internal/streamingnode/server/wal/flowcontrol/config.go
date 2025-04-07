package flowcontrol

import (
	"fmt"

	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// newConfig creates a new configuration for the flow control manager.
func newConfig() (config, error) {
	params := paramtable.Get()
	defaultRate := float64(params.StreamingCfg.FlowControlDefaultWriteRate.GetAsSize())
	throttlingRate := params.StreamingCfg.FlowControlThrottlingWriteRate.GetAsSize()
	hwmThreshold := params.StreamingCfg.FlowControlThrottlingHwmMemoryThreshold.GetAsFloat()
	lwmThreshold := params.StreamingCfg.FlowControlThrottlingLwmMemoryThreshold.GetAsFloat()
	denyThreshold := params.StreamingCfg.FlowControlDenyMemoryThreshold.GetAsFloat()

	if defaultRate <= 0 {
		defaultRate = float64(rate.Inf)
	}
	if throttlingRate <= 0 {
		return config{}, fmt.Errorf("throttling rate must be greater than 0")
	}
	if hwmThreshold <= 0 || hwmThreshold >= 1 {
		return config{}, fmt.Errorf("hwm threshold must be between 0 and 1")
	}
	if lwmThreshold <= 0 || lwmThreshold >= 1 {
		return config{}, fmt.Errorf("lwm threshold must be between 0 and 1")
	}
	if denyThreshold <= 0 || denyThreshold >= 1 {
		return config{}, fmt.Errorf("deny threshold must be between 0 and 1")
	}
	if lwmThreshold >= hwmThreshold || hwmThreshold >= denyThreshold {
		return config{}, fmt.Errorf("lwm threshold must be less than hwm threshold and hwm threshold must be less than deny threshold")
	}
	return config{
		DefaultRate:    rate.Limit(defaultRate),
		ThrottlingRate: rate.Limit(throttlingRate),
		HwmThreshold:   hwmThreshold,
		LwmThreshold:   lwmThreshold,
		DenyThreshold:  denyThreshold,
	}, nil
}

// config holds the configuration for the flow control manager.
type config struct {
	DefaultRate    rate.Limit
	ThrottlingRate rate.Limit
	HwmThreshold   float64
	LwmThreshold   float64
	DenyThreshold  float64
}
