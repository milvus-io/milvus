package segments

import (
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

// ResourceManagerConfig contains configuration for resource management
type ResourceManagerConfig struct {
	// Memory management
	OverloadPercentage float64 // Memory overload threshold (e.g., 0.9 = 90%)
	CacheRatio         float64 // Ratio of memory reserved for evictable data
	
	// Eviction settings
	EvictionEnabled   bool    // Whether eviction is enabled
	LowWatermark      float64 // Low watermark for eviction (e.g., 0.7)
	HighWatermark     float64 // High watermark for eviction (e.g., 0.8)
	EvictionInterval  int     // Eviction check interval in seconds
	
	// Disk management
	DiskCapacityLimit      int64   // Disk capacity limit in bytes
	MaxDiskUsagePercentage float64 // Max disk usage percentage
	
	// Loading behavior
	PreloadInevitableCells bool // Whether to preload inevitable cells
	ConcurrentCellLoads    int  // Number of concurrent cell loads allowed
	
	// Eviction strategy
	EvictionStrategy string // "lru", "lfu", "cost-benefit", "priority", "adaptive"
	
	// Monitoring
	MetricsInterval int // Metrics logging interval in seconds
}

// DefaultResourceManagerConfig returns default configuration
func DefaultResourceManagerConfig() *ResourceManagerConfig {
	return &ResourceManagerConfig{
		OverloadPercentage:     0.9,
		CacheRatio:             0.2,
		EvictionEnabled:        true,
		LowWatermark:           0.7,
		HighWatermark:          0.8,
		EvictionInterval:       3,
		DiskCapacityLimit:      0, // 0 means unlimited
		MaxDiskUsagePercentage: 0.95,
		PreloadInevitableCells: true,
		ConcurrentCellLoads:    4,
		EvictionStrategy:       "lru",
		MetricsInterval:        30,
	}
}

// LoadFromParams loads configuration from paramtable
func LoadResourceManagerConfig() *ResourceManagerConfig {
	params := paramtable.Get()
	config := DefaultResourceManagerConfig()
	
	// Load memory settings
	if v := params.QueryNodeCfg.OverloadedMemoryThresholdPercentage.GetAsFloat(); v > 0 {
		config.OverloadPercentage = v
	}
	
	if v := params.QueryNodeCfg.CacheMemoryRatio.GetAsFloat(); v >= 0 {
		config.CacheRatio = v
	}
	
	// Load eviction settings
	config.EvictionEnabled = params.QueryNodeCfg.LazyLoadEnabled.GetAsBool()
	
	// These would be new parameters that need to be added to paramtable
	// For now, using defaults
	
	// Load disk settings
	if v := params.QueryNodeCfg.DiskCapacityLimit.GetAsInt64(); v > 0 {
		config.DiskCapacityLimit = v
	}
	
	if v := params.QueryNodeCfg.MaxDiskUsagePercentage.GetAsFloat(); v > 0 {
		config.MaxDiskUsagePercentage = v
	}
	
	return config
}

// Validate validates the configuration
func (c *ResourceManagerConfig) Validate() error {
	if c.OverloadPercentage <= 0 || c.OverloadPercentage > 1 {
		return fmt.Errorf("invalid overload percentage: %f", c.OverloadPercentage)
	}
	
	if c.CacheRatio < 0 || c.CacheRatio > 1 {
		return fmt.Errorf("invalid cache ratio: %f", c.CacheRatio)
	}
	
	if c.LowWatermark >= c.HighWatermark {
		return fmt.Errorf("low watermark must be less than high watermark")
	}
	
	if c.LowWatermark <= 0 || c.HighWatermark > 1 {
		return fmt.Errorf("watermarks must be between 0 and 1")
	}
	
	if c.MaxDiskUsagePercentage <= 0 || c.MaxDiskUsagePercentage > 1 {
		return fmt.Errorf("invalid max disk usage percentage: %f", c.MaxDiskUsagePercentage)
	}
	
	return nil
}

// NewSegmentLoadGuardWithConfig creates a new segment load guard with custom config
func NewSegmentLoadGuardWithConfig(ctx context.Context, config *ResourceManagerConfig) (*SegmentLoadGuard, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	
	memoryLimit := uint64(float64(hardware.GetMemoryCount()) * config.OverloadPercentage)
	diskLimit := uint64(float64(config.DiskCapacityLimit) * config.MaxDiskUsagePercentage)
	
	if !config.EvictionEnabled {
		config.CacheRatio = 1.0
	}
	
	ctx, cancel := context.WithCancel(ctx)
	
	guard := &SegmentLoadGuard{
		overloadPercentage: config.OverloadPercentage,
		cacheRatio:         config.CacheRatio,
		evictionEnabled:    config.EvictionEnabled,
		lowWatermark:       config.LowWatermark,
		highWatermark:      config.HighWatermark,
		memoryLimit:        memoryLimit,
		diskLimit:          diskLimit,
		segments:           make(map[UniqueID]*ManagedSegment),
		cells:              make(map[string]*Cell),
		ctx:                ctx,
		cancel:             cancel,
	}
	
	// Set eviction strategy
	switch config.EvictionStrategy {
	case "lru":
		guard.evictionStrategy = NewLRUEvictionStrategy()
	case "lfu":
		guard.evictionStrategy = NewLFUEvictionStrategy()
	case "cost-benefit":
		guard.evictionStrategy = NewCostBenefitEvictionStrategy()
	case "priority":
		guard.evictionStrategy = NewPriorityEvictionStrategy()
	case "adaptive":
		guard.evictionStrategy = NewAdaptiveEvictionStrategy()
	default:
		guard.evictionStrategy = NewLRUEvictionStrategy()
	}
	
	guard.evictionCond = sync.NewCond(&guard.evictionMu)
	
	if config.EvictionEnabled {
		guard.startBackgroundTasks()
	}
	
	return guard, nil
}