package segments

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/milvus-io/milvus/internal/util/hardware"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"go.uber.org/zap"
)

// CellState represents the state of a data cell
type CellState int32

const (
	CellStateNotLoaded CellState = iota
	CellStateLoading
	CellStateLoaded
	CellStateEvicting
)

// Cell represents a unit of data that can be loaded/evicted
type Cell struct {
	ID         string
	SegmentID  UniqueID
	FieldID    int64
	Size       uint64
	Evictable  bool
	state      atomic.Value // CellState
	lastAccess atomic.Value // time.Time
	mu         sync.RWMutex
}

// NewCell creates a new cell
func NewCell(id string, segmentID UniqueID, fieldID int64, size uint64, evictable bool) *Cell {
	c := &Cell{
		ID:        id,
		SegmentID: segmentID,
		FieldID:   fieldID,
		Size:      size,
		Evictable: evictable,
	}
	c.state.Store(CellStateNotLoaded)
	c.lastAccess.Store(time.Now())
	return c
}

// GetState returns the current state of the cell
func (c *Cell) GetState() CellState {
	return c.state.Load().(CellState)
}

// SetState updates the state of the cell
func (c *Cell) SetState(state CellState) {
	c.state.Store(state)
}

// UpdateAccessTime updates the last access time
func (c *Cell) UpdateAccessTime() {
	c.lastAccess.Store(time.Now())
}

// GetAccessTime returns the last access time
func (c *Cell) GetAccessTime() time.Time {
	return c.lastAccess.Load().(time.Time)
}

// ManagedSegment wraps a segment with resource management capabilities
type ManagedSegment struct {
	ID              UniqueID
	MetaSize        uint64
	Cells           []*Cell
	EvictableSize   uint64
	InevitableSize  uint64
	shallowLoaded   atomic.Bool
	mu              sync.RWMutex
}

// NewManagedSegment creates a new managed segment
func NewManagedSegment(id UniqueID, metaSize uint64) *ManagedSegment {
	return &ManagedSegment{
		ID:       id,
		MetaSize: metaSize,
		Cells:    make([]*Cell, 0),
	}
}

// AddCell adds a cell to the segment
func (s *ManagedSegment) AddCell(cell *Cell) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	s.Cells = append(s.Cells, cell)
	if cell.Evictable {
		s.EvictableSize += cell.Size
	} else {
		s.InevitableSize += cell.Size
	}
}

// ResourceUsage tracks resource usage
type ResourceUsage struct {
	// Shallow loaded resources (segments marked as loaded but data not yet loaded)
	ShallowEvictable   atomic.Uint64
	ShallowInevitable  atomic.Uint64
	ShallowMetaSize    atomic.Uint64
	
	// Deep loaded resources (actual data loaded)
	DeepEvictable      atomic.Uint64
	DeepInevitable     atomic.Uint64
	
	// Loading resources (currently being loaded)
	DeepLoading        atomic.Uint64
}

// SegmentLoadGuard manages segment loading with resource protection
type SegmentLoadGuard struct {
	// Configuration
	overloadPercentage float64
	cacheRatio         float64
	evictionEnabled    bool
	lowWatermark       float64
	highWatermark      float64
	
	// Resource limits
	memoryLimit uint64
	diskLimit   uint64
	
	// Resource tracking
	memoryUsage ResourceUsage
	diskUsage   ResourceUsage
	
	// Segment management
	segments map[UniqueID]*ManagedSegment
	cells    map[string]*Cell // cellID -> Cell
	
	// Synchronization
	mu              sync.RWMutex
	evictionMu      sync.Mutex
	evictionCond    *sync.Cond
	
	// Background tasks
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	
	// Eviction strategy
	evictionStrategy EvictionStrategy
	
	// Metrics
	evictionCount    atomic.Uint64
	loadFailCount    atomic.Uint64
	cacheHitCount    atomic.Uint64
	cacheMissCount   atomic.Uint64
}

// NewSegmentLoadGuard creates a new segment load guard
func NewSegmentLoadGuard(ctx context.Context) *SegmentLoadGuard {
	params := paramtable.Get()
	
	overloadPercentage := params.QueryNodeCfg.OverloadedMemoryThresholdPercentage.GetAsFloat()
	cacheRatio := params.QueryNodeCfg.CacheMemoryRatio.GetAsFloat()
	evictionEnabled := params.QueryNodeCfg.LazyLoadEnabled.GetAsBool()
	
	if !evictionEnabled {
		cacheRatio = 1.0
	}
	
	memoryLimit := uint64(float64(hardware.GetMemoryCount()) * overloadPercentage)
	diskLimit := uint64(float64(params.QueryNodeCfg.DiskCapacityLimit.GetAsInt64()) * 
		params.QueryNodeCfg.MaxDiskUsagePercentage.GetAsFloat())
	
	ctx, cancel := context.WithCancel(ctx)
	
	guard := &SegmentLoadGuard{
		overloadPercentage: overloadPercentage,
		cacheRatio:         cacheRatio,
		evictionEnabled:    evictionEnabled,
		lowWatermark:       0.7,
		highWatermark:      0.8,
		memoryLimit:        memoryLimit,
		diskLimit:          diskLimit,
		segments:           make(map[UniqueID]*ManagedSegment),
		cells:              make(map[string]*Cell),
		ctx:                ctx,
		cancel:             cancel,
		evictionStrategy:   NewLRUEvictionStrategy(),
	}
	
	guard.evictionCond = sync.NewCond(&guard.evictionMu)
	
	if evictionEnabled {
		guard.startBackgroundTasks()
	}
	
	return guard
}

// Close stops the background tasks
func (g *SegmentLoadGuard) Close() {
	g.cancel()
	g.evictionCond.Broadcast()
	g.wg.Wait()
}

// startBackgroundTasks starts background eviction and monitoring
func (g *SegmentLoadGuard) startBackgroundTasks() {
	// Eviction loop
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		g.evictionLoop()
	}()
	
	// Metrics collection
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		g.metricsLoop()
	}()
}

// evictionLoop runs periodic eviction checks
func (g *SegmentLoadGuard) evictionLoop() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-g.ctx.Done():
			return
		case <-ticker.C:
			g.checkAndEvict()
		}
	}
}

// metricsLoop collects and logs metrics
func (g *SegmentLoadGuard) metricsLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-g.ctx.Done():
			return
		case <-ticker.C:
			g.logMetrics()
		}
	}
}

// getMemoryShallowUsage calculates shallow memory usage
func (g *SegmentLoadGuard) getMemoryShallowUsage() uint64 {
	metaSize := g.memoryUsage.ShallowMetaSize.Load()
	inevitable := g.memoryUsage.ShallowInevitable.Load()
	evictable := g.memoryUsage.ShallowEvictable.Load()
	
	// Only count meta size for shallow usage
	// The actual data usage will be counted when deep loaded
	return metaSize + inevitable + uint64(float64(evictable)*g.cacheRatio)
}

// getMemoryDeepUsage calculates deep memory usage
func (g *SegmentLoadGuard) getMemoryDeepUsage() uint64 {
	inevitable := g.memoryUsage.DeepInevitable.Load()
	evictable := g.memoryUsage.DeepEvictable.Load()
	loading := g.memoryUsage.DeepLoading.Load()
	
	return inevitable + evictable + loading
}

// getAvailableMemoryForCache returns available memory for cache
func (g *SegmentLoadGuard) getAvailableMemoryForCache() uint64 {
	deepInevitable := g.memoryUsage.DeepInevitable.Load()
	if g.memoryLimit > deepInevitable {
		return g.memoryLimit - deepInevitable
	}
	return 0
}

// checkAndEvict performs eviction if needed
func (g *SegmentLoadGuard) checkAndEvict() {
	available := g.getAvailableMemoryForCache()
	if available == 0 {
		return
	}
	
	highWatermark := uint64(float64(available) * g.highWatermark)
	evictable := g.memoryUsage.DeepEvictable.Load()
	
	if evictable > highWatermark {
		lowWatermark := uint64(float64(available) * g.lowWatermark)
		targetEviction := evictable - lowWatermark
		
		log.Info("Starting eviction",
			zap.Uint64("evictable", evictable),
			zap.Uint64("highWatermark", highWatermark),
			zap.Uint64("targetEviction", targetEviction))
		
		g.tryEvict(targetEviction)
	}
}

// tryEvict attempts to evict the specified amount of memory
func (g *SegmentLoadGuard) tryEvict(targetBytes uint64) bool {
	g.evictionMu.Lock()
	defer g.evictionMu.Unlock()
	
	cells := g.getEvictableCells()
	candidates := g.evictionStrategy.SelectVictims(cells, targetBytes)
	
	evicted := uint64(0)
	for _, cell := range candidates {
		if g.evictCell(cell) {
			evicted += cell.Size
			if evicted >= targetBytes {
				break
			}
		}
	}
	
	if evicted > 0 {
		g.evictionCount.Add(1)
		log.Info("Eviction completed",
			zap.Uint64("targetBytes", targetBytes),
			zap.Uint64("evictedBytes", evicted),
			zap.Int("evictedCells", len(candidates)))
	}
	
	return evicted >= targetBytes
}

// getEvictableCells returns all evictable cells that are loaded
func (g *SegmentLoadGuard) getEvictableCells() []*Cell {
	g.mu.RLock()
	defer g.mu.RUnlock()
	
	cells := make([]*Cell, 0, len(g.cells))
	for _, cell := range g.cells {
		if cell.Evictable && cell.GetState() == CellStateLoaded {
			cells = append(cells, cell)
		}
	}
	
	return cells
}

// evictCell evicts a single cell
func (g *SegmentLoadGuard) evictCell(cell *Cell) bool {
	// TODO: Implement actual eviction logic
	// This would interact with the caching layer to remove the cell from memory
	
	cell.SetState(CellStateEvicting)
	
	// Simulate eviction
	cell.SetState(CellStateNotLoaded)
	g.memoryUsage.DeepEvictable.Add(^uint64(cell.Size - 1)) // Subtract
	
	return true
}

// logMetrics logs resource usage metrics
func (g *SegmentLoadGuard) logMetrics() {
	stats := g.GetResourceStats()
	log.Info("Resource usage statistics",
		zap.Any("stats", stats))
}

// GetResourceStats returns current resource statistics
func (g *SegmentLoadGuard) GetResourceStats() map[string]interface{} {
	memoryUsage := g.getMemoryDeepUsage()
	memoryLimit := g.memoryLimit
	
	cacheHitRate := float64(0)
	totalAccess := g.cacheHitCount.Load() + g.cacheMissCount.Load()
	if totalAccess > 0 {
		cacheHitRate = float64(g.cacheHitCount.Load()) / float64(totalAccess)
	}
	
	return map[string]interface{}{
		"memory_limit":         memoryLimit,
		"memory_usage":         memoryUsage,
		"memory_usage_percent": float64(memoryUsage) / float64(memoryLimit) * 100,
		"shallow_meta_size":    g.memoryUsage.ShallowMetaSize.Load(),
		"shallow_inevitable":   g.memoryUsage.ShallowInevitable.Load(),
		"shallow_evictable":    g.memoryUsage.ShallowEvictable.Load(),
		"deep_inevitable":      g.memoryUsage.DeepInevitable.Load(),
		"deep_evictable":       g.memoryUsage.DeepEvictable.Load(),
		"deep_loading":         g.memoryUsage.DeepLoading.Load(),
		"eviction_count":       g.evictionCount.Load(),
		"load_fail_count":      g.loadFailCount.Load(),
		"cache_hit_rate":       cacheHitRate,
		"segment_count":        len(g.segments),
		"cell_count":           len(g.cells),
	}
}