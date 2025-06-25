package segments

import (
	"fmt"
	"sync/atomic"

	"github.com/milvus-io/milvus/internal/util/hardware"
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"
)

// SegmentLoadRequest represents a segment load request
type SegmentLoadRequest struct {
	SegmentID      UniqueID
	MetaSize       uint64
	Cells          []*CellLoadInfo
}

// CellLoadInfo contains information needed to load a cell
type CellLoadInfo struct {
	FieldID    int64
	Size       uint64
	Evictable  bool
}

// SegmentLoad performs shallow load of a segment
func (g *SegmentLoadGuard) SegmentLoad(req *SegmentLoadRequest) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	
	// Check if segment already loaded
	if _, exists := g.segments[req.SegmentID]; exists {
		return fmt.Errorf("segment %d already loaded", req.SegmentID)
	}
	
	// Create managed segment
	segment := NewManagedSegment(req.SegmentID, req.MetaSize)
	
	// Calculate resource requirements
	var evictableSize, inevitableSize uint64
	for _, cellInfo := range req.Cells {
		cellID := fmt.Sprintf("%d_%d", req.SegmentID, cellInfo.FieldID)
		cell := NewCell(cellID, req.SegmentID, cellInfo.FieldID, cellInfo.Size, cellInfo.Evictable)
		segment.AddCell(cell)
		
		if cellInfo.Evictable {
			evictableSize += cellInfo.Size
		} else {
			inevitableSize += cellInfo.Size
		}
	}
	
	// Logical admission control
	segmentMaxUsage := req.MetaSize + inevitableSize + uint64(float64(evictableSize)*g.cacheRatio)
	currentShallowUsage := g.getMemoryShallowUsage()
	
	if currentShallowUsage+segmentMaxUsage > g.memoryLimit {
		g.loadFailCount.Add(1)
		log.Warn("Segment load rejected due to memory limit",
			zap.Uint64("segmentID", req.SegmentID),
			zap.Uint64("currentUsage", currentShallowUsage),
			zap.Uint64("segmentUsage", segmentMaxUsage),
			zap.Uint64("limit", g.memoryLimit))
		return fmt.Errorf("insufficient memory: current=%d, required=%d, limit=%d",
			currentShallowUsage, segmentMaxUsage, g.memoryLimit)
	}
	
	// Physical admission control
	physicalMemory := hardware.GetUsedMemoryCount()
	if physicalMemory+req.MetaSize > g.memoryLimit {
		g.loadFailCount.Add(1)
		log.Warn("Segment load rejected due to physical memory",
			zap.Uint64("segmentID", req.SegmentID),
			zap.Uint64("physicalMemory", physicalMemory),
			zap.Uint64("metaSize", req.MetaSize),
			zap.Uint64("limit", g.memoryLimit))
		return fmt.Errorf("insufficient physical memory")
	}
	
	// Update resource tracking
	g.memoryUsage.ShallowMetaSize.Add(req.MetaSize)
	g.memoryUsage.ShallowInevitable.Add(inevitableSize)
	g.memoryUsage.ShallowEvictable.Add(evictableSize)
	
	// Register segment and cells
	g.segments[req.SegmentID] = segment
	for _, cell := range segment.Cells {
		g.cells[cell.ID] = cell
	}
	
	segment.shallowLoaded.Store(true)
	
	log.Info("Segment shallow loaded",
		zap.Uint64("segmentID", req.SegmentID),
		zap.Uint64("metaSize", req.MetaSize),
		zap.Uint64("evictableSize", evictableSize),
		zap.Uint64("inevitableSize", inevitableSize))
	
	return nil
}

// SegmentRelease releases a segment and all its cells
func (g *SegmentLoadGuard) SegmentRelease(segmentID UniqueID) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	
	segment, exists := g.segments[segmentID]
	if !exists {
		return fmt.Errorf("segment %d not found", segmentID)
	}
	
	// Release all cells
	for _, cell := range segment.Cells {
		if cell.GetState() == CellStateLoaded {
			g.onCellEvictedOrReleased(cell)
		}
		delete(g.cells, cell.ID)
	}
	
	// Update resource tracking
	g.memoryUsage.ShallowMetaSize.Add(^uint64(segment.MetaSize - 1))
	g.memoryUsage.ShallowInevitable.Add(^uint64(segment.InevitableSize - 1))
	g.memoryUsage.ShallowEvictable.Add(^uint64(segment.EvictableSize - 1))
	
	delete(g.segments, segmentID)
	
	log.Info("Segment released",
		zap.Uint64("segmentID", segmentID))
	
	return nil
}

// PermitLoadCell checks if a cell can be loaded
func (g *SegmentLoadGuard) PermitLoadCell(cellID string) (LoadPermit, error) {
	g.mu.RLock()
	cell, exists := g.cells[cellID]
	g.mu.RUnlock()
	
	if !exists {
		return nil, fmt.Errorf("cell %s not found", cellID)
	}
	
	// Check if already loaded or loading
	state := cell.GetState()
	if state == CellStateLoaded {
		g.cacheHitCount.Add(1)
		cell.UpdateAccessTime()
		return &loadPermit{guard: g, cell: cell, granted: false}, nil
	}
	
	if state == CellStateLoading {
		return nil, fmt.Errorf("cell %s is already loading", cellID)
	}
	
	g.cacheMissCount.Add(1)
	
	// Logical check
	logicalUsed := g.getMemoryDeepUsage()
	logicalAvailable := int64(g.memoryLimit) - int64(logicalUsed)
	
	// Physical check
	physicalUsed := hardware.GetUsedMemoryCount()
	physicalAvailable := int64(g.memoryLimit) - int64(physicalUsed)
	
	available := logicalAvailable
	if physicalAvailable < available {
		available = physicalAvailable
	}
	
	// Check if we have enough space
	if available >= int64(cell.Size) {
		// We have space, grant permit
		return g.grantLoadPermit(cell)
	}
	
	// Try eviction if enabled
	if g.evictionEnabled && cell.Evictable {
		needed := int64(cell.Size) - available
		if needed > 0 && g.tryEvict(uint64(needed)) {
			return g.grantLoadPermit(cell)
		}
	}
	
	g.loadFailCount.Add(1)
	return nil, fmt.Errorf("insufficient memory for cell %s: need=%d, available=%d",
		cellID, cell.Size, available)
}

// grantLoadPermit grants a load permit for a cell
func (g *SegmentLoadGuard) grantLoadPermit(cell *Cell) (LoadPermit, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	
	// Double check state
	if cell.GetState() != CellStateNotLoaded {
		return nil, fmt.Errorf("cell state changed")
	}
	
	cell.SetState(CellStateLoading)
	g.memoryUsage.DeepLoading.Add(cell.Size)
	
	return &loadPermit{
		guard:   g,
		cell:    cell,
		granted: true,
	}, nil
}

// LoadPermit represents permission to load a cell
type LoadPermit interface {
	// Commit marks the load as successful
	Commit()
	// Rollback marks the load as failed
	Rollback()
	// IsGranted returns whether the permit was granted
	IsGranted() bool
}

// loadPermit implements LoadPermit
type loadPermit struct {
	guard   *SegmentLoadGuard
	cell    *Cell
	granted bool
}

func (p *loadPermit) IsGranted() bool {
	return p.granted
}

func (p *loadPermit) Commit() {
	if !p.granted {
		return
	}
	
	p.guard.onCellLoaded(p.cell)
}

func (p *loadPermit) Rollback() {
	if !p.granted {
		return
	}
	
	p.guard.onCellLoadFailed(p.cell)
}

// onCellLoaded is called when a cell is successfully loaded
func (g *SegmentLoadGuard) onCellLoaded(cell *Cell) {
	g.memoryUsage.DeepLoading.Add(^uint64(cell.Size - 1)) // Subtract
	
	if cell.Evictable {
		g.memoryUsage.DeepEvictable.Add(cell.Size)
	} else {
		g.memoryUsage.DeepInevitable.Add(cell.Size)
	}
	
	cell.SetState(CellStateLoaded)
	cell.UpdateAccessTime()
	
	log.Debug("Cell loaded",
		zap.String("cellID", cell.ID),
		zap.Uint64("size", cell.Size),
		zap.Bool("evictable", cell.Evictable))
}

// onCellLoadFailed is called when cell loading fails
func (g *SegmentLoadGuard) onCellLoadFailed(cell *Cell) {
	g.memoryUsage.DeepLoading.Add(^uint64(cell.Size - 1)) // Subtract
	cell.SetState(CellStateNotLoaded)
	
	log.Warn("Cell load failed",
		zap.String("cellID", cell.ID))
}

// onCellEvictedOrReleased is called when a cell is evicted or released
func (g *SegmentLoadGuard) onCellEvictedOrReleased(cell *Cell) {
	if cell.Evictable {
		g.memoryUsage.DeepEvictable.Add(^uint64(cell.Size - 1)) // Subtract
	} else {
		g.memoryUsage.DeepInevitable.Add(^uint64(cell.Size - 1)) // Subtract
	}
	
	cell.SetState(CellStateNotLoaded)
}

// PreloadCells attempts to preload inevitable cells for a segment
func (g *SegmentLoadGuard) PreloadCells(segmentID UniqueID) error {
	g.mu.RLock()
	segment, exists := g.segments[segmentID]
	g.mu.RUnlock()
	
	if !exists {
		return fmt.Errorf("segment %d not found", segmentID)
	}
	
	// Preload all inevitable cells
	for _, cell := range segment.Cells {
		if !cell.Evictable && cell.GetState() == CellStateNotLoaded {
			permit, err := g.PermitLoadCell(cell.ID)
			if err != nil {
				log.Warn("Failed to get permit for inevitable cell",
					zap.String("cellID", cell.ID),
					zap.Error(err))
				continue
			}
			
			if permit.IsGranted() {
				// In real implementation, this would trigger actual data loading
				// For now, we just simulate success
				permit.Commit()
			}
		}
	}
	
	return nil
}

// WaitForCellLoad waits for a cell to be loaded
func (g *SegmentLoadGuard) WaitForCellLoad(cellID string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	
	for {
		g.mu.RLock()
		cell, exists := g.cells[cellID]
		g.mu.RUnlock()
		
		if !exists {
			return fmt.Errorf("cell %s not found", cellID)
		}
		
		state := cell.GetState()
		if state == CellStateLoaded {
			return nil
		}
		
		if state == CellStateNotLoaded {
			// Try to load
			permit, err := g.PermitLoadCell(cellID)
			if err != nil {
				return err
			}
			
			if !permit.IsGranted() {
				return fmt.Errorf("failed to get load permit")
			}
			
			// In real implementation, trigger load here
			permit.Commit()
			return nil
		}
		
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for cell load")
		}
		
		time.Sleep(10 * time.Millisecond)
	}
}