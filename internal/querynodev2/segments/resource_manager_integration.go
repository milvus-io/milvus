package segments

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"go.uber.org/zap"
)

// ResourceManagedLoader wraps the existing loader with resource management
type ResourceManagedLoader struct {
	loader    Loader
	guard     *SegmentLoadGuard
	manager   *Manager
	
	// Cell loading implementation
	cellLoaders map[string]CellLoader // fieldType -> loader
	
	mu sync.RWMutex
}

// CellLoader defines the interface for loading specific types of cells
type CellLoader interface {
	Load(ctx context.Context, segment Segment, cell *Cell, loadInfo *querypb.SegmentLoadInfo) error
	Evict(ctx context.Context, segment Segment, cell *Cell) error
}

// NewResourceManagedLoader creates a new resource managed loader
func NewResourceManagedLoader(
	ctx context.Context,
	manager *Manager,
	cm storage.ChunkManager,
) (*ResourceManagedLoader, error) {
	loader, err := NewLoader(manager, cm)
	if err != nil {
		return nil, err
	}
	
	guard := NewSegmentLoadGuard(ctx)
	
	rml := &ResourceManagedLoader{
		loader:      loader,
		guard:       guard,
		manager:     manager,
		cellLoaders: make(map[string]CellLoader),
	}
	
	// Register cell loaders for different field types
	rml.RegisterCellLoaders()
	
	return rml, nil
}

// RegisterCellLoaders registers loaders for different field types
func (rml *ResourceManagedLoader) RegisterCellLoaders() {
	// Vector field loader
	rml.cellLoaders["vector"] = &VectorCellLoader{loader: rml.loader}
	
	// Scalar field loader
	rml.cellLoaders["scalar"] = &ScalarCellLoader{loader: rml.loader}
	
	// Stats loader
	rml.cellLoaders["stats"] = &StatsCellLoader{loader: rml.loader}
	
	// Delta loader
	rml.cellLoaders["delta"] = &DeltaCellLoader{loader: rml.loader}
}

// Load loads segments with resource management
func (rml *ResourceManagedLoader) Load(
	ctx context.Context,
	collectionID int64,
	segmentType SegmentType,
	version int64,
	infos ...*querypb.SegmentLoadInfo,
) ([]Segment, error) {
	// First, perform shallow load for all segments
	loadRequests := make([]*SegmentLoadRequest, 0, len(infos))
	
	for _, info := range infos {
		req, err := rml.buildLoadRequest(info)
		if err != nil {
			log.Warn("Failed to build load request",
				zap.Int64("segmentID", info.GetSegmentID()),
				zap.Error(err))
			continue
		}
		
		loadRequests = append(loadRequests, req)
	}
	
	// Shallow load all segments
	successfulSegments := make([]*querypb.SegmentLoadInfo, 0, len(loadRequests))
	for i, req := range loadRequests {
		err := rml.guard.SegmentLoad(req)
		if err != nil {
			log.Warn("Failed to shallow load segment",
				zap.Int64("segmentID", req.SegmentID),
				zap.Error(err))
			// Clean up previously loaded segments on failure
			for j := 0; j < i; j++ {
				rml.guard.SegmentRelease(loadRequests[j].SegmentID)
			}
			return nil, err
		}
		successfulSegments = append(successfulSegments, infos[i])
	}
	
	// Create segment objects
	segments := make([]Segment, 0, len(successfulSegments))
	for _, info := range successfulSegments {
		segment, err := rml.createSegment(ctx, collectionID, segmentType, version, info)
		if err != nil {
			log.Warn("Failed to create segment",
				zap.Int64("segmentID", info.GetSegmentID()),
				zap.Error(err))
			continue
		}
		segments = append(segments, segment)
	}
	
	// Preload inevitable cells if configured
	if paramtable.Get().QueryNodeCfg.PreloadInevitableCells.GetAsBool() {
		for _, info := range successfulSegments {
			go func(segmentID int64) {
				if err := rml.guard.PreloadCells(segmentID); err != nil {
					log.Warn("Failed to preload inevitable cells",
						zap.Int64("segmentID", segmentID),
						zap.Error(err))
				}
			}(info.GetSegmentID())
		}
	}
	
	return segments, nil
}

// buildLoadRequest builds a segment load request from load info
func (rml *ResourceManagedLoader) buildLoadRequest(info *querypb.SegmentLoadInfo) (*SegmentLoadRequest, error) {
	// Estimate resource usage
	resource, err := rml.loader.(*segmentLoader).getResourceUsageEstimateOfSegment(info)
	if err != nil {
		return nil, err
	}
	
	// Build cells
	cells := make([]*CellLoadInfo, 0)
	
	// Add field cells
	for fieldID, fieldInfo := range info.GetIndexedFieldInfos() {
		evictable := rml.isFieldEvictable(fieldID, info)
		size := rml.estimateFieldSize(fieldInfo)
		
		cells = append(cells, &CellLoadInfo{
			FieldID:   fieldID,
			Size:      size,
			Evictable: evictable,
		})
	}
	
	// Add stats cells
	if len(info.GetStatslogs()) > 0 {
		statsSize := uint64(0)
		for _, statslog := range info.GetStatslogs() {
			statsSize += uint64(getBinlogDataMemorySize(statslog))
		}
		
		cells = append(cells, &CellLoadInfo{
			FieldID:   -1, // Special ID for stats
			Size:      statsSize,
			Evictable: false, // Stats are not evictable
		})
	}
	
	// Add delta cells
	if len(info.GetDeltalogs()) > 0 {
		deltaSize := uint64(0)
		for _, deltalog := range info.GetDeltalogs() {
			deltaSize += uint64(getBinlogDataMemorySize(deltalog))
		}
		
		cells = append(cells, &CellLoadInfo{
			FieldID:   -2, // Special ID for delta
			Size:      deltaSize,
			Evictable: false, // Delta logs are not evictable
		})
	}
	
	// Calculate meta size (rough estimate)
	metaSize := uint64(1024 * 1024) // 1MB base
	metaSize += uint64(len(cells)) * 1024 // 1KB per field
	
	return &SegmentLoadRequest{
		SegmentID: info.GetSegmentID(),
		MetaSize:  metaSize,
		Cells:     cells,
	}, nil
}

// isFieldEvictable checks if a field can be evicted
func (rml *ResourceManagedLoader) isFieldEvictable(fieldID int64, info *querypb.SegmentLoadInfo) bool {
	// System fields are not evictable
	if fieldID == common.TimeStampField || fieldID == common.RowIDField {
		return false
	}
	
	// Check field-specific configuration
	schema := info.GetSchema()
	for _, field := range schema.GetFields() {
		if field.GetFieldID() == fieldID {
			// Check if field has specific eviction settings
			if field.GetTypeParams() != nil {
				for _, param := range field.GetTypeParams() {
					if param.GetKey() == "evictable" && param.GetValue() == "false" {
						return false
					}
				}
			}
			
			// By default, data fields are evictable if eviction is enabled
			return paramtable.Get().QueryNodeCfg.LazyLoadEnabled.GetAsBool()
		}
	}
	
	return false
}

// estimateFieldSize estimates the size of a field
func (rml *ResourceManagedLoader) estimateFieldSize(fieldInfo *querypb.FieldIndexInfo) uint64 {
	size := uint64(0)
	
	// Add binlog size
	if fieldInfo.GetFieldBinlog() != nil {
		for _, binlog := range fieldInfo.GetFieldBinlog().GetBinlogs() {
			size += uint64(binlog.GetMemorySize())
		}
	}
	
	// Add index size if present
	if fieldInfo.GetIndexInfo() != nil {
		// This would need to call C++ to get accurate estimate
		// For now, use a rough estimate
		size += uint64(fieldInfo.GetIndexInfo().GetIndexSize())
	}
	
	return size
}

// createSegment creates a segment with resource management hooks
func (rml *ResourceManagedLoader) createSegment(
	ctx context.Context,
	collectionID int64,
	segmentType SegmentType,
	version int64,
	info *querypb.SegmentLoadInfo,
) (Segment, error) {
	// Create a wrapped segment that integrates with resource management
	segment := &ResourceManagedSegment{
		segmentID:    info.GetSegmentID(),
		collectionID: collectionID,
		segmentType:  segmentType,
		version:      version,
		loadInfo:     info,
		rml:          rml,
		cellStates:   make(map[string]bool),
	}
	
	// Initialize the underlying segment
	// This would create the actual C++ segment object
	err := segment.Init()
	if err != nil {
		return nil, err
	}
	
	return segment, nil
}

// ResourceManagedSegment wraps a segment with resource management
type ResourceManagedSegment struct {
	segmentID    int64
	collectionID int64
	segmentType  SegmentType
	version      int64
	loadInfo     *querypb.SegmentLoadInfo
	rml          *ResourceManagedLoader
	
	underlying   Segment // The actual segment implementation
	cellStates   map[string]bool // cellID -> loaded
	mu           sync.RWMutex
}

// Init initializes the segment
func (s *ResourceManagedSegment) Init() error {
	// Create the underlying segment
	// This is simplified - actual implementation would create LocalSegment
	s.underlying = NewSegment(
		s.collectionID,
		s.segmentID,
		s.segmentType,
		s.version,
		s.loadInfo,
	)
	
	return nil
}

// Lazy loading implementation - loads cell on demand
func (s *ResourceManagedSegment) ensureCellLoaded(fieldID int64) error {
	cellID := fmt.Sprintf("%d_%d", s.segmentID, fieldID)
	
	s.mu.RLock()
	loaded := s.cellStates[cellID]
	s.mu.RUnlock()
	
	if loaded {
		// Update access time
		if cell, exists := s.rml.guard.cells[cellID]; exists {
			cell.UpdateAccessTime()
		}
		return nil
	}
	
	// Get load permit
	permit, err := s.rml.guard.PermitLoadCell(cellID)
	if err != nil {
		return err
	}
	
	if !permit.IsGranted() {
		// Cell already loaded by another goroutine
		return nil
	}
	
	// Load the cell
	err = s.loadCell(fieldID)
	if err != nil {
		permit.Rollback()
		return err
	}
	
	permit.Commit()
	
	s.mu.Lock()
	s.cellStates[cellID] = true
	s.mu.Unlock()
	
	return nil
}

// loadCell loads a specific cell
func (s *ResourceManagedSegment) loadCell(fieldID int64) error {
	// Determine cell type and get appropriate loader
	var loader CellLoader
	
	if fieldID == -1 {
		loader = s.rml.cellLoaders["stats"]
	} else if fieldID == -2 {
		loader = s.rml.cellLoaders["delta"]
	} else {
		// Check field type
		schema := s.loadInfo.GetSchema()
		for _, field := range schema.GetFields() {
			if field.GetFieldID() == fieldID {
				if typeutil.IsVectorType(field.GetDataType()) {
					loader = s.rml.cellLoaders["vector"]
				} else {
					loader = s.rml.cellLoaders["scalar"]
				}
				break
			}
		}
	}
	
	if loader == nil {
		return fmt.Errorf("no loader found for field %d", fieldID)
	}
	
	cellID := fmt.Sprintf("%d_%d", s.segmentID, fieldID)
	cell := s.rml.guard.cells[cellID]
	
	return loader.Load(context.Background(), s.underlying, cell, s.loadInfo)
}

// Implement Segment interface methods with lazy loading
func (s *ResourceManagedSegment) ID() int64 {
	return s.segmentID
}

func (s *ResourceManagedSegment) Collection() int64 {
	return s.collectionID
}

func (s *ResourceManagedSegment) Type() SegmentType {
	return s.segmentType
}

func (s *ResourceManagedSegment) Search(ctx context.Context, searchReq *SearchRequest) (*SearchResult, error) {
	// Ensure required fields are loaded
	for _, fieldID := range searchReq.RequiredFieldIDs() {
		if err := s.ensureCellLoaded(fieldID); err != nil {
			return nil, err
		}
	}
	
	return s.underlying.Search(ctx, searchReq)
}

// Close releases the segment
func (s *ResourceManagedSegment) Close() {
	s.rml.guard.SegmentRelease(s.segmentID)
	if s.underlying != nil {
		s.underlying.Close()
	}
}

// VectorCellLoader loads vector fields
type VectorCellLoader struct {
	loader Loader
}

func (l *VectorCellLoader) Load(ctx context.Context, segment Segment, cell *Cell, loadInfo *querypb.SegmentLoadInfo) error {
	// Implementation would load vector data
	// This is simplified - actual implementation would interact with C++ layer
	log.Info("Loading vector cell",
		zap.String("cellID", cell.ID),
		zap.Int64("fieldID", cell.FieldID))
	
	// Simulate loading
	time.Sleep(100 * time.Millisecond)
	
	return nil
}

func (l *VectorCellLoader) Evict(ctx context.Context, segment Segment, cell *Cell) error {
	// Implementation would evict vector data
	log.Info("Evicting vector cell",
		zap.String("cellID", cell.ID))
	
	return nil
}

// ScalarCellLoader loads scalar fields
type ScalarCellLoader struct {
	loader Loader
}

func (l *ScalarCellLoader) Load(ctx context.Context, segment Segment, cell *Cell, loadInfo *querypb.SegmentLoadInfo) error {
	log.Info("Loading scalar cell",
		zap.String("cellID", cell.ID),
		zap.Int64("fieldID", cell.FieldID))
	
	// Simulate loading
	time.Sleep(50 * time.Millisecond)
	
	return nil
}

func (l *ScalarCellLoader) Evict(ctx context.Context, segment Segment, cell *Cell) error {
	log.Info("Evicting scalar cell",
		zap.String("cellID", cell.ID))
	
	return nil
}

// StatsCellLoader loads statistics
type StatsCellLoader struct {
	loader Loader
}

func (l *StatsCellLoader) Load(ctx context.Context, segment Segment, cell *Cell, loadInfo *querypb.SegmentLoadInfo) error {
	log.Info("Loading stats cell",
		zap.String("cellID", cell.ID))
	
	// Load stats logs
	if len(loadInfo.GetStatslogs()) > 0 {
		// Actual implementation would load stats
		time.Sleep(20 * time.Millisecond)
	}
	
	return nil
}

func (l *StatsCellLoader) Evict(ctx context.Context, segment Segment, cell *Cell) error {
	// Stats are typically not evictable
	return fmt.Errorf("stats cannot be evicted")
}

// DeltaCellLoader loads delta logs
type DeltaCellLoader struct {
	loader Loader
}

func (l *DeltaCellLoader) Load(ctx context.Context, segment Segment, cell *Cell, loadInfo *querypb.SegmentLoadInfo) error {
	log.Info("Loading delta cell",
		zap.String("cellID", cell.ID))
	
	// Load delta logs
	if len(loadInfo.GetDeltalogs()) > 0 {
		// Actual implementation would load deltas
		time.Sleep(30 * time.Millisecond)
	}
	
	return nil
}

func (l *DeltaCellLoader) Evict(ctx context.Context, segment Segment, cell *Cell) error {
	// Delta logs are typically not evictable
	return fmt.Errorf("delta logs cannot be evicted")
}