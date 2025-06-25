package segments

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type ResourceManagerTestSuite struct {
	suite.Suite
	ctx    context.Context
	cancel context.CancelFunc
	guard  *SegmentLoadGuard
}

func (suite *ResourceManagerTestSuite) SetupTest() {
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.guard = NewSegmentLoadGuard(suite.ctx)
}

func (suite *ResourceManagerTestSuite) TearDownTest() {
	suite.cancel()
	suite.guard.Close()
}

func (suite *ResourceManagerTestSuite) TestBasicSegmentLoad() {
	// Create a segment load request
	req := &SegmentLoadRequest{
		SegmentID: 1001,
		MetaSize:  1024 * 1024, // 1MB
		Cells: []*CellLoadInfo{
			{
				FieldID:   101,
				Size:      10 * 1024 * 1024, // 10MB
				Evictable: true,
			},
			{
				FieldID:   102,
				Size:      5 * 1024 * 1024, // 5MB
				Evictable: false,
			},
		},
	}
	
	// Load segment
	err := suite.guard.SegmentLoad(req)
	assert.NoError(suite.T(), err)
	
	// Check resource usage
	stats := suite.guard.GetResourceStats()
	assert.Equal(suite.T(), uint64(1024*1024), stats["shallow_meta_size"])
	assert.Equal(suite.T(), uint64(5*1024*1024), stats["shallow_inevitable"])
	assert.Equal(suite.T(), uint64(10*1024*1024), stats["shallow_evictable"])
}

func (suite *ResourceManagerTestSuite) TestCellLoading() {
	// First load a segment
	req := &SegmentLoadRequest{
		SegmentID: 1002,
		MetaSize:  1024 * 1024,
		Cells: []*CellLoadInfo{
			{
				FieldID:   201,
				Size:      20 * 1024 * 1024, // 20MB
				Evictable: true,
			},
		},
	}
	
	err := suite.guard.SegmentLoad(req)
	assert.NoError(suite.T(), err)
	
	// Get permit to load cell
	cellID := "1002_201"
	permit, err := suite.guard.PermitLoadCell(cellID)
	assert.NoError(suite.T(), err)
	assert.True(suite.T(), permit.IsGranted())
	
	// Simulate successful load
	permit.Commit()
	
	// Check that cell is loaded
	cell := suite.guard.cells[cellID]
	assert.Equal(suite.T(), CellStateLoaded, cell.GetState())
	
	// Check resource usage
	stats := suite.guard.GetResourceStats()
	assert.Equal(suite.T(), uint64(20*1024*1024), stats["deep_evictable"])
}

func (suite *ResourceManagerTestSuite) TestEviction() {
	// Set a low memory limit to trigger eviction
	suite.guard.memoryLimit = 50 * 1024 * 1024 // 50MB
	
	// Load multiple segments
	for i := 1; i <= 5; i++ {
		req := &SegmentLoadRequest{
			SegmentID: int64(2000 + i),
			MetaSize:  1024 * 1024,
			Cells: []*CellLoadInfo{
				{
					FieldID:   int64(300 + i),
					Size:      15 * 1024 * 1024, // 15MB each
					Evictable: true,
				},
			},
		}
		
		err := suite.guard.SegmentLoad(req)
		assert.NoError(suite.T(), err)
		
		// Load the cell
		cellID := fmt.Sprintf("%d_%d", 2000+i, 300+i)
		permit, err := suite.guard.PermitLoadCell(cellID)
		if err == nil && permit.IsGranted() {
			permit.Commit()
			
			// Simulate access pattern
			time.Sleep(100 * time.Millisecond)
		}
	}
	
	// Trigger eviction check
	suite.guard.checkAndEvict()
	
	// Check that some cells were evicted
	evictedCount := 0
	for _, cell := range suite.guard.cells {
		if cell.GetState() == CellStateNotLoaded && cell.Evictable {
			evictedCount++
		}
	}
	
	assert.Greater(suite.T(), evictedCount, 0, "Should have evicted some cells")
}

func (suite *ResourceManagerTestSuite) TestConcurrentAccess() {
	// Load a segment with multiple cells
	req := &SegmentLoadRequest{
		SegmentID: 3001,
		MetaSize:  1024 * 1024,
		Cells: []*CellLoadInfo{
			{FieldID: 401, Size: 10 * 1024 * 1024, Evictable: true},
			{FieldID: 402, Size: 10 * 1024 * 1024, Evictable: true},
			{FieldID: 403, Size: 10 * 1024 * 1024, Evictable: true},
		},
	}
	
	err := suite.guard.SegmentLoad(req)
	assert.NoError(suite.T(), err)
	
	// Concurrent cell loading
	done := make(chan bool, 3)
	
	for i := 401; i <= 403; i++ {
		go func(fieldID int64) {
			cellID := fmt.Sprintf("3001_%d", fieldID)
			permit, err := suite.guard.PermitLoadCell(cellID)
			if err == nil && permit.IsGranted() {
				// Simulate load time
				time.Sleep(50 * time.Millisecond)
				permit.Commit()
			}
			done <- true
		}(int64(i))
	}
	
	// Wait for all goroutines
	for i := 0; i < 3; i++ {
		<-done
	}
	
	// Check all cells are loaded
	loadedCount := 0
	for _, cell := range suite.guard.cells {
		if cell.SegmentID == 3001 && cell.GetState() == CellStateLoaded {
			loadedCount++
		}
	}
	
	assert.Equal(suite.T(), 3, loadedCount)
}

func (suite *ResourceManagerTestSuite) TestResourceLimitEnforcement() {
	// Set a very low limit
	suite.guard.memoryLimit = 10 * 1024 * 1024 // 10MB
	
	// Try to load a segment that exceeds the limit
	req := &SegmentLoadRequest{
		SegmentID: 4001,
		MetaSize:  1024 * 1024,
		Cells: []*CellLoadInfo{
			{
				FieldID:   501,
				Size:      20 * 1024 * 1024, // 20MB
				Evictable: false, // Cannot be evicted
			},
		},
	}
	
	err := suite.guard.SegmentLoad(req)
	assert.Error(suite.T(), err)
	assert.Contains(suite.T(), err.Error(), "insufficient memory")
}

func (suite *ResourceManagerTestSuite) TestEvictionStrategies() {
	// Test LRU eviction
	lru := NewLRUEvictionStrategy()
	
	// Create cells with different access times
	cells := []*Cell{
		NewCell("1", 1, 1, 1024*1024, true),
		NewCell("2", 1, 2, 1024*1024, true),
		NewCell("3", 1, 3, 1024*1024, true),
	}
	
	// Set different access times
	now := time.Now()
	cells[0].lastAccess.Store(now.Add(-3 * time.Minute))
	cells[1].lastAccess.Store(now.Add(-2 * time.Minute))
	cells[2].lastAccess.Store(now.Add(-1 * time.Minute))
	
	// Set all as loaded
	for _, cell := range cells {
		cell.SetState(CellStateLoaded)
	}
	
	// Select victims
	victims := lru.SelectVictims(cells, 2*1024*1024)
	
	assert.Len(suite.T(), victims, 2)
	assert.Equal(suite.T(), "1", victims[0].ID)
	assert.Equal(suite.T(), "2", victims[1].ID)
}

func TestResourceManagerTestSuite(t *testing.T) {
	suite.Run(t, new(ResourceManagerTestSuite))
}

// Example usage in actual segment loader
func ExampleResourceManagedLoader() {
	ctx := context.Background()
	
	// Create resource managed loader
	manager := &Manager{} // Would be actual manager
	cm := &mockChunkManager{} // Would be actual chunk manager
	
	loader, err := NewResourceManagedLoader(ctx, manager, cm)
	if err != nil {
		panic(err)
	}
	
	// Load segments with resource management
	segments, err := loader.Load(ctx, 1001, SegmentTypeSealed, 1, &querypb.SegmentLoadInfo{
		SegmentID:    1001,
		CollectionID: 100,
		PartitionID:  10,
		NumOfRows:    10000,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:  101,
					Name:     "vec",
					DataType: schemapb.DataType_FloatVector,
				},
				{
					FieldID:  102,
					Name:     "id",
					DataType: schemapb.DataType_Int64,
				},
			},
		},
	})
	
	if err != nil {
		panic(err)
	}
	
	// Use segments - cells will be loaded on demand
	for _, segment := range segments {
		// Search will trigger loading of required cells
		result, err := segment.Search(ctx, &SearchRequest{
			// Search parameters
		})
		
		if err != nil {
			fmt.Printf("Search error: %v\n", err)
		} else {
			fmt.Printf("Search returned %d results\n", len(result.IDs))
		}
	}
	
	// Get resource statistics
	stats := loader.guard.GetResourceStats()
	fmt.Printf("Resource stats: %+v\n", stats)
}

// Mock implementations for testing
type mockChunkManager struct{}

func (m *mockChunkManager) Read(ctx context.Context, path string) ([]byte, error) {
	return []byte("mock data"), nil
}