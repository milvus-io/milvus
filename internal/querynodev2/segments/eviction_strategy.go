package segments

import (
	"container/heap"
	"sort"
	"time"
)

// EvictionStrategy defines the interface for eviction strategies
type EvictionStrategy interface {
	// SelectVictims selects cells to evict to free up targetBytes
	SelectVictims(cells []*Cell, targetBytes uint64) []*Cell
}

// LRUEvictionStrategy implements LRU eviction
type LRUEvictionStrategy struct{}

// NewLRUEvictionStrategy creates a new LRU eviction strategy
func NewLRUEvictionStrategy() EvictionStrategy {
	return &LRUEvictionStrategy{}
}

// SelectVictims selects least recently used cells for eviction
func (s *LRUEvictionStrategy) SelectVictims(cells []*Cell, targetBytes uint64) []*Cell {
	if len(cells) == 0 || targetBytes == 0 {
		return nil
	}
	
	// Sort by access time (oldest first)
	sort.Slice(cells, func(i, j int) bool {
		return cells[i].GetAccessTime().Before(cells[j].GetAccessTime())
	})
	
	victims := make([]*Cell, 0)
	evicted := uint64(0)
	
	for _, cell := range cells {
		if cell.GetState() == CellStateLoaded && cell.Evictable {
			victims = append(victims, cell)
			evicted += cell.Size
			if evicted >= targetBytes {
				break
			}
		}
	}
	
	return victims
}

// LFUEvictionStrategy implements LFU eviction
type LFUEvictionStrategy struct {
	accessCounts map[string]uint64
}

// NewLFUEvictionStrategy creates a new LFU eviction strategy
func NewLFUEvictionStrategy() EvictionStrategy {
	return &LFUEvictionStrategy{
		accessCounts: make(map[string]uint64),
	}
}

// UpdateAccessCount updates the access count for a cell
func (s *LFUEvictionStrategy) UpdateAccessCount(cellID string) {
	s.accessCounts[cellID]++
}

// SelectVictims selects least frequently used cells for eviction
func (s *LFUEvictionStrategy) SelectVictims(cells []*Cell, targetBytes uint64) []*Cell {
	if len(cells) == 0 || targetBytes == 0 {
		return nil
	}
	
	type cellWithCount struct {
		cell  *Cell
		count uint64
	}
	
	cellsWithCount := make([]cellWithCount, 0, len(cells))
	for _, cell := range cells {
		if cell.GetState() == CellStateLoaded && cell.Evictable {
			cellsWithCount = append(cellsWithCount, cellWithCount{
				cell:  cell,
				count: s.accessCounts[cell.ID],
			})
		}
	}
	
	// Sort by access count (least frequent first)
	sort.Slice(cellsWithCount, func(i, j int) bool {
		if cellsWithCount[i].count == cellsWithCount[j].count {
			// Tie-breaker: use access time
			return cellsWithCount[i].cell.GetAccessTime().Before(
				cellsWithCount[j].cell.GetAccessTime())
		}
		return cellsWithCount[i].count < cellsWithCount[j].count
	})
	
	victims := make([]*Cell, 0)
	evicted := uint64(0)
	
	for _, cwc := range cellsWithCount {
		victims = append(victims, cwc.cell)
		evicted += cwc.cell.Size
		if evicted >= targetBytes {
			break
		}
	}
	
	return victims
}

// CostBenefitEvictionStrategy implements cost-benefit based eviction
type CostBenefitEvictionStrategy struct {
	loadTimes map[string]time.Duration
}

// NewCostBenefitEvictionStrategy creates a new cost-benefit eviction strategy
func NewCostBenefitEvictionStrategy() EvictionStrategy {
	return &CostBenefitEvictionStrategy{
		loadTimes: make(map[string]time.Duration),
	}
}

// UpdateLoadTime updates the load time for a cell
func (s *CostBenefitEvictionStrategy) UpdateLoadTime(cellID string, loadTime time.Duration) {
	s.loadTimes[cellID] = loadTime
}

// SelectVictims selects cells based on cost-benefit analysis
func (s *CostBenefitEvictionStrategy) SelectVictims(cells []*Cell, targetBytes uint64) []*Cell {
	if len(cells) == 0 || targetBytes == 0 {
		return nil
	}
	
	type cellWithScore struct {
		cell  *Cell
		score float64
	}
	
	cellsWithScore := make([]cellWithScore, 0, len(cells))
	now := time.Now()
	
	for _, cell := range cells {
		if cell.GetState() == CellStateLoaded && cell.Evictable {
			// Calculate score: lower is better for eviction
			// Score = (time_since_access * size) / load_time
			timeSinceAccess := now.Sub(cell.GetAccessTime()).Seconds()
			loadTime := s.loadTimes[cell.ID].Seconds()
			if loadTime == 0 {
				loadTime = 1 // Avoid division by zero
			}
			
			score := (timeSinceAccess * float64(cell.Size)) / loadTime
			
			cellsWithScore = append(cellsWithScore, cellWithScore{
				cell:  cell,
				score: score,
			})
		}
	}
	
	// Sort by score (highest first - most beneficial to evict)
	sort.Slice(cellsWithScore, func(i, j int) bool {
		return cellsWithScore[i].score > cellsWithScore[j].score
	})
	
	victims := make([]*Cell, 0)
	evicted := uint64(0)
	
	for _, cws := range cellsWithScore {
		victims = append(victims, cws.cell)
		evicted += cws.cell.Size
		if evicted >= targetBytes {
			break
		}
	}
	
	return victims
}

// PriorityEvictionStrategy implements priority-based eviction
type PriorityEvictionStrategy struct {
	priorities map[string]int
}

// NewPriorityEvictionStrategy creates a new priority eviction strategy
func NewPriorityEvictionStrategy() *PriorityEvictionStrategy {
	return &PriorityEvictionStrategy{
		priorities: make(map[string]int),
	}
}

// SetPriority sets the priority for a cell (higher = more important)
func (s *PriorityEvictionStrategy) SetPriority(cellID string, priority int) {
	s.priorities[cellID] = priority
}

// SelectVictims selects cells with lowest priority for eviction
func (s *PriorityEvictionStrategy) SelectVictims(cells []*Cell, targetBytes uint64) []*Cell {
	if len(cells) == 0 || targetBytes == 0 {
		return nil
	}
	
	type cellWithPriority struct {
		cell     *Cell
		priority int
	}
	
	cellsWithPriority := make([]cellWithPriority, 0, len(cells))
	for _, cell := range cells {
		if cell.GetState() == CellStateLoaded && cell.Evictable {
			priority := s.priorities[cell.ID]
			cellsWithPriority = append(cellsWithPriority, cellWithPriority{
				cell:     cell,
				priority: priority,
			})
		}
	}
	
	// Sort by priority (lowest first)
	sort.Slice(cellsWithPriority, func(i, j int) bool {
		if cellsWithPriority[i].priority == cellsWithPriority[j].priority {
			// Tie-breaker: use access time
			return cellsWithPriority[i].cell.GetAccessTime().Before(
				cellsWithPriority[j].cell.GetAccessTime())
		}
		return cellsWithPriority[i].priority < cellsWithPriority[j].priority
	})
	
	victims := make([]*Cell, 0)
	evicted := uint64(0)
	
	for _, cwp := range cellsWithPriority {
		victims = append(victims, cwp.cell)
		evicted += cwp.cell.Size
		if evicted >= targetBytes {
			break
		}
	}
	
	return victims
}

// AdaptiveEvictionStrategy combines multiple strategies
type AdaptiveEvictionStrategy struct {
	lru          *LRUEvictionStrategy
	lfu          *LFUEvictionStrategy
	costBenefit  *CostBenefitEvictionStrategy
	
	// Weights for each strategy
	lruWeight    float64
	lfuWeight    float64
	cbWeight     float64
}

// NewAdaptiveEvictionStrategy creates a new adaptive eviction strategy
func NewAdaptiveEvictionStrategy() *AdaptiveEvictionStrategy {
	return &AdaptiveEvictionStrategy{
		lru:         &LRUEvictionStrategy{},
		lfu:         NewLFUEvictionStrategy().(*LFUEvictionStrategy),
		costBenefit: NewCostBenefitEvictionStrategy().(*CostBenefitEvictionStrategy),
		lruWeight:   0.4,
		lfuWeight:   0.3,
		cbWeight:    0.3,
	}
}

// SelectVictims combines multiple strategies to select victims
func (s *AdaptiveEvictionStrategy) SelectVictims(cells []*Cell, targetBytes uint64) []*Cell {
	if len(cells) == 0 || targetBytes == 0 {
		return nil
	}
	
	// Get victims from each strategy
	lruVictims := s.lru.SelectVictims(cells, targetBytes)
	lfuVictims := s.lfu.SelectVictims(cells, targetBytes)
	cbVictims := s.costBenefit.SelectVictims(cells, targetBytes)
	
	// Score each cell based on all strategies
	cellScores := make(map[string]float64)
	
	// Add scores from each strategy
	for i, victim := range lruVictims {
		score := float64(len(lruVictims)-i) * s.lruWeight
		cellScores[victim.ID] += score
	}
	
	for i, victim := range lfuVictims {
		score := float64(len(lfuVictims)-i) * s.lfuWeight
		cellScores[victim.ID] += score
	}
	
	for i, victim := range cbVictims {
		score := float64(len(cbVictims)-i) * s.cbWeight
		cellScores[victim.ID] += score
	}
	
	// Sort cells by combined score
	type cellScore struct {
		cell  *Cell
		score float64
	}
	
	scoredCells := make([]cellScore, 0, len(cellScores))
	for _, cell := range cells {
		if score, exists := cellScores[cell.ID]; exists {
			scoredCells = append(scoredCells, cellScore{
				cell:  cell,
				score: score,
			})
		}
	}
	
	sort.Slice(scoredCells, func(i, j int) bool {
		return scoredCells[i].score > scoredCells[j].score
	})
	
	// Select victims based on combined score
	victims := make([]*Cell, 0)
	evicted := uint64(0)
	
	for _, sc := range scoredCells {
		victims = append(victims, sc.cell)
		evicted += sc.cell.Size
		if evicted >= targetBytes {
			break
		}
	}
	
	return victims
}