package compaction

import (
	"container/heap"
	"testing"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/stretchr/testify/suite"
)

type PriorityQueueSuite struct {
	suite.Suite
}

func (s *PriorityQueueSuite) PriorityQueueMergeSort() {
	slices := [][]*storage.Value{
		{
			{
				ID: 1,
				PK: &storage.Int64PrimaryKey{
					Value: 1,
				},
				Timestamp: 0,
				IsDeleted: false,
				Value:     1,
			},
			{
				ID: 4,
				PK: &storage.Int64PrimaryKey{
					Value: 4,
				},
				Timestamp: 0,
				IsDeleted: false,
				Value:     4,
			},
			{
				ID: 7,
				PK: &storage.Int64PrimaryKey{
					Value: 7,
				},
				Timestamp: 0,
				IsDeleted: false,
				Value:     7,
			},
			{
				ID: 10,
				PK: &storage.Int64PrimaryKey{
					Value: 10,
				},
				Timestamp: 0,
				IsDeleted: false,
				Value:     10,
			},
		},
		{
			{
				ID: 2,
				PK: &storage.Int64PrimaryKey{
					Value: 2,
				},
				Timestamp: 0,
				IsDeleted: false,
				Value:     2,
			},
			{
				ID: 3,
				PK: &storage.Int64PrimaryKey{
					Value: 3,
				},
				Timestamp: 0,
				IsDeleted: false,
				Value:     3,
			},
			{
				ID: 5,
				PK: &storage.Int64PrimaryKey{
					Value: 5,
				},
				Timestamp: 0,
				IsDeleted: false,
				Value:     5,
			},
			{
				ID: 6,
				PK: &storage.Int64PrimaryKey{
					Value: 6,
				},
				Timestamp: 0,
				IsDeleted: false,
				Value:     6,
			},
		},
	}

	var result []*storage.Value
	pq := make(PriorityQueue, 0)
	heap.Init(&pq)

	for i, s := range slices {
		if len(s) > 0 {
			heap.Push(&pq, &PQItem{
				Value: s[0],
				Index: i,
				Pos:   1,
			})
		}
	}

	for pq.Len() > 0 {
		smallest := heap.Pop(&pq).(*PQItem)
		result = append(result, smallest.Value)
		if smallest.Pos+1 < len(slices[smallest.Index]) {
			next := &PQItem{
				Value: slices[smallest.Index][smallest.Pos+1],
				Index: smallest.Index,
				Pos:   smallest.Pos + 1,
			}
			heap.Push(&pq, next)
		}
	}

}

func TestNewPriorityQueueSuite(t *testing.T) {
	suite.Run(t, new(PriorityQueueSuite))
}
