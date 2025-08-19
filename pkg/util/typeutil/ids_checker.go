// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package typeutil

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

// IDsChecker provides efficient lookup functionality for schema.IDs
// It supports checking if an ID at a specific position in one IDs exists in another IDs
type IDsChecker struct {
	intIDSet map[int64]struct{}
	strIDSet map[string]struct{}
	idType   schemapb.DataType
}

// NewIDsChecker creates a new IDsChecker from the given IDs
// The checker will build an internal set for fast lookup
func NewIDsChecker(ids *schemapb.IDs) (*IDsChecker, error) {
	if ids == nil || ids.GetIdField() == nil {
		return &IDsChecker{}, nil
	}

	checker := &IDsChecker{}

	switch ids.GetIdField().(type) {
	case *schemapb.IDs_IntId:
		checker.idType = schemapb.DataType_Int64
		checker.intIDSet = make(map[int64]struct{})
		data := ids.GetIntId().GetData()
		for _, id := range data {
			checker.intIDSet[id] = struct{}{}
		}
	case *schemapb.IDs_StrId:
		checker.idType = schemapb.DataType_VarChar
		checker.strIDSet = make(map[string]struct{})
		data := ids.GetStrId().GetData()
		for _, id := range data {
			checker.strIDSet[id] = struct{}{}
		}
	default:
		return nil, fmt.Errorf("unsupported ID type in IDs")
	}

	return checker, nil
}

// Contains checks if the ID at the specified cursor position in idsA exists in this checker
// Returns true if the ID exists, false otherwise
// Returns error if cursor is out of bounds or type mismatch
func (c *IDsChecker) Contains(idsA *schemapb.IDs, cursor int) (bool, error) {
	if idsA == nil || idsA.GetIdField() == nil {
		return false, fmt.Errorf("idsA is nil or empty")
	}

	// Check if cursor is within bounds
	size := GetSizeOfIDs(idsA)
	if cursor < 0 || cursor >= size {
		return false, fmt.Errorf("cursor %d is out of bounds [0, %d)", cursor, size)
	}

	// If checker is empty, return false for any query
	if c.IsEmpty() {
		return false, nil
	}

	switch idsA.GetIdField().(type) {
	case *schemapb.IDs_IntId:
		if c.idType != schemapb.DataType_Int64 {
			return false, fmt.Errorf("type mismatch: checker expects %v, got Int64", c.idType)
		}
		if c.intIDSet == nil {
			return false, nil
		}
		id := idsA.GetIntId().GetData()[cursor]
		_, exists := c.intIDSet[id]
		return exists, nil

	case *schemapb.IDs_StrId:
		if c.idType != schemapb.DataType_VarChar {
			return false, fmt.Errorf("type mismatch: checker expects %v, got VarChar", c.idType)
		}
		if c.strIDSet == nil {
			return false, nil
		}
		id := idsA.GetStrId().GetData()[cursor]
		_, exists := c.strIDSet[id]
		return exists, nil

	default:
		return false, fmt.Errorf("unsupported ID type in idsA")
	}
}

// ContainsAny checks if any ID in idsA exists in this checker
// Returns the indices of IDs that exist in the checker
func (c *IDsChecker) ContainsAny(idsA *schemapb.IDs) ([]int, error) {
	if idsA == nil || idsA.GetIdField() == nil {
		return nil, fmt.Errorf("idsA is nil or empty")
	}

	var result []int

	// If checker is empty, return empty result for any query
	if c.IsEmpty() {
		return result, nil
	}

	switch idsA.GetIdField().(type) {
	case *schemapb.IDs_IntId:
		if c.idType != schemapb.DataType_Int64 {
			return nil, fmt.Errorf("type mismatch: checker expects %v, got Int64", c.idType)
		}
		if c.intIDSet == nil {
			return result, nil
		}
		data := idsA.GetIntId().GetData()
		for i, id := range data {
			if _, exists := c.intIDSet[id]; exists {
				result = append(result, i)
			}
		}

	case *schemapb.IDs_StrId:
		if c.idType != schemapb.DataType_VarChar {
			return nil, fmt.Errorf("type mismatch: checker expects %v, got VarChar", c.idType)
		}
		if c.strIDSet == nil {
			return result, nil
		}
		data := idsA.GetStrId().GetData()
		for i, id := range data {
			if _, exists := c.strIDSet[id]; exists {
				result = append(result, i)
			}
		}

	default:
		return nil, fmt.Errorf("unsupported ID type in idsA")
	}

	return result, nil
}

// Size returns the number of unique IDs in this checker
func (c *IDsChecker) Size() int {
	switch c.idType {
	case schemapb.DataType_Int64:
		if c.intIDSet == nil {
			return 0
		}
		return len(c.intIDSet)
	case schemapb.DataType_VarChar:
		if c.strIDSet == nil {
			return 0
		}
		return len(c.strIDSet)
	default:
		return 0
	}
}

// IsEmpty returns true if the checker contains no IDs
func (c *IDsChecker) IsEmpty() bool {
	return c.Size() == 0
}

// GetIDType returns the data type of IDs in this checker
func (c *IDsChecker) GetIDType() schemapb.DataType {
	return c.idType
}

// ContainsIDsAtCursors is a batch operation that checks multiple cursor positions at once
// Returns a slice of booleans indicating whether each cursor position exists in the checker
func (c *IDsChecker) ContainsIDsAtCursors(idsA *schemapb.IDs, cursors []int) ([]bool, error) {
	if idsA == nil || idsA.GetIdField() == nil {
		return nil, fmt.Errorf("idsA is nil or empty")
	}

	size := GetSizeOfIDs(idsA)
	results := make([]bool, len(cursors))

	for i, cursor := range cursors {
		if cursor < 0 || cursor >= size {
			return nil, fmt.Errorf("cursor %d is out of bounds [0, %d)", cursor, size)
		}

		exists, err := c.Contains(idsA, cursor)
		if err != nil {
			return nil, err
		}
		results[i] = exists
	}

	return results, nil
}
