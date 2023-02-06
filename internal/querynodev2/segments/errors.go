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

package segments

import (
	"errors"
	"fmt"
)

var (
	// Manager related errors
	ErrCollectionNotFound = errors.New("CollectionNotFound")
	ErrPartitionNotFound  = errors.New("PartitionNotFound")
	ErrSegmentNotFound    = errors.New("SegmentNotFound")
	ErrFieldNotFound      = errors.New("FieldNotFound")
	ErrSegmentReleased    = errors.New("SegmentReleased")
)

func WrapSegmentNotFound(segmentID int64) error {
	return fmt.Errorf("%w(%v)", ErrSegmentNotFound, segmentID)
}

func WrapCollectionNotFound(collectionID int64) error {
	return fmt.Errorf("%w(%v)", ErrCollectionNotFound, collectionID)
}

func WrapFieldNotFound(fieldID int64) error {
	return fmt.Errorf("%w(%v)", ErrFieldNotFound, fieldID)
}

// WrapSegmentReleased wrap ErrSegmentReleased with segmentID.
func WrapSegmentReleased(segmentID int64) error {
	return fmt.Errorf("%w(%d)", ErrSegmentReleased, segmentID)
}
