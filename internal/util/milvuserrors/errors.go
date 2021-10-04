// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package milvuserrors

import (
	"errors"
	"fmt"
)

const (
	// MsgRootCoordNotServing means that root coordinator not serving
	MsgRootCoordNotServing = "root coordinator is not serving"
	// MsgQueryCoordNotServing means that query coordinator not serving
	MsgQueryCoordNotServing = "query coordinator is not serving"
	// MsgDataCoordNotServing means that data coordinator not serving
	MsgDataCoordNotServing = "data coordinator is not serving"
	// MsgIndexCoordNotServing means that index coordinator not serving
	MsgIndexCoordNotServing = "index coordinator is not serving"
)

// MsgCollectionAlreadyExist is used to construct an error message of collection already exist
func MsgCollectionAlreadyExist(name string) string {
	return fmt.Sprintf("Collection %s already exist", name)
}

// ErrCollectionAlreadyExist is used to construct an error of collection already exist
func ErrCollectionAlreadyExist(name string) error {
	return errors.New(MsgCollectionAlreadyExist(name))
}

// MsgCollectionNotExist is used to construct an error message of collection not exist
func MsgCollectionNotExist(name string) string {
	return fmt.Sprintf("Collection %s not exist", name)
}

// ErrCollectionNotExist is used to construct an error of collection not exist
func ErrCollectionNotExist(name string) error {
	return errors.New(MsgCollectionNotExist(name))
}

// MsgPartitionAlreadyExist is used to construct an error message of partition already exist
func MsgPartitionAlreadyExist(name string) string {
	return fmt.Sprintf("Partition %s already exist", name)
}

// ErrPartitionAlreadyExist is used to construct an error of partition already exist
func ErrPartitionAlreadyExist(name string) error {
	return errors.New(MsgPartitionAlreadyExist(name))
}

// MsgPartitionNotExist is used to construct an error message of partition not exist
func MsgPartitionNotExist(name string) string {
	return fmt.Sprintf("Partition %s not exist", name)
}

// ErrPartitionNotExist is used to construct an error of partition not exist
func ErrPartitionNotExist(name string) error {
	return errors.New(MsgPartitionNotExist(name))
}
