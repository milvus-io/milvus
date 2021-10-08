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
	MsgRootCoordNotServing  = "root coordinator is not serving"
	MsgQueryCoordNotServing = "query coordinator is not serving"
	MsgDataCoordNotServing  = "data coordinator is not serving"
	MsgIndexCoordNotServing = "index coordinator is not serving"
)

func MsgCollectionAlreadyExist(name string) string {
	return fmt.Sprintf("Collection %s already exist", name)
}

func ErrCollectionAlreadyExist(name string) error {
	return errors.New(MsgCollectionAlreadyExist(name))
}

func MsgCollectionNotExist(name string) string {
	return fmt.Sprintf("Collection %s not exist", name)
}

func ErrCollectionNotExist(name string) error {
	return errors.New(MsgCollectionNotExist(name))
}

func MsgPartitionAlreadyExist(name string) string {
	return fmt.Sprintf("Partition %s already exist", name)
}

func ErrPartitionAlreadyExist(name string) error {
	return errors.New(MsgPartitionAlreadyExist(name))
}

func MsgPartitionNotExist(name string) string {
	return fmt.Sprintf("Partition %s not exist", name)
}

func ErrPartitionNotExist(name string) error {
	return errors.New(MsgPartitionNotExist(name))
}
