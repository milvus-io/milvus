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

package indexcoord

import (
	"errors"
	"fmt"
)

var (
	ErrCompareVersion = errors.New("failed to save meta in etcd because version compare failure")
)

// errIndexNodeIsNotOnService return an error that the specified IndexNode is not exists.
func errIndexNodeIsNotOnService(id UniqueID) error {
	return fmt.Errorf("index node %d is not on service", id)
}

// msgIndexCoordIsUnhealthy return an error that the IndexCoord is not healthy.
func msgIndexCoordIsUnhealthy(coordID UniqueID) string {
	return fmt.Sprintf("IndexCoord %d is not ready", coordID)
}

func errIndexCoordIsUnhealthy(coordID UniqueID) error {
	return errors.New(msgIndexCoordIsUnhealthy(coordID))
}
