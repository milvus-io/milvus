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

package indexnode

import (
	"fmt"

	"github.com/cockroachdb/errors"
)

var (
	ErrNoSuchKey        = errors.New("NoSuchKey")
	ErrEmptyInsertPaths = errors.New("empty insert paths")
)

// msgIndexNodeIsUnhealthy return a message tha IndexNode is not healthy.
func msgIndexNodeIsUnhealthy(nodeID UniqueID) string {
	return fmt.Sprintf("index node %d is not ready", nodeID)
}

// errIndexNodeIsUnhealthy return an error that specified IndexNode is not healthy.
func errIndexNodeIsUnhealthy(nodeID UniqueID) error {
	return errors.New(msgIndexNodeIsUnhealthy(nodeID))
}
