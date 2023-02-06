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

package querynodev2

import (
	"errors"
	"fmt"
)

var (
	ErrNodeUnhealthy      = errors.New("NodeIsUnhealthy")
	ErrGetDelegatorFailed = errors.New("GetShardDelefatorFailed")
	ErrInitPipelineFailed = errors.New("InitPipelineFailed")
)

// WrapErrNodeUnhealthy wraps ErrNodeUnhealthy with nodeID.
func WrapErrNodeUnhealthy(nodeID int64) error {
	return fmt.Errorf("%w id: %d", ErrNodeUnhealthy, nodeID)
}

func WrapErrInitPipelineFailed(err error) error {
	return fmt.Errorf("%w err: %s", ErrInitPipelineFailed, err.Error())
}

func msgQueryNodeIsUnhealthy(nodeID int64) string {
	return fmt.Sprintf("query node %d is not ready", nodeID)
}
