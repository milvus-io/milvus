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

package lifetime

import (
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// Singal alias for chan struct{}.
type Signal chan struct{}

// BiState provides pre-defined simple binary state - normal or closed.
type BiState int32

const (
	Normal BiState = 0
	Closed BiState = 1
)

// State provides pre-defined three stage state.
type State int32

const (
	Initializing State = iota
	Working
	Stopped
)

func (s State) String() string {
	switch s {
	case Initializing:
		return "Initializing"
	case Working:
		return "Working"
	case Stopped:
		return "Stopped"
	}

	return "Unknown"
}

func NotStopped(state State) error {
	if state != Stopped {
		return nil
	}
	return merr.WrapErrServiceNotReady(paramtable.GetRole(), paramtable.GetNodeID(), state.String())
}

func IsWorking(state State) error {
	if state == Working {
		return nil
	}
	return merr.WrapErrServiceNotReady(paramtable.GetRole(), paramtable.GetNodeID(), state.String())
}
