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

package metrics

import (
	"testing"
)

func TestRegisterMetrics(t *testing.T) {
	r := NewMilvusRegistry()
	// Make sure it doesn't panic.
	RegisterRootCoord(r.GoRegistry)
	RegisterDataNode(r.GoRegistry)
	RegisterDataCoord(r.GoRegistry)
	RegisterIndexNode(r.GoRegistry)
	RegisterProxy(r.GoRegistry)
	RegisterQueryNode(r.GoRegistry)
	RegisterQueryCoord(r.GoRegistry)
	RegisterMetaMetrics(r.GoRegistry)
	RegisterStorageMetrics(r.GoRegistry)
	RegisterMsgStreamMetrics(r.GoRegistry)
}
