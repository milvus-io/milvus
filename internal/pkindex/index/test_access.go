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

//go:build test

package index

import "github.com/milvus-io/milvus/internal/pkindex/engine"

// This file is compiled only with the "test" build tag. It lets the tests of
// other packages read and seed the engine of an Index.

// EngineForTest returns the engine of the Index. A write through it does not
// mark the Index dirty.
func (idx *Index) EngineForTest() engine.RW {
	return idx.eng
}

// DirtyForTest reports whether the Index has writes since its last rotate.
func (idx *Index) DirtyForTest() bool {
	return idx.dirtyFlag.Load()
}
