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

package rootcoord

import "slices"

func (c *Core) beginTransferProtectedCollectionOperation(collectionID int64) (func(), error) {
	if collectionID <= 0 {
		return func() {}, nil
	}
	return c.transferGate.BeginUserOperation(collectionID, 0)
}

func (c *Core) beginTransferProtectedCollectionOperations(collectionIDs ...int64) (func(), error) {
	collectionIDs = slices.Clone(collectionIDs)
	slices.Sort(collectionIDs)
	collectionIDs = slices.Compact(collectionIDs)

	doneFuncs := make([]func(), 0, len(collectionIDs))
	for _, collectionID := range collectionIDs {
		done, err := c.beginTransferProtectedCollectionOperation(collectionID)
		if err != nil {
			for i := len(doneFuncs) - 1; i >= 0; i-- {
				doneFuncs[i]()
			}
			return nil, err
		}
		doneFuncs = append(doneFuncs, done)
	}
	return func() {
		for i := len(doneFuncs) - 1; i >= 0; i-- {
			doneFuncs[i]()
		}
	}, nil
}

func (c *Core) withTransferProtectedCollectionOperation(collectionID int64, fn func() error) error {
	done, err := c.beginTransferProtectedCollectionOperation(collectionID)
	if err != nil {
		return err
	}
	defer done()
	return fn()
}
