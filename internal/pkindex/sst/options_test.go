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

package sst

import (
	"testing"

	"github.com/cockroachdb/pebble/sstable"
	"github.com/stretchr/testify/assert"
)

// The comparer's name is written into every SST and checked when one is
// opened, so every file already written stays readable only while the name
// stays put. The name is therefore the contract, not the comparer this package
// happens to alias: pinning the literal catches both swapping Comparer out and
// pebble renaming its default underneath us.
func TestComparerNamePinned(t *testing.T) {
	assert.Equal(t, "leveldb.BytewiseComparator", Comparer.Name)
}

// The table format is what other nodes parse, so it is pinned rather than
// tracking whatever pebble considers newest. A pebble upgrade that moves
// FormatMajorVersion fails here, which is the reminder that rolling out a new
// table format means upgrading readers first.
func TestTableFormatPinned(t *testing.T) {
	assert.Equal(t, sstable.TableFormatPebblev4, TableFormat)
}

// Writer and Reader must agree on the bloom filter, which they only do if the
// shared template carries it into both option sets.
func TestPebbleOptionsCarryFilterPolicy(t *testing.T) {
	o := PebbleOptions()
	assert.Equal(t, FilterPolicy, o.MakeWriterOptions(0, TableFormat).FilterPolicy)
	assert.Contains(t, o.MakeReaderOptions().Filters, FilterPolicy.Name())
}
