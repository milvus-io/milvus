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
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
)

// FormatMajorVersion is pinned, never pebble.FormatNewest: it decides the
// table format of memtable flushes, which other nodes read as baseline and
// merge input. Raising it is a rolling-upgrade step: readers first.
const FormatMajorVersion = pebble.FormatVirtualSSTables

// TableFormat is the format every producer writes, derived from
// FormatMajorVersion so that flush output and Writer output cannot diverge.
var TableFormat = FormatMajorVersion.MaxTableFormat()

// Comparer is the comparer shared by every SST producer and consumer, and by
// the engine's pebble DB. Keys are order-preserving encoded (codec package),
// so plain bytewise comparison is correct.
var Comparer = pebble.DefaultComparer

// FilterPolicy is the bloom filter every producer writes into its tables, over
// whole keys (Comparer has no Split). A probe for an absent key, the common
// case when deduplicating inserts, is then answered from the table's filter
// block instead of its data blocks.
var FilterPolicy pebble.FilterPolicy = bloom.FilterPolicy(10)

// PebbleOptions returns the options template shared by the engine's increment
// DBs, Writer and Reader, so that none of them can drift from the others.
// Callers add their own settings on top of the returned value, which is why
// each call builds a fresh one.
func PebbleOptions() *pebble.Options {
	o := &pebble.Options{
		Comparer:           Comparer,
		FormatMajorVersion: FormatMajorVersion,
		Levels:             []pebble.LevelOptions{{FilterPolicy: FilterPolicy}},
	}
	// EnsureDefaults also fills Filters from Levels, which is what lets a
	// Reader built from MakeReaderOptions consult the bloom filter.
	o.EnsureDefaults()
	return o
}
