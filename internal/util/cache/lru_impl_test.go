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

package cache

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type lruTest struct {
	c   cache
	lru lruCache
	t   *testing.T
}

func (t *lruTest) assertLRULen(n int) {
	sz := cacheSize(&t.c)
	lz := t.lru.ls.Len()
	assert.Equal(t.t, n, sz)
	assert.Equal(t.t, n, lz)
}

func (t *lruTest) assertEntry(en *entry, k int, v string, id uint8) {
	if en == nil {
		t.t.Helper()
		t.t.Fatalf("unexpected entry: %v", en)
	}
	ak := en.key.(int)
	av := en.getValue().(string)
	assert.Equal(t.t, k, ak)
	assert.Equal(t.t, v, av)
	assert.Equal(t.t, id, en.listID)
}

func (t *lruTest) assertLRUEntry(k int) {
	en := t.c.get(k, 0)
	assert.NotNil(t.t, en)

	ak := en.key.(int)
	av := en.getValue().(string)
	v := fmt.Sprintf("%d", k)
	assert.Equal(t.t, k, ak)
	assert.Equal(t.t, v, av)
	assert.Equal(t.t, uint8(0), en.listID)
}

func (t *lruTest) assertSLRUEntry(k int, id uint8) {
	en := t.c.get(k, 0)
	assert.NotNil(t.t, en)

	ak := en.key.(int)
	av := en.getValue().(string)
	v := fmt.Sprintf("%d", k)
	assert.Equal(t.t, k, ak)
	assert.Equal(t.t, v, av)
	assert.Equal(t.t, id, en.listID)
}

func TestLRU(t *testing.T) {
	s := lruTest{t: t}
	s.lru.init(&s.c, 3)

	en := createLRUEntries(4)
	remEn := s.lru.write(en[0])
	assert.Nil(t, remEn)

	// 0
	s.assertLRULen(1)
	s.assertLRUEntry(0)
	remEn = s.lru.write(en[1])
	// 1 0
	assert.Nil(t, remEn)

	s.assertLRULen(2)
	s.assertLRUEntry(1)
	s.assertLRUEntry(0)

	s.lru.access(en[0])
	// 0 1

	remEn = s.lru.write(en[2])
	// 2 0 1
	assert.Nil(t, remEn)
	s.assertLRULen(3)

	remEn = s.lru.write(en[3])
	// 3 2 0
	s.assertEntry(remEn, 1, "1", 0)
	s.assertLRULen(3)
	s.assertLRUEntry(3)
	s.assertLRUEntry(2)
	s.assertLRUEntry(0)

	remEn = s.lru.remove(en[2])
	// 3 0
	s.assertEntry(remEn, 2, "2", 0)
	s.assertLRULen(2)
	s.assertLRUEntry(3)
	s.assertLRUEntry(0)
}

func TestLRUWalk(t *testing.T) {
	s := lruTest{t: t}
	s.lru.init(&s.c, 5)

	entries := createLRUEntries(6)
	for _, e := range entries {
		s.lru.write(e)
	}
	// 5 4 3 2 1
	found := ""
	s.lru.iterate(func(en *entry) bool {
		found += en.getValue().(string) + " "
		return true
	})
	assert.Equal(t, "1 2 3 4 5 ", found)
	s.lru.access(entries[1])
	s.lru.access(entries[5])
	s.lru.access(entries[3])
	// 3 5 1 4 2
	found = ""
	s.lru.iterate(func(en *entry) bool {
		found += en.getValue().(string) + " "
		if en.key.(int)%2 == 0 {
			s.lru.remove(en)
		}
		return en.key.(int) != 5
	})
	assert.Equal(t, "2 4 1 5 ", found)

	s.assertLRULen(3)
	s.assertLRUEntry(3)
	s.assertLRUEntry(5)
	s.assertLRUEntry(1)
}

func createLRUEntries(n int) []*entry {
	en := make([]*entry, n)
	for i := range en {
		en[i] = newEntry(i, fmt.Sprintf("%d", i), 0 /* unused */)
	}
	return en
}
