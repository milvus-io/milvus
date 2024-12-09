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

package metautil

import (
	"fmt"
	"regexp"
	"strconv"
	"sync"

	"github.com/milvus-io/milvus/pkg/util/merr"
)

const (
	rgnPhysicalName = `PhysicalName`
	rgnCollectionID = `CollectionID`
	rgnShardIdx     = `ShardIdx`
)

var channelNameFormat = regexp.MustCompile(fmt.Sprintf(`^(?P<%s>.*)_(?P<%s>\d+)v(?P<%s>\d+)$`, rgnPhysicalName, rgnCollectionID, rgnShardIdx))

// ChannelMapper is the interface provides physical channel name mapping functions.
type ChannelMapper interface {
	ChannelIdx(string) int
	ChannelName(int) string
}

// dynamicChannelMapper implements ChannelMapper.
// provides dynamically changed indexing for services without global channel names.
type dynamicChannelMapper struct {
	mut      sync.RWMutex
	nameIdx  map[string]int
	channels []string
}

func (m *dynamicChannelMapper) channelIdx(name string) (int, bool) {
	m.mut.RLock()
	defer m.mut.RUnlock()

	idx, ok := m.nameIdx[name]
	return idx, ok
}

func (m *dynamicChannelMapper) ChannelIdx(name string) int {
	idx, ok := m.channelIdx(name)
	if ok {
		return idx
	}

	m.mut.Lock()
	defer m.mut.Unlock()
	idx, ok = m.nameIdx[name]
	if ok {
		return idx
	}

	idx = len(m.channels)
	m.channels = append(m.channels, name)
	m.nameIdx[name] = idx
	return idx
}

func (m *dynamicChannelMapper) ChannelName(idx int) string {
	m.mut.RLock()
	defer m.mut.RUnlock()

	return m.channels[idx]
}

func NewDynChannelMapper() *dynamicChannelMapper {
	return &dynamicChannelMapper{
		nameIdx: make(map[string]int),
	}
}

// Channel struct maintains the channel information
type Channel struct {
	ChannelMapper
	channelIdx   int
	collectionID int64
	shardIdx     int64
}

func (c Channel) PhysicalName() string {
	return c.ChannelName(c.channelIdx)
}

func (c Channel) VirtualName() string {
	return fmt.Sprintf("%s_%dv%d", c.PhysicalName(), c.collectionID, c.shardIdx)
}

func (c Channel) Equal(ac Channel) bool {
	return c.channelIdx == ac.channelIdx &&
		c.collectionID == ac.collectionID &&
		c.shardIdx == ac.shardIdx
}

func (c Channel) EqualString(str string) bool {
	ac, err := ParseChannel(str, c.ChannelMapper)
	if err != nil {
		return false
	}
	return c.Equal(ac)
}

func (c Channel) IsZero() bool {
	return c.ChannelMapper == nil
}

func ParseChannel(virtualName string, mapper ChannelMapper) (Channel, error) {
	if !channelNameFormat.MatchString(virtualName) {
		return Channel{}, merr.WrapErrParameterInvalidMsg("virtual channel name(%s) is not valid", virtualName)
	}
	matches := channelNameFormat.FindStringSubmatch(virtualName)

	physicalName := matches[channelNameFormat.SubexpIndex(rgnPhysicalName)]
	collectionIDRaw := matches[channelNameFormat.SubexpIndex(rgnCollectionID)]
	shardIdxRaw := matches[channelNameFormat.SubexpIndex(rgnShardIdx)]
	collectionID, err := strconv.ParseInt(collectionIDRaw, 10, 64)
	if err != nil {
		return Channel{}, err
	}
	shardIdx, err := strconv.ParseInt(shardIdxRaw, 10, 64)
	if err != nil {
		return Channel{}, err
	}
	return NewChannel(physicalName, collectionID, shardIdx, mapper), nil
}

// NewChannel returns a Channel instance with provided physical channel and other informations.
func NewChannel(physicalName string, collectionID int64, idx int64, mapper ChannelMapper) Channel {
	c := Channel{
		ChannelMapper: mapper,

		collectionID: collectionID,
		shardIdx:     idx,
	}

	c.channelIdx = c.ChannelIdx(physicalName)

	return c
}
