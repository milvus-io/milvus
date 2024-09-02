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

package datacoord

import "sync"

type channelSingleton struct {
	ch chan UniqueID
}

func (cs *channelSingleton) getChannel() chan UniqueID {
	return cs.ch
}

var buildIndexCh *channelSingleton
var statsTaskCh *channelSingleton
var buildIndexChOnce sync.Once
var statsTaskChOnce sync.Once

func getBuildIndexChSingleton() *channelSingleton {
	buildIndexChOnce.Do(func() {
		buildIndexCh = &channelSingleton{
			ch: make(chan UniqueID, 1024),
		}
	})

	return buildIndexCh
}

func getStatsTaskChSingleton() *channelSingleton {
	statsTaskChOnce.Do(func() {
		statsTaskCh = &channelSingleton{
			ch: make(chan UniqueID, 1024),
		}
	})
	return statsTaskCh
}
