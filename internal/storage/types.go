// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package storage

import (
	"fmt"
)

type StatsLogType int64

const (
	DefaultStatsType StatsLogType = iota + 0

	// CompundStatsType log save multiple stats
	// and bloom filters to one file
	CompoundStatsType
)

func (s StatsLogType) LogIdx() string {
	return fmt.Sprintf("%d", s)
}
