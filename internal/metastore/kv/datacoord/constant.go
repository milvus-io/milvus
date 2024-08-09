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

const (
	MetaPrefix                         = "datacoord-meta"
	SegmentPrefix                      = MetaPrefix + "/s"
	SegmentBinlogPathPrefix            = MetaPrefix + "/binlog"
	SegmentDeltalogPathPrefix          = MetaPrefix + "/deltalog"
	SegmentStatslogPathPrefix          = MetaPrefix + "/statslog"
	ChannelRemovePrefix                = MetaPrefix + "/channel-removal"
	ChannelCheckpointPrefix            = MetaPrefix + "/channel-cp"
	ImportJobPrefix                    = MetaPrefix + "/import-job"
	ImportTaskPrefix                   = MetaPrefix + "/import-task"
	PreImportTaskPrefix                = MetaPrefix + "/preimport-task"
	CompactionTaskPrefix               = MetaPrefix + "/compaction-task"
	AnalyzeTaskPrefix                  = MetaPrefix + "/analyze-task"
	PartitionStatsInfoPrefix           = MetaPrefix + "/partition-stats"
	PartitionStatsCurrentVersionPrefix = MetaPrefix + "/current-partition-stats-version"
	StatsTaskPrefix                    = MetaPrefix + "/stats-task"

	NonRemoveFlagTomestone = "non-removed"
	RemoveFlagTomestone    = "removed"
)
