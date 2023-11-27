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

import (
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type importChecker struct {
	manager ImportTaskManager
	meta    *meta
	cluster *Cluster
}

func (c *importChecker) checkErr(task ImportTask, err error) {
	if !merr.IsRetryableErr(err) {
		err = c.manager.Update(task.ID(), UpdateState(datapb.ImportState_Failed))
		if err != nil {
			log.Warn("")
		}
		return
	}
	switch task.State() {
	case datapb.ImportState_Preparing:
		err = c.manager.Update(task.ID(), UpdateState(datapb.ImportState_Pending))
		if err != nil {
			log.Warn("")
		}
	case datapb.ImportState_InProgress:
		err = c.manager.Update(task.ID(), UpdateState(datapb.ImportState_Preparing))
		if err != nil {
			log.Warn("")
		}
	}
}

func (c *importChecker) check() {
	for _, task := range c.manager.GetBy(WithStates(datapb.ImportState_Preparing, datapb.ImportState_InProgress)) {
		if task.NodeID() == fakeNodeID {
			continue
		}
		req := &datapb.GetImportStateRequest{
			RequestID: task.ReqID(),
			TaskID:    task.ID(),
		}
		resp, err := c.cluster.GetImportState(task.NodeID(), req)
		if err != nil {
			log.Warn("")
			c.checkErr(task, err)
			return
		}
		// TODO: check if rows changed
		err = c.manager.Update(task.ID(), UpdateFileInfo(resp.GetFileInfos()))
		if err != nil {
			log.Warn("")
			return
		}

		for _, info := range resp.GetSegmentInfos() {
			operator := UpdateBinlogsOperator(info.GetID(), info.GetBinlogs(), info.GetStatslogs(), info.GetDeltalogs())
			err := c.meta.UpdateSegmentsInfo(operator)
			if err != nil {
				log.Warn("")
				continue
			}
		}
	}
}
