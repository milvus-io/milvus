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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing
// permissions and limitations under the License.

package querycoordv2

import (
	"context"

	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type prewarmJob struct {
	*job.BaseJob
	server *Server
	req    *querypb.PrewarmRequest
}

func newPrewarmJob(ctx context.Context, server *Server, req *querypb.PrewarmRequest) *prewarmJob {
	msgID := int64(0)
	if req.GetBase() != nil {
		msgID = req.GetBase().GetMsgID()
	}
	return &prewarmJob{
		BaseJob: job.NewBaseJob(ctx, msgID, req.GetCollectionID()),
		server:  server,
		req:     req,
	}
}

func (j *prewarmJob) PreExecute() error {
	return validatePrewarmRequest(j.req)
}

func (j *prewarmJob) Execute() (err error) {
	partitionID := j.req.GetPartitionIDs()[0]
	defer func() {
		clearErr := j.server.clearPrewarmForceSyncWarmup(j.Context(), j.req.GetCollectionID(), partitionID)
		if clearErr != nil {
			err = merr.Combine(err, clearErr)
		}
	}()

	if err := j.server.ensurePrewarmPartitionLoaded(j.Context(), j.req); err != nil {
		return err
	}
	return j.server.prewarmLoadedSegments(j.Context(), j.req)
}
