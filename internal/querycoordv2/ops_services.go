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

package querycoordv2

import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func (s *Server) ListCheckers(ctx context.Context, req *querypb.ListCheckersRequest) (*querypb.ListCheckersResponse, error) {
	log := log.Ctx(ctx)
	log.Info("list checkers request received")
	if err := merr.CheckHealthy(s.State()); err != nil {
		log.Warn("failed to list checkers", zap.Error(err))
		return &querypb.ListCheckersResponse{
			Status: merr.Status(err),
		}, nil
	}
	checkers := s.checkerController.Checkers()
	checkerIDSet := typeutil.NewSet(req.CheckerIDs...)

	resp := &querypb.ListCheckersResponse{
		Status: merr.Success(),
	}
	for _, checker := range checkers {
		if checkerIDSet.Len() == 0 || checkerIDSet.Contain(int32(checker.ID())) {
			resp.CheckerInfos = append(resp.CheckerInfos, &querypb.CheckerInfo{
				Id:                  int32(checker.ID()),
				Activated:           checker.IsActive(),
				Desc:                checker.ID().String(),
				Found:               true,
				InactiveCollections: checker.GetInactiveCollections(),
			})
			checkerIDSet.Remove(int32(checker.ID()))
		}
	}

	for _, id := range checkerIDSet.Collect() {
		resp.CheckerInfos = append(resp.CheckerInfos, &querypb.CheckerInfo{
			Id:    id,
			Found: false,
		})
	}

	return resp, nil
}

func (s *Server) ActivateChecker(ctx context.Context, req *querypb.ActivateCheckerRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(zap.Int32("checker", req.CheckerID)).With(zap.Int64s("collections", req.Collections))
	log.Info("activate checker request received")
	if err := merr.CheckHealthy(s.State()); err != nil {
		log.Warn("failed to activate checker", zap.Error(err))
		return merr.Status(err), nil
	}

	if len(req.Collections) == 0 {
		if err := s.checkerController.Activate(utils.CheckerType(req.CheckerID)); err != nil {
			log.Warn("failed to activate checker", zap.Error(err))
			return merr.Status(merr.WrapErrServiceInternal(err.Error())), nil
		}
	} else {
		for _, collection := range req.Collections {
			if err := s.checkerController.ActivateCollection(utils.CheckerType(req.CheckerID), collection); err != nil {
				log.Warn("failed to activate collection", zap.Int64("collection", collection), zap.Error(err))
			}
		}
	}
	return merr.Success(), nil
}

func (s *Server) DeactivateChecker(ctx context.Context, req *querypb.DeactivateCheckerRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(zap.Int32("checker", req.CheckerID)).With(zap.Int64s("collections", req.Collections))
	log.Info("deactivate checker request received")
	if err := merr.CheckHealthy(s.State()); err != nil {
		log.Warn("failed to deactivate checker", zap.Error(err))
		return merr.Status(err), nil
	}
	if len(req.Collections) == 0 {
		if err := s.checkerController.Deactivate(utils.CheckerType(req.CheckerID)); err != nil {
			log.Warn("failed to deactivate checker", zap.Error(err))
			return merr.Status(merr.WrapErrServiceInternal(err.Error())), nil
		}
	} else {
		for _, collection := range req.Collections {
			if err := s.checkerController.DeactivateCollection(utils.CheckerType(req.CheckerID), collection); err != nil {
				log.Warn("failed to deactivate collection", zap.Error(err))
				return merr.Status(merr.WrapErrServiceInternal(err.Error())), nil
			}
		}
	}
	return merr.Success(), nil
}
