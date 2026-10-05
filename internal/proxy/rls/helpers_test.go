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

package rls

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2/rewriter"
	"github.com/milvus-io/milvus/internal/util/rlsutil"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// Reference merging and convenience setup belong to tests, not the request path.
func MergePredicateToPlan(plan *planpb.PlanNode, rlsPredicate *planpb.Expr) error {
	if rlsPredicate != nil {
		rlsPredicate = proto.Clone(rlsPredicate).(*planpb.Expr)
	}
	return mergePredicateToPlan(plan, rlsPredicate, mergePredicate)
}

func mergePredicate(userPredicate *planpb.Expr, rlsPredicate *planpb.Expr) *planpb.Expr {
	if userPredicate == nil || rewriter.IsAlwaysTrueExpr(userPredicate) {
		return rlsPredicate
	}
	if rlsPredicate == nil || rewriter.IsAlwaysTrueExpr(rlsPredicate) {
		return userPredicate
	}
	switch wrapper := userPredicate.GetExpr().(type) {
	case *planpb.Expr_RandomSampleExpr:
		wrapper.RandomSampleExpr.Predicate = mergePredicate(wrapper.RandomSampleExpr.GetPredicate(), rlsPredicate)
		return userPredicate
	}
	return rewriter.RewriteExpr(combinePredicate(userPredicate, rlsPredicate))
}

func validateCheckForWrite(ctx context.Context, m *manager, collectionID UniqueID, principalName string, action rlsutil.PolicyAction, fieldsData []*schemapb.FieldData, schemaHelper *typeutil.SchemaHelper, rowNum int, operation string) error {
	checkExpr, err := m.resolveCheckPredicate(ctx, collectionID, principalName, action, schemaHelper)
	if err != nil {
		return err
	}
	if err := rlsutil.ValidateStaticCheckPredicate(checkExpr, operation); err != nil {
		return err
	}
	if checkExpr == nil {
		return nil
	}
	return rlsutil.ValidateRowsByPredicate(ctx, fieldsData, rowNum, checkExpr, operation, "check")
}

func (m *manager) refreshPolicies(collectionID UniqueID) error {
	if m == nil {
		return merr.WrapErrServiceInternalMsg("failed to refresh RLS policy snapshot without manager")
	}
	if collectionID == 0 {
		return merr.WrapErrServiceInternalMsg("failed to refresh RLS policy snapshot with empty collection id")
	}
	state, generation := m.beginPolicyRefresh(collectionID)
	if state == nil {
		return merr.WrapErrServiceUnavailableMsg("RLS collection %d was removed before policy refresh", collectionID)
	}
	return m.refreshPoliciesAtGeneration(collectionID, state, generation)
}
