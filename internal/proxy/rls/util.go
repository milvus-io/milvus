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

func QueryAction(isIterator bool) rlsutil.PolicyAction {
	if isIterator {
		return rlsutil.PolicyActionQueryIterator
	}
	return rlsutil.PolicyActionQuery
}

func SearchAction(isAdvanced bool, isIterator bool) rlsutil.PolicyAction {
	if isAdvanced {
		return rlsutil.PolicyActionHybridSearch
	}
	if isIterator {
		return rlsutil.PolicyActionSearchIterator
	}
	return rlsutil.PolicyActionSearch
}

func MergePredicateToPlan(plan *planpb.PlanNode, rlsPredicate *planpb.Expr) error {
	if rlsPredicate != nil {
		rlsPredicate = proto.Clone(rlsPredicate).(*planpb.Expr)
	}
	return mergePredicateToPlan(plan, rlsPredicate, mergePredicate)
}

// MergeNormalizedPredicateToPlan merges parser- and RLS-rewritten predicates
// without walking either tree again. The caller must keep both inputs immutable
// until the plan has been serialized.
func MergeNormalizedPredicateToPlan(plan *planpb.PlanNode, rlsPredicate *planpb.Expr) error {
	return mergePredicateToPlan(plan, rlsPredicate, mergeNormalizedPredicate)
}

// AttachPredicateToRequeryPlan combines independently executable predicates
// without rewriting either tree. Requery primary-key terms may be large.
func AttachPredicateToRequeryPlan(plan *planpb.PlanNode, rlsPredicate *planpb.Expr) error {
	return mergePredicateToPlan(plan, rlsPredicate, combinePredicate)
}

func mergePredicateToPlan(plan *planpb.PlanNode, rlsPredicate *planpb.Expr, merge func(*planpb.Expr, *planpb.Expr) *planpb.Expr) error {
	if rlsPredicate == nil || rewriter.IsAlwaysTrueExpr(rlsPredicate) {
		return nil
	}
	if plan == nil {
		return merr.WrapErrServiceInternalMsg("failed to merge RLS predicate into nil plan")
	}
	switch node := plan.GetNode().(type) {
	case *planpb.PlanNode_Query:
		node.Query.Predicates = merge(node.Query.GetPredicates(), rlsPredicate)
	case *planpb.PlanNode_VectorAnns:
		node.VectorAnns.Predicates = merge(node.VectorAnns.GetPredicates(), rlsPredicate)
	case *planpb.PlanNode_Predicates:
		node.Predicates = merge(node.Predicates, rlsPredicate)
	default:
		return merr.WrapErrServiceInternalMsg("failed to merge RLS predicate into unsupported plan node %T", node)
	}
	return nil
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

func mergeNormalizedPredicate(userPredicate *planpb.Expr, rlsPredicate *planpb.Expr) *planpb.Expr {
	if userPredicate == nil || rewriter.IsAlwaysTrueExpr(userPredicate) {
		return rlsPredicate
	}
	if rlsPredicate == nil || rewriter.IsAlwaysTrueExpr(rlsPredicate) {
		return userPredicate
	}
	switch wrapper := userPredicate.GetExpr().(type) {
	case *planpb.Expr_RandomSampleExpr:
		return &planpb.Expr{
			Expr: &planpb.Expr_RandomSampleExpr{RandomSampleExpr: &planpb.RandomSampleExpr{
				SampleFactor: wrapper.RandomSampleExpr.GetSampleFactor(),
				Predicate: mergeNormalizedPredicate(
					wrapper.RandomSampleExpr.GetPredicate(), rlsPredicate),
			}},
			IsTemplate: userPredicate.GetIsTemplate(),
		}
	}
	return rewriter.MergeNormalizedAnd(userPredicate, rlsPredicate)
}

func combinePredicate(left, right *planpb.Expr) *planpb.Expr {
	if left == nil || rewriter.IsAlwaysTrueExpr(left) {
		return right
	}
	if right == nil || rewriter.IsAlwaysTrueExpr(right) {
		return left
	}
	return &planpb.Expr{Expr: &planpb.Expr_BinaryExpr{BinaryExpr: &planpb.BinaryExpr{
		Op:    planpb.BinaryExpr_LogicalAnd,
		Left:  left,
		Right: right,
	}}}
}

func alwaysTruePredicate() *planpb.Expr {
	return &planpb.Expr{Expr: &planpb.Expr_AlwaysTrueExpr{AlwaysTrueExpr: &planpb.AlwaysTrueExpr{}}}
}

func alwaysFalsePredicate() *planpb.Expr {
	return &planpb.Expr{Expr: &planpb.Expr_UnaryExpr{UnaryExpr: &planpb.UnaryExpr{
		Op:    planpb.UnaryExpr_Not,
		Child: alwaysTruePredicate(),
	}}}
}

func ValidateCheckForWrite(ctx context.Context, collectionID UniqueID, principalName string, action rlsutil.PolicyAction, fieldsData []*schemapb.FieldData, schemaHelper *typeutil.SchemaHelper, rowNum int, operation string) error {
	return validateCheckForWrite(ctx, defaultManager, collectionID, principalName, action, fieldsData, schemaHelper, rowNum, operation)
}

func ResolveCheckForWrite(ctx context.Context, collectionID UniqueID, principalName string, action rlsutil.PolicyAction, schemaHelper *typeutil.SchemaHelper, operation string) (*planpb.Expr, error) {
	checkExpr, err := defaultManager.resolveCheckPredicate(ctx, collectionID, principalName, action, schemaHelper)
	if err != nil {
		return nil, err
	}
	if err := rlsutil.ValidateStaticCheckPredicate(checkExpr, operation); err != nil {
		return nil, err
	}
	return checkExpr, nil
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
