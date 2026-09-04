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

package delegator

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/internal/util/grpcclient"
	"github.com/milvus-io/milvus/internal/util/searchutil/optimizers"
	"github.com/milvus-io/milvus/internal/util/textindex"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type FuzzyTextTermExpander interface {
	ExpandTextTerms(
		fieldID int64,
		prepared []*textindex.PreparedFuzzySearch,
		maxExpansions uint32,
	) ([][]textindex.FuzzyMatch, uint64, error)
}

type FuzzyTextTermExpansionTarget struct {
	SegmentID int64
	Expander  FuzzyTextTermExpander
}

// ValidateFuzzyBM25ExpansionOutputSize applies the same output-size limit used
// by Search and Query without truncating fuzzy term candidates.
func ValidateFuzzyBM25ExpansionOutputSize(response *querypb.ExpandTextTermsResponse) error {
	return ValidateFuzzyBM25ExpansionSize(int64(proto.Size(response)))
}

// ValidateFuzzyBM25ExpansionSize checks a projected expansion wire size.
func ValidateFuzzyBM25ExpansionSize(outputSize int64) error {
	maxOutputSize := paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64()
	if outputSize > maxOutputSize {
		return merr.WrapErrParameterTooLarge(fmt.Sprintf(
			"fuzzy BM25 term expansion result size %d exceeds quotaAndLimits.limits.maxOutputSize %d bytes",
			outputSize, maxOutputSize))
	}
	return nil
}

// FuzzyBM25ExpandedTermWireSize returns one repeated response-term field size.
func FuzzyBM25ExpandedTermWireSize(term *querypb.ExpandedTextTerm) int64 {
	size := proto.Size(term)
	return int64(protowire.SizeTag(2) + protowire.SizeBytes(size))
}

func fuzzyBM25GenerationWireSize(generation *querypb.SegmentTextTermGeneration) int64 {
	size := proto.Size(generation)
	return int64(protowire.SizeTag(3) + protowire.SizeBytes(size))
}

func addFuzzyBM25Candidate(
	candidates map[uint32]map[string]*querypb.ExpandedTextTerm,
	wireSize *int64,
	term *querypb.ExpandedTextTerm,
	sourceCount int,
) error {
	if int(term.GetSourceIndex()) >= sourceCount {
		return merr.WrapErrServiceInternalMsg("expansion returned invalid fuzzy source index %d", term.GetSourceIndex())
	}
	byTerm := candidates[term.GetSourceIndex()]
	key := string(term.GetTerm())
	if current := byTerm[key]; current != nil {
		if term.GetEditDistance() >= current.GetEditDistance() {
			return nil
		}
		updated := &querypb.ExpandedTextTerm{
			SourceIndex:  current.GetSourceIndex(),
			Term:         current.GetTerm(),
			EditDistance: term.GetEditDistance(),
		}
		updatedSize := *wireSize - FuzzyBM25ExpandedTermWireSize(current) + FuzzyBM25ExpandedTermWireSize(updated)
		if err := ValidateFuzzyBM25ExpansionSize(updatedSize); err != nil {
			return err
		}
		current.EditDistance = term.GetEditDistance()
		*wireSize = updatedSize
		return nil
	}

	addedSize := *wireSize + FuzzyBM25ExpandedTermWireSize(term)
	if err := ValidateFuzzyBM25ExpansionSize(addedSize); err != nil {
		return err
	}
	if byTerm == nil {
		byTerm = make(map[string]*querypb.ExpandedTextTerm)
		candidates[term.GetSourceIndex()] = byTerm
	}
	byTerm[key] = &querypb.ExpandedTextTerm{
		SourceIndex:  term.GetSourceIndex(),
		Term:         bytes.Clone(term.GetTerm()),
		EditDistance: term.GetEditDistance(),
	}
	*wireSize = addedSize
	return nil
}

func failFuzzyBM25Expansion(
	response *querypb.ExpandTextTermsResponse,
	err error,
) (*querypb.ExpandTextTermsResponse, error) {
	response.Status = merr.Status(err)
	return response, err
}

// ExpandFuzzyBM25Terms runs the expansion shared by local Delegator targets
// and sealed Worker targets.
func ExpandFuzzyBM25Terms(
	ctx context.Context,
	request *querypb.ExpandTextTermsRequest,
	targets []FuzzyTextTermExpansionTarget,
	nativeSemaphore *syncutil.Semaphore,
) (*querypb.ExpandTextTermsResponse, error) {
	response := &querypb.ExpandTextTermsResponse{Status: merr.Success()}
	fail := func(err error) (*querypb.ExpandTextTermsResponse, error) {
		return failFuzzyBM25Expansion(response, err)
	}
	if len(targets) == 0 {
		return fail(merr.WrapErrServiceInternalMsg("text term expansion requires target segments"))
	}
	if nativeSemaphore == nil {
		return fail(merr.WrapErrServiceInternalMsg("fuzzy BM25 native expansion admission is not initialized"))
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	if err := nativeSemaphore.Acquire(ctx); err != nil {
		return fail(err)
	}
	defer nativeSemaphore.Release()
	prepared, err := textindex.PrepareFuzzySearchTerms(
		request.GetSourceTerms(), request.GetMaxEditDistance(), request.GetPrefixLength())
	if err != nil {
		return fail(err)
	}
	defer func() {
		for _, query := range prepared {
			query.Close()
		}
	}()

	candidates := make(map[uint32]map[string]*querypb.ExpandedTextTerm)
	var candidateWireSize int64
	for _, target := range targets {
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		if target.Expander == nil {
			return fail(merr.WrapErrServiceInternalMsg(
				"segment %d does not support text term expansion", target.SegmentID))
		}
		matches, generation, err := target.Expander.ExpandTextTerms(
			request.GetFieldID(), prepared, request.GetMaxExpansions())
		if err != nil {
			return fail(err)
		}
		response.Generations = append(response.Generations, &querypb.SegmentTextTermGeneration{
			SegmentID:  target.SegmentID,
			Generation: generation,
		})
		for sourceIndex, sourceMatches := range matches {
			for _, match := range sourceMatches {
				if err := addFuzzyBM25Candidate(candidates, &candidateWireSize, &querypb.ExpandedTextTerm{
					SourceIndex:  uint32(sourceIndex),
					Term:         match.Term,
					EditDistance: match.EditDistance,
				}, len(request.GetSourceTerms())); err != nil {
					return fail(err)
				}
			}
		}
	}
	for _, byTerm := range candidates {
		for _, term := range byTerm {
			response.Terms = append(response.Terms, term)
		}
	}
	sort.Slice(response.Terms, func(i, j int) bool {
		if response.Terms[i].GetSourceIndex() != response.Terms[j].GetSourceIndex() {
			return response.Terms[i].GetSourceIndex() < response.Terms[j].GetSourceIndex()
		}
		if response.Terms[i].GetEditDistance() != response.Terms[j].GetEditDistance() {
			return response.Terms[i].GetEditDistance() < response.Terms[j].GetEditDistance()
		}
		return bytes.Compare(response.Terms[i].GetTerm(), response.Terms[j].GetTerm()) < 0
	})
	sort.Slice(response.Generations, func(i, j int) bool {
		return response.Generations[i].GetSegmentID() < response.Generations[j].GetSegmentID()
	})
	if err := ValidateFuzzyBM25ExpansionOutputSize(response); err != nil {
		response.Terms = nil
		response.Generations = nil
		return fail(err)
	}
	return response, nil
}

// expandLocalFuzzyBM25Terms shares one prepared DFA set across every growing
// and sealed segment owned by this QueryNode process.
func (sd *shardDelegator) expandLocalFuzzyBM25Terms(
	ctx context.Context,
	request *querypb.ExpandTextTermsRequest,
	growing []SegmentEntry,
	sealed []SegmentEntry,
) (*querypb.ExpandTextTermsResponse, error) {
	response := &querypb.ExpandTextTermsResponse{Status: merr.Success()}
	fail := func(err error) (*querypb.ExpandTextTermsResponse, error) {
		return failFuzzyBM25Expansion(response, err)
	}
	local := make([]SegmentEntry, 0, len(growing)+len(sealed))
	local = append(local, growing...)
	local = append(local, sealed...)
	targets := make([]FuzzyTextTermExpansionTarget, len(local))
	for index, entry := range local {
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		var segment segments.Segment
		kind := "growing"
		if index < len(growing) {
			segment = sd.segmentManager.GetGrowing(entry.SegmentID)
		} else {
			segment = sd.segmentManager.GetSealed(entry.SegmentID)
			kind = "sealed"
		}
		if segment == nil || segment.Collection() != request.GetCollectionID() {
			return fail(merr.WrapErrSegmentNotLoaded(
				entry.SegmentID, "local %s text term expansion target is unavailable", kind))
		}
		expander, ok := segment.(FuzzyTextTermExpander)
		if !ok {
			return fail(merr.WrapErrServiceInternalMsg(
				"local %s segment %d does not support text term expansion", kind, entry.SegmentID))
		}
		targets[index] = FuzzyTextTermExpansionTarget{SegmentID: entry.SegmentID, Expander: expander}
	}
	return ExpandFuzzyBM25Terms(ctx, request, targets, sd.fuzzyExpansionNativeSemaphore)
}

func buildFuzzyBM25QueryTF(
	queryTF []map[uint32]float32,
	expanded map[uint32][]*querypb.ExpandedTextTerm,
) ([][]byte, error) {
	maxSize := paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64()
	var projectedSize int64
	for _, sourceFrequencies := range queryTF {
		for index := range sourceFrequencies {
			candidateCount := int64(len(expanded[index]))
			if maxSize < projectedSize || candidateCount > (maxSize-projectedSize)/8 {
				return nil, merr.WrapErrParameterTooLarge(fmt.Sprintf(
					"fuzzy BM25 sparse query exceeds quotaAndLimits.limits.maxOutputSize %d bytes",
					maxSize))
			}
			projectedSize += candidateCount * 8
		}
	}

	tfRows := make([][]byte, len(queryTF))
	for queryIndex, sourceFrequencies := range queryTF {
		hashedTF := make(map[uint32]float32)
		for index, frequency := range sourceFrequencies {
			for _, match := range expanded[index] {
				hash := typeutil.HashString2LessUint32(string(match.GetTerm()))
				hashedTF[hash] += frequency
			}
		}
		tfRows[queryIndex] = typeutil.CreateAndSortSparseFloatRow(hashedTF)
	}
	return tfRows, nil
}

func fuzzySearchTargets(sealed []SnapshotItem, growing []SegmentEntry) ([]SnapshotItem, []SegmentEntry) {
	filteredSealed := make([]SnapshotItem, 0, len(sealed))
	for _, item := range sealed {
		segments := lo.Filter(item.Segments, func(segment SegmentEntry, _ int) bool {
			return segment.Level != datapb.SegmentLevel_L0
		})
		if len(segments) > 0 {
			filteredSealed = append(filteredSealed, SnapshotItem{NodeID: item.NodeID, Segments: segments})
		}
	}
	filteredGrowing := lo.Filter(growing, func(segment SegmentEntry, _ int) bool {
		return segment.Level != datapb.SegmentLevel_L0
	})
	return filteredSealed, filteredGrowing
}

func fuzzySearchTargetsBySegmentIDs(
	sealed []SnapshotItem,
	growing []SegmentEntry,
	segmentIDs typeutil.Set[int64],
) ([]SnapshotItem, []SegmentEntry) {
	filteredSealed := make([]SnapshotItem, 0, len(sealed))
	for _, item := range sealed {
		segments := lo.Filter(item.Segments, func(segment SegmentEntry, _ int) bool {
			return segmentIDs.Contain(segment.SegmentID)
		})
		if len(segments) > 0 {
			filteredSealed = append(filteredSealed, SnapshotItem{NodeID: item.NodeID, Segments: segments})
		}
	}
	filteredGrowing := lo.Filter(growing, func(segment SegmentEntry, _ int) bool {
		return segmentIDs.Contain(segment.SegmentID)
	})
	return filteredSealed, filteredGrowing
}

func fuzzySealedRowCount(sealed []SnapshotItem, sealedRowCount map[int64]int64) map[int64]int64 {
	result := make(map[int64]int64)
	for _, item := range sealed {
		for _, segment := range item.Segments {
			if rowCount, ok := sealedRowCount[segment.SegmentID]; ok {
				result[segment.SegmentID] = rowCount
			}
		}
	}
	return result
}

func (sd *shardDelegator) expandFuzzyBM25Terms(
	ctx context.Context,
	inputFieldID int64,
	sourceTerms [][]byte,
	maxEditDistance uint32,
	maxExpansions uint32,
	prefixLength uint32,
	sealed []SnapshotItem,
	growing []SegmentEntry,
	sealedRowCount map[int64]int64,
) (map[uint32][]*querypb.ExpandedTextTerm, []*querypb.SegmentTextTermGeneration, error) {
	if err := textindex.ValidateFuzzySearchTerms(sourceTerms); err != nil {
		return nil, nil, err
	}
	expansionSealed, expansionGrowing := fuzzySearchTargets(sealed, growing)
	expansionRowCount := fuzzySealedRowCount(expansionSealed, sealedRowCount)
	for _, entry := range expansionGrowing {
		if segment, ok := entry.Candidate.(interface{ InsertCount() int64 }); ok {
			expansionRowCount[entry.SegmentID] = segment.InsertCount()
		}
	}
	localNodeID := paramtable.GetNodeID()
	localSealed := make([]SegmentEntry, 0)
	remoteSealed := make([]SnapshotItem, 0, len(expansionSealed))
	for _, item := range expansionSealed {
		if item.NodeID == localNodeID {
			localSealed = append(localSealed, item.Segments...)
		} else {
			remoteSealed = append(remoteSealed, item)
		}
	}
	localIDs := lo.Map(expansionGrowing, func(entry SegmentEntry, _ int) int64 {
		return entry.SegmentID
	})
	localIDs = append(localIDs, lo.Map(localSealed, func(entry SegmentEntry, _ int) int64 {
		return entry.SegmentID
	})...)
	totalSegmentCount := len(localIDs)
	for _, item := range remoteSealed {
		totalSegmentCount += len(item.Segments)
	}
	if totalSegmentCount == 0 {
		return map[uint32][]*querypb.ExpandedTextTerm{}, nil, nil
	}
	request := &querypb.ExpandTextTermsRequest{
		Base:            &commonpb.MsgBase{SourceID: paramtable.GetNodeID()},
		CollectionID:    sd.collectionID,
		FieldID:         inputFieldID,
		SourceTerms:     sourceTerms,
		MaxEditDistance: maxEditDistance,
		MaxExpansions:   maxExpansions,
		PrefixLength:    prefixLength,
	}
	tasks, err := organizeSubTask(ctx, request, remoteSealed, nil, sd, true,
		func(req *querypb.ExpandTextTermsRequest, _ querypb.DataScope, segmentIDs []int64, targetID int64) *querypb.ExpandTextTermsRequest {
			return &querypb.ExpandTextTermsRequest{
				Base: &commonpb.MsgBase{
					SourceID: paramtable.GetNodeID(),
					TargetID: targetID,
				},
				CollectionID:    req.GetCollectionID(),
				FieldID:         req.GetFieldID(),
				SourceTerms:     req.GetSourceTerms(),
				MaxEditDistance: req.GetMaxEditDistance(),
				MaxExpansions:   req.GetMaxExpansions(),
				PrefixLength:    req.GetPrefixLength(),
				SegmentIDs:      segmentIDs,
			}
		})
	if err != nil {
		return nil, nil, err
	}

	successSegments := typeutil.NewSet[int64]()
	failureSegments := make([]int64, 0)
	var expansionErrors []error
	served := make(map[int64]uint64, totalSegmentCount)
	candidates := make(map[uint32]map[string]*querypb.ExpandedTextTerm)
	var aggregateWireSize int64
	partialEnabled := paramtable.Get().QueryNodeCfg.PartialResultRequiredDataRatio.GetAsFloat() < 1
	mergeResponse := func(segmentIDs []int64, response *querypb.ExpandTextTermsResponse) error {
		expected := make(map[int64]struct{}, len(segmentIDs))
		for _, segmentID := range segmentIDs {
			expected[segmentID] = struct{}{}
		}
		responseGenerations := make(map[int64]uint64, len(segmentIDs))
		for _, generation := range response.GetGenerations() {
			if _, ok := expected[generation.GetSegmentID()]; !ok || generation.GetGeneration() == 0 {
				return merr.WrapErrServiceInternalMsg(
					"expansion returned unexpected text term generation for segment %d", generation.GetSegmentID())
			}
			current := generation.GetGeneration()
			if existing, ok := responseGenerations[generation.GetSegmentID()]; ok && existing != current {
				return merr.WrapErrServiceInternalMsg(
					"expansion returned conflicting text term generations for segment %d", generation.GetSegmentID())
			}
			responseGenerations[generation.GetSegmentID()] = current
		}
		if len(responseGenerations) != len(expected) {
			missing := make([]int64, 0)
			for segmentID := range expected {
				if _, ok := responseGenerations[segmentID]; !ok {
					missing = append(missing, segmentID)
				}
			}
			sort.Slice(missing, func(i, j int) bool { return missing[i] < missing[j] })
			return merr.WrapErrServiceInternalMsg("text term expansion missed target segments %v", missing)
		}
		for _, term := range response.GetTerms() {
			if err := addFuzzyBM25Candidate(candidates, &aggregateWireSize, term, len(sourceTerms)); err != nil {
				return err
			}
		}
		for segmentID, current := range responseGenerations {
			if existing, ok := served[segmentID]; ok && existing != current {
				return merr.WrapErrServiceInternalMsg(
					"expansion returned conflicting text term generations for segment %d", segmentID)
			}
			if _, ok := served[segmentID]; !ok {
				nextSize := aggregateWireSize + fuzzyBM25GenerationWireSize(&querypb.SegmentTextTermGeneration{
					SegmentID: segmentID, Generation: current,
				})
				if err := ValidateFuzzyBM25ExpansionSize(nextSize); err != nil {
					return err
				}
				aggregateWireSize = nextSize
			}
			served[segmentID] = current
		}
		return nil
	}
	record := func(segments []int64, response *querypb.ExpandTextTermsResponse, err error) error {
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			failureSegments = append(failureSegments, segments...)
			expansionErrors = append(expansionErrors, err)
			if !partialEnabled {
				return err
			}
			return nil
		}
		if response == nil {
			return merr.WrapErrServiceInternalMsg("text term expansion returned a nil response")
		}
		if err := mergeResponse(segments, response); err != nil {
			return err
		}
		successSegments.Insert(segments...)
		return nil
	}

	if len(localIDs) > 0 {
		response, expandErr := sd.expandLocalFuzzyBM25Terms(ctx, request, expansionGrowing, localSealed)
		if err := record(localIDs, response, expandErr); err != nil {
			return nil, nil, err
		}
	}

	for _, task := range tasks {
		var response *querypb.ExpandTextTermsResponse
		var expandErr error
		switch {
		case task.targetID == -1 || task.worker == nil:
			expandErr = merr.WrapErrServiceInternalMsg(
				"segments not loaded in any worker: %v", task.req.GetSegmentIDs()[:min(len(task.req.GetSegmentIDs()), 10)])
		case sd.fuzzyExpansionRPCSemaphore == nil:
			expandErr = merr.WrapErrServiceInternalMsg("fuzzy BM25 RPC admission is not initialized")
		case !sd.fuzzyExpansionRPCSemaphore.TryAcquire():
			expandErr = merr.WrapErrTooManyRequests(
				int32(sd.fuzzyExpansionRPCSemaphore.Cap()), "fuzzy BM25 Worker RPC concurrency is saturated")
		default:
			response, expandErr = task.worker.ExpandTextTerms(ctx, task.req)
			sd.fuzzyExpansionRPCSemaphore.Release()
			if errors.Is(expandErr, merr.ErrNodeNotFound) || grpcclient.IsServerIDMismatchErr(expandErr) {
				sd.markSegmentOffline(task.req.GetSegmentIDs()...)
			}
			if expandErr != nil {
				expandErr = merr.Wrapf(expandErr, "worker(%d) text term expansion failed", task.targetID)
			}
		}
		if err := record(task.req.GetSegmentIDs(), response, expandErr); err != nil {
			return nil, nil, err
		}
	}

	if len(expansionErrors) > 0 {
		for _, segmentID := range failureSegments {
			if _, ok := expansionRowCount[segmentID]; !ok {
				return nil, nil, merr.Combine(expansionErrors...)
			}
		}
		shouldReturnPartial, accessedDataRatio := NewRowCountBasedEvaluator(expansionRowCount)(
			"ExpandTextTerms", successSegments, failureSegments, expansionErrors)
		if !shouldReturnPartial {
			return nil, nil, merr.Combine(expansionErrors...)
		}
		sd.getLogger(ctx).Info(ctx, "text term expansion returned a partial result",
			mlog.Float64("accessedDataRatio", accessedDataRatio),
			mlog.Int64s("failureSegmentList", failureSegments),
			mlog.Err(merr.Combine(expansionErrors...)),
		)
	}

	generations := make([]*querypb.SegmentTextTermGeneration, 0, len(served))
	for segmentID, generation := range served {
		generations = append(generations, &querypb.SegmentTextTermGeneration{
			SegmentID:  segmentID,
			Generation: generation,
		})
	}
	sort.Slice(generations, func(i, j int) bool { return generations[i].GetSegmentID() < generations[j].GetSegmentID() })
	result := make(map[uint32][]*querypb.ExpandedTextTerm, len(candidates))
	for sourceIndex, byTerm := range candidates {
		for _, term := range byTerm {
			result[sourceIndex] = append(result[sourceIndex], term)
		}
	}
	return result, generations, nil
}

func (sd *shardDelegator) buildFuzzyBM25IDF(
	ctx context.Context,
	req *querypb.SearchRequest,
	runner function.FunctionRunner,
	sealed []SnapshotItem,
	growing []SegmentEntry,
	sealedRowCount map[int64]int64,
) (float64, error) {
	options := req.GetReq().GetFuzzyBm25Options()
	if options == nil || options.GetMaxExpansions() == 0 || options.GetMaxEditDistance() > 2 {
		return 0, merr.WrapErrServiceInternalMsg("invalid fuzzy BM25 options at delegator")
	}
	pb := &commonpb.PlaceholderGroup{}
	if err := proto.Unmarshal(req.GetReq().GetPlaceholderGroup(), pb); err != nil {
		return 0, merr.WrapErrParameterInvalidErr(err, "failed to unmarshal fuzzy BM25 placeholder group")
	}
	if len(pb.GetPlaceholders()) != 1 || len(pb.GetPlaceholders()[0].GetValues()) == 0 ||
		pb.GetPlaceholders()[0].GetType() != commonpb.PlaceholderType_VarChar {
		return 0, merr.WrapErrParameterInvalidMsg("please provide varchar/text for fuzzy BM25 search")
	}
	texts := funcutil.GetVarCharFromPlaceholder(pb.GetPlaceholders()[0])
	for _, text := range texts {
		if !typeutil.IsUTF8(text) {
			return 0, merr.WrapErrParameterInvalidMsg("string data must be utf8 format: %v", text)
		}
	}
	datas := []any{texts}
	if len(runner.GetInputFields()) == 2 {
		analyzerName := req.GetReq().GetAnalyzerName()
		if analyzerName == "" {
			analyzerName = "default"
		}
		analyzerNames := make([]string, len(texts))
		for i := range analyzerNames {
			analyzerNames[i] = analyzerName
		}
		datas = append(datas, analyzerNames)
	}
	analyzer, ok := runner.(function.Analyzer)
	if !ok {
		return 0, merr.WrapErrServiceInternalMsg("BM25 runner does not expose analyzer")
	}
	tokensByQuery, err := analyzer.BatchAnalyze(false, false, datas...)
	if err != nil {
		return 0, err
	}
	if len(runner.GetInputFields()) == 0 {
		return 0, merr.WrapErrServiceInternalMsg("BM25 runner has no input field")
	}

	sourceTerms := make([][]byte, 0)
	sourceIndex := make(map[string]uint32)
	queryTF := make([]map[uint32]float32, len(tokensByQuery))
	for queryIndex, tokens := range tokensByQuery {
		queryTF[queryIndex] = make(map[uint32]float32)
		for _, token := range tokens {
			term := token.GetToken()
			index, ok := sourceIndex[term]
			if !ok {
				index = uint32(len(sourceTerms))
				sourceIndex[term] = index
				sourceTerms = append(sourceTerms, []byte(term))
			}
			queryTF[queryIndex][index]++
		}
	}

	expanded, generations, err := sd.expandFuzzyBM25Terms(
		ctx,
		runner.GetInputFields()[0].GetFieldID(),
		sourceTerms,
		options.GetMaxEditDistance(),
		options.GetMaxExpansions(),
		options.GetPrefixLength(),
		sealed,
		growing,
		sealedRowCount,
	)
	if err != nil {
		return 0, err
	}
	idfOracle := sd.getIDFOracle()
	if idfOracle == nil {
		return 0, merr.WrapErrServiceInternalMsg("bm25 oracle is not initialized")
	}
	tfRows, err := buildFuzzyBM25QueryTF(queryTF, expanded)
	if err != nil {
		return 0, err
	}
	idfRows, avgdl, err := idfOracle.BuildIDF(req.GetReq().GetFieldId(), &schemapb.SparseFloatArray{
		Contents: tfRows,
	})
	if err != nil {
		return 0, err
	}
	if avgdl <= 0 {
		return 0, nil
	}
	for _, idf := range idfRows {
		metrics.QueryNodeSearchFTSNumTokens.WithLabelValues(
			paramtable.GetStringNodeID(), fmt.Sprint(sd.collectionID), fmt.Sprint(req.GetReq().GetFieldId())).
			Observe(float64(typeutil.SparseFloatRowElementCount(idf)))
	}
	if err := SetBM25Params(req.GetReq(), avgdl); err != nil {
		return 0, err
	}
	placeholder := funcutil.SparseVectorDataToPlaceholderGroupBytes(idfRows)
	maxSize := paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64()
	if int64(len(placeholder)) > maxSize {
		return 0, merr.WrapErrParameterTooLarge(fmt.Sprintf(
			"fuzzy BM25 sparse query size %d exceeds quotaAndLimits.limits.maxOutputSize %d bytes",
			len(placeholder), maxSize))
	}
	req.Req.PlaceholderGroup = placeholder
	req.TextTermGenerations = generations
	return avgdl, nil
}

func (sd *shardDelegator) searchFuzzyBM25(
	ctx context.Context,
	req *querypb.SearchRequest,
	sealed []SnapshotItem,
	growing []SegmentEntry,
	sealedRowCount map[int64]int64,
) ([]*internalpb.SearchResults, error) {
	sealed, growing = fuzzySearchTargets(sealed, growing)
	var lastErr error
	for attempt := 1; attempt <= 2; attempt++ {
		attemptReq := typeutil.Clone(req)
		fuzzyRowCount := fuzzySealedRowCount(sealed, sealedRowCount)
		_, skipSearch, err := sd.prepareSearchFunction(ctx, attemptReq, sealed, growing, fuzzyRowCount)
		if err != nil {
			lastErr = err
			if merr.IsRetryableErr(err) && attempt < 2 {
				sd.getLogger(ctx).Warn(ctx, "retry fuzzy BM25 from expansion after transient preparation failure",
					mlog.Int("attempt", attempt), mlog.Err(err))
				continue
			}
			break
		}
		if skipSearch {
			return []*internalpb.SearchResults{}, nil
		}
		servedSegmentIDs := typeutil.NewSet[int64]()
		for _, generation := range attemptReq.GetTextTermGenerations() {
			servedSegmentIDs.Insert(generation.GetSegmentID())
		}
		attemptSealed, attemptGrowing := fuzzySearchTargetsBySegmentIDs(sealed, growing, servedSegmentIDs)
		rowCounts := make([]int64, 0, len(servedSegmentIDs))
		for _, item := range attemptSealed {
			for _, segment := range item.Segments {
				rowCounts = append(rowCounts, sealedRowCount[segment.SegmentID])
			}
		}
		effectiveSegmentNum := optimizers.CalculateEffectiveSegmentNum(
			sd.queryHook, rowCounts, attemptReq.GetReq().GetTopk())
		if optimizers.ShouldUseTwoStageSearch(attemptReq, effectiveSegmentNum) {
			results, fallback, err := sd.twoStageSearch(ctx, attemptReq, attemptSealed, attemptGrowing, sealedRowCount)
			if err == nil && !fallback {
				return results, nil
			}
			if err != nil {
				lastErr = err
				if !merr.IsRetryableErr(err) || attempt == 2 {
					break
				}
				sd.getLogger(ctx).Warn(ctx, "retry fuzzy BM25 from expansion after transient two-stage search failure",
					mlog.Int("attempt", attempt), mlog.Err(err))
				continue
			}
			sd.getLogger(ctx).Debug(ctx, "Two-stage fuzzy BM25 search requested fallback, continuing with normal search")
		}
		const isSecondStageSearch = false
		attemptReq, err = optimizers.OptimizeSearchParams(
			ctx, attemptReq, sd.queryHook, effectiveSegmentNum, isSecondStageSearch, sd.getVectorFieldDim)
		if err != nil {
			return nil, err
		}
		results, err := sd.executeSearchSubTasks(ctx, attemptReq, attemptSealed, attemptGrowing, sealedRowCount)
		if err == nil {
			return results, nil
		}
		lastErr = err
		if !merr.IsRetryableErr(err) || attempt == 2 {
			break
		}
		sd.getLogger(ctx).Warn(ctx, "retry fuzzy BM25 from expansion after transient search failure",
			mlog.Int("attempt", attempt), mlog.Err(err))
	}
	return nil, lastErr
}
