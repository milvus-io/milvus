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

package balance

import (
	"fmt"
	"math"
	"sort"

	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

type EpochBalancePolicy interface {
	Plan(
		snapshot *PlacementSnapshot,
		budget BalanceWaveBudget,
		constraints EpochPlanningConstraints,
		config EpochPolicyConfig,
	) BalanceWave
	Evaluate(
		snapshot *PlacementSnapshot,
		projected *ProjectedPlacement,
		kind PlanKind,
		config EpochPolicyConfig,
	) ScorePotential
}

type EpochPolicyConfig struct {
	GlobalRowCountFactor          float64
	DelegatorMemoryOverloadFactor float64
	CollectionChannelCountFactor  float64
	AutoBalanceChannel            bool
	StreamingServiceEnabled       bool
}

type scoreEpochPolicy struct {
	channelLevel bool
}

func newScoreEpochPolicy(channelLevel bool) EpochBalancePolicy {
	return &scoreEpochPolicy{channelLevel: channelLevel}
}

func (p *scoreEpochPolicy) Plan(
	snapshot *PlacementSnapshot,
	budget BalanceWaveBudget,
	constraints EpochPlanningConstraints,
	config EpochPolicyConfig,
) BalanceWave {
	if snapshot == nil {
		return BalanceWave{Kind: PlanKindSegment, Converged: true}
	}

	var channelAttempt BalanceWave
	if config.AutoBalanceChannel {
		channelAttempt = p.planKind(snapshot, budget, constraints, PlanKindChannel, config)
		if len(channelAttempt.Plans) != 0 {
			return channelAttempt
		}
	}

	segmentWave := p.planKind(snapshot, budget, constraints, PlanKindSegment, config)
	if len(segmentWave.Plans) != 0 {
		return segmentWave
	}
	if config.AutoBalanceChannel {
		segmentWave.Decisions = append(channelAttempt.Decisions, segmentWave.Decisions...)
	}
	segmentWave.Converged = true
	return segmentWave
}

func (p *scoreEpochPolicy) Evaluate(
	snapshot *PlacementSnapshot,
	projected *ProjectedPlacement,
	kind PlanKind,
	config EpochPolicyConfig,
) ScorePotential {
	if snapshot == nil || (kind != PlanKindSegment && kind != PlanKindChannel) {
		return ScorePotential{}
	}
	state := *snapshot
	if projected != nil {
		state = projected.Snapshot()
	}
	workload := buildEpochWorkload(state)
	potential := 0.0
	for _, domain := range p.buildDomains(snapshot, kind, config) {
		scores := calculateEpochDomainScores(state, workload, domain, kind, config)
		for _, nodeID := range sortedEpochScoreNodeIDs(scores) {
			score := scores[nodeID]
			delta := score.current - score.assigned
			potential += delta * delta
		}
	}
	return ScorePotential{Value: potential}
}

func (p *scoreEpochPolicy) planKind(
	snapshot *PlacementSnapshot,
	budget BalanceWaveBudget,
	constraints EpochPlanningConstraints,
	kind PlanKind,
	config EpochPolicyConfig,
) BalanceWave {
	projected := NewProjectedPlacement(*snapshot)
	observed := p.Evaluate(snapshot, projected, kind, config)
	wave := BalanceWave{Kind: kind, Before: observed, After: observed}
	ledger := NewWaveLedger(budget, constraints)
	candidates := p.buildCandidates(snapshot, projected, kind, config)

	for _, candidate := range candidates {
		state := projected.Snapshot()
		targets := sortedCandidateTargets(state, candidate, kind, config)
		for _, target := range targets {
			plan := candidate.plan(snapshot.Token, target)
			before := p.Evaluate(snapshot, projected, kind, config)
			if !ledger.TryReserve(plan) {
				wave.Decisions = append(wave.Decisions, BalancePlanDecision{
					Plan: cloneEpochPolicyPlan(plan), Before: before, After: before,
					Result: "rejected", Explanation: "wave budget or persistent constraint rejected the candidate",
				})
				continue
			}
			if err := projected.Apply(plan); err != nil {
				ledger.Release(plan)
				wave.Decisions = append(wave.Decisions, BalancePlanDecision{
					Plan: cloneEpochPolicyPlan(plan), Before: before, After: before,
					Result: "rejected", Explanation: fmt.Sprintf("projected placement rejected the candidate: %v", err),
				})
				continue
			}
			after := p.Evaluate(snapshot, projected, kind, config)
			if !after.Improves(before) {
				projected.Undo(plan)
				ledger.Release(plan)
				wave.Decisions = append(wave.Decisions, BalancePlanDecision{
					Plan: cloneEpochPolicyPlan(plan), Before: before, After: after,
					Result: "rejected", Explanation: "candidate does not strictly reduce score potential",
				})
				continue
			}

			stored := cloneEpochPolicyPlan(plan)
			wave.Plans = append(wave.Plans, stored)
			wave.PrefixAfter = append(wave.PrefixAfter, after)
			wave.Decisions = append(wave.Decisions, BalancePlanDecision{
				Plan: cloneEpochPolicyPlan(plan), Before: before, After: after,
				Result: "accepted", Explanation: "candidate strictly reduces score potential",
			})
			wave.After = after
			break
		}
	}

	if len(wave.Plans) == 0 {
		return wave
	}
	complete := p.Evaluate(snapshot, projected, kind, config)
	if !complete.Improves(observed) {
		wave.Plans = nil
		wave.PrefixAfter = nil
		wave.After = observed
		return wave
	}
	wave.After = complete
	return wave
}

type epochScoreDomain struct {
	collectionID int64
	replicaID    int64
	channel      string
	targetNodes  []int64
}

func (p *scoreEpochPolicy) buildDomains(
	snapshot *PlacementSnapshot,
	kind PlanKind,
	config EpochPolicyConfig,
) []epochScoreDomain {
	replicaIDs := make([]int64, 0, len(snapshot.EligibleReplicas))
	for replicaID := range snapshot.EligibleReplicas {
		if _, ok := snapshot.Replicas[replicaID]; ok {
			replicaIDs = append(replicaIDs, replicaID)
		}
	}
	sort.Slice(replicaIDs, func(i, j int) bool {
		left, right := snapshot.Replicas[replicaIDs[i]], snapshot.Replicas[replicaIDs[j]]
		if left.CollectionID != right.CollectionID {
			return left.CollectionID < right.CollectionID
		}
		return left.ID < right.ID
	})

	domains := make([]epochScoreDomain, 0, len(replicaIDs))
	for _, replicaID := range replicaIDs {
		replica := snapshot.Replicas[replicaID]
		if !p.channelLevel {
			domains = append(domains, epochScoreDomain{
				collectionID: replica.CollectionID,
				replicaID:    replica.ID,
				targetNodes:  eligibleEpochTargets(snapshot, replicaNodesForKind(replica, kind, config)),
			})
			continue
		}

		target := snapshot.CollectionTargets[replica.CollectionID]
		channels := sortedTargetChannels(target.Current.Channels)
		if len(channels) == 0 {
			continue
		}
		complete := true
		for _, channel := range channels {
			if len(replica.ChannelRWNodes[channel]) == 0 {
				complete = false
				break
			}
		}
		if !complete {
			domains = append(domains, epochScoreDomain{
				collectionID: replica.CollectionID,
				replicaID:    replica.ID,
				targetNodes:  eligibleEpochTargets(snapshot, replicaNodesForKind(replica, kind, config)),
			})
			continue
		}
		for _, channel := range channels {
			domains = append(domains, epochScoreDomain{
				collectionID: replica.CollectionID,
				replicaID:    replica.ID,
				channel:      channel,
				targetNodes:  eligibleEpochTargets(snapshot, replica.ChannelRWNodes[channel]),
			})
		}
	}
	sort.Slice(domains, func(i, j int) bool {
		if domains[i].collectionID != domains[j].collectionID {
			return domains[i].collectionID < domains[j].collectionID
		}
		if domains[i].replicaID != domains[j].replicaID {
			return domains[i].replicaID < domains[j].replicaID
		}
		return domains[i].channel < domains[j].channel
	})
	return domains
}

func replicaNodesForKind(replica ReplicaSnapshot, kind PlanKind, config EpochPolicyConfig) []int64 {
	if kind == PlanKindChannel && config.StreamingServiceEnabled {
		return replica.RWSQNodes
	}
	return replica.RWNodes
}

func eligibleEpochTargets(snapshot *PlacementSnapshot, nodeIDs []int64) []int64 {
	unique := make(map[int64]struct{}, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		node, ok := snapshot.Nodes[nodeID]
		if !ok || !node.Exists || !node.Eligible || node.ResourceExhausted {
			continue
		}
		unique[nodeID] = struct{}{}
	}
	targets := make([]int64, 0, len(unique))
	for nodeID := range unique {
		targets = append(targets, nodeID)
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i] < targets[j] })
	return targets
}

func sortedTargetChannels(channels map[string]TargetChannelSnapshot) []string {
	names := make([]string, 0, len(channels))
	for channel := range channels {
		names = append(names, channel)
	}
	sort.Strings(names)
	return names
}

type epochNodeScore struct {
	nodeID   int64
	current  float64
	assigned float64
}

func sortedEpochScoreNodeIDs(scores map[int64]epochNodeScore) []int64 {
	nodeIDs := make([]int64, 0, len(scores))
	for nodeID := range scores {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Slice(nodeIDs, func(i, j int) bool { return nodeIDs[i] < nodeIDs[j] })
	return nodeIDs
}

func calculateEpochDomainScores(
	state PlacementSnapshot,
	workload epochWorkload,
	domain epochScoreDomain,
	kind PlanKind,
	config EpochPolicyConfig,
) map[int64]epochNodeScore {
	participants := epochDomainParticipants(state, domain, kind)
	for _, nodeID := range domain.targetNodes {
		participants[nodeID] = struct{}{}
	}
	nodeIDs := make([]int64, 0, len(participants))
	for nodeID := range participants {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Slice(nodeIDs, func(i, j int) bool { return nodeIDs[i] < nodeIDs[j] })

	scores := make(map[int64]epochNodeScore, len(nodeIDs))
	total := 0.0
	for _, nodeID := range nodeIDs {
		current := 0.0
		if kind == PlanKindSegment {
			globalRows := workload.historicalRows[nodeID] + workload.growingRows[nodeID] +
				int64(state.PendingWork.SegmentWorkloadByNode[nodeID])
			collectionRows := workload.collectionRows[nodeID][domain.collectionID] +
				workload.collectionSegmentPending[domain.collectionID][nodeID]
			current = float64(collectionRows + int64(int(float64(globalRows)*config.GlobalRowCountFactor)))
		} else {
			weight := math.Max(1, config.CollectionChannelCountFactor)
			resident := 0.0
			for _, channel := range workload.channelsByNode[nodeID] {
				if channel.collectionID == domain.collectionID {
					resident += weight
				} else {
					resident += 1
				}
			}
			current = float64(int(resident) + state.PendingWork.ChannelWorkloadByNode[nodeID] +
				int(workload.collectionChannelPending[domain.collectionID][nodeID]))
		}
		scores[nodeID] = epochNodeScore{nodeID: nodeID, current: current}
		total += current
	}

	if total == 0 || len(domain.targetNodes) == 0 {
		return scores
	}
	assigned := make(map[int64]float64, len(domain.targetNodes))
	allHaveMemory := true
	totalMemory := 0.0
	for _, nodeID := range domain.targetNodes {
		node, ok := state.Nodes[nodeID]
		if !ok || node.MemoryCapacity <= 0 {
			allHaveMemory = false
			break
		}
		totalMemory += node.MemoryCapacity
	}
	if allHaveMemory && totalMemory > 0 {
		for _, nodeID := range domain.targetNodes {
			assigned[nodeID] = state.Nodes[nodeID].MemoryCapacity * total / totalMemory
		}
	} else {
		share := total / float64(len(domain.targetNodes))
		for _, nodeID := range domain.targetNodes {
			assigned[nodeID] = share
		}
	}

	for _, nodeID := range nodeIDs {
		score := scores[nodeID]
		score.assigned = assigned[nodeID]
		if kind == PlanKindSegment {
			delegators := workload.channelCounts[nodeID][domain.collectionID]
			if delegators > 0 {
				score.current += score.assigned * config.DelegatorMemoryOverloadFactor * float64(delegators)
			}
		}
		scores[nodeID] = score
	}
	return scores
}

func epochDomainParticipants(
	state PlacementSnapshot,
	domain epochScoreDomain,
	kind PlanKind,
) map[int64]struct{} {
	participants := make(map[int64]struct{})
	if kind == PlanKindSegment {
		for key, placements := range state.Segments {
			if key.ReplicaID != domain.replicaID || key.Scope != querypb.DataScope_Historical {
				continue
			}
			for _, placement := range placements {
				if !placement.Present || placement.CollectionID != domain.collectionID ||
					(domain.channel != "" && placement.Channel != domain.channel) {
					continue
				}
				participants[placement.NodeID] = struct{}{}
			}
		}
		return participants
	}
	for key, placements := range state.Channels {
		if key.ReplicaID != domain.replicaID || (domain.channel != "" && key.Channel != domain.channel) {
			continue
		}
		for _, placement := range placements {
			if placement.Present && placement.CollectionID == domain.collectionID {
				participants[placement.NodeID] = struct{}{}
			}
		}
	}
	return participants
}

type physicalSegmentIdentity struct {
	collectionID int64
	segmentID    int64
	scope        querypb.DataScope
	nodeID       int64
}

type physicalChannelIdentity struct {
	collectionID int64
	channel      string
	nodeID       int64
}

type epochWorkload struct {
	historicalRows           map[int64]int64
	growingRows              map[int64]int64
	collectionRows           map[int64]map[int64]int64
	channelCounts            map[int64]map[int64]int64
	channelsByNode           map[int64][]physicalChannelIdentity
	collectionSegmentPending map[int64]map[int64]int64
	collectionChannelPending map[int64]map[int64]int64
}

func buildEpochWorkload(state PlacementSnapshot) epochWorkload {
	workload := epochWorkload{
		historicalRows:           make(map[int64]int64),
		growingRows:              make(map[int64]int64),
		collectionRows:           make(map[int64]map[int64]int64),
		channelCounts:            make(map[int64]map[int64]int64),
		channelsByNode:           make(map[int64][]physicalChannelIdentity),
		collectionSegmentPending: make(map[int64]map[int64]int64),
		collectionChannelPending: make(map[int64]map[int64]int64),
	}

	historical := make(map[physicalSegmentIdentity]int64)
	for key, placements := range state.Segments {
		if key.Scope != querypb.DataScope_Historical {
			continue
		}
		for _, placement := range placements {
			if !placement.Present {
				continue
			}
			identity := physicalSegmentIdentity{
				collectionID: placement.CollectionID, segmentID: key.SegmentID,
				scope: key.Scope, nodeID: placement.NodeID,
			}
			if placement.RowCount > historical[identity] {
				historical[identity] = placement.RowCount
			}
		}
	}
	for identity, rows := range historical {
		workload.historicalRows[identity.nodeID] += rows
		addEpochCollectionValue(workload.collectionRows, identity.nodeID, identity.collectionID, rows)
	}

	growing := make(map[physicalChannelIdentity]int64)
	channels := make(map[physicalChannelIdentity]struct{})
	for key, placements := range state.Channels {
		for _, placement := range placements {
			if !placement.Present {
				continue
			}
			identity := physicalChannelIdentity{
				collectionID: placement.CollectionID, channel: key.Channel, nodeID: placement.NodeID,
			}
			channels[identity] = struct{}{}
			if placement.NumOfGrowingRows > growing[identity] {
				growing[identity] = placement.NumOfGrowingRows
			}
		}
	}
	channelIdentities := make([]physicalChannelIdentity, 0, len(channels))
	for identity := range channels {
		channelIdentities = append(channelIdentities, identity)
	}
	sort.Slice(channelIdentities, func(i, j int) bool {
		if channelIdentities[i].nodeID != channelIdentities[j].nodeID {
			return channelIdentities[i].nodeID < channelIdentities[j].nodeID
		}
		if channelIdentities[i].collectionID != channelIdentities[j].collectionID {
			return channelIdentities[i].collectionID < channelIdentities[j].collectionID
		}
		return channelIdentities[i].channel < channelIdentities[j].channel
	})
	for _, identity := range channelIdentities {
		addEpochCollectionValue(workload.channelCounts, identity.nodeID, identity.collectionID, 1)
		workload.channelsByNode[identity.nodeID] = append(workload.channelsByNode[identity.nodeID], identity)
	}
	for identity, rows := range growing {
		workload.growingRows[identity.nodeID] += rows
		addEpochCollectionValue(workload.collectionRows, identity.nodeID, identity.collectionID, rows)
	}

	for _, pending := range state.PendingWork.Tasks {
		for _, action := range pending.Actions {
			if epochActionReflected(state, pending.CollectionID, action) {
				continue
			}
			if action.Channel != "" {
				addEpochCollectionValue(
					workload.collectionChannelPending, pending.CollectionID, action.NodeID, int64(action.Workload),
				)
			} else if action.SegmentID != 0 {
				addEpochCollectionValue(
					workload.collectionSegmentPending, pending.CollectionID, action.NodeID, int64(action.Workload),
				)
			}
		}
	}
	return workload
}

func addEpochCollectionValue(values map[int64]map[int64]int64, outer, inner, delta int64) {
	if values[outer] == nil {
		values[outer] = make(map[int64]int64)
	}
	values[outer][inner] += delta
}

func epochActionReflected(
	state PlacementSnapshot,
	collectionID int64,
	action task.PendingBalanceActionSnapshot,
) bool {
	present := false
	if action.Channel != "" {
		for key, placements := range state.Channels {
			if key.Channel != action.Channel {
				continue
			}
			for _, placement := range placements {
				if placement.CollectionID == collectionID && placement.NodeID == action.NodeID && placement.Present {
					present = true
					break
				}
			}
			if present {
				break
			}
		}
	} else {
		for key, placements := range state.Segments {
			if key.SegmentID != action.SegmentID || key.Scope != action.Scope {
				continue
			}
			for _, placement := range placements {
				if placement.CollectionID == collectionID && placement.NodeID == action.NodeID && placement.Present &&
					(action.Shard == "" || placement.Channel == action.Shard) {
					present = true
					break
				}
			}
			if present {
				break
			}
		}
	}
	if action.Type == task.ActionTypeGrow {
		return present
	}
	if action.Type == task.ActionTypeReduce {
		return !present
	}
	return action.Workload == 0
}

type epochCandidate struct {
	domain         epochScoreDomain
	source         int64
	sourceOverload float64
	segmentID      int64
	channel        string
	shard          string
	rowCount       int64
	scope          querypb.DataScope
}

func (c epochCandidate) plan(token SnapshotToken, target int64) EpochPlan {
	plan := EpochPlan{
		CollectionID: c.domain.collectionID,
		ReplicaID:    c.domain.replicaID,
		Shard:        c.shard,
		RowCount:     c.rowCount,
		From:         c.source,
		To:           target,
	}
	admission := AdmissionToken{
		Snapshot:           cloneSnapshotToken(token),
		CollectionID:       plan.CollectionID,
		ReplicaID:          plan.ReplicaID,
		ExpectedSourceNode: plan.From,
	}
	if c.segmentID != 0 {
		plan.Kind = PlanKindSegment
		plan.SegmentID = c.segmentID
		plan.Scope = c.scope
		admission.Segment = &SegmentObjectKey{
			ReplicaID: plan.ReplicaID, SegmentID: plan.SegmentID, Scope: plan.Scope,
		}
	} else {
		plan.Kind = PlanKindChannel
		plan.Channel = c.channel
		admission.Channel = &ChannelObjectKey{ReplicaID: plan.ReplicaID, Channel: plan.Channel}
	}
	plan.Token = admission
	return plan
}

type physicalCandidateIdentity struct {
	kind         PlanKind
	collectionID int64
	segmentID    int64
	channel      string
	scope        querypb.DataScope
	source       int64
}

func (p *scoreEpochPolicy) buildCandidates(
	snapshot *PlacementSnapshot,
	projected *ProjectedPlacement,
	kind PlanKind,
	config EpochPolicyConfig,
) []epochCandidate {
	state := projected.Snapshot()
	workload := buildEpochWorkload(state)
	candidates := make([]epochCandidate, 0)
	seen := make(map[physicalCandidateIdentity]struct{})
	aliases := aliasedPhysicalCandidates(state, kind)
	for _, domain := range p.buildDomains(snapshot, kind, config) {
		scores := calculateEpochDomainScores(state, workload, domain, kind, config)
		if kind == PlanKindSegment {
			keys := sortedSegmentKeys(state.Segments)
			for _, key := range keys {
				if key.ReplicaID != domain.replicaID || key.Scope != querypb.DataScope_Historical ||
					!segmentTargeted(snapshot, domain.collectionID, key.SegmentID) {
					continue
				}
				placements := state.Segments[key]
				placement, ok := uniquePresentSegment(placements, domain.collectionID, domain.channel)
				if !ok {
					continue
				}
				identity := physicalCandidateIdentity{
					kind: kind, collectionID: placement.CollectionID, segmentID: key.SegmentID,
					scope: key.Scope, source: placement.NodeID,
				}
				if _, aliased := aliases[identity]; aliased {
					continue
				}
				if _, ok := seen[identity]; ok {
					continue
				}
				seen[identity] = struct{}{}
				score := scores[placement.NodeID]
				candidates = append(candidates, epochCandidate{
					domain: domain, source: placement.NodeID, sourceOverload: score.current - score.assigned,
					segmentID: key.SegmentID, shard: placement.Channel,
					rowCount: placement.RowCount, scope: key.Scope,
				})
			}
			continue
		}

		keys := sortedChannelKeys(state.Channels)
		for _, key := range keys {
			if key.ReplicaID != domain.replicaID || (domain.channel != "" && key.Channel != domain.channel) {
				continue
			}
			placement, ok := uniquePresentChannel(state.Channels[key], domain.collectionID)
			if !ok {
				continue
			}
			identity := physicalCandidateIdentity{
				kind: kind, collectionID: placement.CollectionID, channel: key.Channel, source: placement.NodeID,
			}
			if _, aliased := aliases[identity]; aliased {
				continue
			}
			if _, ok := seen[identity]; ok {
				continue
			}
			seen[identity] = struct{}{}
			score := scores[placement.NodeID]
			candidates = append(candidates, epochCandidate{
				domain: domain, source: placement.NodeID, sourceOverload: score.current - score.assigned,
				channel: key.Channel, shard: key.Channel,
			})
		}
	}

	sort.Slice(candidates, func(i, j int) bool {
		left, right := candidates[i], candidates[j]
		if left.domain.collectionID != right.domain.collectionID {
			return left.domain.collectionID < right.domain.collectionID
		}
		if left.domain.replicaID != right.domain.replicaID {
			return left.domain.replicaID < right.domain.replicaID
		}
		if left.domain.channel != right.domain.channel {
			return left.domain.channel < right.domain.channel
		}
		if left.sourceOverload != right.sourceOverload {
			return left.sourceOverload > right.sourceOverload
		}
		if kind == PlanKindSegment && left.segmentID != right.segmentID {
			return left.segmentID < right.segmentID
		}
		if kind == PlanKindChannel && left.channel != right.channel {
			return left.channel < right.channel
		}
		return left.source < right.source
	})
	return candidates
}

func aliasedPhysicalCandidates(
	state PlacementSnapshot,
	kind PlanKind,
) map[physicalCandidateIdentity]struct{} {
	replicas := make(map[physicalCandidateIdentity]map[int64]struct{})
	if kind == PlanKindSegment {
		for key, placements := range state.Segments {
			if key.Scope != querypb.DataScope_Historical {
				continue
			}
			for _, placement := range placements {
				if !placement.Present {
					continue
				}
				identity := physicalCandidateIdentity{
					kind: kind, collectionID: placement.CollectionID, segmentID: key.SegmentID,
					scope: key.Scope, source: placement.NodeID,
				}
				if replicas[identity] == nil {
					replicas[identity] = make(map[int64]struct{})
				}
				replicas[identity][key.ReplicaID] = struct{}{}
			}
		}
	} else {
		for key, placements := range state.Channels {
			for _, placement := range placements {
				if !placement.Present {
					continue
				}
				identity := physicalCandidateIdentity{
					kind: kind, collectionID: placement.CollectionID,
					channel: key.Channel, source: placement.NodeID,
				}
				if replicas[identity] == nil {
					replicas[identity] = make(map[int64]struct{})
				}
				replicas[identity][key.ReplicaID] = struct{}{}
			}
		}
	}

	aliases := make(map[physicalCandidateIdentity]struct{})
	for identity, replicaIDs := range replicas {
		if len(replicaIDs) > 1 {
			aliases[identity] = struct{}{}
		}
	}
	return aliases
}

func sortedSegmentKeys(segments map[SegmentObjectKey][]SegmentPlacement) []SegmentObjectKey {
	keys := make([]SegmentObjectKey, 0, len(segments))
	for key := range segments {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].ReplicaID != keys[j].ReplicaID {
			return keys[i].ReplicaID < keys[j].ReplicaID
		}
		if keys[i].SegmentID != keys[j].SegmentID {
			return keys[i].SegmentID < keys[j].SegmentID
		}
		return keys[i].Scope < keys[j].Scope
	})
	return keys
}

func sortedChannelKeys(channels map[ChannelObjectKey][]ChannelPlacement) []ChannelObjectKey {
	keys := make([]ChannelObjectKey, 0, len(channels))
	for key := range channels {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].ReplicaID != keys[j].ReplicaID {
			return keys[i].ReplicaID < keys[j].ReplicaID
		}
		return keys[i].Channel < keys[j].Channel
	})
	return keys
}

func segmentTargeted(snapshot *PlacementSnapshot, collectionID, segmentID int64) bool {
	target := snapshot.CollectionTargets[collectionID]
	_, current := target.Current.Segments[segmentID]
	_, next := target.Next.Segments[segmentID]
	return current || next
}

func uniquePresentSegment(
	placements []SegmentPlacement,
	collectionID int64,
	channel string,
) (SegmentPlacement, bool) {
	present := make([]SegmentPlacement, 0, 1)
	for _, placement := range placements {
		if placement.Present {
			present = append(present, placement)
		}
	}
	if len(present) != 1 || present[0].CollectionID != collectionID ||
		(channel != "" && present[0].Channel != channel) {
		return SegmentPlacement{}, false
	}
	return present[0], true
}

func uniquePresentChannel(placements []ChannelPlacement, collectionID int64) (ChannelPlacement, bool) {
	present := make([]ChannelPlacement, 0, 1)
	for _, placement := range placements {
		if placement.Present {
			present = append(present, placement)
		}
	}
	if len(present) != 1 || present[0].CollectionID != collectionID {
		return ChannelPlacement{}, false
	}
	return present[0], true
}

func sortedCandidateTargets(
	state PlacementSnapshot,
	candidate epochCandidate,
	kind PlanKind,
	config EpochPolicyConfig,
) []int64 {
	workload := buildEpochWorkload(state)
	scores := calculateEpochDomainScores(state, workload, candidate.domain, kind, config)
	targets := make([]int64, 0, len(candidate.domain.targetNodes))
	for _, nodeID := range candidate.domain.targetNodes {
		if nodeID == candidate.source || candidateExistsOnNode(state, candidate, nodeID) {
			continue
		}
		targets = append(targets, nodeID)
	}
	sort.Slice(targets, func(i, j int) bool {
		left := scores[targets[i]].assigned - scores[targets[i]].current
		right := scores[targets[j]].assigned - scores[targets[j]].current
		if left != right {
			return left > right
		}
		return targets[i] < targets[j]
	})
	return targets
}

func candidateExistsOnNode(state PlacementSnapshot, candidate epochCandidate, nodeID int64) bool {
	if candidate.segmentID != 0 {
		key := SegmentObjectKey{
			ReplicaID: candidate.domain.replicaID, SegmentID: candidate.segmentID, Scope: candidate.scope,
		}
		for _, placement := range state.Segments[key] {
			if placement.NodeID == nodeID {
				return true
			}
		}
		return false
	}
	key := ChannelObjectKey{ReplicaID: candidate.domain.replicaID, Channel: candidate.channel}
	for _, placement := range state.Channels[key] {
		if placement.NodeID == nodeID {
			return true
		}
	}
	return false
}

func cloneEpochPolicyPlan(plan EpochPlan) EpochPlan {
	clone := plan
	clone.Token.Snapshot = cloneSnapshotToken(plan.Token.Snapshot)
	if plan.Token.Segment != nil {
		segment := *plan.Token.Segment
		clone.Token.Segment = &segment
	}
	if plan.Token.Channel != nil {
		channel := *plan.Token.Channel
		clone.Token.Channel = &channel
	}
	return clone
}
