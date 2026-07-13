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

package storageprofile

import (
	"encoding/json"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const MaxContributionSize = 1 << 20

type contributionEnvelope struct {
	Contributions []contributionWire `json:"contributions"`
}

type contributionWire struct {
	Identity ContributionIdentity `json:"identity"`
	Profile  *profileWire         `json:"profile,omitempty"`
}

type operationWire struct {
	Operation StorageOperation `json:"operation"`
	Stats     OperationStats   `json:"stats"`
}

type profileWire struct {
	SchemaVersion uint32      `json:"schema_version"`
	BucketSchema  uint32      `json:"bucket_schema"`
	Attribution   Attribution `json:"attribution"`

	Operations                []operationWire           `json:"operations,omitempty"`
	OperationBreakdown        []OperationBreakdownEntry `json:"operation_breakdown,omitempty"`
	OperationBreakdownDropped uint64                    `json:"operation_breakdown_dropped,omitempty"`
	Cache                     CacheStats                `json:"cache"`
	Coverage                  ProfileCoverage           `json:"coverage"`
	StartedAtUnixNano         int64                     `json:"started_at_unix_nano"`
	FinishedAtUnixNano        int64                     `json:"finished_at_unix_nano"`
	QuantilesComplete         bool                      `json:"quantiles_complete"`
}

func MarshalContribution(contribution Contribution) ([]byte, error) {
	return MarshalContributions([]Contribution{contribution})
}

func MarshalContributions(contributions []Contribution) ([]byte, error) {
	wire := contributionEnvelope{Contributions: make([]contributionWire, 0, len(contributions))}
	for _, contribution := range contributions {
		wire.Contributions = append(wire.Contributions, contributionWire{
			Identity: contribution.Identity,
			Profile:  encodeProfile(contribution.Profile),
		})
	}
	data, err := json.Marshal(wire)
	if err != nil {
		return nil, merr.WrapErrSerializationFailed(err, "marshal storage profile contribution")
	}
	if len(data) > MaxContributionSize {
		return nil, merr.WrapErrServiceInternalMsg("storage profile contribution exceeds %d bytes", MaxContributionSize)
	}
	return data, nil
}

func UnmarshalContribution(data []byte) (Contribution, error) {
	contributions, err := UnmarshalContributions(data)
	if err != nil || len(contributions) == 0 {
		return Contribution{}, err
	}
	return contributions[0], nil
}

func UnmarshalContributions(data []byte) ([]Contribution, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) > MaxContributionSize {
		return nil, merr.WrapErrServiceInternalMsg("storage profile contribution exceeds %d bytes", MaxContributionSize)
	}
	var envelope contributionEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, merr.WrapErrSerializationFailed(err, "unmarshal storage profile contribution")
	}
	contributions := make([]Contribution, 0, len(envelope.Contributions))
	for _, wire := range envelope.Contributions {
		profile, err := decodeProfile(wire.Profile)
		if err != nil {
			return nil, err
		}
		contributions = append(contributions, Contribution{Identity: wire.Identity, Profile: profile})
	}
	return contributions, nil
}

func MergeContributionPayloads(payloads ...[]byte) ([]byte, error) {
	contributions := make([]Contribution, 0, len(payloads))
	missingContribution := false
	for _, payload := range payloads {
		if len(payload) == 0 {
			missingContribution = true
			continue
		}
		decoded, err := UnmarshalContributions(payload)
		if err != nil {
			return nil, err
		}
		contributions = append(contributions, decoded...)
	}
	if len(contributions) == 0 {
		return nil, nil
	}
	if missingContribution {
		for contributionIndex := range contributions {
			markMissingContribution(contributions[contributionIndex].Profile)
		}
	}
	return MarshalContributions(contributions)
}

func markMissingContribution(profile *StorageProfile) {
	if profile == nil {
		return
	}
	profile.QuantilesComplete = false
	profile.Coverage = mergeCoverage(profile.Coverage, ProfileCoverage{
		GoStorageOperations:  CoverageUnavailable,
		CppStorageOperations: CoverageUnavailable,
		StorageBytes:         CoverageUnavailable,
		StreamingTTFB:        CoverageUnavailable,
		TieredCacheUsage:     CoverageUnavailable,
		CacheWait:            CoverageUnavailable,
		ProviderAccess:       CoverageUnavailable,
	})
}

func encodeProfile(profile *StorageProfile) *profileWire {
	if profile == nil {
		return nil
	}
	wire := &profileWire{
		SchemaVersion:             profile.SchemaVersion,
		BucketSchema:              profile.BucketSchema,
		Attribution:               profile.Attribution,
		OperationBreakdownDropped: profile.OperationBreakdownDropped,
		Cache:                     profile.Cache,
		Coverage:                  profile.Coverage,
		StartedAtUnixNano:         profile.StartedAtUnixNano,
		FinishedAtUnixNano:        profile.FinishedAtUnixNano,
		QuantilesComplete:         profile.QuantilesComplete,
	}
	for operation := StorageOperationUnknown; operation < StorageOperationCount; operation++ {
		stats := profile.Operations[operation]
		if stats.Count != 0 {
			wire.Operations = append(wire.Operations, operationWire{Operation: operation, Stats: stats})
		}
	}
	count := min(int(profile.OperationBreakdownCount), MaxOperationBreakdownEntries)
	if count > 0 {
		wire.OperationBreakdown = append(wire.OperationBreakdown, profile.OperationBreakdown[:count]...)
	}
	return wire
}

func decodeProfile(wire *profileWire) (*StorageProfile, error) {
	if wire == nil {
		return nil, nil
	}
	profile := &StorageProfile{
		SchemaVersion:             wire.SchemaVersion,
		BucketSchema:              wire.BucketSchema,
		Attribution:               wire.Attribution.Bounded(),
		OperationBreakdownDropped: wire.OperationBreakdownDropped,
		Cache:                     wire.Cache,
		Coverage:                  wire.Coverage,
		StartedAtUnixNano:         wire.StartedAtUnixNano,
		FinishedAtUnixNano:        wire.FinishedAtUnixNano,
		QuantilesComplete:         wire.QuantilesComplete,
	}
	for _, operation := range wire.Operations {
		if !operation.Operation.Valid() {
			return nil, merr.WrapErrSerializationFailed(nil, "storage profile contains unknown operation %d", operation.Operation)
		}
		mergeOperationStats(&profile.Operations[operation.Operation], operation.Stats, true)
	}
	for _, breakdown := range wire.OperationBreakdown {
		if !breakdown.Operation.Valid() || !breakdown.Phase.Valid() || !breakdown.StorageRole.Valid() {
			return nil, merr.WrapErrSerializationFailed(nil, "storage profile contains invalid operation breakdown")
		}
		stats := profile.findOrCreateBreakdown(breakdown.Operation, breakdown.Phase, breakdown.StorageRole)
		if stats == nil {
			profile.OperationBreakdownDropped += breakdown.Stats.Count
			continue
		}
		mergeOperationStats(stats, breakdown.Stats, true)
	}
	return profile, nil
}
