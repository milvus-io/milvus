package types

import "github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"

const (
	UpdateMaskPathWALBalancePolicyAllowRebalance = "config.allow_rebalance"
)

type (
	UpdateWALBalancePolicyRequest  = streamingpb.UpdateWALBalancePolicyRequest
	UpdateWALBalancePolicyResponse = streamingpb.UpdateWALBalancePolicyResponse
)
