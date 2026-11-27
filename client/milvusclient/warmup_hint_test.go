package milvusclient

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadCollectionWarmupHintOption(t *testing.T) {
	ordinary := NewLoadCollectionOption("items").Request()
	require.Empty(t, ordinary.GetLoadParams())
	option := NewLoadCollectionOption("items").WithWarmupHint()
	request := option.Request()
	require.Equal(t, "true", request.GetLoadParams()["warmup_hint"])
	request.LoadParams["warmup_hint"] = "false"
	require.Equal(t, "true", option.Request().GetLoadParams()["warmup_hint"], "requests must not share mutable maps")
}
