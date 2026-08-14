package sessionutil

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// The absence of the label is the answer milvus gave before it existed, so an
// unlabelled session must read as query serving.
func TestSessionWithoutTheLabelServesQueries(t *testing.T) {
	assert.False(t, (&SessionRaw{}).HasNoQueryService())
	assert.False(t, (&SessionRaw{ServerLabels: map[string]string{}}).HasNoQueryService())
	assert.False(t, (&SessionRaw{ServerLabels: map[string]string{
		LabelResourceGroup: "rg_a",
	}}).HasNoQueryService())
}

func TestSessionWithTheLabelDoesNotServeQueries(t *testing.T) {
	assert.True(t, (&SessionRaw{ServerLabels: map[string]string{
		LabelStreamingNodeNoQueryService: "1",
	}}).HasNoQueryService())
}

// Only "1" declares it. Anything else is not a declaration, and reading it as
// one would silently take a node out of delegator placement.
func TestOnlyOneDeclaresNoQueryService(t *testing.T) {
	for _, value := range []string{"", "0", "true", "yes"} {
		assert.False(t, (&SessionRaw{ServerLabels: map[string]string{
			LabelStreamingNodeNoQueryService: value,
		}}).HasNoQueryService(), "value %q must not declare it", value)
	}
}

// The label a deployment sets from the environment must reach the streaming
// node's session, and only that role's.
func TestTheEnvironmentLabelReachesTheStreamingNodeSession(t *testing.T) {
	key := NewServerLabel(typeutil.StreamingNodeRole, LabelStreamingNodeNoQueryService)
	t.Setenv(key, "1")

	assert.Equal(t, "1", getServerLabelsFromEnv(typeutil.StreamingNodeRole)[LabelStreamingNodeNoQueryService])
	// Other roles must not pick it up.
	assert.Empty(t, getServerLabelsFromEnv(typeutil.QueryNodeRole)[LabelStreamingNodeNoQueryService])
}

// Milvus's own binaries never set the label, so nothing carries it unless a
// deployment asks for it.
func TestNoLabelWithoutTheEnvironment(t *testing.T) {
	assert.Empty(t, getServerLabelsFromEnv(typeutil.StreamingNodeRole)[LabelStreamingNodeNoQueryService])
}
