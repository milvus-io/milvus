package streamingpb

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestVChannelMetaContainsSegmentDataVersionSummary(t *testing.T) {
	field := (&VChannelMeta{}).ProtoReflect().Descriptor().Fields().ByName(
		protoreflect.Name("segment_data_version_summary"),
	)
	require.NotNil(t, field)
}
