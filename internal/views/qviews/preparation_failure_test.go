package qviews

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestPreparationFailureWireCompatibility(t *testing.T) {
	node := &viewpb.QueryViewOfQueryNode{NodeId: 7, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000, 1001}}}}
	base, err := proto.Marshal(node)
	require.NoError(t, err)
	node.FailedSegments = []*viewpb.SegmentPerparingFailure{}
	empty, err := proto.Marshal(node)
	require.NoError(t, err)
	require.Equal(t, base, empty, "empty repeated failures cost zero wire bytes")
	// Model the old descriptor, which knows only node_id and partitions.
	legacy := protodesc.ToDescriptorProto(node.ProtoReflect().Descriptor())
	legacy.Name = proto.String("LegacyQueryViewOfQueryNode")
	legacy.Field = legacy.Field[:2]
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name: proto.String("legacy_query_view.proto"), Package: proto.String("legacy"), Syntax: proto.String("proto3"),
		Dependency: []string{viewpb.File_view_proto.Path()}, MessageType: []*descriptorpb.DescriptorProto{legacy},
	}, protoregistry.GlobalFiles)
	require.NoError(t, err)
	old := dynamicpb.NewMessage(file.Messages().Get(0))
	require.NoError(t, proto.Unmarshal(base, old))
	oldBytes, err := proto.Marshal(old)
	require.NoError(t, err)
	require.Len(t, oldBytes, len(base), "adding an unpopulated field does not enlarge normal reports")
	node.FailedSegments = []*viewpb.SegmentPerparingFailure{{SegmentId: 1000, Reason: viewpb.SegmentPerparingFailure_DiskLimitExceeded}}
	withFailure, err := proto.Marshal(node)
	require.NoError(t, err)
	require.Greater(t, len(withFailure), len(base))
	require.NoError(t, proto.Unmarshal(withFailure, old))
	require.EqualValues(t, 7, old.Get(file.Messages().Get(0).Fields().ByNumber(1)).Int())
	forwarded, err := proto.Marshal(old)
	require.NoError(t, err)
	decoded := &viewpb.QueryViewOfQueryNode{}
	require.NoError(t, proto.Unmarshal(forwarded, decoded))
	require.True(t, proto.Equal(node, decoded), "old readers preserve unknown failure fields when forwarding")
}
