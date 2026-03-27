package vtproto_bench

import (
	"fmt"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
)

func TestInspectMessages(t *testing.T) {
	fmt.Println("=== SearchResults 1KB ===")
	sr1k := makeSearchResults(1024)
	fmt.Printf("  Serialized size: %d bytes\n", proto.Size(sr1k))
	fmt.Printf("  SlicedBlob len:  %d bytes\n", len(sr1k.SlicedBlob))
	fmt.Printf("  JSON (truncated blob):\n")
	sr1kCopy := proto.Clone(sr1k).(*internalpbAlias)
	_ = sr1kCopy // just show structure
	printJSON(sr1k, 800)

	fmt.Println("\n=== SearchResults 1MB ===")
	sr1m := makeSearchResults(1024 * 1024)
	fmt.Printf("  Serialized size: %d bytes\n", proto.Size(sr1m))
	fmt.Printf("  SlicedBlob len:  %d bytes\n", len(sr1m.SlicedBlob))

	fmt.Println("\n=== PlanNode 2000PK ===")
	plan := makePlanNode(2000)
	fmt.Printf("  Serialized size: %d bytes\n", proto.Size(plan))
	fmt.Printf("  Num PK values:   %d\n", len(plan.GetVectorAnns().GetPredicates().GetTermExpr().GetValues()))
	fmt.Println("  JSON (first 5 PKs shown):")
	planSmall := makePlanNode(5)
	printJSON(planSmall, 2000)

	fmt.Println("\n=== SearchResultData 10K ===")
	srd := makeSearchResultData(10000)
	fmt.Printf("  Serialized size: %d bytes\n", proto.Size(srd))
	fmt.Printf("  Num scores:      %d\n", len(srd.Scores))
	fmt.Printf("  Num IDs:         %d\n", len(srd.GetIds().GetIntId().GetData()))
	fmt.Println("  JSON (first few entries):")
	srdSmall := makeSearchResultData(10)
	printJSON(srdSmall, 2000)
}

func printJSON(msg proto.Message, maxLen int) {
	opts := protojson.MarshalOptions{Indent: "    ", Multiline: true}
	data, _ := opts.Marshal(msg)
	s := string(data)
	if len(s) > maxLen {
		s = s[:maxLen] + "\n    ... (truncated)"
	}
	fmt.Println(s)
}

// alias to avoid import cycle issues in printJSON
type internalpbAlias = internalpb.SearchResults
