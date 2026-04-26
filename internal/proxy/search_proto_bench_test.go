package proxy

import (
	"fmt"
	"math/rand"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
)

// buildBenchSearchResultData builds a SearchResultData with n int64 IDs and n float32 scores.
func buildBenchSearchResultData(n int) *schemapb.SearchResultData {
	ids := make([]int64, n)
	scores := make([]float32, n)
	for i := 0; i < n; i++ {
		ids[i] = rand.Int63()
		scores[i] = rand.Float32()
	}
	return &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       int64(n),
		Scores:     scores,
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: ids},
			},
		},
		Topks: []int64{int64(n)},
	}
}

// buildBenchSearchResults builds a full SearchResults (as sent over GRPC between QN→Proxy)
// with the SearchResultData pre-marshaled into SlicedBlob.
func buildBenchSearchResults(n int) (*internalpb.SearchResults, error) {
	data := buildBenchSearchResultData(n)
	blob, err := proto.Marshal(data)
	if err != nil {
		return nil, err
	}
	return &internalpb.SearchResults{
		Status:     &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		MetricType: "IP",
		NumQueries: 1,
		TopK:       int64(n),
		SlicedBlob: blob,
		ChannelsMvcc: map[string]uint64{
			"dml_0_channel": 100,
		},
		CostAggregation: &internalpb.CostAggregation{
			ResponseTime:         10,
			ServiceTime:          8,
			TotalNQ:              1,
			TotalRelatedDataSize: 1024,
		},
	}, nil
}

// ---------- SearchResultData (inner blob) benchmarks ----------

func BenchmarkSearchResultData_Marshal(b *testing.B) {
	for _, n := range []int{100, 1000, 3000, 10000} {
		data := buildBenchSearchResultData(n)
		b.Run(fmt.Sprintf("%d_ids", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, err := proto.Marshal(data)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkSearchResultData_Unmarshal(b *testing.B) {
	for _, n := range []int{100, 1000, 3000, 10000} {
		data := buildBenchSearchResultData(n)
		blob, _ := proto.Marshal(data)
		b.Run(fmt.Sprintf("%d_ids/size=%dB", n, len(blob)), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var out schemapb.SearchResultData
				if err := proto.Unmarshal(blob, &out); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// ---------- SearchResults (full GRPC response) benchmarks ----------

func BenchmarkSearchResults_Marshal(b *testing.B) {
	for _, n := range []int{100, 1000, 3000, 10000} {
		resp, err := buildBenchSearchResults(n)
		if err != nil {
			b.Fatal(err)
		}
		totalSize := proto.Size(resp)
		b.Run(fmt.Sprintf("%d_ids/total=%dB", n, totalSize), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, err := proto.Marshal(resp)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkSearchResults_Unmarshal(b *testing.B) {
	for _, n := range []int{100, 1000, 3000, 10000} {
		resp, err := buildBenchSearchResults(n)
		if err != nil {
			b.Fatal(err)
		}
		wire, _ := proto.Marshal(resp)
		b.Run(fmt.Sprintf("%d_ids/wire=%dB", n, len(wire)), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var out internalpb.SearchResults
				if err := proto.Unmarshal(wire, &out); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// ---------- Full round-trip: simulates QN serialize → GRPC transport → Proxy deserialize ----------

func BenchmarkSearchResults_FullRoundTrip(b *testing.B) {
	for _, n := range []int{100, 1000, 3000, 10000} {
		data := buildBenchSearchResultData(n)
		b.Run(fmt.Sprintf("%d_ids", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				// Step 1: QN marshals inner SearchResultData → SlicedBlob
				blob, err := proto.Marshal(data)
				if err != nil {
					b.Fatal(err)
				}

				// Step 2: QN builds SearchResults response
				resp := &internalpb.SearchResults{
					Status:       &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
					MetricType:   "IP",
					NumQueries:   1,
					TopK:         int64(n),
					SlicedBlob:   blob,
					ChannelsMvcc: map[string]uint64{"ch": 100},
				}

				// Step 3: GRPC framework marshals full response (QN side)
				wire, err := proto.Marshal(resp)
				if err != nil {
					b.Fatal(err)
				}

				// Step 4: GRPC framework unmarshals full response (Proxy side)
				var recvResp internalpb.SearchResults
				if err := proto.Unmarshal(wire, &recvResp); err != nil {
					b.Fatal(err)
				}

				// Step 5: Proxy decodeSearchResults - unmarshal inner blob
				var recvData schemapb.SearchResultData
				if err := proto.Unmarshal(recvResp.SlicedBlob, &recvData); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
