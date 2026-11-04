// Load generator: identical groups of eight logical searches in all modes.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
)

type collector struct {
	sync.Mutex
	Latencies []float64
	Errors    int
	Completed int
}

func (c *collector) add(start time.Time, failed int, count int) {
	c.Lock()
	defer c.Unlock()
	c.Latencies = append(c.Latencies, float64(time.Since(start).Microseconds())/1000)
	c.Errors += failed
	c.Completed += count
}
func (c *collector) summary() map[string]any {
	sort.Float64s(c.Latencies)
	q := func(p float64) float64 {
		if len(c.Latencies) == 0 {
			return 0
		}
		return c.Latencies[int(float64(len(c.Latencies)-1)*p)]
	}
	return map[string]any{"samples": len(c.Latencies), "logical_completed": c.Completed, "logical_errors": c.Errors, "p50_ms": q(.5), "p95_ms": q(.95), "p99_ms": q(.99), "latencies_ms": c.Latencies}
}
func main() {
	target := flag.String("target", "localhost:29560", "")
	mode := flag.String("mode", "search", "")
	workers := flag.Int("workers", 4, "concurrent groups")
	duration := flag.Duration("duration", 30*time.Second, "")
	warmup := flag.Duration("warmup", 5*time.Second, "")
	rate := flag.Float64("rate", 0, "offered groups per second; zero is closed-loop")
	out := flag.String("out", "results.json", "")
	fixture := flag.String("fixture", "artifacts/multisearch-v2/fixture.json", "")
	flag.Parse()
	data, err := os.ReadFile(*fixture)
	if err != nil {
		panic(err)
	}
	var input struct{ Requests []string }
	if err = json.Unmarshal(data, &input); err != nil {
		panic(err)
	}
	requests := make([]*milvuspb.SearchRequest, len(input.Requests))
	for i, s := range input.Requests {
		b, e := base64.StdEncoding.DecodeString(s)
		if e != nil {
			panic(e)
		}
		requests[i] = &milvuspb.SearchRequest{}
		if e = proto.Unmarshal(b, requests[i]); e != nil {
			panic(e)
		}
	}
	conn, err := grpc.NewClient(*target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	client := milvuspb.NewMilvusServiceClient(conn)
	base := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", base64.StdEncoding.EncodeToString([]byte("root:Milvus"))))
	begin := time.Now()
	measureStart := begin.Add(*warmup)
	end := measureStart.Add(*duration)
	var group, probe collector
	var next atomic.Uint64
	var wg sync.WaitGroup
	var dropped atomic.Int64
	run := func() {
		start := time.Now()
		index := int(next.Add(1))
		items := make([]*milvuspb.MultiSearchItem, 8)
		for j := range items {
			items[j] = &milvuspb.MultiSearchItem{Id: fmt.Sprint(j), Search: requests[((index+j)*16+index%16)%len(requests)]}
		}
		ctx, cancel := context.WithTimeout(base, 10*time.Second)
		defer cancel()
		failed := 0
		if *mode == "multi" {
			r, e := client.MultiSearch(ctx, &milvuspb.MultiSearchRequest{Items: items, MaxConcurrentSearches: 8})
			if e != nil || r.GetStatus().GetCode() != 0 || r.GetStatus().GetErrorCode() != 0 || len(r.GetItems()) != 8 {
				failed = 8
			} else {
				for _, x := range r.Items {
					if x.GrpcCode != 0 || x.GetResult().GetStatus().GetCode() != 0 || x.GetResult().GetStatus().GetErrorCode() != 0 {
						failed++
					}
				}
			}
		} else {
			var children sync.WaitGroup
			var errors atomic.Int64
			for _, item := range items {
				children.Add(1)
				go func(req *milvuspb.SearchRequest) {
					defer children.Done()
					r, e := client.Search(ctx, req)
					if e != nil || r.GetStatus().GetCode() != 0 || r.GetStatus().GetErrorCode() != 0 {
						errors.Add(1)
					}
				}(item.Search)
			}
			children.Wait()
			failed = int(errors.Load())
		}
		if !start.Before(measureStart) && start.Before(end) {
			group.add(start, failed, 8)
		}
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for now := range ticker.C {
			if now.After(end) {
				return
			}
			ctx, cancel := context.WithTimeout(base, 10*time.Second)
			start := time.Now()
			r, e := client.Search(ctx, requests[int(next.Load())%len(requests)])
			cancel()
			if !start.Before(measureStart) && start.Before(end) {
				f := 0
				if e != nil || r.GetStatus().GetCode() != 0 || r.GetStatus().GetErrorCode() != 0 {
					f = 1
				}
				probe.add(start, f, 1)
			}
		}
	}()
	if *rate == 0 {
		for range *workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for time.Now().Before(end) {
					run()
				}
			}()
		}
	} else {
		jobs := make(chan struct{})
		for range *workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range jobs {
					run()
				}
			}()
		}
		ticker := time.NewTicker(time.Duration(float64(time.Second) / *rate))
		for now := range ticker.C {
			if now.After(end) {
				break
			}
			select {
			case jobs <- struct{}{}:
			default:
				if !now.Before(measureStart) {
					dropped.Add(1)
				}
			}
		}
		ticker.Stop()
		close(jobs)
	}
	wg.Wait()
	result := map[string]any{"target": *target, "mode": *mode, "group_size": 8, "group_workers": *workers, "offered_groups_per_second": *rate, "dropped_groups": dropped.Load(), "measurement_seconds": duration.Seconds(), "wall_seconds": time.Since(begin).Seconds(), "groups": group.summary(), "ordinary_search_probe": probe.summary(), "logical_success_qps": float64(group.Completed-group.Errors) / duration.Seconds()}
	b, _ := json.MarshalIndent(result, "", "  ")
	if err = os.WriteFile(*out, b, 0644); err != nil {
		panic(err)
	}
	fmt.Printf("%s workers=%d qps=%.1f errors=%d groups=%d\n", *mode, *workers, result["logical_success_qps"], group.Errors, len(group.Latencies))
}
