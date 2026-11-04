// Mixed-load experiment: continuous individual Search traffic, with a second
// client added and removed while the resident traffic remains uninterrupted.
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
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
)

type phase struct {
	Name       string `json:"name"`
	Start, End float64
}
type bucket struct {
	Completed  int            `json:"logical_completed"`
	Errors     int            `json:"logical_errors"`
	Latencies  []float64      `json:"latencies_ms"`
	ErrorKinds map[string]int `json:"error_kinds"`
}
type collector struct {
	sync.Mutex
	Phases  map[string]*bucket
	Seconds map[int]*bucket
	Windows []phase
	Start   time.Time
}

func (c *collector) add(start time.Time, count, failed int, kind string) {
	end := time.Now()
	elapsed := end.Sub(c.Start).Seconds()
	started := start.Sub(c.Start).Seconds()
	c.Lock()
	defer c.Unlock()
	add := func(b *bucket, latency bool) {
		b.Completed += count
		b.Errors += failed
		if latency {
			b.Latencies = append(b.Latencies, float64(end.Sub(start).Microseconds())/1000)
		}
		if failed > 0 {
			if b.ErrorKinds == nil {
				b.ErrorKinds = map[string]int{}
			}
			b.ErrorKinds[kind] += failed
		}
	}
	sec := int(elapsed)
	if c.Seconds[sec] == nil {
		c.Seconds[sec] = &bucket{}
	}
	add(c.Seconds[sec], false)
	for _, p := range c.Windows {
		if elapsed >= p.Start && elapsed < p.End {
			b := c.Phases[p.Name]
			if b == nil {
				b = &bucket{}
				c.Phases[p.Name] = b
			}
			add(b, started >= p.Start)
			break
		}
	}
}
func (c *collector) output() map[string]any {
	output := map[string]any{}
	for _, p := range c.Windows {
		b := c.Phases[p.Name]
		if b == nil {
			b = &bucket{}
		}
		sort.Float64s(b.Latencies)
		q := func(v float64) float64 {
			if len(b.Latencies) == 0 {
				return 0
			}
			return b.Latencies[int(float64(len(b.Latencies)-1)*v)]
		}
		output[p.Name] = map[string]any{"window_start_s": p.Start, "window_end_s": p.End, "duration_s": p.End - p.Start, "logical_completed": b.Completed, "logical_errors": b.Errors, "success_qps": float64(b.Completed-b.Errors) / (p.End - p.Start), "samples": len(b.Latencies), "p50_ms": q(.5), "p95_ms": q(.95), "p99_ms": q(.99), "latencies_ms": b.Latencies, "error_kinds": b.ErrorKinds}
	}
	return output
}
func failedSearch(r *milvuspb.SearchResults, err error) (int, string) {
	if err != nil {
		return 1, "grpc:" + status.Code(err).String()
	}
	if r == nil || r.Status == nil {
		return 1, "missing_status"
	}
	if r.Status.Code != 0 || r.Status.ErrorCode != 0 {
		return 1, fmt.Sprintf("milvus:%d/%d", r.Status.Code, r.Status.ErrorCode)
	}
	return 0, ""
}
func main() {
	target := flag.String("target", "localhost:29561", "")
	mode := flag.String("mode", "multi", "none, search or multi")
	residentWorkers := flag.Int("resident-workers", 128, "")
	groups := flag.Int("groups", 16, "secondary concurrent group cap")
	rate := flag.Float64("rate", 0, "secondary offered logical searches/s; zero means closed-loop")
	warm := flag.Duration("warmup", 5*time.Second, "")
	before := flag.Duration("baseline", 15*time.Second, "")
	during := flag.Duration("mixed", 30*time.Second, "")
	after := flag.Duration("recovery", 15*time.Second, "")
	trim := flag.Duration("trim", 3*time.Second, "exclude transition prefix from stable windows")
	fixture := flag.String("fixture", "artifacts/multisearch-v2/fixture.json", "")
	out := flag.String("out", "mixed.json", "")
	flag.Parse()
	if *mode != "none" && *mode != "search" && *mode != "multi" {
		panic("invalid mode")
	}
	if *residentWorkers < 1 || *groups < 1 || *rate < 0 || *before <= *trim || *during <= *trim || *after <= *trim {
		panic("invalid experiment limits")
	}
	data, err := os.ReadFile(*fixture)
	if err != nil {
		panic(err)
	}
	var input struct{ Requests []string }
	if err = json.Unmarshal(data, &input); err != nil {
		panic(err)
	}
	if len(input.Requests) != 128 {
		panic("expected 8 collections x 16 vectors")
	}
	reqs := make([]*milvuspb.SearchRequest, len(input.Requests))
	for i, s := range input.Requests {
		b, e := base64.StdEncoding.DecodeString(s)
		if e != nil {
			panic(e)
		}
		reqs[i] = &milvuspb.SearchRequest{}
		if e = proto.Unmarshal(b, reqs[i]); e != nil {
			panic(e)
		}
	}
	connect := func() (*grpc.ClientConn, milvuspb.MilvusServiceClient) {
		c, e := grpc.NewClient(*target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if e != nil {
			panic(e)
		}
		return c, milvuspb.NewMilvusServiceClient(c)
	}
	connA, a := connect()
	defer connA.Close()
	connB, b := connect()
	defer connB.Close()
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", base64.StdEncoding.EncodeToString([]byte("root:Milvus"))))
	start := time.Now()
	mixedStart := start.Add(*warm + *before)
	mixedEnd := mixedStart.Add(*during)
	end := mixedEnd.Add(*after)
	windows := []phase{{"baseline", (*warm + *trim).Seconds(), (*warm + *before).Seconds()}, {"mixed", (*warm + *before + *trim).Seconds(), (*warm + *before + *during).Seconds()}, {"recovery", (*warm + *before + *during + *trim).Seconds(), (*warm + *before + *during + *after).Seconds()}}
	makeCollector := func() *collector {
		return &collector{Phases: map[string]*bucket{}, Seconds: map[int]*bucket{}, Windows: windows, Start: start}
	}
	resident, secondary := makeCollector(), makeCollector()
	var aNext, bNext atomic.Uint64
	var aActive, bActive atomic.Int64
	var all sync.WaitGroup
	var residentTotal, residentErrors, secondaryTotal, secondaryErrors atomic.Int64
	for range *residentWorkers {
		all.Add(1)
		go func() {
			defer all.Done()
			for time.Now().Before(end) {
				i := int(aNext.Add(1) - 1)
				req := reqs[(i%8)*16+(i/8)%16]
				s := time.Now()
				aActive.Add(1)
				call, cancel := context.WithTimeout(ctx, 10*time.Second)
				r, e := a.Search(call, req)
				cancel()
				aActive.Add(-1)
				f, k := failedSearch(r, e)
				residentTotal.Add(1)
				residentErrors.Add(int64(f))
				resident.add(s, 1, f, k)
			}
		}()
	}
	runGroup := func() {
		s := time.Now()
		i := int(bNext.Add(1) - 1)
		items := make([]*milvuspb.MultiSearchItem, 8)
		for j := range items {
			items[j] = &milvuspb.MultiSearchItem{Id: fmt.Sprint(j), Search: reqs[((i+j)%8)*16+(i/8)%16]}
		}
		bActive.Add(1)
		defer bActive.Add(-1)
		call, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		failed := 0
		kind := ""
		if *mode == "multi" {
			r, e := b.MultiSearch(call, &milvuspb.MultiSearchRequest{Items: items, MaxConcurrentSearches: 8})
			if e != nil {
				failed = 8
				kind = "grpc:" + status.Code(e).String()
			} else if r == nil || r.Status == nil || r.Status.Code != 0 || r.Status.ErrorCode != 0 || len(r.Items) != 8 {
				failed = 8
				kind = "envelope_failure"
			} else {
				for _, item := range r.Items {
					if item.GrpcCode != 0 {
						failed++
						kind = fmt.Sprintf("item_grpc:%d", item.GrpcCode)
					} else {
						f, k := failedSearch(item.Result, nil)
						failed += f
						if f > 0 {
							kind = k
						}
					}
				}
			}
		} else {
			var children sync.WaitGroup
			var failures atomic.Int64
			for _, item := range items {
				children.Add(1)
				go func(req *milvuspb.SearchRequest) {
					defer children.Done()
					r, e := b.Search(call, req)
					f, _ := failedSearch(r, e)
					failures.Add(int64(f))
				}(item.Search)
			}
			children.Wait()
			failed = int(failures.Load())
			if failed > 0 {
				kind = "child_search_failure"
			}
		}
		secondaryTotal.Add(8)
		secondaryErrors.Add(int64(failed))
		secondary.add(s, 8, failed, kind)
	}
	var scheduled, admitted, dropped, late atomic.Int64
	all.Add(1)
	go func() {
		defer all.Done()
		if *mode == "none" {
			return
		}
		if *rate == 0 {
			time.Sleep(time.Until(mixedStart))
			var workers sync.WaitGroup
			for range *groups {
				workers.Add(1)
				go func() {
					defer workers.Done()
					for time.Now().Before(mixedEnd) {
						admitted.Add(1)
						runGroup()
					}
				}()
			}
			workers.Wait()
			return
		}
		jobs := make(chan struct{})
		var workers sync.WaitGroup
		for range *groups {
			workers.Add(1)
			go func() {
				defer workers.Done()
				for range jobs {
					runGroup()
				}
			}()
		}
		interval := time.Duration(float64(time.Second) * 8 / (*rate))
		for due := mixedStart; due.Before(mixedEnd); due = due.Add(interval) {
			time.Sleep(time.Until(due))
			scheduled.Add(1)
			if time.Now().After(due.Add(interval)) {
				late.Add(1)
				continue
			}
			select {
			case jobs <- struct{}{}:
				admitted.Add(1)
			default:
				dropped.Add(1)
			}
		}
		close(jobs)
		workers.Wait()
	}()
	inflight := []map[string]any{}
	for time.Now().Before(end) {
		inflight = append(inflight, map[string]any{"elapsed_s": time.Since(start).Seconds(), "resident_calls": aActive.Load(), "secondary_groups": bActive.Load()})
		time.Sleep(100 * time.Millisecond)
	}
	all.Wait()
	seconds := []map[string]any{}
	for sec := 0; sec < int(end.Sub(start).Seconds())+11; sec++ {
		aa, bb := resident.Seconds[sec], secondary.Seconds[sec]
		if aa == nil && bb == nil {
			continue
		}
		if aa == nil {
			aa = &bucket{}
		}
		if bb == nil {
			bb = &bucket{}
		}
		seconds = append(seconds, map[string]any{"second": sec, "resident_successes": aa.Completed - aa.Errors, "secondary_successes": bb.Completed - bb.Errors, "resident_errors": aa.Errors, "secondary_errors": bb.Errors})
	}
	result := map[string]any{"mode": *mode, "target": *target, "resident_workers": *residentWorkers, "secondary_groups": *groups, "secondary_group_size": 8, "offered_secondary_logical_qps": *rate, "start_unix_s": float64(start.UnixNano()) / 1e9, "injection_start_s": mixedStart.Sub(start).Seconds(), "injection_end_s": mixedEnd.Sub(start).Seconds(), "end_s": end.Sub(start).Seconds(), "trim_s": trim.Seconds(), "resident": resident.output(), "secondary": secondary.output(), "seconds": seconds, "inflight_samples": inflight, "scheduled_groups": scheduled.Load(), "admitted_groups": admitted.Load(), "dropped_groups": dropped.Load(), "generator_late_groups": late.Load(), "resident_total": residentTotal.Load(), "resident_errors_all": residentErrors.Load(), "secondary_total": secondaryTotal.Load(), "secondary_errors_all": secondaryErrors.Load()}
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		panic(err)
	}
	if err = os.WriteFile(*out, encoded, 0644); err != nil {
		panic(err)
	}
	fmt.Printf("%s added_rate=%.0f resident=%d secondary=%d errors=%d drops=%d late=%d\n", *mode, *rate, residentTotal.Load(), secondaryTotal.Load(), residentErrors.Load()+secondaryErrors.Load(), dropped.Load(), late.Load())
}
