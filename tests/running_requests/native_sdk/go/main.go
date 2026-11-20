// Native SDK worker. Python owns only orchestration and assertions.
package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	mc "github.com/milvus-io/milvus/client/v3/milvusclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"math"
	"os"
	"strings"
	"sync"
	"time"
)

type M = map[string]any
type Command struct {
	Op, Client, URI, Token, DB, Collection, User, Reason, Kind, Call, Expr, Vectors string
	VectorKind, RequestID, AnnsField                                                string
	OutputFields                                                                    []string
	ReturnQueryRows                                                                 bool
	IDs                                                                             []string
	MinMS                                                                           int64
	NQ, Limit, NProbe, BatchSize                                                    int
}
type batchIterator interface {
	Next(context.Context) (mc.ResultSet, error)
}

type Worker struct {
	iterators map[string]batchIterator
	clients   map[string]*mc.Client
	jobs      map[string]chan M
	counts    map[string]map[string]int
	mu        sync.Mutex
}

func failure(e error) M {
	code := int32(-1)
	var ce interface{ Code() int32 }
	if errors.As(e, &ce) {
		code = ce.Code()
	}
	return M{"ok": false, "code": code, "message": e.Error()}
}
func rows(rs []*entity.RunningRequest) []M {
	out := []M{}
	for _, r := range rs {
		out = append(out, M{"request_id": r.ID, "proxy_id": r.ProxyID, "type": r.Type, "db_name": r.DBName, "collection_name": r.CollectionName, "user": r.User, "client_addr": r.ClientAddr, "nq": r.NQ, "topk": r.TopK, "expr": r.Expr, "start_time_ms": r.StartTime.UnixMilli(), "queued_ms": r.Queued.Milliseconds(), "elapsed_ms": r.Elapsed.Milliseconds(), "state": r.State, "task_ids": r.TaskIDs, "trace_id": r.TraceID, "cancellable": r.Cancellable})
	}
	return out
}
func nodes(ns []entity.RequestNodeResult) []M {
	out := []M{}
	for _, n := range ns {
		s := ""
		if n.Err != nil {
			s = n.Err.Error()
		}
		out = append(out, M{"node_id": n.NodeID, "answered": n.Err == nil, "unimplemented": n.Unimplemented, "error": s})
	}
	return out
}
func readQueryVectors(c Command) ([]entity.Vector, error) {
	raw, err := os.ReadFile(c.Vectors)
	if err != nil {
		return nil, err
	}
	vectors := make([]entity.Vector, c.NQ)
	if c.VectorKind == "" {
		const dim = 128
		if len(raw)/4/dim < c.NQ {
			return nil, fmt.Errorf("dense input has fewer than %d vectors", c.NQ)
		}
		for i := range vectors {
			v := make(entity.FloatVector, dim)
			for j := range v {
				v[j] = math.Float32frombits(binary.LittleEndian.Uint32(raw[(i*dim+j)*4:]))
			}
			vectors[i] = v
		}
		return vectors, nil
	}

	var inputs []json.RawMessage
	if err := json.Unmarshal(raw, &inputs); err != nil {
		return nil, err
	}
	if len(inputs) < c.NQ {
		return nil, fmt.Errorf("indexed input has fewer than %d vectors", c.NQ)
	}
	for i := range vectors {
		switch c.VectorKind {
		case "binary":
			var value []byte
			if err := json.Unmarshal(inputs[i], &value); err != nil {
				return nil, err
			}
			if len(value) != 16 {
				return nil, fmt.Errorf("binary input %d must contain 16 bytes", i)
			}
			vectors[i] = entity.BinaryVector(value)
		case "sparse":
			var value map[uint32]float32
			if err := json.Unmarshal(inputs[i], &value); err != nil {
				return nil, err
			}
			positions := make([]uint32, 0, len(value))
			weights := make([]float32, 0, len(value))
			for position, weight := range value {
				positions = append(positions, position)
				weights = append(weights, weight)
			}
			embedding, err := entity.NewSliceSparseEmbedding(positions, weights)
			if err != nil {
				return nil, err
			}
			vectors[i] = embedding
		case "struct-embedding":
			var value []entity.FloatVector
			if err := json.Unmarshal(inputs[i], &value); err != nil {
				return nil, err
			}
			if len(value) != 1 || len(value[0]) != 32 {
				return nil, fmt.Errorf("struct input %d must contain one 32-dimensional embedding", i)
			}
			vectors[i] = entity.FloatVectorArray(value)
		case "bm25":
			var value string
			if err := json.Unmarshal(inputs[i], &value); err != nil {
				return nil, err
			}
			vectors[i] = entity.Text(value)
		default:
			return nil, fmt.Errorf("unknown vector kind %q", c.VectorKind)
		}
	}
	return vectors, nil
}

func (w *Worker) query(c Command) M {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	client := w.clients[c.Client]
	if c.Kind == "Query" {
		fields := c.OutputFields
		if len(fields) == 0 {
			fields = []string{"id", "vector"}
		}
		r, err := client.Query(ctx, mc.NewQueryOption(c.Collection).WithFilter(c.Expr).WithOutputFields(fields...).WithLimit(c.Limit))
		if err != nil {
			return failure(err)
		}
		if r.Err != nil {
			return failure(r.Err)
		}
		result := M{"ok": true, "count": r.ResultCount}
		if c.ReturnQueryRows {
			rows := make([]M, r.ResultCount)
			for i := range rows {
				rows[i] = M{}
			}
			for _, field := range fields {
				column := r.GetColumn(field)
				if column == nil {
					return failure(fmt.Errorf("missing Query output field %q", field))
				}
				for i := range rows {
					value, err := column.Get(i)
					if err != nil {
						return failure(err)
					}
					rows[i][field] = value
				}
			}
			result["rows"] = rows
		}
		return result
	}
	vectors, e := readQueryVectors(c)
	if e != nil {
		return failure(e)
	}
	var rs []mc.ResultSet
	if c.Kind == "HybridSearch" {
		a := mc.NewAnnRequest("vector", c.Limit, vectors...).WithFilter(c.Expr)
		rs, e = client.HybridSearch(ctx, mc.NewHybridSearchOption(c.Collection, c.Limit, a, a).WithReranker(mc.NewRRFReranker()))
	} else {
		field := c.AnnsField
		if field == "" {
			field = "vector"
		}
		option := mc.NewSearchOption(c.Collection, c.Limit, vectors).WithANNSField(field).WithFilter(c.Expr)
		if len(c.OutputFields) > 0 {
			option.WithOutputFields(c.OutputFields...)
		}
		if c.VectorKind == "struct-embedding" {
			option.WithAnnParam(index.NewHNSWAnnParam(64))
			option.WithSearchParam("metric_type", "MAX_SIM_COSINE")
		}
		if c.NProbe > 0 {
			option.WithAnnParam(index.NewIvfAnnParam(c.NProbe))
		}
		rs, e = client.Search(ctx, option)
	}
	if e != nil {
		return failure(e)
	}
	hits := [][]M{}
	for _, r := range rs {
		if r.Err != nil {
			return failure(r.Err)
		}
		h := []M{}
		for i := 0; i < r.ResultCount; i++ {
			id, err := r.IDs.GetAsInt64(i)
			if err != nil {
				return failure(err)
			}
			hit := M{"id": id, "distance": r.Scores[i]}
			if c.Kind == "Search" && len(c.OutputFields) > 0 {
				fields := M{}
				for _, field := range c.OutputFields {
					column := r.GetColumn(field)
					if column == nil {
						return failure(fmt.Errorf("missing Search output field %q", field))
					}
					isNull, err := column.IsNull(i)
					if err != nil {
						return failure(err)
					}
					fields[field] = nil
					if !isNull {
						value, err := column.Get(i)
						if err != nil {
							return failure(err)
						}
						fields[field] = value
					}
				}
				hit["entity"] = fields
			}
			h = append(h, hit)
		}
		hits = append(hits, h)
	}
	return M{"ok": true, "hits": hits}
}
func (w *Worker) openIterator(ctx context.Context, c Command) error {
	client := w.clients[c.Client]
	var iterator batchIterator
	var err error
	switch c.Kind {
	case "Search":
		vectors, readErr := readQueryVectors(c)
		if readErr != nil {
			return readErr
		}
		if len(vectors) != 1 {
			return fmt.Errorf("Search Iterator requires one vector")
		}
		iterator, err = client.SearchIterator(ctx, mc.NewSearchIteratorOption(c.Collection, vectors[0]).
			WithANNSField("vector").WithFilter(c.Expr).WithBatchSize(c.BatchSize).WithIteratorLimit(int64(c.Limit)))
	case "Query":
		iterator, err = client.QueryIterator(ctx, mc.NewQueryIteratorOption(c.Collection).
			WithFilter(c.Expr).WithOutputFields("id", "vector").WithBatchSize(c.BatchSize).WithIteratorLimit(int64(c.Limit)))
	default:
		return fmt.Errorf("unknown iterator kind %q", c.Kind)
	}
	if err == nil {
		w.iterators[c.Call] = iterator
	}
	return err
}

func iteratorNext(iterator batchIterator, kind string) M {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	r, err := iterator.Next(ctx)
	if err != nil {
		return failure(err)
	}
	if r.Err != nil {
		return failure(r.Err)
	}
	column := r.IDs
	if kind == "Query" {
		column = r.GetColumn("id")
	}
	if column == nil {
		return failure(fmt.Errorf("iterator batch is missing IDs"))
	}
	ids := make([]int64, r.ResultCount)
	for i := range ids {
		ids[i], err = column.GetAsInt64(i)
		if err != nil {
			return failure(err)
		}
	}
	return M{"ok": true, "ids": ids}
}

func (w *Worker) run(c Command) M {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	switch c.Op {
	case "connect":
		w.counts[c.Client] = map[string]int{}
		interceptor := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			w.mu.Lock()
			w.counts[c.Client][method[strings.LastIndex(method, "/")+1:]]++
			w.mu.Unlock()
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		x, e := mc.New(ctx, &mc.ClientConfig{Address: c.URI, APIKey: c.Token, DBName: c.DB, DialOptions: []grpc.DialOption{grpc.WithUnaryInterceptor(interceptor)}})
		if e != nil {
			return failure(e)
		}
		w.clients[c.Client] = x
	case "list":
		// The Go option accepts a nanosecond duration; reject an unrepresentable
		// worker input before multiplying, so no wrapped threshold reaches Milvus.
		const maxDurationMS = int64(1<<63-1) / int64(time.Millisecond)
		if c.MinMS < -maxDurationMS || c.MinMS > maxDurationMS {
			return failure(fmt.Errorf("minElapsedMs %d cannot be represented as a Go duration", c.MinMS))
		}
		if c.RequestID != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, "client-request-id", c.RequestID)
		}
		r, e := w.clients[c.Client].ListRunningRequests(ctx, mc.NewListRunningRequestsOption().WithDBName(c.DB).WithCollectionName(c.Collection).WithUser(c.User).WithMinElapsed(time.Duration(c.MinMS)*time.Millisecond))
		if e != nil {
			return failure(e)
		}
		return M{"ok": true, "requests": rows(r.Requests), "nodes": nodes(r.NodeResults)}
	case "cancel":
		r, e := w.clients[c.Client].CancelRequests(ctx, mc.NewCancelRequestsOption(c.IDs...).WithUser(c.User).WithReason(c.Reason))
		if e != nil {
			return failure(e)
		}
		return M{"ok": true, "canceled": rows(r.Canceled), "not_found": r.NotFound, "undetermined": r.Undetermined, "nodes": nodes(r.NodeResults)}
	case "iterator-open":
		openContext, finish := context.WithTimeout(context.Background(), 120*time.Second)
		defer finish()
		if err := w.openIterator(openContext, c); err != nil {
			return failure(err)
		}
	case "iterator-next":
		it, ok := w.iterators[c.RequestID]
		if !ok {
			return failure(fmt.Errorf("unknown iterator %q", c.RequestID))
		}
		ch := make(chan M, 1)
		w.jobs[c.Call] = ch
		go func() { ch <- iteratorNext(it, c.Kind) }()
	case "iterator-close":
		delete(w.iterators, c.RequestID)
	case "start":
		ch := make(chan M, 1)
		w.jobs[c.Call] = ch
		go func() { ch <- w.query(c) }()
	case "await":
		return <-w.jobs[c.Call]
	case "poll":
		select {
		case r := <-w.jobs[c.Call]:
			w.jobs[c.Call] <- r
			return M{"ok": true, "done": true, "result": r}
		default:
			return M{"ok": true, "done": false}
		}
	case "counts":
		w.mu.Lock()
		defer w.mu.Unlock()
		out := map[string]int{}
		for k, v := range w.counts[c.Client] {
			out[k] = v
		}
		return M{"ok": true, "counts": out}
	default:
		return failure(fmt.Errorf("unknown operation %q", c.Op))
	}
	return M{"ok": true}
}
func main() {
	w := Worker{iterators: map[string]batchIterator{}, clients: map[string]*mc.Client{}, jobs: map[string]chan M{}, counts: map[string]map[string]int{}}
	defer func() {
		for _, c := range w.clients {
			c.Close(context.Background())
		}
	}()
	s := bufio.NewScanner(os.Stdin)
	s.Buffer(make([]byte, 4096), 1024*1024)
	enc := json.NewEncoder(os.Stdout)
	for s.Scan() {
		var c Command
		if e := json.Unmarshal(s.Bytes(), &c); e != nil {
			enc.Encode(failure(e))
			continue
		}
		enc.Encode(w.run(c))
	}
}
