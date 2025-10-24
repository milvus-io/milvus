package main

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"google.golang.org/protobuf/proto"

	schemapb "github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	_ "github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type parseRequest struct {
	ID        string `json:"id"`
	Op        string `json:"op"`
	SchemaB64 string `json:"schema_b64"`
	Expr      string `json:"expr"`
	Options   struct {
		IsCount bool  `json:"is_count"`
		Limit   int64 `json:"limit"`
	} `json:"options"`
}

type parseResponse struct {
	ID      string `json:"id"`
	OK      bool   `json:"ok"`
	PlanB64 string `json:"plan_b64,omitempty"`
	Error   string `json:"error,omitempty"`
}

func handle(line string) parseResponse {
	line = strings.TrimSpace(line)
	if line == "" {
		return parseResponse{ID: "", OK: false, Error: "empty line"}
	}

	var req parseRequest
	if err := json.Unmarshal([]byte(line), &req); err != nil {
		return parseResponse{ID: req.ID, OK: false, Error: fmt.Sprintf("invalid json: %v", err)}
	}
	if req.Op != "parse_expr" {
		return parseResponse{ID: req.ID, OK: false, Error: "unsupported op"}
	}
	if req.SchemaB64 == "" {
		return parseResponse{ID: req.ID, OK: false, Error: "missing schema_b64"}
	}
	if req.Expr == "" {
		return parseResponse{ID: req.ID, OK: false, Error: "missing expr"}
	}

	schemaBytes, err := base64.StdEncoding.DecodeString(req.SchemaB64)
	if err != nil {
		return parseResponse{ID: req.ID, OK: false, Error: fmt.Sprintf("decode schema_b64 failed: %v", err)}
	}
	var schema schemapb.CollectionSchema
	if err := proto.Unmarshal(schemaBytes, &schema); err != nil {
		return parseResponse{ID: req.ID, OK: false, Error: fmt.Sprintf("unmarshal schema failed: %v", err)}
	}

	helper, err := typeutil.CreateSchemaHelper(&schema)
	if err != nil {
		return parseResponse{ID: req.ID, OK: false, Error: fmt.Sprintf("schema helper error: %v", err)}
	}

	planNode, err := planparserv2.CreateRetrievePlan(helper, req.Expr, nil)
	if err != nil {
		return parseResponse{ID: req.ID, OK: false, Error: fmt.Sprintf("parse error: %v", err)}
	}

	// Apply options if provided
	if q := planNode.GetQuery(); q != nil {
		q.IsCount = req.Options.IsCount
		if req.Options.Limit > 0 {
			q.Limit = req.Options.Limit
		}
	}

	planBytes, err := proto.Marshal(planNode)
	if err != nil {
		return parseResponse{ID: req.ID, OK: false, Error: fmt.Sprintf("marshal plan failed: %v", err)}
	}
	return parseResponse{ID: req.ID, OK: true, PlanB64: base64.StdEncoding.EncodeToString(planBytes)}
}

func writeResp(w *bufio.Writer, resp parseResponse) {
	b, _ := json.Marshal(resp)
	_, _ = w.Write(b)
	_ = w.WriteByte('\n')
	_ = w.Flush()
}

func main() {
	in := bufio.NewScanner(os.Stdin)
	buf := make([]byte, 0, 1024*1024)
	in.Buffer(buf, 16*1024*1024)
	w := bufio.NewWriter(os.Stdout)

	for {
		if !in.Scan() {
			if err := in.Err(); err != nil && err != io.EOF {
				writeResp(w, parseResponse{ID: "", OK: false, Error: fmt.Sprintf("scan error: %v", err)})
			}
			break
		}
		resp := handle(in.Text())
		writeResp(w, resp)
	}
}
