// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package milvus

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/distributed/proxy/httpserver"
)

const (
	ExplainCmd = "explain"

	explainPath = "/v2/vectordb" + httpserver.EntityCategory + httpserver.ExplainAction
)

// explain posts a search request body to the RESTful explain route of a
// running Milvus and prints the plan the search would run.
type explain struct {
	address string
	token   string
	format  string
	file    string
}

func (c *explain) execute(args []string, flags *flag.FlagSet) {
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, explainLine)
	}
	c.formatFlags(args, flags)
	if err := c.run(os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(-1)
	}
}

func (c *explain) formatFlags(args []string, flags *flag.FlagSet) {
	flags.StringVar(&c.address, "address", "http://localhost:19530", "base URL of the Milvus RESTful API")
	flags.StringVar(&c.token, "token", "", "credentials sent as a bearer token, e.g. root:Milvus")
	flags.StringVar(&c.format, "format", httpserver.ExplainFormatJSON, "output format, json or text")
	flags.StringVar(&c.file, "file", "-", "file holding the search request body, - for stdin")
	if err := flags.Parse(args[2:]); err != nil {
		os.Exit(-1)
	}
}

func (c *explain) run(stdin io.Reader, stdout io.Writer) error {
	body, err := c.requestBody(stdin)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, strings.TrimRight(c.address, "/")+explainPath, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	var ret struct {
		Code    int32           `json:"code"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(payload, &ret); err != nil {
		return fmt.Errorf("unexpected response (HTTP %d): %s", resp.StatusCode, bytes.TrimSpace(payload))
	}
	if ret.Code != 0 {
		return fmt.Errorf("explain failed (code %d): %s", ret.Code, ret.Message)
	}
	// JSON that is not Milvus's envelope, as a gateway's error body: code
	// decodes as 0 and data is missing
	if resp.StatusCode != http.StatusOK || len(ret.Data) == 0 {
		return fmt.Errorf("unexpected response (HTTP %d): %s", resp.StatusCode, bytes.TrimSpace(payload))
	}
	if c.format == httpserver.ExplainFormatText {
		var text string
		if err := json.Unmarshal(ret.Data, &text); err != nil {
			return fmt.Errorf("unexpected text response: %s", ret.Data)
		}
		_, err = io.WriteString(stdout, text)
		return err
	}
	var out bytes.Buffer
	if err := json.Indent(&out, ret.Data, "", "  "); err != nil {
		return err
	}
	out.WriteByte('\n')
	_, err = out.WriteTo(stdout)
	return err
}

// requestBody reads the search body and sets the output format from the
// flag, replacing a format key the body may carry, since the output branch
// in run keys on the flag. The other values are forwarded as JSON text
// rather than Go values, so numbers keep their digits.
func (c *explain) requestBody(stdin io.Reader) ([]byte, error) {
	src := stdin
	if c.file != "-" {
		f, err := os.OpenFile(c.file, os.O_RDONLY, 0)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		src = f
	}
	raw, err := io.ReadAll(src)
	if err != nil {
		return nil, err
	}
	var body map[string]json.RawMessage
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, fmt.Errorf("the search request body must be a JSON object: %w", err)
	}
	if body == nil { // a JSON null decodes without error
		return nil, errors.New("the search request body must be a JSON object, got null")
	}
	body["format"], _ = json.Marshal(c.format) // a string always encodes
	return json.Marshal(body)
}
