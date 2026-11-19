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

package proxy

import (
	"html/template"
	"net/http"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
)

// maxSearchProfiles is how many profiles the Proxy keeps for its console
// pages, as many as StarRocks' FE lists on its queries page.
const maxSearchProfiles = 100

// searchProfile is a finished search that asked for a profile.
type searchProfile struct {
	ID         int64
	User       string
	Database   string
	Collection string
	Expr       string
	Type       string
	Start      time.Time
	End        time.Time
	Total      time.Duration
	State      string
	// Text is the profile drawn as a tree, without its terminal colors.
	Text string
}

// searchProfileStore keeps the profiles of the latest searches that asked
// for one. Its zero value is ready to use.
type searchProfileStore struct {
	mu       sync.Mutex
	profiles []*searchProfile
}

var ansiEscape = regexp.MustCompile("\x1b\\[[0-9;]*m")

func (s *searchProfileStore) add(p *searchProfile) {
	p.Text = ansiEscape.ReplaceAllString(p.Text, "")
	s.mu.Lock()
	defer s.mu.Unlock()
	s.profiles = append(s.profiles, p)
	if over := len(s.profiles) - maxSearchProfiles; over > 0 {
		s.profiles = append([]*searchProfile(nil), s.profiles[over:]...)
	}
}

// list returns the kept profiles, newest first.
func (s *searchProfileStore) list() []*searchProfile {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*searchProfile, 0, len(s.profiles))
	for i := len(s.profiles) - 1; i >= 0; i-- {
		out = append(out, s.profiles[i])
	}
	return out
}

func (s *searchProfileStore) get(id int64) *searchProfile {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range s.profiles {
		if p.ID == id {
			return p
		}
	}
	return nil
}

// The pages follow StarRocks' FE: a table of the latest queries, each linking
// to its profile.
var searchProfilePages = template.Must(template.New("layout").Funcs(template.FuncMap{
	"time": func(t time.Time) string { return t.UTC().Format("2006-01-02 15:04:05.000 (Z)") },
	"ms": func(d time.Duration) string {
		return strconv.FormatFloat(float64(d.Microseconds())/1000, 'f', 3, 64) + "ms"
	},
}).Parse(`{{define "head"}}<!doctype html><html><head><meta charset="utf-8"><title>Milvus Searches</title><style>
body{margin:0;font:14px/1.45 -apple-system,"Segoe UI",Helvetica,Arial,sans-serif;color:#222;background:#fff}
nav{position:sticky;top:0;display:flex;gap:24px;align-items:center;padding:12px 24px;border-bottom:1px solid #ddd;background:#f8f8f8}
nav b{font-size:20px} nav a{color:#555;text-decoration:none} nav a.on{color:#222}
main{max-width:1200px;margin:0 auto;padding:24px 16px}
table{width:100%;border-collapse:collapse} th,td{border:1px solid #ddd;padding:8px;text-align:left;vertical-align:top}
th{background:#fafafa} td.expr{max-width:320px;overflow-wrap:anywhere} a{color:#337ab7}
pre{background:#f5f5f5;border:1px solid #ccc;border-radius:4px;padding:10px;font:13px/1.45 Menlo,Consolas,monospace;overflow:auto}
</style></head><body><nav><b>Milvus</b><a class="on" href="profiles">searches</a></nav><main>{{end}}
{{define "list"}}{{template "head"}}<h1>Finished Searches</h1><p>This table lists the latest {{.Max}} searches that asked for a profile (search param profile=true).</p>
<table><tr><th>User</th><th>Database</th><th>Collection</th><th>Expr</th><th>Search Type</th><th>Start Time</th><th>End Time</th><th>Total</th><th>State</th><th>Profile</th></tr>
{{range .Profiles}}<tr><td>{{.User}}</td><td>{{.Database}}</td><td>{{.Collection}}</td><td class="expr">{{.Expr}}</td><td>{{.Type}}</td><td>{{time .Start}}</td><td>{{time .End}}</td><td>{{ms .Total}}</td><td>{{.State}}</td><td><a href="profile?id={{.ID}}">{{.ID}}</a></td></tr>
{{else}}<tr><td colspan="10">No search has asked for a profile yet.</td></tr>{{end}}</table></main></body></html>{{end}}
{{define "profile"}}{{template "head"}}<pre>{{.Text}}</pre></main></body></html>{{end}}`))

// recordSearchProfile keeps the profile SetSearchProfile put in status for the
// console pages. A search that failed has none and is not kept.
func (node *Proxy) recordSearchProfile(id int64, user string, request *milvuspb.SearchRequest, total time.Duration, status *commonpb.Status) {
	text, ok := status.GetExtraInfo()[SearchProfileTextKey]
	if !ok {
		return
	}
	end := time.Now()
	searchType := "Search"
	if len(request.GetSubReqs()) > 0 {
		searchType = "HybridSearch"
	}
	node.searchProfiles.add(&searchProfile{
		ID:         id,
		User:       user,
		Database:   request.GetDbName(),
		Collection: request.GetCollectionName(),
		Expr:       request.GetDsl(),
		Type:       searchType,
		Start:      end.Add(-total),
		End:        end,
		Total:      total,
		State:      "Finished",
		Text:       text,
	})
}

// getSearchProfiles serves the page that lists the latest searches with a
// profile.
func getSearchProfiles(node *Proxy) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Content-Type", "text/html; charset=utf-8")
		c.Status(http.StatusOK)
		_ = searchProfilePages.ExecuteTemplate(c.Writer, "list", map[string]any{
			"Max":      maxSearchProfiles,
			"Profiles": node.searchProfiles.list(),
		})
	}
}

// getSearchProfile serves the profile of one search, given as ?id=.
func getSearchProfile(node *Proxy) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, err := strconv.ParseInt(c.Query("id"), 10, 64)
		p := node.searchProfiles.get(id)
		if err != nil || p == nil {
			c.String(http.StatusNotFound, "no profile for search %q: it asked for none, or it is older than the latest %d", c.Query("id"), maxSearchProfiles)
			return
		}
		c.Header("Content-Type", "text/html; charset=utf-8")
		c.Status(http.StatusOK)
		_ = searchProfilePages.ExecuteTemplate(c.Writer, "profile", p)
	}
}
