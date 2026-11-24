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

package dql

import (
	"bytes"
	"strconv"
	"strings"

	"github.com/samber/lo"
	"github.com/tidwall/gjson"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/agg"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// outputBinding is the result of binding a request's output_fields.
type outputBinding struct {
	// resultFieldNames are the stored fields to read, by schema name.
	resultFieldNames []string
	// userOutputFields are the final output column names.
	userOutputFields []string
	// userDynamicFields are the dynamic-field keys named explicitly; empty
	// when the whole dynamic field is returned.
	userDynamicFields []string
	aggregates        []agg.AggregateBase
	// sourceOutputFields are the items without their alias clause, in request
	// order. The aggregation field map matches them against group-by fields
	// and aggregate names.
	sourceOutputFields             []string
	userRequestedPkFieldExplicitly bool
	projection                     *outputProjection
}

// outputAlias renames one result column: source is the name the column has
// when it reaches the proxy's last step, alias is the output name.
type outputAlias struct {
	source string
	alias  string
}

// requestLocalOutputIDStart is the output ID of the first virtual-column item
// of a request; later items count downward. Schema fields use IDs from 100
// up, system fields 0 and 1, and segcore keeps -100..-999 for its own hidden
// columns, so these IDs collide with none of them. No component interprets
// the value: an ID is looked up in the request's projection list.
const requestLocalOutputIDStart int64 = -1000

// virtualItem is one virtual-column item of output_fields.
type virtualItem struct {
	id       int64
	column   string
	argKind  planparserv2.VirtualColumnArgKind
	argField string
	argIndex int64
	// name is the output column name: the alias, or the item text.
	name     string
	dataType schemapb.DataType
	// subRequest is the hybrid sub-request an argument form selects. It is
	// resolved once the sub-requests are known.
	subRequest int
}

// text is the item as written in output_fields, without optional whitespace
// and without the alias.
func (v *virtualItem) text() string {
	switch v.argKind {
	case planparserv2.VirtualColumnArgField:
		return v.column + "(" + v.argField + ")"
	case planparserv2.VirtualColumnArgIndex:
		return v.column + "(" + strconv.FormatInt(v.argIndex, 10) + ")"
	}
	return v.column
}

// evaluatedAtProxy reports whether the proxy computes the column in its last
// step. The other items are computed on the QueryNode and travel in
// fields_data under their output ID.
func (v *virtualItem) evaluatedAtProxy(isSearch bool) bool {
	if v.argKind != planparserv2.VirtualColumnNoArg {
		return true
	}
	return isSearch && v.column == planparserv2.VirtualColumnScore
}

func virtualColumnDataType(column string) schemapb.DataType {
	switch column {
	case planparserv2.VirtualColumnSegID, planparserv2.VirtualColumnSegOffset, planparserv2.VirtualColumnRank:
		return schemapb.DataType_Int64
	default:
		return schemapb.DataType_Float
	}
}

// canonicalVirtualColumnText is the default output name of a virtual-column
// item: the item text without optional whitespace.
func canonicalVirtualColumnText(item planparserv2.SelectItem) string {
	switch item.VirtualArgKind {
	case planparserv2.VirtualColumnArgField:
		return item.VirtualColumn + "(" + item.VirtualArgField + ")"
	case planparserv2.VirtualColumnArgIndex:
		return item.VirtualColumn + "(" + strconv.FormatInt(item.VirtualArgIndex, 10) + ")"
	default:
		return item.VirtualColumn
	}
}

// outputProjection is the alias table and the virtual-column list of one
// request, applied to the result as the proxy's last step.
type outputProjection struct {
	aliases []outputAlias
	// virtualItems are the virtual-column items in output_fields order.
	virtualItems []*virtualItem
	// hiddenDistance is set when a hybrid request has a $distance(x) item:
	// every sub-request then also returns its raw distance, so that the
	// value exists for a sub-request whose scores were rewritten on the
	// QueryNode. The column is not part of the output.
	hiddenDistance bool
	// keepSource holds the sources that are also returned under their own
	// name, because the request lists them without an alias or through "*".
	keepSource map[string]bool
	// wholeDynamic is true when the request returns the whole dynamic field
	// ("*" or "$meta") rather than selected keys.
	wholeDynamic bool
}

func (p *outputProjection) empty() bool {
	return p == nil || (len(p.aliases) == 0 && len(p.virtualItems) == 0)
}

// addVirtualColumn records a virtual-column item and assigns its output ID.
// Two items with the same column, argument and output name are one item.
func (b *outputBinding) addVirtualColumn(names *outputNameSet, userOutputFieldsMap map[string]bool, item planparserv2.SelectItem) error {
	text := canonicalVirtualColumnText(item)
	name := text
	if item.Alias != "" {
		name = item.Alias
	}
	if err := names.claim(name, "virtual:"+text, true); err != nil {
		return err
	}
	userOutputFieldsMap[name] = true
	p := b.projection
	for _, existing := range p.virtualItems {
		if existing.name == name {
			return nil
		}
	}
	p.virtualItems = append(p.virtualItems, &virtualItem{
		id:       requestLocalOutputIDStart - int64(len(p.virtualItems)),
		column:   item.VirtualColumn,
		argKind:  item.VirtualArgKind,
		argField: item.VirtualArgField,
		argIndex: item.VirtualArgIndex,
		name:     name,
		dataType: virtualColumnDataType(item.VirtualColumn),
	})
	if item.VirtualColumn == planparserv2.VirtualColumnDistance && item.VirtualArgKind != planparserv2.VirtualColumnNoArg {
		p.hiddenDistance = true
	}
	return nil
}

// checkVirtualColumnRequestKind rejects the forms that have no value for the
// kind of request, independent of the request's other parameters.
func checkVirtualColumnRequestKind(item planparserv2.SelectItem, isSearch bool) error {
	if isSearch {
		return nil
	}
	text := canonicalVirtualColumnText(item)
	if item.VirtualArgKind != planparserv2.VirtualColumnNoArg {
		return merr.WrapErrParameterInvalidMsg("virtual column %s is only available in hybrid search", text)
	}
	switch item.VirtualColumn {
	case planparserv2.VirtualColumnScore:
		return merr.WrapErrParameterInvalidMsg("virtual column $score is available in a query only with a ranker, which is not supported yet")
	case planparserv2.VirtualColumnDistance:
		return merr.WrapErrParameterInvalidMsg("virtual column $distance is not available in a query: a query runs no vector search")
	}
	return nil
}

// outputNames lists the alias and virtual-column output names.
func (p *outputProjection) outputNames() []string {
	names := make([]string, 0, len(p.aliases)+len(p.virtualItems))
	for _, a := range p.aliases {
		names = append(names, a.alias)
	}
	for _, v := range p.virtualItems {
		names = append(names, v.name)
	}
	return names
}

// virtualItemByID returns the virtual-column item with the given output ID.
func (p *outputProjection) virtualItemByID(id int64) *virtualItem {
	if p == nil {
		return nil
	}
	for _, v := range p.virtualItems {
		if v.id == id {
			return v
		}
	}
	return nil
}

// appendSearchScoreColumns adds one Float column per bare $score item, filled
// from the final scores of the result.
func (p *outputProjection) appendSearchScoreColumns(result *schemapb.SearchResultData) {
	if p == nil || result == nil {
		return
	}
	for _, v := range p.virtualItems {
		if v.column != planparserv2.VirtualColumnScore || v.argKind != planparserv2.VirtualColumnNoArg {
			continue
		}
		result.FieldsData = append(result.FieldsData, &schemapb.FieldData{
			Type:      schemapb.DataType_Float,
			FieldName: v.name,
			FieldId:   v.id,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{
					Data: append(make([]float32, 0, len(result.GetScores())), result.GetScores()...),
				}},
			}},
		})
	}
}

// sourceOf resolves a name in the output scope: an alias of a stored field
// resolves to that field's name, any other name to itself.
func (p *outputProjection) sourceOf(name string) (string, bool) {
	if p == nil {
		return name, false
	}
	for _, a := range p.aliases {
		if a.alias == name {
			return a.source, true
		}
	}
	return name, false
}

// renamed reports whether source is returned only under an alias.
func (p *outputProjection) renamed(source string) bool {
	if p == nil || p.keepSource[source] {
		return false
	}
	for _, a := range p.aliases {
		if a.source == source {
			return true
		}
	}
	return false
}

// resolveOrderByAliases rewrites "name[:direction]" order_by specs of a Query:
// a name that is an alias is replaced by the stored field it names. Ordering
// by the alias of an aggregate is rejected, like ordering by the aggregate.
func resolveOrderByAliases(specs []string, p *outputProjection) ([]string, error) {
	if p.empty() {
		return specs, nil
	}
	resolved := make([]string, len(specs))
	for i, spec := range specs {
		name, rest := spec, ""
		if idx := strings.Index(spec, ":"); idx >= 0 {
			name, rest = spec[:idx], spec[idx:]
		}
		source, ok := p.sourceOf(strings.TrimSpace(name))
		if !ok {
			resolved[i] = spec
			continue
		}
		if strings.Contains(source, "(") {
			return nil, merr.WrapErrParameterInvalidMsg("ORDER BY on aggregate expression '%s' (alias %s) is not yet supported", source, strings.TrimSpace(name))
		}
		resolved[i] = source + rest
	}
	return resolved, nil
}

// outputNameSet checks that final output column names are unique. Two items
// may share a name only when they denote the same column.
//
// The check applies to names the request chooses: an alias, or a virtual
// column. Two plain items can also end up under one name, a stored field and
// a dynamic key requested as $meta["<that name>"]; such requests were
// accepted before aliases existed and return the stored field, so they stay
// accepted.
type outputNameSet struct {
	entries map[string]outputNameEntry
}

type outputNameEntry struct {
	identity string
	// chosen is true when an alias or a virtual column claimed the name.
	chosen bool
}

func newOutputNameSet() *outputNameSet {
	return &outputNameSet{entries: make(map[string]outputNameEntry)}
}

func (s *outputNameSet) claim(name, identity string, chosen bool) error {
	existing, ok := s.entries[name]
	if ok && existing.identity != identity && (existing.chosen || chosen) {
		return merr.WrapErrParameterInvalidMsg("duplicate output field name %s: output field names must be unique", name)
	}
	if !ok || chosen {
		s.entries[name] = outputNameEntry{identity: identity, chosen: chosen || existing.chosen}
	}
	return nil
}

// addOutput records one item's output column: under its own name when alias
// is empty, under the alias otherwise.
func (b *outputBinding) addOutput(names *outputNameSet, userOutputFieldsMap map[string]bool, source, alias, identity string) error {
	if alias == "" {
		b.projection.keepSource[source] = true
		userOutputFieldsMap[source] = true
		return names.claim(source, identity, false)
	}
	if err := names.claim(alias, identity, true); err != nil {
		return err
	}
	userOutputFieldsMap[alias] = true
	for _, a := range b.projection.aliases {
		if a.source == source && a.alias == alias {
			return nil
		}
	}
	b.projection.aliases = append(b.projection.aliases, outputAlias{source: source, alias: alias})
	return nil
}

// Names under which clients already return the built-in similarity value of
// a Search hit.
const (
	searchBuiltinDistanceName = "distance"
	searchBuiltinScoreName    = "score"
)

func validateOutputAlias(item planparserv2.SelectItem, primaryFieldName string, isSearch bool) error {
	alias := item.Alias
	// The primary key is part of every result, so an alias equal to its name
	// can never be unique.
	if alias == primaryFieldName {
		return merr.WrapErrParameterInvalidMsg("alias %s is the primary key field name, which is always returned", alias)
	}
	if !isSearch || (alias != searchBuiltinDistanceName && alias != searchBuiltinScoreName) {
		return nil
	}
	if item.Kind == planparserv2.SelectItemVirtualColumn && item.VirtualArgKind == planparserv2.VirtualColumnNoArg {
		switch {
		case item.VirtualColumn == planparserv2.VirtualColumnDistance && alias == searchBuiltinDistanceName:
			return nil
		case item.VirtualColumn == planparserv2.VirtualColumnScore:
			return nil
		}
	}
	return merr.WrapErrParameterInvalidMsg(
		"alias %s is reserved in search results for the built-in similarity value; "+
			"only $distance AS distance, $score AS distance and $score AS score are allowed", alias)
}

// apply renames result columns according to the alias table and returns the
// new column list. A source that is not returned under its own name is
// renamed; otherwise the alias gets a second column that shares the data.
// When the whole dynamic field is returned, top-level keys equal to an alias
// are removed from every row of it: the explicit output column takes
// precedence over a same-named dynamic key.
func (p *outputProjection) apply(fieldsData []*schemapb.FieldData) []*schemapb.FieldData {
	if p.empty() {
		return fieldsData
	}
	bySource := make(map[string][]string, len(p.aliases))
	for _, a := range p.aliases {
		bySource[a.source] = append(bySource[a.source], a.alias)
	}
	aliasNames := p.outputNames()
	out := make([]*schemapb.FieldData, 0, len(fieldsData)+len(p.aliases))
	for _, fd := range fieldsData {
		if fd.GetIsDynamic() {
			if p.wholeDynamic {
				removeDynamicKeys(fd, aliasNames)
			}
			out = append(out, fd)
			continue
		}
		aliases := bySource[fd.GetFieldName()]
		if len(aliases) == 0 {
			out = append(out, fd)
			continue
		}
		if p.keepSource[fd.GetFieldName()] {
			out = append(out, fd)
		}
		for _, alias := range aliases {
			out = append(out, &schemapb.FieldData{
				Type:      fd.GetType(),
				FieldName: alias,
				Field:     fd.GetField(),
				FieldId:   fd.GetFieldId(),
				ValidData: fd.GetValidData(),
			})
		}
	}
	return out
}

// removeDynamicKeys deletes the given top-level keys from every row of a
// dynamic-field column. A row is rewritten only when its bytes can contain
// one of the keys: the quoted name appears literally, or the row has an
// escape sequence, which can spell a key without the literal bytes.
func removeDynamicKeys(fd *schemapb.FieldData, keys []string) {
	rows := fd.GetScalars().GetJsonData().GetData()
	if len(rows) == 0 || len(keys) == 0 {
		return
	}
	quoted := make([][]byte, len(keys))
	keySet := make(map[string]struct{}, len(keys))
	for i, key := range keys {
		quoted[i] = []byte(`"` + key + `"`)
		keySet[key] = struct{}{}
	}
	for i, row := range rows {
		candidate := bytes.IndexByte(row, '\\') >= 0
		for j := 0; !candidate && j < len(quoted); j++ {
			candidate = bytes.Contains(row, quoted[j])
		}
		if !candidate {
			continue
		}
		parsed := gjson.ParseBytes(row)
		if !parsed.IsObject() {
			continue
		}
		var sb strings.Builder
		removed := false
		sb.Grow(len(row))
		sb.WriteByte('{')
		first := true
		parsed.ForEach(func(key, value gjson.Result) bool {
			if _, drop := keySet[key.String()]; drop {
				removed = true
				return true
			}
			if !first {
				sb.WriteByte(',')
			}
			first = false
			sb.WriteString(key.Raw)
			sb.WriteByte(':')
			sb.WriteString(value.Raw)
			return true
		})
		sb.WriteByte('}')
		if removed {
			rows[i] = []byte(sb.String())
		}
	}
}

// virtualColumnImplemented gates the virtual columns that the server can
// produce today.
func virtualColumnImplemented(item planparserv2.SelectItem, isSearch bool) bool {
	if item.VirtualArgKind != planparserv2.VirtualColumnNoArg {
		return isSearch
	}
	switch item.VirtualColumn {
	case planparserv2.VirtualColumnScore:
		return isSearch
	case planparserv2.VirtualColumnSegID, planparserv2.VirtualColumnSegOffset:
		return true
	case planparserv2.VirtualColumnDistance:
		return isSearch
	}
	return false
}

func planVirtualColumn(column string) planpb.VirtualColumn {
	switch column {
	case planparserv2.VirtualColumnSegID:
		return planpb.VirtualColumn_VirtualColumnSegmentID
	case planparserv2.VirtualColumnSegOffset:
		return planpb.VirtualColumn_VirtualColumnSegmentOffset
	case planparserv2.VirtualColumnDistance:
		return planpb.VirtualColumn_VirtualColumnDistance
	case planparserv2.VirtualColumnScore:
		return planpb.VirtualColumn_VirtualColumnScore
	}
	return planpb.VirtualColumn_VirtualColumnInvalid
}

// planItems returns the items evaluated on the QueryNode, in the form the
// plan carries them.
func (p *outputProjection) planItems(isSearch bool) []*planpb.ProjectionItem {
	if p == nil {
		return nil
	}
	var items []*planpb.ProjectionItem
	for _, v := range p.virtualItems {
		if v.evaluatedAtProxy(isSearch) {
			continue
		}
		items = append(items, &planpb.ProjectionItem{
			OutputId:      v.id,
			VirtualColumn: planVirtualColumn(v.column),
			DataType:      v.dataType,
		})
	}
	if p.hiddenDistance {
		items = append(items, &planpb.ProjectionItem{
			OutputId:      p.hiddenDistanceID(),
			VirtualColumn: planpb.VirtualColumn_VirtualColumnDistance,
			DataType:      schemapb.DataType_Float,
		})
	}
	return items
}

// hiddenDistanceID is the output ID of the hidden distance column. It is
// numbered after the request's own items.
func (p *outputProjection) hiddenDistanceID() int64 {
	return requestLocalOutputIDStart - int64(len(p.virtualItems))
}

// queryNodeItemIDs returns the output IDs of the items evaluated on the
// QueryNode.
func (p *outputProjection) queryNodeItemIDs(isSearch bool) []int64 {
	if p == nil {
		return nil
	}
	var ids []int64
	for _, v := range p.virtualItems {
		if !v.evaluatedAtProxy(isSearch) {
			ids = append(ids, v.id)
		}
	}
	if p.hiddenDistance {
		ids = append(ids, p.hiddenDistanceID())
	}
	return ids
}

// columnRowCount returns the number of rows of a scalar result column, or -1
// when the column carries no row data the proxy can count here.
func columnRowCount(fd *schemapb.FieldData) int {
	switch data := fd.GetScalars().GetData().(type) {
	case *schemapb.ScalarField_LongData:
		return len(data.LongData.GetData())
	case *schemapb.ScalarField_IntData:
		return len(data.IntData.GetData())
	case *schemapb.ScalarField_FloatData:
		return len(data.FloatData.GetData())
	case *schemapb.ScalarField_DoubleData:
		return len(data.DoubleData.GetData())
	case *schemapb.ScalarField_StringData:
		return len(data.StringData.GetData())
	case *schemapb.ScalarField_BoolData:
		return len(data.BoolData.GetData())
	case *schemapb.ScalarField_JsonData:
		return len(data.JsonData.GetData())
	}
	return -1
}

func emptyVirtualColumn(v *virtualItem) *schemapb.FieldData {
	fd := &schemapb.FieldData{Type: v.dataType, FieldName: v.name, FieldId: v.id}
	if v.dataType == schemapb.DataType_Int64 {
		fd.Field = &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{}},
		}}
	} else {
		fd.Field = &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{}},
		}}
	}
	return fd
}

// completeQueryNodeColumns names the virtual-column columns that came back
// from the QueryNodes and checks that each requested one is present. A column
// may be absent only from a result without rows; it is then added empty. A
// result with rows and a missing virtual column fails the request: the value
// is never replaced by 0 or NULL.
//
// rowCount is the number of result rows when the caller knows it, or -1 to
// derive it from the columns.
func (p *outputProjection) completeQueryNodeColumns(fieldsData []*schemapb.FieldData, isSearch bool, rowCount int) ([]*schemapb.FieldData, error) {
	if p == nil || len(p.virtualItems) == 0 {
		return fieldsData, nil
	}
	present := make(map[int64]bool, len(p.virtualItems))
	hasRows := rowCount > 0
	if p.hiddenDistance {
		fieldsData = lo.Filter(fieldsData, func(fd *schemapb.FieldData, _ int) bool {
			return fd.GetFieldId() != p.hiddenDistanceID()
		})
	}
	for _, fd := range fieldsData {
		if v := p.virtualItemByID(fd.GetFieldId()); v != nil {
			fd.FieldName = v.name
			fd.Type = v.dataType
			fd.IsDynamic = false
			present[v.id] = true
		}
		if columnRowCount(fd) > 0 {
			hasRows = true
		}
	}
	for _, v := range p.virtualItems {
		if v.evaluatedAtProxy(isSearch) || present[v.id] {
			continue
		}
		if hasRows {
			return nil, merr.WrapErrServiceInternalMsg("virtual column %s is missing from the result", v.name)
		}
		fieldsData = append(fieldsData, emptyVirtualColumn(v))
	}
	return fieldsData, nil
}

// virtualColumnRowKey identifies one result row inside a Search response:
// the query it answers, its primary key, and for element-level results the
// element index.
type virtualColumnRowKey struct {
	query        int
	pk           any
	elementLevel bool
	elementIndex int64
}

// searchRowKeys returns the key of every row of a Search result, in row
// order. The rows of query i are the next Topks[i] rows.
func searchRowKeys(data *schemapb.SearchResultData, elementLevel bool) []virtualColumnRowKey {
	total := typeutil.GetSizeOfIDs(data.GetIds())
	keys := make([]virtualColumnRowKey, 0, total)
	elementIndices := data.GetElementIndices().GetData()
	row := 0
	appendRow := func(query int) {
		key := virtualColumnRowKey{query: query, pk: typeutil.GetPK(data.GetIds(), int64(row))}
		if elementLevel && row < len(elementIndices) {
			key.elementLevel = true
			key.elementIndex = elementIndices[row]
		}
		keys = append(keys, key)
		row++
	}
	for query, topk := range data.GetTopks() {
		for i := int64(0); i < topk && row < total; i++ {
			appendRow(query)
		}
	}
	// A result without Topks holds the rows of a single query.
	for row < total {
		appendRow(0)
	}
	return keys
}

// virtualColumnStash keeps the virtual columns of the reduced Search results
// until the end of the pipeline. The pipeline nodes after the reduce replace
// fields_data: a requery overwrites it with the result of a second Query by
// primary key, which cannot produce these columns (the row may have moved to
// another segment since the Search, and a Query has no distance).
type virtualColumnStash struct {
	columnIDs  []int64
	subResults []stashedSubResult
}

// stashedSubResult is one reduced result: one per Search, one per
// sub-request in hybrid search.
type stashedSubResult struct {
	// rows maps a row key to its row index. An element-level result is
	// indexed twice: by (primary key, element index) and, for the first
	// element of each primary key, by primary key alone.
	rows    map[virtualColumnRowKey]int
	columns map[int64]*schemapb.FieldData
	// scores are the values the sub-request ranked its rows by, in the
	// metric's natural direction unless rewritten is set. ranks holds each
	// row's 1-based position inside its query's result.
	scores    []float32
	ranks     []int64
	rewritten bool
}

// newVirtualColumnStash returns nil when the request needs no stash: it has
// no virtual column computed on the QueryNodes and no per-sub-request column.
func newVirtualColumnStash(columnIDs []int64, subRequestColumns bool) *virtualColumnStash {
	if len(columnIDs) == 0 && !subRequestColumns {
		return nil
	}
	return &virtualColumnStash{columnIDs: columnIDs}
}

// capture records the virtual columns of the reduced results. It is called
// by the reduce operators, before any later node changes the results.
func (s *virtualColumnStash) capture(reduced []*milvuspb.SearchResults, scoreInfos []searchScoreInfo) {
	if s == nil {
		return
	}
	s.subResults = make([]stashedSubResult, len(reduced))
	for r, sub := range reduced {
		data := sub.GetResults()
		stashed := stashedSubResult{
			rows:    make(map[virtualColumnRowKey]int),
			columns: make(map[int64]*schemapb.FieldData),
			scores:  append(make([]float32, 0, len(data.GetScores())), data.GetScores()...),
			ranks:   make([]int64, 0, len(data.GetScores())),
		}
		if r < len(scoreInfos) {
			stashed.rewritten = scoreInfos[r].Rewritten
		}
		for _, topk := range data.GetTopks() {
			for rank := int64(1); rank <= topk; rank++ {
				stashed.ranks = append(stashed.ranks, rank)
			}
		}
		for _, fd := range data.GetFieldsData() {
			if lo.Contains(s.columnIDs, fd.GetFieldId()) {
				stashed.columns[fd.GetFieldId()] = fd
			}
		}
		elementLevel := data.GetElementIndices() != nil
		for row, key := range searchRowKeys(data, elementLevel) {
			if _, ok := stashed.rows[key]; !ok {
				stashed.rows[key] = row
			}
			if elementLevel {
				rowKey := virtualColumnRowKey{query: key.query, pk: key.pk}
				if _, ok := stashed.rows[rowKey]; !ok {
					stashed.rows[rowKey] = row
				}
			}
		}
		s.subResults[r] = stashed
	}
}

// carry adds to result every stashed virtual column that it does not hold,
// reading the values by row key. A row is taken from the first reduced result
// that has both the column and the row; a row found in none fails the
// request.
func (s *virtualColumnStash) carry(result *schemapb.SearchResultData) error {
	if s == nil || result == nil || typeutil.GetSizeOfIDs(result.GetIds()) == 0 {
		return nil
	}
	present := make(map[int64]bool, len(result.GetFieldsData()))
	for _, fd := range result.GetFieldsData() {
		present[fd.GetFieldId()] = true
	}
	rowKeys := searchRowKeys(result, result.GetElementIndices() != nil)
	for _, id := range s.columnIDs {
		if present[id] {
			continue
		}
		var template *schemapb.FieldData
		for _, sub := range s.subResults {
			if fd := sub.columns[id]; fd != nil {
				template = fd
				break
			}
		}
		if template == nil {
			// No reduced result carries the column; the presence check that
			// follows the pipeline reports it.
			continue
		}
		locate := func(i int, key virtualColumnRowKey) (*schemapb.FieldData, int, error) {
			for _, sub := range s.subResults {
				fd := sub.columns[id]
				if fd == nil {
					continue
				}
				if row, ok := sub.rows[key]; ok {
					return fd, row, nil
				}
			}
			return nil, 0, merr.WrapErrServiceInternalMsg("virtual column %d: result row %d is not in the reduced search result", id, i)
		}
		out := &schemapb.FieldData{Type: template.GetType(), FieldId: id}
		switch template.GetScalars().GetData().(type) {
		case *schemapb.ScalarField_LongData:
			values := make([]int64, len(rowKeys))
			for i, key := range rowKeys {
				fd, row, err := locate(i, key)
				if err != nil {
					return err
				}
				values[i] = fd.GetScalars().GetLongData().GetData()[row]
			}
			out.Field = &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: values}},
			}}
		case *schemapb.ScalarField_FloatData:
			values := make([]float32, len(rowKeys))
			for i, key := range rowKeys {
				fd, row, err := locate(i, key)
				if err != nil {
					return err
				}
				values[i] = fd.GetScalars().GetFloatData().GetData()[row]
			}
			out.Field = &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: values}},
			}}
		default:
			return merr.WrapErrServiceInternalMsg("virtual column %d has an unexpected column type %s", id, template.GetType().String())
		}
		result.FieldsData = append(result.FieldsData, out)
	}
	return nil
}

// overwriteScoresWithDistance implements "$distance AS distance": the scores
// of the result, which every client shows as the built-in similarity value of
// a hit, are replaced by the raw distance column. The column itself stays in
// fields_data.
func (p *outputProjection) overwriteScoresWithDistance(result *schemapb.SearchResultData) {
	if p == nil || result == nil {
		return
	}
	for _, v := range p.virtualItems {
		if v.column != planparserv2.VirtualColumnDistance || v.argKind != planparserv2.VirtualColumnNoArg || v.name != searchBuiltinDistanceName {
			continue
		}
		for _, fd := range result.GetFieldsData() {
			if fd.GetFieldId() != v.id {
				continue
			}
			distances := fd.GetScalars().GetFloatData().GetData()
			if len(distances) == len(result.GetScores()) {
				result.Scores = append(make([]float32, 0, len(distances)), distances...)
			}
			return
		}
	}
}

// roundVirtualFloatColumns applies round_decimal to the Float virtual columns
// that came from the QueryNodes, as the end of the pipeline does for Scores.
func roundVirtualFloatColumns(result *schemapb.SearchResultData, columnIDs []int64, roundDecimal int64) {
	if result == nil || roundDecimal < 0 || len(columnIDs) == 0 {
		return
	}
	for _, fd := range result.GetFieldsData() {
		if !lo.Contains(columnIDs, fd.GetFieldId()) {
			continue
		}
		values := fd.GetScalars().GetFloatData().GetData()
		for i, value := range values {
			values[i] = roundScore(value, roundDecimal)
		}
	}
}

// hasSubRequestColumns reports whether the request has an argument form such
// as $score(x).
func (p *outputProjection) hasSubRequestColumns() bool {
	if p == nil {
		return false
	}
	for _, v := range p.virtualItems {
		if v.argKind != planparserv2.VirtualColumnNoArg {
			return true
		}
	}
	return false
}

// subRequestTarget describes one hybrid sub-request for argument resolution.
type subRequestTarget struct {
	annsField string
	isBM25    bool
}

// resolveSubRequests binds the argument of every argument form to a hybrid
// sub-request: a vector field name, which must be searched by exactly one
// sub-request, or a zero-based position.
func (p *outputProjection) resolveSubRequests(targets []subRequestTarget) error {
	if p == nil {
		return nil
	}
	for _, v := range p.virtualItems {
		switch v.argKind {
		case planparserv2.VirtualColumnNoArg:
			continue
		case planparserv2.VirtualColumnArgIndex:
			if v.argIndex >= int64(len(targets)) {
				return merr.WrapErrParameterInvalidMsg("virtual column %s: the request has %d sub-requests, positions start at 0", v.text(), len(targets))
			}
			v.subRequest = int(v.argIndex)
		case planparserv2.VirtualColumnArgField:
			match := -1
			for i, target := range targets {
				if target.annsField != v.argField {
					continue
				}
				if match >= 0 {
					return merr.WrapErrParameterInvalidMsg("virtual column %s: more than one sub-request searches field %s; select one by position, for example %s(%d)",
						v.text(), v.argField, v.column, match)
				}
				match = i
			}
			if match < 0 {
				return merr.WrapErrParameterInvalidMsg("virtual column %s: no sub-request searches field %s", v.text(), v.argField)
			}
			v.subRequest = match
		}
		if v.column == planparserv2.VirtualColumnDistance && targets[v.subRequest].isBM25 {
			return merr.WrapErrParameterInvalidMsg("virtual column %s is not available: sub-request %d is a full-text search, and BM25 is a relevance score, not a vector distance; use $score(%d)",
				v.text(), v.subRequest, v.subRequest)
		}
	}
	return nil
}

// resolveSingleSearchArguments handles argument forms in a Search with one
// vector field. $distance(x) is accepted when x is that field or position 0
// and then means $distance; the other argument forms need sub-requests.
func (p *outputProjection) resolveSingleSearchArguments(annsField string) error {
	if p == nil {
		return nil
	}
	for _, v := range p.virtualItems {
		if v.argKind == planparserv2.VirtualColumnNoArg {
			continue
		}
		if v.column != planparserv2.VirtualColumnDistance {
			return merr.WrapErrParameterInvalidMsg("virtual column %s is only available in hybrid search", v.text())
		}
		matches := (v.argKind == planparserv2.VirtualColumnArgIndex && v.argIndex == 0) ||
			(v.argKind == planparserv2.VirtualColumnArgField && v.argField == annsField)
		if !matches {
			return merr.WrapErrParameterInvalidMsg("virtual column %s: the request searches field %s only; use $distance, $distance(%s) or $distance(0)", v.text(), annsField, annsField)
		}
		v.argKind = planparserv2.VirtualColumnNoArg
	}
	// One vector field: the request's own $distance items carry the value.
	p.hiddenDistance = false
	return nil
}

// appendSubRequestColumns adds one nullable column per argument form of a
// hybrid search. A row that sub-request x did not return has NULL: the value
// is the one the sub-search produced and the fusion consumed, so there is
// none to report for such a row.
func (p *outputProjection) appendSubRequestColumns(result *schemapb.SearchResultData, stash *virtualColumnStash) error {
	if !p.hasSubRequestColumns() || result == nil {
		return nil
	}
	rowKeys := searchRowKeys(result, result.GetElementIndices() != nil)
	for _, v := range p.virtualItems {
		if v.argKind == planparserv2.VirtualColumnNoArg {
			continue
		}
		if stash == nil || v.subRequest >= len(stash.subResults) {
			if len(rowKeys) == 0 {
				result.FieldsData = append(result.FieldsData, emptyVirtualColumn(v))
				continue
			}
			return merr.WrapErrServiceInternalMsg("virtual column %s: sub-request %d has no reduced result", v.name, v.subRequest)
		}
		sub := stash.subResults[v.subRequest]
		valid := make([]bool, len(rowKeys))
		fd := &schemapb.FieldData{Type: v.dataType, FieldName: v.name, FieldId: v.id, ValidData: valid}
		if v.column == planparserv2.VirtualColumnRank {
			values := make([]int64, len(rowKeys))
			for i, key := range rowKeys {
				if row, ok := sub.rows[key]; ok && row < len(sub.ranks) {
					values[i], valid[i] = sub.ranks[row], true
				}
			}
			fd.Field = &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: values}},
			}}
		} else {
			source := sub.scores
			if v.column == planparserv2.VirtualColumnDistance && sub.rewritten {
				// The sub-request's scores were rewritten on the QueryNode;
				// its raw distance travels in the hidden column.
				hidden := sub.columns[p.hiddenDistanceID()]
				if hidden == nil && len(sub.scores) > 0 {
					return merr.WrapErrServiceInternalMsg("virtual column %s: sub-request %d returned no raw distance", v.name, v.subRequest)
				}
				source = hidden.GetScalars().GetFloatData().GetData()
			}
			values := make([]float32, len(rowKeys))
			for i, key := range rowKeys {
				if row, ok := sub.rows[key]; ok && row < len(source) {
					values[i], valid[i] = source[row], true
				}
			}
			fd.Field = &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: values}},
			}}
		}
		result.FieldsData = append(result.FieldsData, fd)
	}
	return nil
}
