package materialize

import (
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"

	"github.com/milvus-io/milvus/internal/util/arrowconv"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Schema is a parsed Materialize batch schema: the Schema message, the Arrow
// schema it encodes, and the layout of a batch -- the pk column, the system
// columns and the user columns with their field ids -- worked out once, so
// that reading a body costs no schema parse and no per-batch schema
// construction (design doc 20261019 §3, §7). A Schema is immutable and safe
// for concurrent use.
type Schema struct {
	arrow  *arrow.Schema
	sysIDs []int64
	tsCol  int // index into sysIDs of the timestamp column, or -1
	pkType arrow.Type

	// The user columns' reading (gather.go), from the worker's retrieve
	// metadata; orderErr when it is unusable, reported once rows are gathered.
	order     []int64 // milvus.field_order: every result column's field id
	userIDs   []int64 // the user columns' field ids, in column order
	validData map[int64]struct{}
	orderErr  error
}

// ParseSchema parses an encapsulated Schema message from EncodeBatch and
// checks that it has a Materialize batch layout the reader can read. Any
// failure is ServiceInternal: the message comes from another Milvus node.
func ParseSchema(schemaMsg []byte) (*Schema, error) {
	full, err := readSchema(schemaMsg)
	if err != nil {
		return nil, err
	}
	if full.NumFields() < 1 || full.Field(0).Name != pkColumnName {
		return nil, merr.WrapErrServiceInternalMsg("materialize: batch schema has no pk column")
	}
	if !full.IsNativeEndian() {
		return nil, merr.WrapErrServiceInternalMsg("materialize: batch schema is not native-endian")
	}
	for i, f := range full.Fields() {
		if _, _, ok := colLayoutOf(f.Type); !ok {
			return nil, merr.WrapErrServiceInternalMsg("materialize: batch column %d (%s) has type %s, which the reader does not read", i, f.Name, f.Type)
		}
	}
	s := &Schema{arrow: full, tsCol: -1}
	switch pk := full.Field(0).Type.ID(); pk {
	case arrow.INT64, arrow.STRING:
		s.pkType = pk
	default:
		return nil, merr.WrapErrServiceInternalMsg("materialize: pk column is %s", full.Field(0).Type)
	}
	// The system columns are the $sys:<field id> columns right after the pk.
	for i := 1; i < full.NumFields(); i++ {
		f := full.Field(i)
		name, ok := strings.CutPrefix(f.Name, sysColumnName)
		if !ok {
			break
		}
		id, err := strconv.ParseInt(name, 10, 64)
		if err != nil {
			return nil, merr.WrapErrServiceInternalMsg("materialize: bad system column name %q", f.Name)
		}
		if f.Type.ID() != arrow.INT64 {
			return nil, merr.WrapErrServiceInternalMsg("materialize: system column %d is %s, not int64", id, f.Type)
		}
		if id == common.TimeStampField {
			s.tsCol = len(s.sysIDs)
		}
		s.sysIDs = append(s.sysIDs, id)
	}
	s.parseRetrieveMetadata()
	return s, nil
}

// parseRetrieveMetadata reads the worker's retrieve metadata as
// queryutil.MaterializeArrowSelection does: milvus.field_order lists every
// result column, the system ones included; the user columns are the rest, in
// column order; milvus.valid_data_fields lists the columns whose source
// carried a validity bitmap.
func (s *Schema) parseRetrieveMetadata() {
	md := s.arrow.Metadata()
	order, err := arrowconv.FieldOrderOf(md)
	if err != nil {
		s.orderErr = err
		return
	}
	seen := make(map[int64]bool, len(order))
	for _, id := range order {
		// Duplicate ids mean the columns are identified positionally (an
		// aggregation result), which this reassembly cannot do.
		if seen[id] {
			s.orderErr = merr.WrapErrServiceInternalMsg("materialize: field order has duplicate field id %d", id)
			return
		}
		seen[id] = true
	}
	sys := make(map[int64]bool, len(s.sysIDs))
	for _, id := range s.sysIDs {
		sys[id] = true
	}
	for _, id := range order {
		if !sys[id] {
			s.userIDs = append(s.userIDs, id)
		}
	}
	if len(s.userIDs) != s.arrow.NumFields()-s.lead() {
		s.orderErr = merr.WrapErrServiceInternalMsg("materialize: batch has %d user columns but %d user field ids",
			s.arrow.NumFields()-s.lead(), len(s.userIDs))
		return
	}
	valid, err := arrowconv.ValidDataFieldsOf(md)
	if err != nil {
		s.orderErr = err
		return
	}
	s.order = order
	s.validData = valid
}

// StringPK reports whether the pk column is utf8 rather than int64.
func (s *Schema) StringPK() bool { return s.pkType == arrow.STRING }

func (s *Schema) lead() int { return 1 + len(s.sysIDs) }

// Batch is one body entry, read (reader.go): its pk and system columns
// copied out, its user columns left in the entry. The entry must stay valid
// -- its body unreleased -- until the batch has been gathered; nothing that
// the gather returns points into it.
type Batch struct {
	// Rows is the batch's row count.
	Rows int
	// IntPKs or StrPKs holds the pk column, as Schema.StringPK says; the
	// strings are copies.
	IntPKs []int64
	StrPKs []string
	// Sys holds the system columns' values, in batch order.
	Sys [][]int64

	cols []column // the user columns
}

// Timestamps returns the batch's timestamp column, nil when it has none.
func (b *Batch) Timestamps(s *Schema) []int64 {
	if s.tsCol < 0 {
		return nil
	}
	return b.Sys[s.tsCol]
}

// RowSize is what Query's maxOutputSize guard charges for row on the
// delegator's Arrow path (colResultSize, sysResultSize). The pk column is
// not charged: Query does not charge the ids, and a pk the request outputs
// is also a user column.
func (b *Batch) RowSize(row int) int64 {
	size := sysResultSize(len(b.Sys), 1)
	for i := range b.cols {
		c := &b.cols[i]
		nulls, varBytes := 0, 0
		if !c.isValid(row) {
			nulls = 1
		} else if c.offsets != nil {
			varBytes = int(c.offsets[row+1] - c.offsets[row])
		}
		size += colResultSize(c.typ, c.width, 1, nulls, varBytes)
	}
	return size
}

// ReadEntry reads one body entry, held as parts (resource.MaterializeBody),
// with s. idx names the entry in errors. rawSize is the reply's
// body_raw_size, which bounds what a compressed var-len buffer may inflate
// to; 0 when unknown. On error nothing needs releasing. The Batch reads the
// entry (see reader.go): do not use it after the body is released.
func (s *Schema) ReadEntry(parts [][]byte, idx int, rawSize int64) (*Batch, error) {
	cols, rows, err := readColumns(s.arrow, parts, idx, rawSize)
	if err != nil {
		return nil, err
	}
	b := &Batch{Rows: rows, Sys: make([][]int64, len(s.sysIDs)), cols: cols[s.lead():]}
	for i := 0; i < s.lead(); i++ {
		if cols[i].nulls > 0 {
			return nil, merr.WrapErrServiceInternalMsg("materialize: arrow message %d: header column %d has %d nulls", idx, i, cols[i].nulls)
		}
	}
	if s.StringPK() {
		b.StrPKs = strs(&cols[0])
	} else {
		b.IntPKs = int64s(&cols[0])
	}
	for i := range s.sysIDs {
		b.Sys[i] = int64s(&cols[1+i])
	}
	return b, nil
}
