package materialize

import (
	"encoding/binary"
	"math"
	"strconv"
	"sync"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/bitutil"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/cespare/xxhash/v2"
	flatbuffers "github.com/google/flatbuffers/go"
	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/materialize/internal/flatbuf"
	"github.com/milvus-io/milvus/internal/util/queryutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// The fused writer is BuildBatch followed by EncodeBatch(CodecNone) in one
// pass (design doc 20261019 §3). Its output is byte-identical to the
// two-step path (fused_test.go), which stays for zstd and for whatever the
// fused writer declines (encodeFused).
//
// Per layout it keeps the RecordBatch metadata of a one-row batch encoded by
// the stock writer and overwrites its data-dependent slots. That is sound
// because every such value sits in a fixed-size scalar or struct, and none
// is ever the FlatBuffers default 0 that the builder would omit (rows >= 1,
// and the pk column alone makes the body non-empty).

// Encoded is a worker's Materialize reply body: one batch.
type Encoded struct {
	// SchemaMsg is the batch's encapsulated Schema message, shared per
	// layout: callers must not modify it.
	SchemaMsg []byte
	// Entry is the encapsulated RecordBatch message; nil when the header
	// holds no rows.
	Entry []byte
	// RawSize is RawSize of the batch BuildBatch would have built.
	RawSize int64
	// ResultSize is what Query's maxOutputSize guard charges for the rows:
	// the sum of Batch.RowSize over the batch.
	ResultSize int64
}

// Encode builds and encodes the worker's batch with the codec the
// configuration and the reply call for: ReplyCodec of ResolveCodec.
func Encode(header *segcorepb.RetrieveResults, sel *queryutil.ArrowSelection) (Encoded, error) {
	return encodeWith(header, sel, ReplyCodec(header, sel, ResolveCodec()))
}

// encodeWith builds and encodes the batch with codec: BuildBatch then
// EncodeBatch, fused into one pass when codec is none and the fused writer
// supports the batch. The result is the same either way.
func encodeWith(header *segcorepb.RetrieveResults, sel *queryutil.ArrowSelection, codec Codec) (Encoded, error) {
	if codec == CodecNone {
		if enc, ok, err := encodeFused(header, sel); ok || err != nil {
			return enc, err
		}
	}
	rec, err := BuildBatch(header, sel)
	if err != nil || rec == nil {
		return Encoded{}, err
	}
	defer rec.Release()
	schemaMsg, entry, err := EncodeBatch(rec, codec)
	if err != nil {
		return Encoded{}, err
	}
	return Encoded{
		SchemaMsg:  schemaMsg,
		Entry:      entry,
		RawSize:    RawSize(rec),
		ResultSize: resultSize(rec, 1+len(header.GetFieldsData())),
	}, nil
}

// resultSize is the sum of Batch.RowSize over rec, whose first lead columns
// are the pk and the system columns. rec is BuildBatch's: its user columns
// were gathered with builders, which give a null value no bytes, so a
// var-len column's non-null bytes are its offsets' span, with no pass over
// its rows.
func resultSize(rec arrow.Record, lead int) int64 {
	rows := int(rec.NumRows())
	size := sysResultSize(lead-1, rows)
	for _, col := range rec.Columns()[lead:] {
		kind, width, _ := colLayoutOf(col.DataType())
		varBytes := 0
		if kind == kindVarLen {
			if offs := varLenOffsets(col); len(offs) > rows {
				varBytes = int(offs[rows] - offs[0])
			}
		}
		size += colResultSize(col.DataType().ID(), width, rows, col.NullN(), varBytes)
	}
	return size
}

// varLenOffsets is a utf8 or binary array's value offsets, nil otherwise.
func varLenOffsets(col arrow.Array) []int32 {
	switch a := col.(type) {
	case *array.String:
		return a.ValueOffsets()
	case *array.Binary:
		return a.ValueOffsets()
	}
	return nil
}

// ReplyCodec is the codec a worker encodes one reply with when configured
// with codec (design doc 20261019 §3). Under zstd a batch that carries a
// vector column, or whose uncompressed body is under
// queryNode.materialize.zstdMinBodySize, goes raw through the fused writer:
// vector buffers barely compress (<10%), and a small reply costs more in
// per-frame CPU than it saves in bytes. A size of 0 turns both rules off, for
// a cluster that pays for bytes more than for CPU. The reader takes either per
// reply.
//
// On BenchmarkMaterializePipeline's text shapes (N = 8; design doc 20261019,
// Impact) zstd costs the worker 70-140 µs per reply of 2-60 KB, plus the
// proxy's decode, and saves 1.6-2.9x in bytes. The default of 16 KB caps what
// a raw reply gives up at about 10 KB, while 64 KB would send typical
// topK-1000 text replies raw at 2.9x the bytes, and 4 KB would leave most of
// the CPU in place.
func ReplyCodec(header *segcorepb.RetrieveResults, sel *queryutil.ArrowSelection, codec Codec) Codec {
	tpl := sel.Template()
	if codec != CodecZstd || tpl == nil {
		return codec
	}
	zstdMinBody := paramtable.Get().QueryNodeCfg.MaterializeZstdMinBodySize.GetAsInt()
	if zstdMinBody <= 0 {
		return codec
	}
	rows := len(sel.Rows)
	// pk and system columns, int64 each (a varchar pk adds its bytes below).
	n := 8 * rows * (1 + len(header.GetFieldsData()))
	var varCols []int
	for c, f := range tpl.Schema().Fields() {
		if f.Type.ID() == arrow.FIXED_SIZE_BINARY {
			// Every vector type the export carries (carried).
			return CodecNone
		}
		switch kind, width, _ := colLayoutOf(f.Type); kind {
		case kindFixed:
			n += width * rows
		case kindBool:
			n += int(bitutil.BytesForBits(int64(rows)))
		case kindVarLen:
			n += 4 * (rows + 1)
			varCols = append(varCols, c)
		}
	}
	for _, s := range header.GetIds().GetStrId().GetData() {
		if n >= zstdMinBody {
			return codec
		}
		n += len(s)
	}
	for i := 0; i < rows && n < zstdMinBody; i++ {
		ri, row := sel.Row(i)
		if ri < 0 || ri >= len(sel.Records) || sel.Records[ri] == nil || row < 0 || row >= sel.Records[ri].NumRows() ||
			sel.Records[ri].NumCols() != tpl.NumCols() {
			// Encode reports the bad selection.
			return codec
		}
		for _, c := range varCols {
			if v, ok := sel.Records[ri].Column(c).(interface{ ValueLen(int) int }); ok {
				n += v.ValueLen(int(row))
			}
		}
	}
	if n < zstdMinBody {
		return CodecNone
	}
	return codec
}

// userCol is one user column of a layout.
type userCol struct {
	kind  colKind
	id    arrow.Type
	width int // kindFixed: bytes per value
}

// userColOf returns the layout of an Arrow type the fused writer gathers
// (colLayoutOf); anything else goes to the two-step path.
func userColOf(dt arrow.DataType) (userCol, bool) {
	kind, width, ok := colLayoutOf(dt)
	return userCol{kind: kind, id: dt.ID(), width: width}, ok
}

// sameType reports whether dt has c's physical layout.
func (c userCol) sameType(dt arrow.DataType) bool {
	if dt.ID() != c.id {
		return false
	}
	if c.id == arrow.FIXED_SIZE_BINARY {
		return dt.(*arrow.FixedSizeBinaryType).ByteWidth == c.width
	}
	return true
}

// fusedLayout is one batch layout with its encoding templates.
type fusedLayout struct {
	// What the layout was built from, to confirm a key hit.
	pkType arrow.Type
	sysIDs []int64
	fields []arrow.Field
	md     arrow.Metadata

	cols      []userCol
	schemaMsg []byte
	// prefix is the RecordBatch message up to its body: continuation, metadata
	// length, metadata, padding. The positions below index into it.
	prefix     []byte
	bodyLenPos int
	lengthPos  int
	nodePos    []int // per column: its FieldNode
	bufPos     []int // per buffer, in IPC order: its Buffer
}

// fusedLayoutCacheSize bounds the layouts kept, as batchLayoutCacheSize.
const fusedLayoutCacheSize = 256

var fusedLayouts = func() *lru.Cache[uint64, *fusedLayout] {
	c, err := lru.New[uint64, *fusedLayout](fusedLayoutCacheSize)
	if err != nil {
		panic(err)
	}
	return c
}()

// unsupportedLayout marks a layout the fused writer declines.
var unsupportedLayout = &fusedLayout{}

// layoutHash hashes everything the Schema message encodes for the batch:
// pk type, system field ids, every template field's name, nullability,
// physical type and metadata, and the template's metadata. It allocates
// nothing. ok is false when a field has a type the fused writer does not
// gather.
func layoutHash(pkType arrow.Type, sys []*schemapb.FieldData, schema *arrow.Schema) (uint64, bool) {
	var d xxhash.Digest
	d.Reset()
	var b [8]byte
	putInt := func(v int64) {
		binary.LittleEndian.PutUint64(b[:], uint64(v))
		_, _ = d.Write(b[:])
	}
	putStr := func(s string) {
		putInt(int64(len(s)))
		_, _ = d.WriteString(s)
	}
	putMD := func(md arrow.Metadata) {
		keys, vals := md.Keys(), md.Values()
		putInt(int64(len(keys)))
		for i := range keys {
			putStr(keys[i])
			putStr(vals[i])
		}
	}
	putInt(int64(pkType))
	putInt(int64(len(sys)))
	for _, fd := range sys {
		putInt(fd.GetFieldId())
	}
	putInt(int64(schema.NumFields()))
	for i := 0; i < schema.NumFields(); i++ {
		f := schema.Field(i)
		c, ok := userColOf(f.Type)
		if !ok {
			return 0, false
		}
		putStr(f.Name)
		if f.Nullable {
			putInt(1)
		} else {
			putInt(0)
		}
		putInt(int64(c.id))
		putInt(int64(c.width))
		putMD(f.Metadata)
	}
	putMD(schema.Metadata())
	return d.Sum64(), true
}

func metadataEqual(a, b arrow.Metadata) bool {
	ak, av, bk, bv := a.Keys(), a.Values(), b.Keys(), b.Values()
	if len(ak) != len(bk) {
		return false
	}
	for i := range ak {
		if ak[i] != bk[i] || av[i] != bv[i] {
			return false
		}
	}
	return true
}

// matches reports whether l was built for exactly this layout.
func (l *fusedLayout) matches(pkType arrow.Type, sys []*schemapb.FieldData, schema *arrow.Schema) bool {
	if l.pkType != pkType || len(l.sysIDs) != len(sys) || len(l.fields) != schema.NumFields() {
		return false
	}
	for i, fd := range sys {
		if l.sysIDs[i] != fd.GetFieldId() {
			return false
		}
	}
	for i := range l.fields {
		f, g := &l.fields[i], schema.Field(i)
		if f.Name != g.Name || f.Nullable != g.Nullable || !l.cols[i].sameType(g.Type) ||
			!metadataEqual(f.Metadata, g.Metadata) {
			return false
		}
	}
	return metadataEqual(l.md, schema.Metadata())
}

// lookupLayout returns the layout for the batch, building it on a miss; nil
// when the fused writer declines it.
func lookupLayout(pkType arrow.Type, sys []*schemapb.FieldData, schema *arrow.Schema) *fusedLayout {
	key, ok := layoutHash(pkType, sys, schema)
	if !ok {
		return nil
	}
	if l, ok := fusedLayouts.Get(key); ok {
		if l != unsupportedLayout && l.matches(pkType, sys, schema) {
			return l
		}
		return nil // declined, or a hash collision: the two-step path is always right
	}
	l, err := newFusedLayout(pkType, sys, schema)
	if err != nil {
		// Not an error for the request: the two-step path encodes it, and
		// reports whatever is really wrong.
		l = unsupportedLayout
	}
	fusedLayouts.Add(key, l)
	if l == unsupportedLayout {
		return nil
	}
	return l
}

// newFusedLayout encodes a one-row batch of the layout with the stock writer
// (through BuildBatch's schema and EncodeBatch) and records where the
// data-dependent values sit in its metadata.
func newFusedLayout(pkType arrow.Type, sys []*schemapb.FieldData, schema *arrow.Schema) (*fusedLayout, error) {
	l := &fusedLayout{pkType: pkType, md: schema.Metadata()}
	nbuf := 2 // pk int64: validity, values
	pkDT := pkArrowType(pkType)
	if pkType == arrow.STRING {
		nbuf = 3
	}
	fields := make([]arrow.Field, 0, 1+len(sys)+schema.NumFields())
	fields = append(fields, arrow.Field{Name: pkColumnName, Type: pkDT})
	for _, fd := range sys {
		l.sysIDs = append(l.sysIDs, fd.GetFieldId())
		fields = append(fields, arrow.Field{Name: sysColumnName + strconv.FormatInt(fd.GetFieldId(), 10), Type: arrow.PrimitiveTypes.Int64})
		nbuf += 2
	}
	for _, f := range schema.Fields() {
		c, ok := userColOf(f.Type)
		if !ok {
			return nil, merr.WrapErrServiceInternalMsg("materialize: fused writer does not gather %s", f.Type)
		}
		l.cols = append(l.cols, c)
		l.fields = append(l.fields, f)
		fields = append(fields, f)
		nbuf += bodyBuffers(c.kind)
	}
	full := batchLayouts.schemaFor(fields, l.md)

	cols := make([]arrow.Array, len(fields))
	for i, f := range fields {
		cols[i] = array.MakeArrayOfNull(memory.DefaultAllocator, f.Type, 1)
	}
	rec := array.NewRecord(full, cols, 1)
	for _, c := range cols {
		c.Release()
	}
	defer rec.Release()
	schemaMsg, batch, err := EncodeBatch(rec, CodecNone)
	if err != nil {
		return nil, err
	}
	l.schemaMsg = schemaMsg
	if err := l.parseTemplate(batch, len(fields), nbuf); err != nil {
		return nil, err
	}
	return l, nil
}

func pkArrowType(t arrow.Type) arrow.DataType {
	if t == arrow.STRING {
		return arrow.BinaryTypes.String
	}
	return arrow.PrimitiveTypes.Int64
}

// parseTemplate keeps the RecordBatch message's prefix and finds its
// data-dependent slots.
func (l *fusedLayout) parseTemplate(batch []byte, ncols, nbuf int) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = merr.WrapErrServiceInternalMsg("materialize: fused template: corrupt metadata: %v", p)
		}
	}()
	if len(batch) < 8 {
		return merr.WrapErrServiceInternalMsg("materialize: fused template is %d bytes", len(batch))
	}
	prefix := 8 + int(binary.LittleEndian.Uint32(batch[4:]))
	if prefix > len(batch) || prefix%ipcAlignment != 0 {
		return merr.WrapErrServiceInternalMsg("materialize: fused template has metadata length %d", prefix-8)
	}
	meta := batch[8:prefix]
	msg := flatbuf.GetRootAsMessage(meta, 0)
	var tab flatbuffers.Table
	if msg.HeaderType() != flatbuf.MessageHeaderRecordBatch || !msg.Header(&tab) {
		return merr.WrapErrServiceInternalMsg("materialize: fused template is not a record batch")
	}
	var rb flatbuf.RecordBatch
	rb.Init(tab.Bytes, tab.Pos)
	mt, rt := msg.Table(), rb.Table()
	bodyLenOff, lengthOff := mt.Offset(10), rt.Offset(4)
	if bodyLenOff == 0 || lengthOff == 0 || rb.Compression(nil) != nil || rb.VariadicBufferCountsLength() != 0 ||
		rb.NodesLength() != ncols || rb.BuffersLength() != nbuf {
		return merr.WrapErrServiceInternalMsg("materialize: fused template has an unexpected record batch layout")
	}
	l.prefix = batch[:prefix:prefix]
	l.bodyLenPos = 8 + int(mt.Pos) + int(bodyLenOff)
	l.lengthPos = 8 + int(rt.Pos) + int(lengthOff)
	l.nodePos = make([]int, ncols)
	var node flatbuf.FieldNode
	for j := range l.nodePos {
		rb.Nodes(&node, j)
		l.nodePos[j] = 8 + int(node.Table().Pos)
	}
	l.bufPos = make([]int, nbuf)
	var buf flatbuf.Buffer
	for j := range l.bufPos {
		rb.Buffers(&buf, j)
		l.bufPos[j] = 8 + int(buf.Table().Pos)
	}
	// Each scalar slot is an int64; each FieldNode and Buffer two.
	end := max(l.bodyLenPos, l.lengthPos) + 8
	for _, p := range append(append([]int{}, l.nodePos...), l.bufPos...) {
		end = max(end, p+16)
	}
	if end > prefix {
		return merr.WrapErrServiceInternalMsg("materialize: fused template slot ends at %d, past the metadata", end)
	}
	return nil
}

// colSource is one record's column, resolved once per call.
type colSource struct {
	values  []byte  // kindFixed: offset-adjusted; otherwise the whole buffer
	offsets []int32 // kindVarLen: the array's offsets, from its own offset
	valid   []byte  // validity bitmap, nil when the gather ignores it
	bitOff  int     // the array's offset, for valid and kindBool values
}

// colState is one user column's sizing.
type colState struct {
	anySrcNulls bool // kindFixed: some source has nulls, so BuildBatch gives it a bitmap
	nulls       int
	total       int // kindVarLen: value bytes
}

type fusedScratch struct {
	srcs   []colSource // record-major: srcs[rec*ncols+col]
	state  []colState
	bufLen []int
}

var fusedScratchPool = sync.Pool{New: func() any { return &fusedScratch{} }}

func grow[T any](s []T, n int) []T {
	if cap(s) < n {
		return make([]T, n)
	}
	s = s[:n]
	clear(s)
	return s
}

// builderBitmapLen is the validity bitmap an arrow-go builder holds after
// Reserve(n) on a fresh builder: its capacity rounds up to the next power of
// two, at least minCap.
func builderBitmapLen(n, minCap int) int {
	c := bitutil.NextPowerOf2(n)
	if c < minCap {
		c = minCap
	}
	return bitutil.CeilByte(c) / 8
}

// Minimum builder capacities: the numeric and boolean builders raise a
// smaller one to minBuilderCapacity (arrow/array), the binary builder does not.
const (
	numericMinCap = 32
	binaryMinCap  = 0
)

// ipcValidityLen is the validity buffer's length in the stock writer's
// output for a column of n rows with nulls of them null, given the length of
// the bitmap the column holds: dropped without nulls, a zeroed 64-byte
// padded bitmap when every row is null, otherwise the held bitmap truncated
// to the 64-byte padded length.
func ipcValidityLen(n, nulls, held int) int {
	m := pad64(int(bitutil.BytesForBits(int64(n))))
	switch nulls {
	case 0:
		return 0
	case n:
		return m
	}
	if m < held {
		return m
	}
	return held
}

// encodeFused is the fused writer. ok is false when it declines the batch;
// the two-step path then encodes it, or reports what is wrong with it. That
// covers every input BuildBatch rejects, an empty header, and a column type
// userColOf does not list.
func encodeFused(header *segcorepb.RetrieveResults, sel *queryutil.ArrowSelection) (Encoded, bool, error) {
	if header.GetElementLevel() || len(header.GetElementIndices()) > 0 {
		return Encoded{}, false, nil
	}
	rows := typeutil.GetSizeOfIDs(header.GetIds())
	if rows == 0 {
		return Encoded{}, false, nil
	}
	var pkType arrow.Type
	var intPKs []int64
	var strPKs []string
	switch header.GetIds().GetIdField().(type) {
	case *schemapb.IDs_IntId:
		pkType, intPKs = arrow.INT64, header.GetIds().GetIntId().GetData()
	case *schemapb.IDs_StrId:
		pkType, strPKs = arrow.STRING, header.GetIds().GetStrId().GetData()
	default:
		return Encoded{}, false, nil
	}
	sys := header.GetFieldsData()
	for _, fd := range sys {
		id := fd.GetFieldId()
		if (id != common.RowIDField && id != common.TimeStampField) || fd.GetType() != schemapb.DataType_Int64 ||
			len(fd.GetScalars().GetLongData().GetData()) != rows {
			return Encoded{}, false, nil
		}
	}
	tpl := sel.Template()
	if tpl == nil || sel.Empty() || len(sel.Rows) != rows {
		return Encoded{}, false, nil
	}
	schema := tpl.Schema()
	ncols := schema.NumFields()
	tplMD := schema.Metadata()
	for _, r := range sel.Records {
		if r == nil {
			continue
		}
		if int(r.NumCols()) != ncols || (r.Schema() != schema && !metadataEqual(r.Schema().Metadata(), tplMD)) {
			return Encoded{}, false, nil
		}
	}
	for i := 0; i < rows; i++ {
		ri, row := sel.Row(i)
		if ri < 0 || ri >= len(sel.Records) || sel.Records[ri] == nil || row < 0 || row >= sel.Records[ri].NumRows() {
			return Encoded{}, false, nil
		}
	}
	l := lookupLayout(pkType, sys, schema)
	if l == nil {
		return Encoded{}, false, nil
	}

	sc := fusedScratchPool.Get().(*fusedScratch)
	defer func() {
		// Keep no source buffer reachable from the pool.
		clear(sc.srcs)
		fusedScratchPool.Put(sc)
	}()
	if !sc.resolve(l, sel.Records) {
		return Encoded{}, false, nil
	}
	if !sc.size(l, sel, rows) {
		return Encoded{}, false, nil
	}

	// Buffer lengths in IPC order, and RawSize.
	sc.bufLen = grow(sc.bufLen, len(l.bufPos))
	bl := sc.bufLen[:0]
	var raw int64
	pkTotal := 0
	if pkType == arrow.INT64 {
		bl = append(bl, 0, 8*rows)
		raw += int64(builderBitmapLen(rows, numericMinCap) + 8*rows)
	} else {
		for _, s := range strPKs {
			pkTotal += len(s)
		}
		if pkTotal > math.MaxInt32 {
			return Encoded{}, false, nil
		}
		bl = append(bl, 0, 4*(rows+1), pkTotal)
		raw += int64(builderBitmapLen(rows, binaryMinCap) + 4*(rows+1) + pkTotal)
	}
	for range sys {
		bl = append(bl, 0, 8*rows)
		raw += int64(builderBitmapLen(rows, numericMinCap) + 8*rows)
	}
	bitmap := int(bitutil.BytesForBits(int64(rows)))
	for c, col := range l.cols {
		st := &sc.state[c]
		switch col.kind {
		case kindFixed:
			held := 0
			if st.anySrcNulls {
				held = bitmap
			}
			bl = append(bl, ipcValidityLen(rows, st.nulls, held), col.width*rows)
			raw += int64(held + col.width*rows)
		case kindBool:
			held := builderBitmapLen(rows, numericMinCap)
			bl = append(bl, ipcValidityLen(rows, st.nulls, held), bitmap)
			raw += int64(held + bitmap)
		case kindVarLen:
			held := builderBitmapLen(rows, binaryMinCap)
			bl = append(bl, ipcValidityLen(rows, st.nulls, held), 4*(rows+1), st.total)
			raw += int64(held + 4*(rows+1) + st.total)
		}
	}
	if len(bl) != len(l.bufPos) {
		return Encoded{}, false, merr.WrapErrServiceInternalMsg("materialize: fused writer sized %d buffers, layout has %d", len(bl), len(l.bufPos))
	}
	bodyLen := 0
	for _, n := range bl {
		bodyLen += bitutil.CeilByte(n)
	}

	prefix := len(l.prefix)
	msg := make([]byte, prefix+bodyLen)
	copy(msg, l.prefix)
	le := binary.LittleEndian
	le.PutUint64(msg[l.bodyLenPos:], uint64(bodyLen))
	le.PutUint64(msg[l.lengthPos:], uint64(rows))
	for j, p := range l.nodePos {
		nulls := 0
		if j > len(sys) {
			nulls = sc.state[j-1-len(sys)].nulls
		}
		le.PutUint64(msg[p:], uint64(rows))
		le.PutUint64(msg[p+8:], uint64(nulls))
	}
	off := 0
	for j, p := range l.bufPos {
		le.PutUint64(msg[p:], uint64(off))
		le.PutUint64(msg[p+8:], uint64(bl[j]))
		off += bitutil.CeilByte(bl[j])
	}
	// next returns buffer j's region of the body; buffers are taken in order.
	body := msg[prefix:]
	at := 0
	next := func(j int) []byte {
		b := body[at : at+bl[j]]
		at += bitutil.CeilByte(bl[j])
		return b
	}

	j := 0
	if pkType == arrow.INT64 {
		next(j)
		copy(next(j+1), arrow.Int64Traits.CastToBytes(intPKs))
		j += 2
	} else {
		next(j)
		writeStrings(next(j+1), next(j+2), strPKs)
		j += 3
	}
	for _, fd := range sys {
		next(j)
		copy(next(j+1), arrow.Int64Traits.CastToBytes(fd.GetScalars().GetLongData().GetData()))
		j += 2
	}
	for c, col := range l.cols {
		st := &sc.state[c]
		srcs := sc.srcs[c:]
		// The validity bits are written only when the stock writer sends
		// them: some, not all, rows null.
		var valid []byte
		if v := next(j); st.nulls > 0 && st.nulls < rows {
			valid = v
		}
		switch col.kind {
		case kindFixed:
			gatherFixed(next(j+1), valid, srcs, ncols, sel, rows, col.width, st.anySrcNulls)
			j += 2
		case kindBool:
			gatherBool(next(j+1), valid, srcs, ncols, sel, rows)
			j += 2
		case kindVarLen:
			gatherVarLenInto(next(j+1), next(j+2), valid, srcs, ncols, sel, rows)
			j += 3
		}
	}
	if at != bodyLen {
		return Encoded{}, false, merr.WrapErrServiceInternalMsg("materialize: fused writer wrote %d body bytes, sized %d", at, bodyLen)
	}
	return Encoded{SchemaMsg: l.schemaMsg, Entry: msg, RawSize: raw, ResultSize: sc.resultSize(l, rows, len(sys))}, true, nil
}

// resultSize is resultSize from the sizing (size): Batch.RowSize summed
// over the rows. A column's nulls are counted whenever a source has some,
// the only case colResultSize reads them for (a vector).
func (sc *fusedScratch) resultSize(l *fusedLayout, rows, nsys int) int64 {
	size := sysResultSize(nsys, rows)
	for c, col := range l.cols {
		st := &sc.state[c]
		size += colResultSize(col.id, col.width, rows, st.nulls, st.total)
	}
	return size
}

// resolve fills sc.srcs for every record that can be referenced (non-nil,
// with rows) and checks its columns have the layout's types. False declines
// the batch.
func (sc *fusedScratch) resolve(l *fusedLayout, records []arrow.Record) bool {
	ncols := len(l.cols)
	sc.srcs = grow(sc.srcs, len(records)*ncols)
	sc.state = grow(sc.state, ncols)
	for ri, r := range records {
		if r == nil || r.NumRows() == 0 {
			continue
		}
		for c, col := range l.cols {
			arr := r.Column(c)
			if !col.sameType(arr.DataType()) {
				return false
			}
			data := arr.Data()
			bufs := data.Buffers()
			s := &sc.srcs[ri*ncols+c]
			s.bitOff = data.Offset()
			switch col.kind {
			case kindFixed:
				if len(bufs) < 2 || bufs[1] == nil {
					return false
				}
				s.values = bufs[1].Bytes()[data.Offset()*col.width:]
				// As gatherFixedWidth: the bitmap only of a source with nulls.
				if arr.NullN() != 0 {
					s.valid = arr.NullBitmapBytes()
					sc.state[c].anySrcNulls = true
				}
			case kindBool:
				if len(bufs) < 2 || bufs[1] == nil {
					return false
				}
				s.values = bufs[1].Bytes()
				s.valid = arr.NullBitmapBytes()
			case kindVarLen:
				if s.offsets = varLenOffsets(arr); s.offsets == nil {
					return false
				}
				if len(bufs) > 2 && bufs[2] != nil {
					s.values = bufs[2].Bytes()
				}
				s.valid = arr.NullBitmapBytes()
			}
			if len(s.valid) == 0 {
				s.valid = nil
			}
		}
	}
	return true
}

func isNull(s *colSource, row int) bool {
	return s.valid != nil && !bitutil.BitIsSet(s.valid, s.bitOff+row)
}

// size counts each user column's selected nulls and var-len value bytes.
func (sc *fusedScratch) size(l *fusedLayout, sel *queryutil.ArrowSelection, rows int) bool {
	ncols := len(l.cols)
	for c, col := range l.cols {
		st := &sc.state[c]
		if col.kind == kindFixed && !st.anySrcNulls {
			continue
		}
		for i := 0; i < rows; i++ {
			ri, row := sel.Row(i)
			s := &sc.srcs[ri*ncols+c]
			if isNull(s, int(row)) {
				st.nulls++
				continue
			}
			if col.kind == kindVarLen {
				st.total += int(s.offsets[row+1] - s.offsets[row])
			}
		}
		if st.total > math.MaxInt32 {
			return false
		}
	}
	return true
}

// gatherFixed copies each selected row's value into dst, runs of
// consecutive rows of one record at once; a null row's value stays zero.
func gatherFixed(dst, valid []byte, srcs []colSource, ncols int, sel *queryutil.ArrowSelection, rows, width int, nullable bool) {
	if !nullable {
		for i := 0; i < rows; {
			ri, row := sel.Row(i)
			n := 1
			for i+n < rows {
				rj, rowj := sel.Row(i + n)
				if rj != ri || rowj != row+int64(n) {
					break
				}
				n++
			}
			src := srcs[ri*ncols].values
			copy(dst[i*width:(i+n)*width], src[int(row)*width:(int(row)+n)*width])
			i += n
		}
		return
	}
	for i := 0; i < rows; i++ {
		ri, row := sel.Row(i)
		s := &srcs[ri*ncols]
		if isNull(s, int(row)) {
			continue
		}
		if valid != nil {
			bitutil.SetBit(valid, i)
		}
		copy(dst[i*width:(i+1)*width], s.values[int(row)*width:(int(row)+1)*width])
	}
}

func gatherBool(dst, valid []byte, srcs []colSource, ncols int, sel *queryutil.ArrowSelection, rows int) {
	for i := 0; i < rows; i++ {
		ri, row := sel.Row(i)
		s := &srcs[ri*ncols]
		if isNull(s, int(row)) {
			continue
		}
		if valid != nil {
			bitutil.SetBit(valid, i)
		}
		if bitutil.BitIsSet(s.values, s.bitOff+int(row)) {
			bitutil.SetBit(dst, i)
		}
	}
}

func gatherVarLenInto(offsets, values, valid []byte, srcs []colSource, ncols int, sel *queryutil.ArrowSelection, rows int) {
	offs := arrow.Int32Traits.CastFromBytes(offsets)
	cur := 0
	for i := 0; i < rows; i++ {
		ri, row := sel.Row(i)
		s := &srcs[ri*ncols]
		if !isNull(s, int(row)) {
			if valid != nil {
				bitutil.SetBit(valid, i)
			}
			b, e := int(s.offsets[row]), int(s.offsets[row+1])
			cur += copy(values[cur:], s.values[b:e])
		}
		offs[i+1] = int32(cur)
	}
}

func writeStrings(offsets, values []byte, strs []string) {
	offs := arrow.Int32Traits.CastFromBytes(offsets)
	cur := 0
	for i, s := range strs {
		cur += copy(values[cur:], s)
		offs[i+1] = int32(cur)
	}
}
