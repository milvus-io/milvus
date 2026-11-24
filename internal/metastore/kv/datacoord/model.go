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

package datacoord

import (
	"context"
	"path"
	"reflect"
	"slices"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/txn"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// This file is the generic layer of the catalog: how an UpdateAction on a
// value of a registered metastore.ModelType becomes
// KV ops, and stored values become values again. The catalog builds every
// key, Root/c1/.../cn, from the type and a coordinate. Nothing here is per
// type.

// checkType rejects a type RegisterModelType did not return.
func checkType(t *metastore.ModelType) error {
	if !t.Registered() {
		return merr.WrapErrServiceInternalMsg("catalog: unregistered model type %v", t)
	}
	return nil
}

// buildKey joins t's root and parts into a key, after checking that no part
// is empty, "." or "..", or contains "/". A record key has Depth parts; a
// prefix has fewer and ends with "/".
func buildKey(t *metastore.ModelType, parts []string) (string, error) {
	for _, part := range parts {
		if part == "" || part == "." || part == ".." || strings.Contains(part, "/") {
			return "", merr.WrapErrServiceInternalMsg("catalog: %s coordinate %q has a malformed component %q",
				t.Root, parts, part)
		}
	}
	key := path.Join(append([]string{t.Root}, parts...)...)
	if len(parts) < t.Depth {
		key += "/"
	}
	return key, nil
}

// recordKey returns the key of the record of t at coord.
func recordKey(t *metastore.ModelType, coord []string) (string, error) {
	if len(coord) != t.Depth {
		return "", merr.WrapErrServiceInternalMsg("catalog: %s coordinate %q has %d components, want %d",
			t.Root, coord, len(coord), t.Depth)
	}
	return buildKey(t, coord)
}

// prefixKey returns the key prefix of the records of t under prefix; verb
// names the primitive for the error.
func prefixKey(t *metastore.ModelType, prefix []string, verb string) (string, error) {
	if len(prefix) >= t.Depth {
		return "", merr.WrapErrServiceInternalMsg("catalog: a %s %s prefix %q names a record, not a prefix; use Get",
			t.Root, verb, prefix)
	}
	return buildKey(t, prefix)
}

// coordinateOf returns the coordinate of v, a value of t's Go type, and
// rejects a nil pointer.
func coordinateOf(t *metastore.ModelType, v any) ([]string, error) {
	if rv := reflect.ValueOf(v); rv.Kind() == reflect.Pointer && rv.IsNil() {
		return nil, merr.WrapErrServiceInternalMsg("catalog: nil %s value", t.Root)
	}
	return t.Coordinate(v), nil
}

// applyGeneric records action into b when its entry is a value of a
// registered model type, and reports whether it was one.
//
// guarded tells that the Update is conditional on a read revision (see
// metastore.UnchangedSince): a value saved with ActionUpdate must then still
// exist.
func applyGeneric(b *txn.Builder, action metastore.UpdateAction, guarded bool) (bool, error) {
	if t, ok := metastore.ModelTypeOf(action.Entry); ok {
		return true, applyValue(b, action, t, action.Entry, guarded)
	}
	return false, nil
}

// applyValue records one action on v, a value of t, into b. The action
// decides how to write (save or delete one record); the type supplies the
// coordinate and, for a save, the encoded value.
func applyValue(b *txn.Builder, action metastore.UpdateAction, t *metastore.ModelType, v any, guarded bool) error {
	coord, err := coordinateOf(t, v)
	if err != nil {
		return err
	}
	key, err := recordKey(t, coord)
	if err != nil {
		return err
	}
	switch action.Type {
	case metastore.ActionAdd, metastore.ActionUpdate:
		value, err := t.Marshal(v)
		if err != nil {
			return merr.Wrapf(err, "marshal %s %q", t.Root, key)
		}
		b.Save(key, string(value))
		if guarded && action.Type == metastore.ActionUpdate {
			// the revision guard passes for a record deleted after the read:
			// do not write it back
			b.Require(predicates.KeyExist(key))
		}
	case metastore.ActionDelete:
		b.Remove(key)
	default:
		return merr.WrapErrServiceInternalMsg("catalog: unsupported action type %v on %s", action.Type, t.Root)
	}
	return nil
}

// Get loads the record of type t at coordinate and decodes it. A missing record
// is MetaKv.Load's merr.ErrIoKeyNotFound.
func (kc *Catalog) Get(ctx context.Context, t *metastore.ModelType, coordinate []string) (any, metastore.Revision, error) {
	if err := checkType(t); err != nil {
		return nil, 0, err
	}
	key, err := recordKey(t, coordinate)
	if err != nil {
		return nil, 0, err
	}
	var value string
	var revision int64
	if loader, ok := kc.MetaKv.(revisionLoader); ok {
		value, revision, err = loader.LoadWithRevision(ctx, key)
	} else {
		value, err = kc.MetaKv.Load(ctx, key)
	}
	if err != nil {
		return nil, 0, err
	}
	v, err := decodeRecord(kc.metaRootpath, t, key, []byte(value))
	if err != nil {
		return nil, 0, err
	}
	return v, metastore.Revision(revision), nil
}

// List implements metastore.GenericCatalog.List. With the pageScanner
// capability, an unpaged, unpinned List restarts on the latest snapshot when
// the backend compacts its snapshot; after maxListAttempts it walks without a
// snapshot and returns revision 0, as a recovery reload did before snapshots,
// rather than fail. A paged or pinned one reports kv.ErrCompacted. Without the
// capability List walks with
// WalkWithPrefix, skipping to the cursor (each page re-reads the prefix up to
// it), and returns revision 0.
func (kc *Catalog) List(ctx context.Context, t *metastore.ModelType, coordinate []string, opts metastore.ListOptions) (metastore.ListResult, error) {
	if err := checkType(t); err != nil {
		return metastore.ListResult{}, err
	}
	keyPrefix, err := prefixKey(t, coordinate, "list")
	if err != nil {
		return metastore.ListResult{}, err
	}
	switch {
	case opts.Limit < 0:
		return metastore.ListResult{}, merr.WrapErrServiceInternalMsg("catalog: negative list limit %d", opts.Limit)
	case opts.After != "" && !strings.HasPrefix(opts.After, keyPrefix):
		return metastore.ListResult{}, merr.WrapErrServiceInternalMsg(
			"catalog: list cursor %q is outside the scope %q", opts.After, keyPrefix)
	case opts.KeysOnly && opts.Filter != nil:
		return metastore.ListResult{}, merr.WrapErrServiceInternalMsg(
			"catalog: a keys-only %s list cannot filter values", t.Root)
	}
	l := lister{kc: kc, typ: t, opts: opts, prefix: keyPrefix}
	scanner, ok := kc.MetaKv.(pageScanner)
	if !ok {
		if opts.Revision != 0 {
			return metastore.ListResult{}, merr.Wrap(merr.ErrServiceUnimplemented,
				"meta kv backend cannot read at a revision")
		}
		return l.walk(ctx)
	}
	for attempt := 1; ; attempt++ {
		res, err := l.scan(ctx, scanner)
		switch {
		case err == nil:
			return res, nil
		case !errors.Is(err, kv.ErrCompacted):
			return metastore.ListResult{}, err
		case opts.Limit != 0 || opts.Revision != 0:
			return metastore.ListResult{}, err
		case attempt >= maxListAttempts:
			return l.walk(ctx)
		}
	}
}

// lister runs one List.
type lister struct {
	kc     *Catalog
	typ    *metastore.ModelType
	opts   metastore.ListOptions
	prefix string
}

// visit decodes the record stored under key and adds it to res when it
// matches; a keys-only List adds its coordinate without decoding.
func (l *lister) visit(res *metastore.ListResult, key string, value []byte) error {
	if l.opts.KeysOnly {
		coord, err := recordCoordinateOfKey(l.kc.metaRootpath, l.typ, key)
		if err != nil {
			return err
		}
		res.Coordinates = append(res.Coordinates, coord)
		return nil
	}
	v, err := decodeRecord(l.kc.metaRootpath, l.typ, key, value)
	if err != nil {
		return err
	}
	if l.opts.Filter == nil || l.opts.Filter(v) {
		res.Values = append(res.Values, v)
	}
	return nil
}

// full reports whether res holds a whole page.
func (l *lister) full(res *metastore.ListResult) bool {
	return l.opts.Limit > 0 && l.count(res) == l.opts.Limit
}

// count returns the matches res holds.
func (l *lister) count(res *metastore.ListResult) int {
	if l.opts.KeysOnly {
		return len(res.Coordinates)
	}
	return len(res.Values)
}

// scan reads pages from the cursor at one revision until the list or the
// page is complete. A read stops at most one record past the page, which
// tells whether a next page exists.
func (l *lister) scan(ctx context.Context, scanner pageScanner) (metastore.ListResult, error) {
	res := metastore.ListResult{Revision: l.opts.Revision}
	after := l.opts.After
	for {
		// The read size is metastore.paginationSize, as for every other
		// catalog walk (0: no limit); a page reads at most one record past
		// its last match.
		// A filter can reject any number of records, so only an unfiltered
		// page shrinks the read to its limit.
		size := l.kc.paginationSize
		if want := l.opts.Limit - l.count(&res) + 1; l.opts.Limit > 0 && l.opts.Filter == nil && (size <= 0 || want < size) {
			size = want
		}
		var last, pageEnd string
		rest := false // a record follows the page's last match
		rev, more, err := scanner.WalkPrefixPage(ctx, l.prefix, after, size, int64(res.Revision), func(k, value []byte) error {
			key := string(k)
			last = key
			if l.full(&res) {
				rest = true
				return nil
			}
			pageEnd = key
			return l.visit(&res, key, value)
		})
		if err != nil {
			return metastore.ListResult{}, err
		}
		if res.Revision == 0 {
			res.Revision = metastore.Revision(rev)
		}
		switch {
		case l.full(&res):
			if rest || more {
				res.Next = pageEnd
			}
			return res, nil
		case !more || last == "":
			return res, nil
		}
		after = last
	}
}

// errPageFull stops a fallback walk after the record following a full page.
var errPageFull = errors.New("catalog: page full")

// walk lists without the pageScanner capability: one WalkWithPrefix,
// skipping the records up to the cursor and stopping after a full page. It
// reads no snapshot and returns revision 0.
func (l *lister) walk(ctx context.Context) (metastore.ListResult, error) {
	var res metastore.ListResult
	var pageEnd string
	root := rootPrefix(l.kc.metaRootpath)
	err := l.kc.MetaKv.WalkWithPrefix(ctx, l.prefix, l.kc.paginationSize, func(k, value []byte) error {
		key := strings.TrimPrefix(string(k), root)
		switch {
		case key <= l.opts.After:
			return nil
		case l.full(&res):
			res.Next = pageEnd
			return errPageFull
		}
		pageEnd = key
		return l.visit(&res, key, value)
	})
	if err != nil && !errors.Is(err, errPageFull) {
		return metastore.ListResult{}, err
	}
	return res, nil
}

// coordinateOfKey returns the coordinate of the record of t stored under
// key.
func coordinateOfKey(t *metastore.ModelType, key string) []string {
	return strings.Split(strings.TrimPrefix(key, t.Root+"/"), "/")
}

// recordCoordinateOfKey returns the coordinate of the record of t stored
// under key, without its value, and fails closed on a key that is not a
// record of t's depth.
func recordCoordinateOfKey(root string, t *metastore.ModelType, key string) ([]string, error) {
	coord := coordinateOfKey(t, key)
	if len(coord) != t.Depth || slices.Contains(coord, "") {
		return nil, merr.WrapErrDataIntegrityMsg("catalog: key %s is not a %s record", path.Join(root, key), t.Root)
	}
	return coord, nil
}

// decodeRecord decodes the record of t stored under key and checks that the
// record agrees with its key, so a value written under the wrong key fails
// closed instead of being served under a foreign identity. Errors name the
// absolute key under root, so an operator facing a fail-closed startup can
// find and remove the record.
func decodeRecord(root string, t *metastore.ModelType, key string, value []byte) (any, error) {
	v, err := t.Unmarshal(value)
	if err != nil {
		return nil, merr.Wrapf(err, "decode %s at key %s", t.Root, path.Join(root, key))
	}
	if reflect.TypeOf(v) != t.GoType {
		return nil, merr.WrapErrServiceInternalMsg("catalog: %s Unmarshal returned %T, want %v", t.Root, v, t.GoType)
	}
	got, err := coordinateOf(t, v)
	if err != nil {
		return nil, merr.Wrapf(err, "decode %s at key %s", t.Root, path.Join(root, key))
	}
	if want := coordinateOfKey(t, key); !slices.Equal(got, want) {
		return nil, merr.WrapErrDataIntegrityMsg("catalog: %s record at key %s decodes to coordinate %q",
			t.Root, path.Join(root, key), got)
	}
	return v, nil
}
