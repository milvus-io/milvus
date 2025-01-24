// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package transaction

import (
	"fmt"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"

	"github.com/milvus-io/milvus/internal/storagev2/common/errors"
	"github.com/milvus-io/milvus/internal/storagev2/common/log"
	"github.com/milvus-io/milvus/internal/storagev2/common/utils"
	"github.com/milvus-io/milvus/internal/storagev2/file/blob"
	"github.com/milvus-io/milvus/internal/storagev2/file/fragment"
	"github.com/milvus-io/milvus/internal/storagev2/io/format"
	"github.com/milvus-io/milvus/internal/storagev2/io/format/parquet"
	"github.com/milvus-io/milvus/internal/storagev2/io/fs"
	"github.com/milvus-io/milvus/internal/storagev2/storage/lock"
	"github.com/milvus-io/milvus/internal/storagev2/storage/manifest"
	"github.com/milvus-io/milvus/internal/storagev2/storage/options"
)

type SpaceMeta interface {
	Path() string
	Fs() fs.Fs
	Manifest() *manifest.Manifest
	LockManager() lock.LockManager
	SetManifest(manifest *manifest.Manifest)
}

type Transaction interface {
	Write(reader array.RecordReader, options *options.WriteOptions) Transaction
	Delete(reader array.RecordReader) Transaction
	WriteBlob(content []byte, name string, replace bool) Transaction
	Commit() error
}

type ConcurrentWriteTransaction struct {
	operations []Operation
	commit     manifest.ManifestCommit
	space      SpaceMeta
}

func (t *ConcurrentWriteTransaction) Write(reader array.RecordReader, options *options.WriteOptions) Transaction {
	operation := &WriteOperation{
		reader:      reader,
		options:     options,
		space:       t.space,
		transaction: t,
	}
	t.operations = append(t.operations, operation)
	return t
}

func (t *ConcurrentWriteTransaction) Delete(reader array.RecordReader) Transaction {
	operation := &DeleteOperation{
		reader:      reader,
		space:       t.space,
		transaction: t,
	}
	t.operations = append(t.operations, operation)
	return t
}

func (t *ConcurrentWriteTransaction) WriteBlob(content []byte, name string, replace bool) Transaction {
	operation := &WriteBlobOperation{
		content:     content,
		name:        name,
		replace:     replace,
		space:       t.space,
		transaction: t,
	}
	t.operations = append(t.operations, operation)
	return t
}

func (t *ConcurrentWriteTransaction) Commit() error {
	for _, op := range t.operations {
		op.Execute()
	}
	nxtManifest, err := t.commit.Commit()
	if err != nil {
		return err
	}
	t.space.SetManifest(nxtManifest)
	return nil
}

func NewConcurrentWriteTransaction(space SpaceMeta) *ConcurrentWriteTransaction {
	return &ConcurrentWriteTransaction{
		operations: make([]Operation, 0),
		commit:     manifest.NewManifestCommit(space.LockManager(), manifest.NewManifestReaderWriter(space.Fs(), space.Path())),
		space:      space,
	}
}

type Operation interface {
	Execute() error
}

type WriteOperation struct {
	reader      array.RecordReader
	options     *options.WriteOptions
	space       SpaceMeta
	transaction *ConcurrentWriteTransaction
}

func (w *WriteOperation) Execute() error {
	if !w.space.Manifest().GetSchema().Schema().Equal(w.reader.Schema()) {
		return errors.ErrSchemaNotMatch
	}

	scalarSchema, vectorSchema := w.space.Manifest().GetSchema().ScalarSchema(), w.space.Manifest().GetSchema().VectorSchema()
	var (
		scalarWriter format.Writer
		vectorWriter format.Writer
	)
	scalarFragment := fragment.NewFragment()
	vectorFragment := fragment.NewFragment()

	isEmpty := true
	for w.reader.Next() {
		rec := w.reader.Record()

		if rec.NumRows() == 0 {
			continue
		}

		var err error
		scalarWriter, err = w.write(scalarSchema, rec, scalarWriter, &scalarFragment, w.options, true)
		if err != nil {
			return err
		}
		vectorWriter, err = w.write(vectorSchema, rec, vectorWriter, &vectorFragment, w.options, false)
		if err != nil {
			return err
		}
		isEmpty = false
	}

	if scalarWriter != nil {
		if err := scalarWriter.Close(); err != nil {
			return err
		}
	}
	if vectorWriter != nil {
		if err := vectorWriter.Close(); err != nil {
			return err
		}
	}

	if isEmpty {
		return nil
	}

	op1 := manifest.AddScalarFragmentOp{ScalarFragment: scalarFragment}
	op2 := manifest.AddVectorFragmentOp{VectorFragment: vectorFragment}
	w.transaction.commit.AddOp(op1, op2)
	return nil
}

func (w *WriteOperation) write(
	schema *arrow.Schema,
	rec arrow.Record,
	writer format.Writer,
	fragment *fragment.Fragment,
	opt *options.WriteOptions,
	isScalar bool,
) (format.Writer, error) {
	var columns []arrow.Array
	cols := rec.Columns()
	for k := range cols {
		_, has := schema.FieldsByName(rec.ColumnName(k))
		if has {
			columns = append(columns, cols[k])
		}
	}

	var rootPath string
	if isScalar {
		// add offset column for scalar
		offsetValues := make([]int64, rec.NumRows())
		for i := 0; i < int(rec.NumRows()); i++ {
			offsetValues[i] = int64(i)
		}
		builder := array.NewInt64Builder(memory.DefaultAllocator)
		builder.AppendValues(offsetValues, nil)
		offsetColumn := builder.NewArray()
		columns = append(columns, offsetColumn)
		rootPath = utils.GetScalarDataDir(w.space.Path())
	} else {
		rootPath = utils.GetVectorDataDir(w.space.Path())
	}

	var err error

	record := array.NewRecord(schema, columns, rec.NumRows())

	if writer == nil {
		filePath := utils.GetNewParquetFilePath(rootPath)
		writer, err = parquet.NewFileWriter(schema, w.space.Fs(), filePath)
		if err != nil {
			return nil, err
		}
		fragment.AddFile(filePath)
	}

	err = writer.Write(record)
	if err != nil {
		return nil, err
	}

	if writer.Count() >= opt.MaxRecordPerFile {
		log.Debug("close writer", log.Any("count", writer.Count()))
		err = writer.Close()
		if err != nil {
			return nil, err
		}
		writer = nil
	}

	return writer, nil
}

type DeleteOperation struct {
	reader      array.RecordReader
	space       SpaceMeta
	transaction *ConcurrentWriteTransaction
}

func (o *DeleteOperation) Execute() error {
	schema := o.space.Manifest().GetSchema().DeleteSchema()
	fragment := fragment.NewFragment()
	var (
		err        error
		writer     format.Writer
		deleteFile string
	)

	for o.reader.Next() {
		rec := o.reader.Record()
		if rec.NumRows() == 0 {
			continue
		}

		if writer == nil {
			deleteFile = utils.GetNewParquetFilePath(utils.GetDeleteDataDir(o.space.Path()))
			writer, err = parquet.NewFileWriter(schema, o.space.Fs(), deleteFile)
			if err != nil {
				return err
			}
			fragment.AddFile(deleteFile)
		}

		if err = writer.Write(rec); err != nil {
			return err
		}
	}

	if writer != nil {
		if err = writer.Close(); err != nil {
			return err
		}

		op := manifest.AddDeleteFragmentOp{DeleteFragment: fragment}
		o.transaction.commit.AddOp(op)
	}
	return nil
}

type WriteBlobOperation struct {
	content     []byte
	name        string
	replace     bool
	space       SpaceMeta
	transaction *ConcurrentWriteTransaction
}

func (o *WriteBlobOperation) Execute() error {
	if !o.replace && o.space.Manifest().HasBlob(o.name) {
		return errors.ErrBlobAlreadyExist
	}

	blobFile := utils.GetBlobFilePath(o.space.Path())
	f, err := o.space.Fs().OpenFile(blobFile)
	if err != nil {
		return err
	}

	n, err := f.Write(o.content)
	if err != nil {
		return err
	}

	if n != len(o.content) {
		return fmt.Errorf("blob not written completely, written %d but expect %d", n, len(o.content))
	}

	if err = f.Close(); err != nil {
		return err
	}

	op := manifest.AddBlobOp{
		Replace: o.replace,
		Blob: blob.Blob{
			Name: o.name,
			Size: int64(len(o.content)),
			File: blobFile,
		},
	}
	o.transaction.commit.AddOp(op)
	return nil
}
