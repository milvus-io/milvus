package external

import (
	"context"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/function/embedding"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// defaultReadBufferSize matches the buffer size used elsewhere for FFIPackedReader.
const defaultReadBufferSize = 64 * 1024 * 1024

// ExecuteFunctionsForSegment computes function-output columns for an external
// segment and returns a manifest that references both the external original
// files (for input columns) and a newly written packed file (for function
// output columns).
//
// Streaming pipeline (no full-segment InsertData materialization):
//  1. Build an input manifest referencing the segment's external fragments.
//  2. Open FFIPackedReader on input + FFIPackedWriter on outputs.
//  3. For each Arrow batch read: convert to InsertData, run functions, append
//     BM25 stats, build output Record, write to packed file.
//  4. Close writer (commits manifest).
//  5. Serialize accumulated BM25 stats and add to manifest.
//
// Memory: peak ~ one Arrow batch (default 64 MiB) regardless of segment size.
// Input columns are never copied — segments reference the external original
// files via the same column-group layout Segcore already uses.
func ExecuteFunctionsForSegment(
	ctx context.Context,
	schema *schemapb.CollectionSchema,
	fragments []packed.Fragment,
	format string,
	storageConfig *indexpb.StorageConfig,
	collectionID int64,
	segmentID int64,
	clusterID string,
) (string, error) {
	log := log.Ctx(ctx)
	log.Info("executing functions for external table segment",
		zap.Int64("segmentID", segmentID),
		zap.Int("numFragments", len(fragments)),
		zap.Int("numFunctions", len(schema.GetFunctions())))

	basePath := fmt.Sprintf("external/%d/segments/%d", collectionID, segmentID)
	inputColumns := packed.GetColumnNamesFromSchema(schema)

	inputManifestPath, err := packed.CreateSegmentManifestWithBasePath(
		ctx, basePath, format, inputColumns, fragments, storageConfig)
	if err != nil {
		return "", fmt.Errorf("create input manifest: %w", err)
	}
	_, inputVersion, err := packed.UnmarshalManifestPath(inputManifestPath)
	if err != nil {
		return "", fmt.Errorf("parse input manifest path: %w", err)
	}

	outputFields, outputSchema, err := buildOutputSchema(schema)
	if err != nil {
		return "", err
	}
	outputArrow, err := storage.ConvertToArrowSchema(outputSchema, false)
	if err != nil {
		return "", err
	}

	reader, err := openInputReader(schema, inputManifestPath, inputColumns, storageConfig, collectionID)
	if err != nil {
		return "", err
	}
	defer reader.Release()

	colGroups := []storagecommon.ColumnGroup{{Columns: lo.Range(len(outputFields))}}
	writer, err := packed.NewFFIPackedWriter(
		basePath, inputVersion, outputArrow, colGroups, storageConfig, nil)
	if err != nil {
		return "", fmt.Errorf("open output writer: %w", err)
	}
	writer.AsNewColumnGroups()

	bm25Acc := newBM25Accumulators(schema)

	totalRows, err := streamBatches(ctx, schema, outputSchema, outputArrow,
		reader, writer, bm25Acc, clusterID)
	if err != nil {
		return "", err
	}

	manifestPath, err := writer.Close()
	if err != nil {
		return "", fmt.Errorf("close output writer: %w", err)
	}

	manifestPath, err = finalizeBM25Stats(ctx, bm25Acc, storageConfig, manifestPath)
	if err != nil {
		return "", err
	}

	log.Info("function execution completed",
		zap.Int64("segmentID", segmentID),
		zap.Int64("rows", totalRows),
		zap.String("manifestPath", manifestPath))
	return manifestPath, nil
}

// buildOutputSchema returns the output FieldSchema list and a wrapping
// CollectionSchema used for arrow conversion. Errors if the schema declares no
// function outputs (the executor should not have been invoked at all).
func buildOutputSchema(schema *schemapb.CollectionSchema) ([]*schemapb.FieldSchema, *schemapb.CollectionSchema, error) {
	var outputFields []*schemapb.FieldSchema
	for _, f := range schema.GetFields() {
		if f.GetIsFunctionOutput() {
			outputFields = append(outputFields, f)
		}
	}
	if len(outputFields) == 0 {
		return nil, nil, fmt.Errorf("no function output fields; executor should not have been invoked")
	}
	return outputFields, &schemapb.CollectionSchema{
		Name:   schema.GetName(),
		Fields: outputFields,
	}, nil
}

func openInputReader(
	schema *schemapb.CollectionSchema,
	manifestPath string,
	inputColumns []string,
	storageConfig *indexpb.StorageConfig,
	collectionID int64,
) (*packed.FFIPackedReader, error) {
	inputSchema := buildInputSchema(schema)
	arrowSchema, err := storage.ConvertToArrowSchema(inputSchema, false)
	if err != nil {
		return nil, fmt.Errorf("convert input schema to arrow: %w", err)
	}
	reader, err := packed.NewFFIPackedReader(
		manifestPath, arrowSchema, inputColumns, defaultReadBufferSize,
		storageConfig, nil,
		packed.ExternalReaderContext{
			CollectionID: collectionID,
			Source:       schema.GetExternalSource(),
			Spec:         schema.GetExternalSpec(),
		},
	)
	if err != nil {
		return nil, fmt.Errorf("open input manifest: %w", err)
	}
	return reader, nil
}

// buildInputSchema returns a CollectionSchema with only fields that exist in
// external source data: drops function outputs and external system fields.
func buildInputSchema(schema *schemapb.CollectionSchema) *schemapb.CollectionSchema {
	inputSchema := &schemapb.CollectionSchema{Name: schema.GetName()}
	for _, f := range schema.GetFields() {
		if f.GetIsFunctionOutput() || typeutil.IsExternalSystemOrVirtualField(f.GetName()) {
			continue
		}
		inputSchema.Fields = append(inputSchema.Fields, f)
	}
	return inputSchema
}

func streamBatches(
	ctx context.Context,
	schema *schemapb.CollectionSchema,
	outputSchema *schemapb.CollectionSchema,
	outputArrow *arrow.Schema,
	reader *packed.FFIPackedReader,
	writer *packed.FFIPackedWriter,
	bm25Acc map[int64]*storage.BM25Stats,
	clusterID string,
) (int64, error) {
	var totalRows int64
	for {
		rec, err := reader.ReadNext()
		if err == io.EOF {
			break
		}
		if err != nil {
			return totalRows, fmt.Errorf("read input batch: %w", err)
		}
		if rec == nil {
			break
		}

		batch, err := storage.ArrowRecordToInsertData(rec, schema)
		rec.Release()
		if err != nil {
			return totalRows, fmt.Errorf("arrow→InsertData: %w", err)
		}
		if batch.GetRowNum() == 0 {
			continue
		}

		if err := embedding.RunAll(ctx, schema, batch, embedding.RunOptions{
			ClusterID: clusterID,
			DBName:    schema.GetDbName(),
		}); err != nil {
			return totalRows, fmt.Errorf("execute functions: %w", err)
		}

		if err := accumulateBM25Stats(batch, bm25Acc); err != nil {
			return totalRows, err
		}

		if err := writeOutputBatch(batch, outputSchema, outputArrow, writer); err != nil {
			return totalRows, fmt.Errorf("write output batch: %w", err)
		}
		totalRows += int64(batch.GetRowNum())
	}
	return totalRows, nil
}

func writeOutputBatch(
	batch *storage.InsertData,
	outputSchema *schemapb.CollectionSchema,
	outputArrow *arrow.Schema,
	writer *packed.FFIPackedWriter,
) error {
	builder := array.NewRecordBuilder(memory.DefaultAllocator, outputArrow)
	defer builder.Release()
	if err := storage.BuildRecord(builder, batch, outputSchema); err != nil {
		return err
	}
	rec := builder.NewRecord()
	defer rec.Release()
	return writer.WriteRecordBatch(rec)
}

// newBM25Accumulators creates a stats accumulator per BM25 output field id.
// Returns an empty map if the schema declares no BM25 functions.
func newBM25Accumulators(schema *schemapb.CollectionSchema) map[int64]*storage.BM25Stats {
	acc := make(map[int64]*storage.BM25Stats)
	for _, fn := range schema.GetFunctions() {
		if fn.GetType() != schemapb.FunctionType_BM25 {
			continue
		}
		for _, outID := range fn.GetOutputFieldIds() {
			acc[outID] = storage.NewBM25Stats()
		}
	}
	return acc
}

// accumulateBM25Stats appends per-batch sparse vectors into the running
// per-field stats accumulator. avgdl/IDF need full-segment counts, so the
// accumulator persists across batches and is serialized once at the end.
func accumulateBM25Stats(batch *storage.InsertData, acc map[int64]*storage.BM25Stats) error {
	for outID, stats := range acc {
		fd, ok := batch.Data[outID].(*storage.SparseFloatVectorFieldData)
		if !ok || fd == nil {
			return fmt.Errorf(
				"BM25 output field %d is not SparseFloatVectorFieldData (got %T); executeFunctions contract violated",
				outID, batch.Data[outID])
		}
		stats.AppendFieldData(fd)
	}
	return nil
}

// finalizeBM25Stats serializes each per-field accumulator, writes the blob to
// its packed BM25StatsPath, and registers entries on the manifest. Required
// by QueryNode's idf_oracle for BM25 search.
func finalizeBM25Stats(
	ctx context.Context,
	acc map[int64]*storage.BM25Stats,
	storageConfig *indexpb.StorageConfig,
	manifestPath string,
) (string, error) {
	if len(acc) == 0 {
		return manifestPath, nil
	}
	log := log.Ctx(ctx)

	basePath, _, err := packed.UnmarshalManifestPath(manifestPath)
	if err != nil {
		return "", fmt.Errorf("parse manifest path: %w", err)
	}

	entries := make([]packed.StatEntry, 0, len(acc))
	for outID, stats := range acc {
		blob, err := stats.Serialize()
		if err != nil {
			return "", fmt.Errorf("serialize bm25 stats for field %d: %w", outID, err)
		}
		fullPath := packed.BM25StatsPath(basePath, outID, 0)
		if err := packed.WriteFile(storageConfig, fullPath, blob); err != nil {
			return "", fmt.Errorf("write bm25 stats file %s: %w", fullPath, err)
		}
		entries = append(entries, packed.BM25StatsEntry(outID, fullPath, len(blob)))
		log.Info("registered bm25 stats",
			zap.Int64("fieldID", outID),
			zap.Int("bytes", len(blob)),
			zap.Int64("numRow", stats.NumRow()))
	}
	return packed.AddStatsToManifest(manifestPath, storageConfig, entries)
}
