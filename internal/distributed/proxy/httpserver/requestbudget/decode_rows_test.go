package requestbudget

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestDecodeDataRowsPreservesOtherFields(t *testing.T) {
	body := []byte(`{"collectionName":"books","data":[{"id":1,"vector":[1,2]},{"id":2}],"partialUpdate":true}`)
	metadata, rows, err := DecodeDataRows(context.Background(), body, MaxJSONUnitBytes)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0]["id"] != float64(1) || rows[1]["id"] != float64(2) {
		t.Fatalf("decoded rows = %#v", rows)
	}
	var rest map[string]any
	if err := json.Unmarshal(metadata, &rest); err != nil {
		t.Fatal(err)
	}
	if rest["collectionName"] != "books" || rest["partialUpdate"] != true {
		t.Fatalf("metadata = %#v", rest)
	}
	if !bytes.Equal(body, []byte(`{"collectionName":"books","data":[{"id":1,"vector":[1,2]},{"id":2}],"partialUpdate":true}`)) {
		t.Fatal("original body was modified")
	}
}

func TestDecodeDataRowsRejectsOversizeBeforeSonic(t *testing.T) {
	body := []byte(`{"data":[{"text":"` + strings.Repeat("x", 100) + `"}]}`)
	_, _, err := DecodeDataRows(context.Background(), body, 64)
	if !errors.Is(err, merr.ErrParameterInvalid) {
		t.Fatalf("error = %v, want parameter invalid", err)
	}
}

func TestDecodeDataRowsRespectsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := DecodeDataRows(ctx, []byte(`{"data":[{"id":1}]}`), MaxJSONUnitBytes)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context canceled", err)
	}
}

type cancelAfterChecks struct {
	context.Context
	checks int
}

func (c *cancelAfterChecks) Err() error {
	c.checks++
	if c.checks >= 4 {
		return context.Canceled
	}
	return nil
}

func TestDecodeDataRowsChecksBetweenRows(t *testing.T) {
	ctx := &cancelAfterChecks{Context: context.Background()}
	_, _, err := DecodeDataRows(ctx, []byte(`{"data":[{"id":1},{"id":2},{"id":3},{"id":4}]}`), MaxJSONUnitBytes)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want cancellation between rows", err)
	}
}

func TestDecodeDataRowsFourMiBBoundary(t *testing.T) {
	// Account for the JSON object/string framing inside the row itself.
	row := `{"text":"` + strings.Repeat("x", MaxJSONUnitBytes-len(`{"text":""}`)) + `"}`
	body := []byte(`{"data":[` + row + `]}`)
	_, rows, err := DecodeDataRows(context.Background(), body, MaxJSONUnitBytes)
	if err != nil || len(rows) != 1 {
		t.Fatalf("exactly 4 MiB row: rows=%d error=%v", len(rows), err)
	}
	body = []byte(`{"data":[` + row[:len(row)-2] + `x"}]}`)
	_, _, err = DecodeDataRows(context.Background(), body, MaxJSONUnitBytes)
	if !errors.Is(err, merr.ErrParameterInvalid) {
		t.Fatalf("4 MiB + 1 row: error=%v, want parameter invalid", err)
	}
}

func TestDecodeDataRowsEscapedTopLevelDataKey(t *testing.T) {
	body := []byte(`{"collectionName":"books","d\u0061ta":[{"id":1}],"extra":{"data":[42]}}`)
	metadata, rows, err := DecodeDataRows(context.Background(), body, MaxJSONUnitBytes)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0]["id"] != float64(1) {
		t.Fatalf("rows = %#v", rows)
	}
	var rest map[string]any
	if err := json.Unmarshal(metadata, &rest); err != nil {
		t.Fatal(err)
	}
	if _, ok := rest["extra"].(map[string]any)["data"]; !ok {
		t.Fatalf("nested data was lost: %#v", rest)
	}
}

func TestDecodeDataRowsRejectsTrailingJSON(t *testing.T) {
	_, _, err := DecodeDataRows(context.Background(), []byte(`{"data":[{"id":1}]} garbage`), MaxJSONUnitBytes)
	if err == nil {
		t.Fatal("trailing content was accepted")
	}
}

func TestDecodeDataRowsRejectsOversizeMetadataUnit(t *testing.T) {
	body := []byte(`{"collectionName":"` + strings.Repeat("x", MaxJSONUnitBytes) + `","data":[{"id":1}]}`)
	_, _, err := DecodeDataRows(context.Background(), body, MaxJSONUnitBytes)
	if !errors.Is(err, merr.ErrParameterInvalid) {
		t.Fatalf("error = %v, want parameter invalid", err)
	}

	body = []byte(`{"collectionName":"` + strings.Repeat("x", MaxJSONUnitBytes) + `"}`)
	_, _, err = DecodeDataRows(context.Background(), body, MaxJSONUnitBytes)
	if !errors.Is(err, merr.ErrParameterInvalid) {
		t.Fatalf("without data: error = %v, want parameter invalid", err)
	}
}

func TestDecodeDataRowsRejectsDuplicateDataKey(t *testing.T) {
	body := []byte(`{"data":[{"id":1}],"d\u0061ta":[{"id":2}]}`)
	_, _, err := DecodeDataRows(context.Background(), body, MaxJSONUnitBytes)
	if !errors.Is(err, merr.ErrParameterInvalid) {
		t.Fatalf("error = %v, want ambiguous duplicate data rejected", err)
	}
}
