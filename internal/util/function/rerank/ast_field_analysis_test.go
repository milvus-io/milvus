package rerank

import (
	"testing"
)

func TestAnalyzeRequiredFields_SimpleMember(t *testing.T) {
	expr := "fields.price"
	fields := []string{"price", "qty"}
	req, idx, err := analyzeRequiredFields(expr, fields)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(req) != 1 || req[0] != "price" {
		t.Fatalf("unexpected required fields: %v", req)
	}
	if v, ok := idx["price"]; !ok || v != 0 {
		t.Fatalf("unexpected field index map: %v", idx)
	}
}

func TestAnalyzeRequiredFields_SliceIndex(t *testing.T) {
	expr := `fields["qty"]`
	fields := []string{"price", "qty"}
	req, idx, err := analyzeRequiredFields(expr, fields)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(req) != 1 || req[0] != "qty" {
		t.Fatalf("unexpected required fields: %v", req)
	}
	if v, ok := idx["qty"]; !ok || v != 1 {
		t.Fatalf("unexpected field index map: %v", idx)
	}
}

func TestAnalyzeRequiredFields_NoFields(t *testing.T) {
	expr := "score * 2"
	fields := []string{"price", "qty"}
	req, idx, err := analyzeRequiredFields(expr, fields)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(req) != 0 || len(idx) != 0 {
		t.Fatalf("expected no required fields, got req=%v idx=%v", req, idx)
	}
}
