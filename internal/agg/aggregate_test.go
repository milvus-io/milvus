package agg

import "testing"

func TestMatchAggregationExpression(t *testing.T) {
	tests := []struct {
		expression       string
		expectedIsValid  bool
		expectedOperator string
		expectedParam    string
	}{
		{"count(*)", true, "count", "*"},
		{"count(a)", true, "count", "a"},
		{"sum(b)", true, "sum", "b"},
		{"avg(c)", true, "avg", "c"},
		{"min(d)", true, "min", "d"},
		{"max(e)", true, "max", "e"},
		{"invalidExpression", false, "", ""},
		{"sum ( x )", true, "sum", "x"},
		{"SUM(Z)", true, "sum", "Z"},
		{"AVG( y )", true, "avg", "y"},
	}

	for _, test := range tests {
		isValid, operator, param := MatchAggregationExpression(test.expression)
		if isValid != test.expectedIsValid || operator != test.expectedOperator || param != test.expectedParam {
			t.Errorf("MatchAggregationExpression(%q) = (%v, %q, %q), want (%v, %q, %q)",
				test.expression, isValid, operator, param, test.expectedIsValid, test.expectedOperator, test.expectedParam)
		}
	}
}
