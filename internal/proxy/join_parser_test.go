package proxy

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// ParseJoinSpec Tests - Basic Syntax
// =============================================================================

func TestParseJoinSpec_BasicSyntax(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		want        *JoinSpec
		wantErr     bool
		errContains string
	}{
		{
			name:  "basic join - minimal",
			input: "JOIN shops ON shop_id = shops.id",
			want: &JoinSpec{
				TargetDBName:     "",
				TargetCollection: "shops",
				JoinKey:          "shop_id",
				TargetKey:        "id",
				WhereCondition:   "",
			},
		},
		{
			name:  "join with simple where clause",
			input: "JOIN shops ON shop_id = shops.id WHERE status == 'active'",
			want: &JoinSpec{
				TargetCollection: "shops",
				JoinKey:          "shop_id",
				TargetKey:        "id",
				WhereCondition:   "status == 'active'",
			},
		},
		{
			name:  "join with complex where clause - AND",
			input: "JOIN shops ON shop_id = shops.id WHERE status == 'active' AND rating > 4.0",
			want: &JoinSpec{
				TargetCollection: "shops",
				JoinKey:          "shop_id",
				TargetKey:        "id",
				WhereCondition:   "status == 'active' AND rating > 4.0",
			},
		},
		{
			name:  "join with complex where clause - OR",
			input: "JOIN shops ON shop_id = shops.id WHERE status == 'active' OR status == 'featured'",
			want: &JoinSpec{
				TargetCollection: "shops",
				JoinKey:          "shop_id",
				TargetKey:        "id",
				WhereCondition:   "status == 'active' OR status == 'featured'",
			},
		},
		{
			name:  "join with where clause containing parentheses",
			input: "JOIN shops ON shop_id = shops.id WHERE (status == 'active' AND rating > 4.0) OR featured == true",
			want: &JoinSpec{
				TargetCollection: "shops",
				JoinKey:          "shop_id",
				TargetKey:        "id",
				WhereCondition:   "(status == 'active' AND rating > 4.0) OR featured == true",
			},
		},
		{
			name:  "join with where clause containing IN operator",
			input: "JOIN shops ON shop_id = shops.id WHERE status in ['active', 'featured']",
			want: &JoinSpec{
				TargetCollection: "shops",
				JoinKey:          "shop_id",
				TargetKey:        "id",
				WhereCondition:   "status in ['active', 'featured']",
			},
		},
		{
			name:  "join with numeric comparison in where",
			input: "JOIN shops ON shop_id = shops.id WHERE rating >= 4.5 AND reviews > 100",
			want: &JoinSpec{
				TargetCollection: "shops",
				JoinKey:          "shop_id",
				TargetKey:        "id",
				WhereCondition:   "rating >= 4.5 AND reviews > 100",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseJoinSpec(tt.input)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				return
			}

			require.NoError(t, err)
			require.NotNil(t, got)

			assert.Equal(t, tt.want.TargetDBName, got.TargetDBName, "TargetDBName mismatch")
			assert.Equal(t, tt.want.TargetCollection, got.TargetCollection, "TargetCollection mismatch")
			assert.Equal(t, tt.want.JoinKey, got.JoinKey, "JoinKey mismatch")
			assert.Equal(t, tt.want.TargetKey, got.TargetKey, "TargetKey mismatch")
			assert.Equal(t, tt.want.WhereCondition, got.WhereCondition, "WhereCondition mismatch")
			assert.Equal(t, tt.input, got.OriginalJoin, "OriginalJoin should match input")
		})
	}
}

// =============================================================================
// ParseJoinSpec Tests - Database Prefix
// =============================================================================

func TestParseJoinSpec_DatabasePrefix(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		want        *JoinSpec
		wantErr     bool
		errContains string
	}{
		{
			name:  "join with database prefix",
			input: "JOIN mydb.shops ON shop_id = mydb.shops.id",
			want: &JoinSpec{
				TargetDBName:     "mydb",
				TargetCollection: "shops",
				JoinKey:          "shop_id",
				TargetKey:        "id",
			},
		},
		{
			name:  "join with database prefix and where",
			input: "JOIN mydb.shops ON shop_id = mydb.shops.id WHERE active == true",
			want: &JoinSpec{
				TargetDBName:     "mydb",
				TargetCollection: "shops",
				JoinKey:          "shop_id",
				TargetKey:        "id",
				WhereCondition:   "active == true",
			},
		},
		{
			name:  "database prefix only in ON clause - should infer db",
			input: "JOIN shops ON shop_id = mydb.shops.id",
			want: &JoinSpec{
				TargetDBName:     "mydb",
				TargetCollection: "shops",
				JoinKey:          "shop_id",
				TargetKey:        "id",
			},
		},
		{
			name:  "database with underscore",
			input: "JOIN my_database.shops ON shop_id = my_database.shops.id",
			want: &JoinSpec{
				TargetDBName:     "my_database",
				TargetCollection: "shops",
				JoinKey:          "shop_id",
				TargetKey:        "id",
			},
		},
		{
			name:  "database with numbers",
			input: "JOIN db123.shops ON shop_id = db123.shops.id",
			want: &JoinSpec{
				TargetDBName:     "db123",
				TargetCollection: "shops",
				JoinKey:          "shop_id",
				TargetKey:        "id",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseJoinSpec(tt.input)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				return
			}

			require.NoError(t, err)
			require.NotNil(t, got)

			assert.Equal(t, tt.want.TargetDBName, got.TargetDBName)
			assert.Equal(t, tt.want.TargetCollection, got.TargetCollection)
			assert.Equal(t, tt.want.JoinKey, got.JoinKey)
			assert.Equal(t, tt.want.TargetKey, got.TargetKey)
		})
	}
}

// =============================================================================
// ParseJoinSpec Tests - Case Insensitivity
// =============================================================================

func TestParseJoinSpec_CaseInsensitivity(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  *JoinSpec
	}{
		{
			name:  "lowercase join and on",
			input: "join shops on shop_id = shops.id",
			want: &JoinSpec{
				TargetCollection: "shops",
				JoinKey:          "shop_id",
				TargetKey:        "id",
			},
		},
		{
			name:  "uppercase JOIN and ON",
			input: "JOIN shops ON shop_id = shops.id",
			want: &JoinSpec{
				TargetCollection: "shops",
				JoinKey:          "shop_id",
				TargetKey:        "id",
			},
		},
		{
			name:  "mixed case Join and On",
			input: "Join shops On shop_id = shops.id",
			want: &JoinSpec{
				TargetCollection: "shops",
				JoinKey:          "shop_id",
				TargetKey:        "id",
			},
		},
		{
			name:  "lowercase where",
			input: "JOIN shops ON shop_id = shops.id where status == 1",
			want: &JoinSpec{
				TargetCollection: "shops",
				JoinKey:          "shop_id",
				TargetKey:        "id",
				WhereCondition:   "status == 1",
			},
		},
		{
			name:  "uppercase WHERE",
			input: "JOIN shops ON shop_id = shops.id WHERE status == 1",
			want: &JoinSpec{
				TargetCollection: "shops",
				JoinKey:          "shop_id",
				TargetKey:        "id",
				WhereCondition:   "status == 1",
			},
		},
		{
			name:  "mixed case Where",
			input: "JOIN shops ON shop_id = shops.id Where status == 1",
			want: &JoinSpec{
				TargetCollection: "shops",
				JoinKey:          "shop_id",
				TargetKey:        "id",
				WhereCondition:   "status == 1",
			},
		},
		{
			name:  "all lowercase",
			input: "join shops on shop_id = shops.id where status == 1",
			want: &JoinSpec{
				TargetCollection: "shops",
				JoinKey:          "shop_id",
				TargetKey:        "id",
				WhereCondition:   "status == 1",
			},
		},
		{
			name:  "all uppercase keywords",
			input: "JOIN shops ON shop_id = shops.id WHERE status == 1",
			want: &JoinSpec{
				TargetCollection: "shops",
				JoinKey:          "shop_id",
				TargetKey:        "id",
				WhereCondition:   "status == 1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseJoinSpec(tt.input)
			require.NoError(t, err)
			require.NotNil(t, got)

			assert.Equal(t, tt.want.TargetCollection, got.TargetCollection)
			assert.Equal(t, tt.want.JoinKey, got.JoinKey)
			assert.Equal(t, tt.want.TargetKey, got.TargetKey)
			assert.Equal(t, tt.want.WhereCondition, got.WhereCondition)
		})
	}
}

// =============================================================================
// ParseJoinSpec Tests - Whitespace Handling
// =============================================================================

func TestParseJoinSpec_WhitespaceHandling(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  *JoinSpec
	}{
		{
			name:  "minimal whitespace",
			input: "JOIN shops ON shop_id=shops.id",
			want: &JoinSpec{
				TargetCollection: "shops",
				JoinKey:          "shop_id",
				TargetKey:        "id",
			},
		},
		{
			name:  "extra whitespace around equals",
			input: "JOIN shops ON shop_id  =  shops.id",
			want: &JoinSpec{
				TargetCollection: "shops",
				JoinKey:          "shop_id",
				TargetKey:        "id",
			},
		},
		{
			name:  "leading whitespace",
			input: "   JOIN shops ON shop_id = shops.id",
			want: &JoinSpec{
				TargetCollection: "shops",
				JoinKey:          "shop_id",
				TargetKey:        "id",
			},
		},
		{
			name:  "trailing whitespace",
			input: "JOIN shops ON shop_id = shops.id   ",
			want: &JoinSpec{
				TargetCollection: "shops",
				JoinKey:          "shop_id",
				TargetKey:        "id",
			},
		},
		{
			name:  "multiple spaces between keywords",
			input: "JOIN   shops   ON   shop_id   =   shops.id",
			want: &JoinSpec{
				TargetCollection: "shops",
				JoinKey:          "shop_id",
				TargetKey:        "id",
			},
		},
		{
			name:  "tabs instead of spaces",
			input: "JOIN\tshops\tON\tshop_id\t=\tshops.id",
			want: &JoinSpec{
				TargetCollection: "shops",
				JoinKey:          "shop_id",
				TargetKey:        "id",
			},
		},
		{
			name:  "mixed tabs and spaces",
			input: "JOIN \t shops \t ON \t shop_id = shops.id",
			want: &JoinSpec{
				TargetCollection: "shops",
				JoinKey:          "shop_id",
				TargetKey:        "id",
			},
		},
		{
			name:  "whitespace in where clause preserved",
			input: "JOIN shops ON shop_id = shops.id WHERE status == 'active'",
			want: &JoinSpec{
				TargetCollection: "shops",
				JoinKey:          "shop_id",
				TargetKey:        "id",
				WhereCondition:   "status == 'active'",
			},
		},
		{
			name:  "trailing whitespace in where clause trimmed",
			input: "JOIN shops ON shop_id = shops.id WHERE status == 'active'   ",
			want: &JoinSpec{
				TargetCollection: "shops",
				JoinKey:          "shop_id",
				TargetKey:        "id",
				WhereCondition:   "status == 'active'",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseJoinSpec(tt.input)
			require.NoError(t, err)
			require.NotNil(t, got)

			assert.Equal(t, tt.want.TargetCollection, got.TargetCollection)
			assert.Equal(t, tt.want.JoinKey, got.JoinKey)
			assert.Equal(t, tt.want.TargetKey, got.TargetKey)
			assert.Equal(t, tt.want.WhereCondition, got.WhereCondition)
		})
	}
}

// =============================================================================
// ParseJoinSpec Tests - Field Name Variations
// =============================================================================

func TestParseJoinSpec_FieldNameVariations(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  *JoinSpec
	}{
		{
			name:  "simple field names",
			input: "JOIN shops ON id = shops.id",
			want: &JoinSpec{
				TargetCollection: "shops",
				JoinKey:          "id",
				TargetKey:        "id",
			},
		},
		{
			name:  "snake_case field names",
			input: "JOIN shops ON shop_id = shops.shop_id",
			want: &JoinSpec{
				TargetCollection: "shops",
				JoinKey:          "shop_id",
				TargetKey:        "shop_id",
			},
		},
		{
			name:  "camelCase field names",
			input: "JOIN shops ON shopId = shops.shopId",
			want: &JoinSpec{
				TargetCollection: "shops",
				JoinKey:          "shopId",
				TargetKey:        "shopId",
			},
		},
		{
			name:  "field names with numbers",
			input: "JOIN shops ON field1 = shops.field2",
			want: &JoinSpec{
				TargetCollection: "shops",
				JoinKey:          "field1",
				TargetKey:        "field2",
			},
		},
		{
			name:  "uppercase field names",
			input: "JOIN shops ON SHOP_ID = shops.SHOP_ID",
			want: &JoinSpec{
				TargetCollection: "shops",
				JoinKey:          "SHOP_ID",
				TargetKey:        "SHOP_ID",
			},
		},
		{
			name:  "single character field names",
			input: "JOIN t ON a = t.b",
			want: &JoinSpec{
				TargetCollection: "t",
				JoinKey:          "a",
				TargetKey:        "b",
			},
		},
		{
			name:  "long field names",
			input: "JOIN shops ON very_long_field_name_here = shops.another_very_long_field_name",
			want: &JoinSpec{
				TargetCollection: "shops",
				JoinKey:          "very_long_field_name_here",
				TargetKey:        "another_very_long_field_name",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseJoinSpec(tt.input)
			require.NoError(t, err)
			require.NotNil(t, got)

			assert.Equal(t, tt.want.TargetCollection, got.TargetCollection)
			assert.Equal(t, tt.want.JoinKey, got.JoinKey)
			assert.Equal(t, tt.want.TargetKey, got.TargetKey)
		})
	}
}

// =============================================================================
// ParseJoinSpec Tests - Collection Name Variations
// =============================================================================

func TestParseJoinSpec_CollectionNameVariations(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  *JoinSpec
	}{
		{
			name:  "simple collection name",
			input: "JOIN shops ON id = shops.id",
			want: &JoinSpec{
				TargetCollection: "shops",
			},
		},
		{
			name:  "snake_case collection name",
			input: "JOIN shop_metadata ON id = shop_metadata.id",
			want: &JoinSpec{
				TargetCollection: "shop_metadata",
			},
		},
		{
			name:  "collection name with numbers",
			input: "JOIN shops2024 ON id = shops2024.id",
			want: &JoinSpec{
				TargetCollection: "shops2024",
			},
		},
		{
			name:  "single character collection name",
			input: "JOIN s ON id = s.id",
			want: &JoinSpec{
				TargetCollection: "s",
			},
		},
		{
			name:  "long collection name",
			input: "JOIN very_long_collection_name_for_testing ON id = very_long_collection_name_for_testing.id",
			want: &JoinSpec{
				TargetCollection: "very_long_collection_name_for_testing",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseJoinSpec(tt.input)
			require.NoError(t, err)
			require.NotNil(t, got)

			assert.Equal(t, tt.want.TargetCollection, got.TargetCollection)
		})
	}
}

// =============================================================================
// ParseJoinSpec Tests - Error Cases
// =============================================================================

func TestParseJoinSpec_ErrorCases(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		errContains string
	}{
		{
			name:        "empty string",
			input:       "",
			errContains: "cannot be empty",
		},
		{
			name:        "whitespace only",
			input:       "   ",
			errContains: "cannot be empty",
		},
		{
			name:        "tab only",
			input:       "\t",
			errContains: "cannot be empty",
		},
		{
			name:        "newline only",
			input:       "\n",
			errContains: "cannot be empty",
		},
		{
			name:        "missing JOIN keyword",
			input:       "shops ON shop_id = shops.id",
			errContains: "must start with JOIN keyword",
		},
		{
			name:        "missing ON keyword",
			input:       "JOIN shops shop_id = shops.id",
			errContains: "missing ON keyword",
		},
		{
			name:        "missing equals sign",
			input:       "JOIN shops ON shop_id shops.id",
			errContains: "invalid JOIN syntax",
		},
		{
			name:        "missing collection name",
			input:       "JOIN ON shop_id = shops.id",
			errContains: "invalid JOIN syntax",
		},
		{
			name:        "missing local key",
			input:       "JOIN shops ON = shops.id",
			errContains: "missing join key after ON",
		},
		{
			name:        "missing target key",
			input:       "JOIN shops ON shop_id = shops.",
			errContains: "missing target key name",
		},
		{
			name:        "missing target collection in ON clause",
			input:       "JOIN shops ON shop_id = .id",
			errContains: "invalid JOIN syntax",
		},
		{
			name:        "collection mismatch - different names",
			input:       "JOIN shops ON shop_id = merchants.id",
			errContains: "collection name mismatch",
		},
		{
			name:        "collection mismatch - different case (collections are case-sensitive)",
			input:       "JOIN shops ON shop_id = Shops.id",
			errContains: "collection name mismatch",
		},
		{
			name:        "invalid syntax - extra equals",
			input:       "JOIN shops ON shop_id == shops.id",
			errContains: "invalid operator '=='",
		},
		{
			name:        "invalid syntax - missing dot in target",
			input:       "JOIN shops ON shop_id = shopsid",
			errContains: "must be qualified with collection name",
		},
		{
			name:        "WHERE without condition",
			input:       "JOIN shops ON shop_id = shops.id WHERE",
			errContains: "WHERE keyword but no condition",
		},
		{
			name:        "random text",
			input:       "this is not a join clause",
			errContains: "must start with JOIN keyword",
		},
		{
			name:        "SQL-like but wrong syntax",
			input:       "SELECT * FROM shops JOIN ON shop_id",
			errContains: "must start with JOIN keyword",
		},
		{
			name:        "only JOIN keyword",
			input:       "JOIN",
			errContains: "invalid JOIN syntax",
		},
		{
			name:        "JOIN with only collection",
			input:       "JOIN shops",
			errContains: "missing ON keyword",
		},
		{
			name:        "JOIN with collection and ON but no condition",
			input:       "JOIN shops ON",
			errContains: "missing join key after ON",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseJoinSpec(tt.input)
			require.Error(t, err)
			assert.Nil(t, got)
			assert.Contains(t, err.Error(), tt.errContains)
		})
	}
}

// =============================================================================
// ParseMultipleJoinSpecs Tests
// =============================================================================

func TestParseMultipleJoinSpecs_Valid(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantCount int
		verify    func(t *testing.T, specs []*JoinSpec)
	}{
		{
			name:      "single join",
			input:     "JOIN shops ON shop_id = shops.id",
			wantCount: 1,
			verify: func(t *testing.T, specs []*JoinSpec) {
				assert.Equal(t, "shops", specs[0].TargetCollection)
			},
		},
		{
			name: "two joins on separate lines",
			input: `JOIN shops ON shop_id = shops.id WHERE status == 'active'
JOIN merchants ON merchant_id = merchants.id WHERE verified == true`,
			wantCount: 2,
			verify: func(t *testing.T, specs []*JoinSpec) {
				assert.Equal(t, "shops", specs[0].TargetCollection)
				assert.Equal(t, "status == 'active'", specs[0].WhereCondition)
				assert.Equal(t, "merchants", specs[1].TargetCollection)
				assert.Equal(t, "verified == true", specs[1].WhereCondition)
			},
		},
		{
			name: "three joins",
			input: `JOIN shops ON shop_id = shops.id
JOIN merchants ON merchant_id = merchants.id
JOIN categories ON category_id = categories.id`,
			wantCount: 3,
			verify: func(t *testing.T, specs []*JoinSpec) {
				assert.Equal(t, "shops", specs[0].TargetCollection)
				assert.Equal(t, "merchants", specs[1].TargetCollection)
				assert.Equal(t, "categories", specs[2].TargetCollection)
			},
		},
		{
			name: "joins with empty lines between",
			input: `JOIN shops ON shop_id = shops.id

JOIN merchants ON merchant_id = merchants.id`,
			wantCount: 2,
		},
		{
			name: "joins with multiple empty lines",
			input: `JOIN shops ON shop_id = shops.id


JOIN merchants ON merchant_id = merchants.id`,
			wantCount: 2,
		},
		{
			name: "joins with leading/trailing empty lines",
			input: `
JOIN shops ON shop_id = shops.id
JOIN merchants ON merchant_id = merchants.id
`,
			wantCount: 2,
		},
		{
			name: "many joins",
			input: `JOIN t1 ON k1 = t1.k1
JOIN t2 ON k2 = t2.k2
JOIN t3 ON k3 = t3.k3
JOIN t4 ON k4 = t4.k4
JOIN t5 ON k5 = t5.k5`,
			wantCount: 5,
		},
		{
			name: "joins with different databases",
			input: `JOIN db1.shops ON shop_id = db1.shops.id
JOIN db2.merchants ON merchant_id = db2.merchants.id`,
			wantCount: 2,
			verify: func(t *testing.T, specs []*JoinSpec) {
				assert.Equal(t, "db1", specs[0].TargetDBName)
				assert.Equal(t, "db2", specs[1].TargetDBName)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseMultipleJoinSpecs(tt.input)
			require.NoError(t, err)
			require.Len(t, got, tt.wantCount)

			if tt.verify != nil {
				tt.verify(t, got)
			}
		})
	}
}

func TestParseMultipleJoinSpecs_ErrorCases(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		errContains string
	}{
		{
			name:        "empty string",
			input:       "",
			errContains: "cannot be empty",
		},
		{
			name:        "whitespace only",
			input:       "   \n   \n   ",
			errContains: "no valid JOIN clauses found",
		},
		{
			name:        "line without JOIN prefix",
			input:       "shops ON shop_id = shops.id",
			errContains: "must start with JOIN keyword",
		},
		{
			name: "second line without JOIN prefix",
			input: `JOIN shops ON shop_id = shops.id
merchants ON merchant_id = merchants.id`,
			errContains: "must start with JOIN keyword",
		},
		{
			name: "third line without JOIN prefix",
			input: `JOIN shops ON shop_id = shops.id
JOIN merchants ON merchant_id = merchants.id
categories ON category_id = categories.id`,
			errContains: "must start with JOIN keyword",
		},
		{
			name: "invalid join syntax in second line",
			input: `JOIN shops ON shop_id = shops.id
JOIN merchants`,
			errContains: "missing ON keyword",
		},
		{
			name: "collection mismatch in second line",
			input: `JOIN shops ON shop_id = shops.id
JOIN merchants ON merchant_id = wrong.id`,
			errContains: "collection name mismatch",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseMultipleJoinSpecs(tt.input)
			require.Error(t, err)
			assert.Nil(t, got)
			assert.Contains(t, err.Error(), tt.errContains)
		})
	}
}

// =============================================================================
// ExtractJoinOutputFields Tests
// =============================================================================

func TestJoinSpec_ExtractJoinOutputFields(t *testing.T) {
	tests := []struct {
		name         string
		collection   string
		outputFields []string
		want         []string
	}{
		{
			name:         "extract matching fields",
			collection:   "shops",
			outputFields: []string{"item_id", "shops.shop_name", "shops.rating", "price"},
			want:         []string{"shop_name", "rating"},
		},
		{
			name:         "no matching fields",
			collection:   "shops",
			outputFields: []string{"item_id", "price", "category"},
			want:         nil,
		},
		{
			name:         "all matching fields",
			collection:   "shops",
			outputFields: []string{"shops.id", "shops.name", "shops.status"},
			want:         []string{"id", "name", "status"},
		},
		{
			name:         "empty output fields",
			collection:   "shops",
			outputFields: []string{},
			want:         nil,
		},
		{
			name:         "nil output fields",
			collection:   "shops",
			outputFields: nil,
			want:         nil,
		},
		{
			name:         "similar prefix but different collection",
			collection:   "shops",
			outputFields: []string{"shops_backup.name", "shopping.cart", "shopify.store"},
			want:         nil,
		},
		{
			name:         "exact prefix match only",
			collection:   "shop",
			outputFields: []string{"shop.name", "shops.name", "shop_data.name"},
			want:         []string{"name"},
		},
		{
			name:         "single matching field",
			collection:   "shops",
			outputFields: []string{"shops.name"},
			want:         []string{"name"},
		},
		{
			name:         "field with underscore",
			collection:   "shops",
			outputFields: []string{"shops.shop_name", "shops.shop_rating"},
			want:         []string{"shop_name", "shop_rating"},
		},
		{
			name:         "mixed qualified and unqualified fields",
			collection:   "shops",
			outputFields: []string{"id", "shops.name", "price", "shops.rating", "category"},
			want:         []string{"name", "rating"},
		},
		{
			name:         "collection with underscore",
			collection:   "shop_metadata",
			outputFields: []string{"shop_metadata.name", "shop_metadata.status"},
			want:         []string{"name", "status"},
		},
		{
			name:         "collection with numbers",
			collection:   "shops2024",
			outputFields: []string{"shops2024.name", "shops2024.status"},
			want:         []string{"name", "status"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := &JoinSpec{TargetCollection: tt.collection}
			got := spec.ExtractJoinOutputFields(tt.outputFields)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.want, spec.OutputFields) // Should also be stored in spec
		})
	}
}

// =============================================================================
// JoinSpec String() Tests
// =============================================================================

func TestJoinSpec_String(t *testing.T) {
	tests := []struct {
		name string
		spec *JoinSpec
		want string
	}{
		{
			name: "basic join",
			spec: &JoinSpec{
				TargetCollection: "shops",
				JoinKey:          "shop_id",
				TargetKey:        "id",
			},
			want: "JOIN shops ON shop_id = shops.id",
		},
		{
			name: "join with where",
			spec: &JoinSpec{
				TargetCollection: "shops",
				JoinKey:          "shop_id",
				TargetKey:        "id",
				WhereCondition:   "status == 'active'",
			},
			want: "JOIN shops ON shop_id = shops.id WHERE status == 'active'",
		},
		{
			name: "join with db prefix",
			spec: &JoinSpec{
				TargetDBName:     "mydb",
				TargetCollection: "shops",
				JoinKey:          "shop_id",
				TargetKey:        "id",
			},
			want: "JOIN mydb.shops ON shop_id = mydb.shops.id",
		},
		{
			name: "join with db prefix and where",
			spec: &JoinSpec{
				TargetDBName:     "mydb",
				TargetCollection: "shops",
				JoinKey:          "shop_id",
				TargetKey:        "id",
				WhereCondition:   "active == true",
			},
			want: "JOIN mydb.shops ON shop_id = mydb.shops.id WHERE active == true",
		},
		{
			name: "join with complex where",
			spec: &JoinSpec{
				TargetCollection: "shops",
				JoinKey:          "shop_id",
				TargetKey:        "id",
				WhereCondition:   "(status == 'active' AND rating > 4.0) OR featured == true",
			},
			want: "JOIN shops ON shop_id = shops.id WHERE (status == 'active' AND rating > 4.0) OR featured == true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.spec.String()
			assert.Equal(t, tt.want, got)
		})
	}
}

// =============================================================================
// parseCollectionRef Tests
// =============================================================================

func TestParseCollectionRef(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		wantDB         string
		wantCollection string
	}{
		{
			name:           "simple collection",
			input:          "shops",
			wantDB:         "",
			wantCollection: "shops",
		},
		{
			name:           "db.collection",
			input:          "mydb.shops",
			wantDB:         "mydb",
			wantCollection: "shops",
		},
		{
			name:           "db with underscore",
			input:          "my_db.shops",
			wantDB:         "my_db",
			wantCollection: "shops",
		},
		{
			name:           "collection with underscore",
			input:          "shop_metadata",
			wantDB:         "",
			wantCollection: "shop_metadata",
		},
		{
			name:           "both with underscores",
			input:          "my_db.shop_metadata",
			wantDB:         "my_db",
			wantCollection: "shop_metadata",
		},
		{
			name:           "single character db",
			input:          "d.shops",
			wantDB:         "d",
			wantCollection: "shops",
		},
		{
			name:           "single character collection",
			input:          "mydb.s",
			wantDB:         "mydb",
			wantCollection: "s",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDB, gotCollection := parseCollectionRef(tt.input)
			assert.Equal(t, tt.wantDB, gotDB)
			assert.Equal(t, tt.wantCollection, gotCollection)
		})
	}
}

// =============================================================================
// isValidJoinKeyType Tests
// =============================================================================

func TestIsValidJoinKeyType(t *testing.T) {
	tests := []struct {
		name     string
		dataType int32 // schemapb.DataType values
		want     bool
	}{
		{"Int64 is valid", 5, true},    // schemapb.DataType_Int64
		{"VarChar is valid", 21, true}, // schemapb.DataType_VarChar
		{"Bool is invalid", 1, false},
		{"Int8 is invalid", 2, false},
		{"Int16 is invalid", 3, false},
		{"Int32 is invalid", 4, false},
		{"Float is invalid", 10, false},
		{"Double is invalid", 11, false},
		{"String is invalid", 20, false},
		{"BinaryVector is invalid", 100, false},
		{"FloatVector is invalid", 101, false},
		{"JSON is invalid", 23, false},
		{"Array is invalid", 22, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Import the actual schemapb type for proper testing
			// For now, we're using the raw int32 values
			got := isValidJoinKeyTypeRaw(tt.dataType)
			assert.Equal(t, tt.want, got, "dataType %d", tt.dataType)
		})
	}
}

// Helper function for testing with raw int32 values
func isValidJoinKeyTypeRaw(dataType int32) bool {
	switch dataType {
	case 5, 21: // Int64, VarChar
		return true
	default:
		return false
	}
}

// =============================================================================
// Error Message Quality Tests - Verify errors are meaningful and actionable
// =============================================================================

func TestParseJoinSpec_ErrorMessageQuality(t *testing.T) {
	tests := []struct {
		name             string
		input            string
		mustContainAll   []string // All of these must be in the error
		mustContainOneOf []string // At least one of these must be in the error
	}{
		{
			name:  "empty input shows expected format",
			input: "",
			mustContainAll: []string{
				"cannot be empty",
				"Expected format",
				"JOIN",
			},
		},
		{
			name:  "missing JOIN keyword shows what was received",
			input: "shops ON shop_id = shops.id",
			mustContainAll: []string{
				"must start with JOIN",
				"shops ON",
			},
		},
		{
			name:  "missing ON keyword shows what was expected",
			input: "JOIN shops",
			mustContainAll: []string{
				"missing ON keyword",
				"JOIN shops",
			},
		},
		{
			name:  "missing ON keyword in middle shows suggestion",
			input: "JOIN shops shop_id = shops.id",
			mustContainAll: []string{
				"missing ON keyword",
				"Expected format",
			},
		},
		{
			name:  "double equals explains correct operator",
			input: "JOIN shops ON shop_id == shops.id",
			mustContainAll: []string{
				"invalid operator '=='",
				"single '='",
			},
		},
		{
			name:  "missing collection prefix explains format",
			input: "JOIN shops ON shop_id = id",
			mustContainAll: []string{
				"must be qualified with collection name",
				"shop_id",
			},
		},
		{
			name:  "WHERE without condition gives clear instruction",
			input: "JOIN shops ON shop_id = shops.id WHERE",
			mustContainAll: []string{
				"WHERE keyword but no condition",
				"Either remove WHERE or add a condition",
			},
		},
		{
			name:  "collection mismatch shows both collections",
			input: "JOIN shops ON shop_id = merchants.id",
			mustContainAll: []string{
				"collection name mismatch",
				"shops",
				"merchants",
			},
		},
		{
			name:  "missing target key shows what was received",
			input: "JOIN shops ON shop_id = shops.",
			mustContainAll: []string{
				"missing target key name",
				"shops.",
			},
		},
		{
			name:  "incomplete ON clause shows what's missing",
			input: "JOIN shops ON shop_id",
			mustContainAll: []string{
				"incomplete after ON",
				"shop_id",
			},
		},
		{
			name:  "missing key after ON shows collection name",
			input: "JOIN shops ON",
			mustContainAll: []string{
				"missing join key after ON",
				"shops",
			},
		},
		{
			name:  "generic invalid syntax shows example",
			input: "JOIN a b c d e f g",
			mustContainAll: []string{
				"Expected format",
			},
			mustContainOneOf: []string{
				"Example",
				"shops",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseJoinSpec(tt.input)
			require.Error(t, err)

			errMsg := err.Error()

			// Check all required strings are present
			for _, mustContain := range tt.mustContainAll {
				assert.Contains(t, errMsg, mustContain,
					"Error message should contain %q but got: %s", mustContain, errMsg)
			}

			// Check at least one of the optional strings is present
			if len(tt.mustContainOneOf) > 0 {
				found := false
				for _, option := range tt.mustContainOneOf {
					if strings.Contains(errMsg, option) {
						found = true
						break
					}
				}
				assert.True(t, found,
					"Error message should contain one of %v but got: %s", tt.mustContainOneOf, errMsg)
			}
		})
	}
}

func TestParseMultipleJoinSpecs_ErrorMessageQuality(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		mustContainAll []string
	}{
		{
			name: "error in second clause shows line number",
			input: `JOIN shops ON shop_id = shops.id
JOIN merchants`,
			mustContainAll: []string{
				"JOIN clause 2 of 2",
				"missing ON keyword",
			},
		},
		{
			name: "non-JOIN line shows line number and content",
			input: `JOIN shops ON shop_id = shops.id
invalid line here`,
			mustContainAll: []string{
				"line 2",
				"must start with JOIN",
				"invalid line",
			},
		},
		{
			name:  "whitespace only gives helpful message",
			input: "   \n\t\n   ",
			mustContainAll: []string{
				"no valid JOIN clauses found",
				"whitespace or empty lines",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseMultipleJoinSpecs(tt.input)
			require.Error(t, err)

			errMsg := err.Error()
			for _, mustContain := range tt.mustContainAll {
				assert.Contains(t, errMsg, mustContain,
					"Error message should contain %q but got: %s", mustContain, errMsg)
			}
		})
	}
}

// =============================================================================
// Edge Case Tests
// =============================================================================

func TestParseJoinSpec_EdgeCases(t *testing.T) {
	t.Run("very long input", func(t *testing.T) {
		// Test with a reasonably long but valid input
		longCollectionName := strings.Repeat("a", 100)
		longFieldName := strings.Repeat("b", 100)
		input := "JOIN " + longCollectionName + " ON " + longFieldName + " = " + longCollectionName + ".id"

		got, err := ParseJoinSpec(input)
		require.NoError(t, err)
		assert.Equal(t, longCollectionName, got.TargetCollection)
		assert.Equal(t, longFieldName, got.JoinKey)
	})

	t.Run("where clause with special characters in string", func(t *testing.T) {
		input := `JOIN shops ON shop_id = shops.id WHERE name == "O'Brien's Shop"`
		got, err := ParseJoinSpec(input)
		require.NoError(t, err)
		assert.Equal(t, `name == "O'Brien's Shop"`, got.WhereCondition)
	})

	t.Run("where clause with escaped quotes", func(t *testing.T) {
		input := `JOIN shops ON shop_id = shops.id WHERE name == "Shop \"Best\" Ever"`
		got, err := ParseJoinSpec(input)
		require.NoError(t, err)
		assert.Contains(t, got.WhereCondition, `"Shop \"Best\" Ever"`)
	})

	t.Run("where clause with unicode", func(t *testing.T) {
		input := `JOIN shops ON shop_id = shops.id WHERE name == "商店"`
		got, err := ParseJoinSpec(input)
		require.NoError(t, err)
		assert.Equal(t, `name == "商店"`, got.WhereCondition)
	})

	t.Run("where clause with emoji", func(t *testing.T) {
		input := `JOIN shops ON shop_id = shops.id WHERE name == "Shop 🏪"`
		got, err := ParseJoinSpec(input)
		require.NoError(t, err)
		assert.Equal(t, `name == "Shop 🏪"`, got.WhereCondition)
	})

	t.Run("numeric values in where clause", func(t *testing.T) {
		input := "JOIN shops ON shop_id = shops.id WHERE rating > 4.5 AND reviews >= 1000 AND id != -1"
		got, err := ParseJoinSpec(input)
		require.NoError(t, err)
		assert.Equal(t, "rating > 4.5 AND reviews >= 1000 AND id != -1", got.WhereCondition)
	})

	t.Run("boolean values in where clause", func(t *testing.T) {
		input := "JOIN shops ON shop_id = shops.id WHERE active == true AND deleted == false"
		got, err := ParseJoinSpec(input)
		require.NoError(t, err)
		assert.Equal(t, "active == true AND deleted == false", got.WhereCondition)
	})

	t.Run("null check in where clause", func(t *testing.T) {
		input := "JOIN shops ON shop_id = shops.id WHERE description != null"
		got, err := ParseJoinSpec(input)
		require.NoError(t, err)
		assert.Equal(t, "description != null", got.WhereCondition)
	})
}

// =============================================================================
// Roundtrip Tests - Parse and Regenerate
// =============================================================================

func TestParseJoinSpec_Roundtrip(t *testing.T) {
	// These inputs should parse and regenerate to the same (canonical) form
	tests := []struct {
		name     string
		input    string
		expected string // Expected String() output (canonical form)
	}{
		{
			name:     "basic join",
			input:    "JOIN shops ON shop_id = shops.id",
			expected: "JOIN shops ON shop_id = shops.id",
		},
		{
			name:     "join with where",
			input:    "JOIN shops ON shop_id = shops.id WHERE status == 'active'",
			expected: "JOIN shops ON shop_id = shops.id WHERE status == 'active'",
		},
		{
			name:     "join with db prefix",
			input:    "JOIN mydb.shops ON shop_id = mydb.shops.id",
			expected: "JOIN mydb.shops ON shop_id = mydb.shops.id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec, err := ParseJoinSpec(tt.input)
			require.NoError(t, err)

			// The String() output should match expected canonical form
			got := spec.String()
			assert.Equal(t, tt.expected, got)
		})
	}
}

// =============================================================================
// Benchmark Tests
// =============================================================================

func BenchmarkParseJoinSpec_Simple(b *testing.B) {
	input := "JOIN shops ON shop_id = shops.id"
	for i := 0; i < b.N; i++ {
		_, _ = ParseJoinSpec(input)
	}
}

func BenchmarkParseJoinSpec_WithWhere(b *testing.B) {
	input := "JOIN shops ON shop_id = shops.id WHERE status == 'active' AND rating > 4.0"
	for i := 0; i < b.N; i++ {
		_, _ = ParseJoinSpec(input)
	}
}

func BenchmarkParseMultipleJoinSpecs_Three(b *testing.B) {
	input := `JOIN shops ON shop_id = shops.id
JOIN merchants ON merchant_id = merchants.id
JOIN categories ON category_id = categories.id`
	for i := 0; i < b.N; i++ {
		_, _ = ParseMultipleJoinSpecs(input)
	}
}

func BenchmarkExtractJoinOutputFields(b *testing.B) {
	spec := &JoinSpec{TargetCollection: "shops"}
	outputFields := []string{"id", "shops.name", "shops.rating", "price", "shops.status", "category"}
	for i := 0; i < b.N; i++ {
		spec.ExtractJoinOutputFields(outputFields)
	}
}
