# Milvus Go Client Test Framework

## Overview
This is a comprehensive test framework for the Milvus Go Client, designed to validate various functionalities of the Milvus vector database client. The framework provides a structured approach to writing tests with reusable components and helper functions.

## Framework Architecture

### Directory Structure
```
/go_client/
├── testcases/           # Main test cases
│   ├── helper/          # Helper functions and utilities
│   │   ├── helper.go
│   │   ├── data_helper.go
│   │   └── collection_helper.go
│   ├── search_test.go   # Search functionality tests
│   ├── index_test.go    # Index management tests
│   └── ...
├── common/             # Common utilities and constants
└── base/               # Base infrastructure code
```

### Key Components
- **Collection Preparation**: Utilities for creating and managing collections
- **Data Generation**: Tools for generating test data
- **Helper Functions**: Common operations and validations
- **Test Cases**: Organized by functionality

## Writing Test Cases

### Basic Test Structure
```go
func TestYourFeature(t *testing.T) {
    // 1. Setup context and client
    ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
    mc := createDefaultMilvusClient(ctx, t)

    // 2. Prepare collection
    prepare, schema := hp.CollPrepare.CreateCollection(
        ctx, t, mc,
        hp.NewCreateCollectionParams(hp.Int64Vec),
        hp.TNewFieldsOption(),
        hp.TNewSchemaOption(),
    )

    // 3. Insert test data
    prepare.InsertData(ctx, t, mc,
        hp.NewInsertParams(schema),
        hp.TNewDataOption(),
    )

    // 4. Execute test operations
    // ... your test logic here ...

    // 5. Validate results
    require.NoError(t, err)
    require.Equal(t, expected, actual)
}
```

### Using Custom Parameters

1. **Collection Creation Parameters**
```go
fieldsOption := hp.TNewFieldsOption().
    TWithEnableAnalyzer(true).
    TWithAnalyzerParams(map[string]any{
        "tokenizer": "standard",
    })

schemaOption := hp.TNewSchemaOption().
    TWithEnableDynamicField(true).
    TWithDescription("Custom schema").
    TWithAutoID(false)
```

2. **Data Insertion Options**
```go
insertOption := hp.TNewDataOption().
    TWithNb(1000).           // Number of records
    TWithDim(128).           // Vector dimension
    TWithStart(100).         // Starting ID
    TWithMaxLen(256).        // Maximum length
    TWithTextLang("en")      // Text language
```

3. **Index Parameters**
```go
indexParams := hp.TNewIndexParams(schema).
    TWithFieldIndex(map[string]index.Index{
        common.DefaultVectorFieldName: index.NewIVFSQIndex(
            &index.IVFSQConfig{
                MetricType: entity.L2,
                NList:     128,
            },
        ),
    })
```

4. **Search Parameters**
```go
searchOpt := client.NewSearchOption(schema.CollectionName, 100, vectors).
    WithOffset(0).
    WithLimit(100).
    WithConsistencyLevel(entity.ClStrong).
    WithFilter("int64 >= 100").
    WithOutputFields([]string{"*"}).
    WithSearchParams(map[string]any{
        "nprobe": 16,
        "ef":     64,
    })
```

## Adding New Parameters

1. **Define New Option Type**
```go
// In helper/data_helper.go
type YourNewOption struct {
    newParam1 string
    newParam2 int
}
```

2. **Add Constructor**
```go
func TNewYourOption() *YourNewOption {
    return &YourNewOption{
        newParam1: "default",
        newParam2: 0,
    }
}
```

3. **Add Parameter Methods**
```go
func (opt *YourNewOption) TWithNewParam1(value string) *YourNewOption {
    opt.newParam1 = value
    return opt
}

func (opt *YourNewOption) TWithNewParam2(value int) *YourNewOption {
    opt.newParam2 = value
    return opt
}
```

## Best Practices

1. **Test Organization**
   - Group related tests in the same file
   - Use clear and descriptive test names
   - Add comments explaining test purpose

2. **Data Generation**
   - Use helper functions for generating test data
   - Ensure data is appropriate for the test case
   - Clean up test data after use

3. **Error Handling**
   - Use `common.CheckErr` for consistent error checking
   - Test both success and failure scenarios
   - Validate error messages when appropriate

4. **Performance Considerations**
   - Use appropriate timeouts
   - Clean up resources after tests
   - Consider test execution time

## Running Tests

```bash
# Run all tests
go test ./testcases/...

# Run specific test
go test -run TestYourFeature ./testcases/

# Run with verbose output
go test -v ./testcases/...

# gotestsum
Recommend you to use gotestsum https://github.com/gotestyourself/gotestsum

# Run all default cases
gotestsum --format testname --hide-summary=output -v ./testcases/... --addr=127.0.0.1:19530 -timeout=30m

# Run a specified file
gotestsum --format testname --hide-summary=output ./testcases/collection_test.go ./testcases/main_test.go --addr=127.0.0.1:19530

# Run L3 rg cases
gotestsum --format testname --hide-summary=output -v ./testcases/advcases/... --addr=127.0.0.1:19530 -timeout=30m -tags=rg

# Run advanced rg cases and default cases
# rg cases conflicts with default cases, so -p=1 is required
gotestsum --format testname --hide-summary=output -v ./testcases/... --addr=127.0.0.1:19530 -timeout=30m -tags=rg -p 1
```

## Contributing
1. Follow the existing code structure
2. Add comprehensive test cases
3. Document new parameters and options
4. Update this README for significant changes
5. Ensure code quality standards:
   - Run `golangci-lint run` to check for style mistakes
   - Use `gofmt -w your/code/path` to format your code before submitting
   - CI will verify both golint and go format compliance