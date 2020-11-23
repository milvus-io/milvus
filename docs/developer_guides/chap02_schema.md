

## 2. Schema

#### 2.1 Collection Schema

``` go
type CollectionSchema struct {
  Name string
  Description string
  AutoId bool
  Fields []FieldSchema
}
```

#### 2.2 Field Schema

``` go
type FieldSchema struct {
  Name string
  Description string
  DataType DataType 
  TypeParams map[string]string
  IndexParams map[string]string
}
```

###### 2.2.1 Data Types

###### 2.2.2 Type Params

###### 2.2.3 Index Params

