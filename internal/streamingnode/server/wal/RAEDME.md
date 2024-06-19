# WAL

`wal` package is the basic defination of wal interface of milvus streamingnode.

## Project arrangement

- `/`: only define exposed interfaces.
- `/walimpls/`: define the underlying message system interfaces need to be implemented.
- `/registry/`: A static lifetime registry to regsiter new implementation for inverting dependency.
- `/adaptor/`: adaptors to implement `wal` interface from `walimpls` interface
- `/helper/`: A utility used to help developer to implement `walimpls` conveniently.
- `/utility/`: A utility code for common logic or data structure.

## Lifetime Of Interfaces

- `OpenerBuilder` has a static lifetime in a programs:
- `Opener` keep same lifetime with underlying resources (such as mq client).
- `WAL` keep same lifetime with underlying writer of wal, and it's lifetime is always included in related `Opener`.
- `Scanner` keep same lifetime with underlying reader of wal, and it's lifetime is always included in related `WAL`.

## Add New Implemetation Of WAL

developper who want to add a new implementation of `wal` should implements the `walimpls` package interfaces. following interfaces is required:

- `walimpls.OpenerBuilderImpls`
- `walimpls.OpenerImpls`
- `walimpls.ScannerImpls`
- `walimpls.WALImpls`

`OpenerBuilderImpls` create `OpenerImpls`; `OpenerImpls` creates `WALImpls`; `WALImpls` create `ScannerImpls`. 
Then register the implmentation of `walimpls.OpenerBuilderImpls` into `registry` package.

```
var _ OpenerBuilderImpls = b{};
registry.RegisterBuilder(b{})
```

All things have been done.

## Use WAL

```
name := "your builder name"
var yourCh *streamingpb.PChannelInfo

opener, err := registry.MustGetBuilder(name).Build()
if err != nil {
    panic(err)
}
ctx := context.Background()
logger, err := opener.Open(ctx, wal.OpenOption{
    Channel: yourCh  
})
if err != nil {
    panic(err)
}
```
## Adaptor

package `adaptor` is used to adapt `walimpls` and `wal` together.
common wal function should be implement by it. Such as:

- lifetime management
- interceptor implementation
- scanner wrapped up
- write ahead cache implementation
