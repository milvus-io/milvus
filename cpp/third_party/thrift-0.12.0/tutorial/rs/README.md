# Rust Language Bindings for Thrift

## Getting Started

1. Get the [Thrift compiler](https://thrift.apache.org).

2. Add the following crates to your `Cargo.toml`.

```toml
thrift = "x.y.z" # x.y.z is the version of the thrift compiler
ordered_float = "0.3.0"
try_from = "0.2.0"
```

3. Add the same crates to your `lib.rs` or `main.rs`.

```rust
extern crate ordered_float;
extern crate thrift;
extern crate try_from;
```

4. Generate Rust sources for your IDL (for example, `Tutorial.thrift`).

```shell
thrift -out my_rust_program/src --gen rs -r Tutorial.thrift
```

5. Use the generated source in your code.

```rust
// add extern crates here, or in your lib.rs
extern crate ordered_float;
extern crate thrift;
extern crate try_from;

// generated Rust module
use tutorial;

use thrift::protocol::{TCompactInputProtocol, TCompactOutputProtocol};
use thrift::protocol::{TInputProtocol, TOutputProtocol};
use thrift::transport::{TFramedReadTransport, TFramedWriteTransport};
use thrift::transport::{TIoChannel, TTcpChannel};
use tutorial::{CalculatorSyncClient, TCalculatorSyncClient};
use tutorial::{Operation, Work};

fn main() {
    match run() {
        Ok(()) => println!("client ran successfully"),
        Err(e) => {
            println!("client failed with {:?}", e);
            std::process::exit(1);
        }
    }
}

fn run() -> thrift::Result<()> {
    //
    // build client
    //

    println!("connect to server on 127.0.0.1:9090");
    let mut c = TTcpTransport::new();
    c.open("127.0.0.1:9090")?;

    let (i_chan, o_chan) = c.split()?;
    
    let i_prot = TCompactInputProtocol::new(
        TFramedReadTransport::new(i_chan)
    );
    let o_prot = TCompactOutputProtocol::new(
        TFramedWriteTransport::new(o_chan)
    );

    let client = CalculatorSyncClient::new(i_prot, o_prot);

    //
    // alright! - let's make some calls
    //

    // two-way, void return
    client.ping()?;

    // two-way with some return
    let res = client.calculate(
        72,
        Work::new(7, 8, Operation::MULTIPLY, None)
    )?;
    println!("multiplied 7 and 8, got {}", res);

    // two-way and returns a Thrift-defined exception
    let res = client.calculate(
        77,
        Work::new(2, 0, Operation::DIVIDE, None)
    );
    match res {
        Ok(v) => panic!("shouldn't have succeeded with result {}", v),
        Err(e) => println!("divide by zero failed with {:?}", e),
    }

    // one-way
    client.zip()?;

    // done!
    Ok(())
}
```

## Code Generation

### Thrift Files and Generated Modules

The Thrift code generator takes each Thrift file and generates a Rust module
with the same name snake-cased. For example, running the compiler on
`ThriftTest.thrift` creates `thrift_test.rs`. To use these generated files add
`mod ...` and `use ...` declarations to your `lib.rs` or `main.rs` - one for
each generated file.

### Results and Errors

The Thrift runtime library defines a `thrift::Result` and a `thrift::Error` type,
both of which are used throught the runtime library and in all generated code.
Conversions are defined from `std::io::Error`, `str` and `String` into
`thrift::Error`.

### Thrift Type and their Rust Equivalents

Thrift defines a number of types, each of which is translated into its Rust
equivalent by the code generator.

* Primitives (bool, i8, i16, i32, i64, double, string, binary)
* Typedefs
* Enums
* Containers
* Structs
* Unions
* Exceptions
* Services
* Constants (primitives, containers, structs)

In addition, unless otherwise noted, thrift includes are translated into
`use ...` statements in the generated code, and all declarations, parameters,
traits and types in the generated code are namespaced appropriately.

The following subsections cover each type and their generated Rust equivalent.

### Primitives

Thrift primitives have straightforward Rust equivalents.

* bool: `bool`
* i8: `i8`
* i16: `i16`
* i32: `i32`
* i64: `i64`
* double: `OrderedFloat<f64>`
* string: `String`
* binary: `Vec<u8>`

### Typedefs

A typedef is translated to a `pub type` declaration.

```thrift
typedef i64 UserId

typedef map<string, UserId> MapType
```
```rust
pub type UserId = i64;

pub type MapType = BTreeMap<String, Bonk>;
```

### Enums

A Thrift enum is represented as a Rust enum, and each variant is transcribed 1:1.

```thrift
enum Numberz
{
    ONE = 1,
    TWO,
    THREE,
    FIVE = 5,
    SIX,
    EIGHT = 8
}
```

```rust
#[derive(Copy, Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Numberz {
    ONE = 1,
    TWO = 2,
    THREE = 3,
    FIVE = 5,
    SIX = 6,
    EIGHT = 8,
}

impl TryFrom<i32> for Numberz {
    // ...
}

```

### Containers

Thrift has three container types: list, set and map. They are translated into
Rust `Vec`, `BTreeSet` and `BTreeMap` respectively. Any Thrift type (this
includes structs, enums and typedefs) can be a list/set element or a map
key/value.

#### List

```thrift
list <i32> numbers
```

```rust
numbers: Vec<i32>
```

#### Set

```thrift
set <i32> numbers
```

```rust
numbers: BTreeSet<i32>
```

#### Map

```thrift
map <string, i32> numbers
```

```rust
numbers: BTreeMap<String, i32>
```

### Structs

A Thrift struct is represented as a Rust struct, and each field transcribed 1:1.

```thrift
struct CrazyNesting {
    1: string string_field,
    2: optional set<Insanity> set_field,
    3: required list<
         map<set<i32>, map<i32,set<list<map<Insanity,string>>>>>
       >
    4: binary binary_field
}
```
```rust
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct CrazyNesting {
    pub string_field: Option<String>,
    pub set_field: Option<BTreeSet<Insanity>>,
    pub list_field: Vec<
        BTreeMap<
            BTreeSet<i32>,
            BTreeMap<i32, BTreeSet<Vec<BTreeMap<Insanity, String>>>>
        >
    >,
    pub binary_field: Option<Vec<u8>>,
}

impl CrazyNesting {
    pub fn read_from_in_protocol(i_prot: &mut TInputProtocol)
    ->
    thrift::Result<CrazyNesting> {
        // ...
    }
    pub fn write_to_out_protocol(&self, o_prot: &mut TOutputProtocol)
    ->
    thrift::Result<()> {
        // ...
    }
}

```
##### Optionality

Thrift has 3 "optionality" types:

1. Required
2. Optional
3. Default

The Rust code generator encodes *Required* fields as the bare type itself, while
*Optional* and *Default* fields are encoded as `Option<TypeName>`.

```thrift
struct Foo {
    1: required string bar  // 1. required
    2: optional string baz  // 2. optional
    3: string qux           // 3. default
}
```

```rust
pub struct Foo {
    bar: String,            // 1. required
    baz: Option<String>,    // 2. optional
    qux: Option<String>,    // 3. default
}
```

## Known Issues

* Struct constants are not supported
* Map, list and set constants require a const holder struct
