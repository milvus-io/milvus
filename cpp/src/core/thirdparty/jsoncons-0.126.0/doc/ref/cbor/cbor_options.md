### jsoncons::cbor::cbor_options

Specifies options for encoding and decoding CBOR.

#### Header
```c++
#include <jsoncons/cbor/cbor_options.hpp>
```

![cbor_options](./diagrams/cbor_options.png)

#### Constructors

    cbor_options()
Constructs a `cbor_options` with default values. 

#### Modifiers

    cbor_options& pack_strings(bool value)

If set to `true`, then encode will store text strings and
byte strings once, and use string references to represent repeated occurences
of the strings. Decoding the resulting CBOR requires a decoder
that supports the 
[stringref extension to CBOR](http://cbor.schmorp.de/stringref), such as
jsoncons itself, or [Perl CBOR::XS](http://software.schmorp.de/pkg/CBOR-XS.html)

If set to `false` (the default), then encode
will encode strings the usual CBOR way. 

This option does not affect decode - jsoncons will always decode
string references if present.

#### Static member functions

    static const cbor_options& default_options()
Default CBOR encode and decode options.

### See also

[cbor_decode_options](cbor_decode_options.md)
[cbor_encode_options](cbor_encode_options.md)

