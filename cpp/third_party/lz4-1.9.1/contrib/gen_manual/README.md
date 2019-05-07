gen_manual - a program for automatic generation of manual from source code
==========================================================================

#### Introduction

This simple C++ program generates a single-page HTML manual from `lz4.h`.

The format of recognized comment blocks is following:
- comments of type `/*!` mean: this is a function declaration; switch comments with declarations
- comments of type `/**` and `/*-` mean: this is a comment; use a `<H2>` header for the first line
- comments of type `/*=` and `/**=` mean: use a `<H3>` header and show also all functions until first empty line
- comments of type `/*X` where `X` is different from above-mentioned are ignored

Moreover:
- `LZ4LIB_API` is removed to improve readability
- `typedef` are detected and included even if uncommented
- comments of type `/**<` and `/*!<` are detected and only function declaration is highlighted (bold)


#### Usage

The program requires 3 parameters:
```
gen_manual [lz4_version] [input_file] [output_html]
```

To compile program and generate lz4 manual we have used:
```
make
./gen_manual.exe 1.7.3 ../../lib/lz4.h lz4_manual.html
```
