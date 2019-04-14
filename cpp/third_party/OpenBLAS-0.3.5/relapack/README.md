ReLAPACK
========

[![Build Status](https://travis-ci.org/HPAC/ReLAPACK.svg?branch=master)](https://travis-ci.org/HPAC/ReLAPACK)

[Recursive LAPACK Collection](https://github.com/HPAC/ReLAPACK)

ReLAPACK offers a collection of recursive algorithms for many of LAPACK's
compute kernels.  Since it preserves LAPACK's established interfaces, ReLAPACK
integrates effortlessly into existing application codes.  ReLAPACK's routines
not only outperform the reference LAPACK but also improve upon the performance
of tuned implementations, such as OpenBLAS and MKL.


Coverage
--------
For a detailed list of covered operations and an overview of operations to which
recursion is not efficiently applicable, see [coverage.md](coverage.md).


Installation
------------
To compile with the default configuration, simply run `make` to create the
library `librelapack.a`.

### Linking with MKL
Note that to link with MKL, you currently need to set the flag
`COMPLEX_FUNCTIONS_AS_ROUTINES` to `1` to avoid problems in `ctrsyl` and
`ztrsyl`.  For further configuration options see [config.md](config.md).


### Dependencies
ReLAPACK builds on top of [BLAS](http://www.netlib.org/blas/) and unblocked
kernels from [LAPACK](http://www.netlib.org/lapack/).  There are many optimized
and machine specific implementations of these libraries, which are commonly
provided by hardware vendors or available as open source (e.g.,
[OpenBLAS](http://www.openblas.net/)).


Testing
-------
ReLAPACK's test suite compares its routines numerically with LAPACK's
counterparts.  To set up the tests (located int `test/`) you need to specify
link flags for BLAS and LAPACK (version 3.5.0 or newer) in `make.inc`; then
`make test` runs the tests.  For details on the performed tests, see
[test/README.md](test/README.md).


Examples
--------
Since ReLAPACK replaces parts of LAPACK, any LAPACK example involving the
covered routines applies directly to ReLAPACK.  A few separate examples are
given in `examples/`. For details, see [examples/README.md](examples/README.md).


Citing
------
When referencing ReLAPACK, please cite the preprint of the paper
[Recursive Algorithms for Dense Linear Algebra: The ReLAPACK Collection](http://arxiv.org/abs/1602.06763):

    @article{relapack,
      author    = {Elmar Peise and Paolo Bientinesi},
      title     = {Recursive Algorithms for Dense Linear Algebra: The ReLAPACK Collection},
      journal   = {CoRR},
      volume    = {abs/1602.06763},
      year      = {2016},
      url       = {http://arxiv.org/abs/1602.06763},
    }
