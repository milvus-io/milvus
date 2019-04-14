RELAPACK Configuration
======================

ReLAPACK has two configuration files: `make.inc`, which is included by the
Makefile, and `config.h` which is included in the source files.


Build and Testing Environment
-----------------------------
The build environment (compiler and flags) and the test configuration (linker
flags for BLAS and LAPACK) are specified in `make.inc`.  The test matrix size
and error bounds are defined in `test/config.h`.

The library `librelapack.a` is compiled by invoking `make`.  The tests are
performed by either `make test` or calling `make` in the test folder.


BLAS/LAPACK complex function interfaces
---------------------------------------
For BLAS and LAPACK functions that return a complex number, there exist two
conflicting (FORTRAN compiler dependent) calling conventions: either the result
is returned as a `struct` of two floating point numbers or an additional first
argument with a pointer to such a `struct` is used.  By default ReLAPACK uses
the former (which is what gfortran uses), but it can switch to the latter by
setting `COMPLEX_FUNCTIONS_AS_ROUTINES` (or explicitly the BLAS and LAPACK
specific counterparts) to `1` in `config.h`.

**For MKL, `COMPLEX_FUNCTIONS_AS_ROUTINES` must be set to `1`.**

(Using the wrong convention will break `ctrsyl` and `ztrsyl` and the test cases
will segfault or return errors on the order of 1 or larger.)


BLAS extension `xgemmt`
-----------------------
The LDL decompositions require a general matrix-matrix product that updates only
a triangular matrix called `xgemmt`.  If the BLAS implementation linked against
provides such a routine, set the flag `HAVE_XGEMMT` to `1` in `config.h`;
otherwise, ReLAPACK uses its own recursive implementation of these kernels.

`xgemmt` is provided by MKL.


Routine Selection
-----------------
ReLAPACK's routines are named `RELAPACK_X` (e.g., `RELAPACK_dgetrf`).  If the
corresponding `INCLUDE_X` flag in `config.h` (e.g., `INCLUDE_DGETRF`) is set to
`1`, ReLAPACK additionally provides a wrapper under the LAPACK name (e.g.,
`dgetrf_`).  By default, wrappers for all routines are enabled.


Crossover Size
--------------
The crossover size determines below which matrix sizes ReLAPACK's recursive
algorithms switch to LAPACK's unblocked routines to avoid tiny BLAS Level 3
routines.  The crossover size is set in `config.h` and can be chosen either
globally for the entire library, by operation, or individually by routine.


Allowing Temporary Buffers
--------------------------
Two of ReLAPACK's routines make use of temporary buffers, which are allocated
and freed within ReLAPACK.  Setting `ALLOW_MALLOC` (or one of the routine
specific counterparts) to 0 in `config.h` will disable these buffers.  The
affected routines are:

 * `xsytrf`: The LDL decomposition requires a buffer of size n^2 / 2.  As in
   LAPACK, this size can be queried by setting `lWork = -1` and the passed
   buffer will be used if it is large enough; only if it is not, a local buffer
   will be allocated.

   The advantage of this mechanism is that ReLAPACK will seamlessly work even
   with codes that statically provide too little memory instead of breaking
   them.

 * `xsygst`: The reduction of a real symmetric-definite generalized eigenproblem
   to standard form can use an auxiliary buffer of size n^2 / 2 to avoid
   redundant computations.  It thereby performs about 30% less FLOPs than
   LAPACK.


FORTRAN symbol names
--------------------
ReLAPACK is commonly linked to BLAS and LAPACK with standard FORTRAN interfaces.
Since these libraries usually have an underscore to their symbol names, ReLAPACK
has configuration switches in `config.h` to adjust the corresponding routine
names.
