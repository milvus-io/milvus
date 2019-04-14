ReLAPACK Test Suite
===================
This test suite compares ReLAPACK's recursive routines with LAPACK's compute
routines in terms of accuracy:  For each test-case, we execute both ReLAPACK's
and LAPACK's routine on the same data and consider the numerical difference
between the two solutions.

This difference is computed as the maximum error across all elements of the
routine's outputs, where the error for each element is the minimum of the
absolute error and the relative error (with LAPACK as the reference).  If the
error is below the error bound configured in `config.h` (default: 1e-5 for
single precision and 1e-14 for double precision) the test-case is considered as
passed.

For each routine the test-cases cover a variety of input argument combinations
to ensure that ReLAPACK's routines match the functionality of LAPACK for all use
cases.

The matrix size for all experiments (default: 100) can also be specified in
`config.h`.


Implementation
--------------
`test.h` provides the framework for our tests:  It provides macros that allow to
generalize the tests for each operation in one file covering all data-types.
Such a file is structured as follows:

 * All matrices required by the test-cases are declared globally.  For each
   matrix, an array of two pointers is declared; one for the matrix copy passed
   to ReLAPACK and one passed to LAPACK.

 * `tests()` contains the main control flow: it allocates (and later frees) the
   copies of the globally declared matrices.  It then defines the macro
   `ROUTINE` to contain the name of the currently tested routine.
   It then uses the macro `TEST` to perform the test-cases.
   It receives the arguments of the routine, where matrices of which ReLAPACK
   and LAPACK receive a copy are index with `i`. (Example: `TEST("L", &n, A[i],
   &n, info);`)

 * The macro `TEST` first calls `pre()`, which initializes all relevant
   matrices, then executes the ReLAPACK algorithm on the matrices with `i` = `0`
   and then the LAPACK counter part with `i` = `1`.  It then calls `post()`,
   which computes the difference between the results, storing it in `error`.
   Finally, the error is printed out and compared to the error bound.

If all test-cases pass the error bound test, the program will have a `0` return
value, otherwise it is `1`, indicating an error.
