Coverage of ReLAPACK
====================

This file lists all LAPACK compute routines that are covered by recursive
algorithms in ReLAPACK, it also lists all of LAPACK's blocked algorithms which
are not (yet) part of ReLAPACK.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [List of covered LAPACK routines](#list-of-covered-lapack-routines)
  - [`xlauum`](#xlauum)
  - [`xsygst`](#xsygst)
  - [`xtrtri`](#xtrtri)
  - [`xpotrf`](#xpotrf)
  - [`xpbtrf`](#xpbtrf)
  - [`xsytrf`](#xsytrf)
  - [`xgetrf`](#xgetrf)
  - [`xgbtrf`](#xgbtrf)
  - [`xtrsyl`](#xtrsyl)
  - [`xtgsyl`](#xtgsyl)
- [Covered BLAS extension](#covered-blas-extension)
  - [`xgemmt`](#xgemmt)
- [Not covered yet](#not-covered-yet)
  - [`xpstrf`](#xpstrf)
- [Not covered: extra FLOPs](#not-covered-extra-flops)
  - [QR decomposition (and related)](#qr-decomposition-and-related)
  - [Symmetric reduction to tridiagonal](#symmetric-reduction-to-tridiagonal)
  - [Symmetric reduction to bidiagonal](#symmetric-reduction-to-bidiagonal)
  - [Reduction to upper Hessenberg](#reduction-to-upper-hessenberg)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->


List of covered LAPACK routines
-------------------------------

### `xlauum`
Multiplication of a triangular matrix with its (complex conjugate) transpose,
resulting in a symmetric (Hermitian) matrix.

Routines: `slauum`, `dlauum`, `clauum`, `zlauum`

Operations:
* A = L^T L
* A = U U^T

### `xsygst`
Simultaneous two-sided multiplication of a symmetric matrix with a triangular
matrix and its transpose

Routines: `ssygst`, `dsygst`, `chegst`, `zhegst`

Operations:
* A = inv(L) A inv(L^T)
* A = inv(U^T) A inv(U)
* A = L^T A L
* A = U A U^T

### `xtrtri`
Inversion of a triangular matrix

Routines: `strtri`, `dtrtri`, `ctrtri`, `ztrtri`

Operations:
* L = inv(L)
* U = inv(U)

### `xpotrf`
Cholesky decomposition of a symmetric (Hermitian) positive definite matrix

Routines: `spotrf`, `dpotrf`, `cpotrf`, `zpotrf`

Operations:
* L L^T = A
* U^T U = A

### `xpbtrf`
Cholesky decomposition of a banded symmetric (Hermitian) positive definite matrix

Routines: `spbtrf`, `dpbtrf`, `cpbtrf`, `zpbtrf`

Operations:
* L L^T = A
* U^T U = A

### `xsytrf`
LDL decomposition of a symmetric (or Hermitian) matrix

Routines:
* `ssytrf`, `dsytrf`, `csytrf`, `chetrf`, `zsytrf`, `zhetrf`,
* `ssytrf_rook`, `dsytrf_rook`, `csytrf_rook`, `chetrf_rook`, `zsytrf_rook`,
  `zhetrf_rook`

Operations:
* L D L^T = A
* U^T D U = A

### `xgetrf`
LU decomposition of a general matrix with pivoting

Routines: `sgetrf`, `dgetrf`, `cgetrf`, `zgetrf`

Operation: P L U = A

### `xgbtrf`
LU decomposition of a general banded matrix with pivoting

Routines: `sgbtrf`, `dgbtrf`, `cgbtrf`, `zgbtrf`

Operation: L U = A

### `xtrsyl`
Solution of the quasi-triangular Sylvester equation

Routines: `strsyl`, `dtrsyl`, `ctrsyl`, `ztrsyl`

Operations:
* A X + B Y = C -> X
* A^T X + B Y = C -> X
* A X + B^T Y = C -> X
* A^T X + B^T Y = C -> X
* A X - B Y = C -> X
* A^T X - B Y = C -> X
* A X - B^T Y = C -> X
* A^T X - B^T Y = C -> X

### `xtgsyl`
Solution of the generalized Sylvester equations

Routines: `stgsyl`, `dtgsyl`, `ctgsyl`, `ztgsyl`

Operations:
* A R - L B = C, D R - L E = F -> L, R
* A^T R + D^T L = C, R B^T - L E^T = -F -> L, R


Covered BLAS extension
----------------------

### `xgemmt`
Matrix-matrix product updating only a triangular part of the result

Routines: `sgemmt`, `dgemmt`, `cgemmt`, `zgemmt`

Operations:
* C = alpha A B + beta C
* C = alpha A B^T + beta C
* C = alpha A^T B + beta C
* C = alpha A^T B^T + beta C


Not covered yet
---------------
The following operation is implemented as a blocked algorithm in LAPACK but
currently not yet covered in ReLAPACK as a recursive algorithm

### `xpstrf`
Cholesky decomposition of a positive semi-definite matrix with complete pivoting.

Routines: `spstrf`, `dpstrf`, `cpstrf`, `zpstrf`

Operations:
* P L L^T P^T = A
* P U^T U P^T = A


Not covered: extra FLOPs
------------------------
The following routines are not covered because recursive variants would require
considerably more FLOPs or operate on banded matrices.

### QR decomposition (and related)
Routines:
* `sgeqrf`, `dgeqrf`, `cgeqrf`, `zgeqrf`
* `sgerqf`, `dgerqf`, `cgerqf`, `zgerqf`
* `sgeqlf`, `dgeqlf`, `cgeqlf`, `zgeqlf`
* `sgelqf`, `dgelqf`, `cgelqf`, `zgelqf`
* `stzrzf`, `dtzrzf`, `ctzrzf`, `ztzrzf`

Operations: Q R = A, R Q = A, Q L = A, L Q = A, R Z = A

Routines for multiplication with Q:
* `sormqr`, `dormqr`, `cunmqr`, `zunmqr`
* `sormrq`, `dormrq`, `cunmrq`, `zunmrq`
* `sormql`, `dormql`, `cunmql`, `zunmql`
* `sormlq`, `dormlq`, `cunmlq`, `zunmlq`
* `sormrz`, `dormrz`, `cunmrz`, `zunmrz`

Operations: C = Q C, C = C Q, C = Q^T C, C = C Q^T

Routines for construction of Q:
* `sorgqr`, `dorgqr`, `cungqr`, `zungqr`
* `sorgrq`, `dorgrq`, `cungrq`, `zungrq`
* `sorgql`, `dorgql`, `cungql`, `zungql`
* `sorglq`, `dorglq`, `cunglq`, `zunglq`

### Symmetric reduction to tridiagonal
Routines: `ssytrd`, `dsytrd`, `csytrd`, `zsytrd`

Operation: Q T Q^T = A

### Symmetric reduction to bidiagonal
Routines: `ssybrd`, `dsybrd`, `csybrd`, `zsybrd`

Operation: Q T P^T = A

### Reduction to upper Hessenberg
Routines: `sgehrd`, `dgehrd`, `cgehrd`, `zgehrd`

Operation: Q H Q^T = A
