# LAPACK

[![Build Status](https://travis-ci.org/Reference-LAPACK/lapack.svg?branch=master)](https://travis-ci.org/Reference-LAPACK/lapack)
[![Appveyor](https://ci.appveyor.com/api/projects/status/bh38iin398msrbtr?svg=true)](https://ci.appveyor.com/project/langou/lapack/)
[![codecov](https://codecov.io/gh/Reference-LAPACK/lapack/branch/master/graph/badge.svg)](https://codecov.io/gh/Reference-LAPACK/lapack)


* VERSION 1.0   :  February 29, 1992
* VERSION 1.0a  :  June 30, 1992
* VERSION 1.0b  :  October 31, 1992
* VERSION 1.1   :  March 31, 1993
* VERSION 2.0   :  September 30, 1994
* VERSION 3.0   :  June 30, 1999
* VERSION 3.0 + update :  October 31, 1999
* VERSION 3.0 + update :  May 31, 2000
* VERSION 3.1   : November 2006
* VERSION 3.1.1 : February 2007
* VERSION 3.2   : November 2008
* VERSION 3.2.1 : April 2009
* VERSION 3.2.2 : June 2010
* VERSION 3.3.0 : November 2010
* VERSION 3.3.1 : April 2011
* VERSION 3.4.0 : November 2011
* VERSION 3.4.1 : April 2012
* VERSION 3.4.2 : September 2012
* VERSION 3.5.0 : November 2013
* VERSION 3.6.0 : November 2015
* VERSION 3.6.1 : June 2016
* VERSION 3.7.0 : December 2016
* VERSION 3.7.1 : June 2017
* VERSION 3.8.0 : November 2017

LAPACK is a library of Fortran subroutines for solving the most commonly
occurring problems in numerical linear algebra.

LAPACK is a freely-available software package. It can be included in commercial
software packages (and has been). We only ask that that proper credit be given
to the authors, for example by citing the LAPACK Users' Guide. The license used
for the software is the modified BSD license, see:
https://github.com/Reference-LAPACK/lapack/blob/master/LICENSE

Like all software, it is copyrighted. It is not trademarked, but we do ask the
following: if you modify the source for these routines we ask that you change
the name of the routine and comment the changes made to the original.

We will gladly answer any questions regarding the software. If a modification
is done, however, it is the responsibility of the person who modified the
routine to provide support.

LAPACK is available from github at:
https://github.com/reference-lapack/lapack

LAPACK releases are also available on netlib at:
http://www.netlib.org/lapack/

The distribution contains (1) the Fortran source for LAPACK, and (2) its
testing programs.  It also contains (3) the Fortran reference implementation of
the Basic Linear Algebra Subprograms (the Level 1, 2, and 3 BLAS) needed by
LAPACK.  However this code is intended for use only if there is no other
implementation of the BLAS already available on your machine; the efficiency of
LAPACK depends very much on the efficiency of the BLAS.  It also contains (4)
CBLAS, a C interface to the BLAS, and (5) LAPACKE, a C interface to LAPACK.

## Installation

 - LAPACK can be installed with `make`. The configuration have to be set in the
   `make.inc` file. A `make.inc.example` for a Linux machine running GNU compilers
   is given in the main directory. Some specific `make.inc` are also available in
   the `INSTALL` directory.
 - LAPACK includes also the CMake build. You will need to have CMake installed
   on your machine (CMake is available at http://www.cmake.org/). CMake will
   allow an easy installation on a Windows Machine.
 - Specific information to run LAPACK under Windows is available at
   http://icl.cs.utk.edu/lapack-for-windows/lapack/.


## User Support

LAPACK has been thoroughly tested, on many different types of computers. The
LAPACK project supports the package in the sense that reports of errors or poor
performance will gain immediate attention from the developers. Such reports,
descriptions of interesting applications, and other comments should be sent by
electronic mail to lapack@icl.utk.edu.

For further information on LAPACK please read our FAQ at
http://www.netlib.org/lapack/#_faq.

A list of known problems, bugs, and compiler errors for LAPACK is
maintained on netlib
http://www.netlib.org/lapack/release_notes.html.
Please see as well
https://github.com/Reference-LAPACK/lapack/issues.

A User forum is also available to help you with the LAPACK library at
http://icl.cs.utk.edu/lapack-forum/.
You can also contact directly the LAPACK team at lapack@icl.utk.edu.


## Testing

LAPACK includes a thorough test suite. We recommend that, after compilation,
you run the test suite. 

For complete information on the LAPACK Testing please consult LAPACK Working
Note 41 "Installation Guide for LAPACK".

## User Guide

To view an HTML version of the Users' Guide please refer to the URL
  http://www.netlib.org/lapack/lug/lapack_lug.html.

## LAPACKE

LAPACK now includes the LAPACKE package.  LAPACKE is a Standard C language API
for LAPACK This was born from a collaboration of the LAPACK and INTEL Math
Kernel Library teams. See:
http://www.netlib.org/lapack/#_standard_c_language_apis_for_lapack.

