/*********************************************************************/
/* Copyright 2009, 2010 The University of Texas at Austin.           */
/* All rights reserved.                                              */
/*                                                                   */
/* Redistribution and use in source and binary forms, with or        */
/* without modification, are permitted provided that the following   */
/* conditions are met:                                               */
/*                                                                   */
/*   1. Redistributions of source code must retain the above         */
/*      copyright notice, this list of conditions and the following  */
/*      disclaimer.                                                  */
/*                                                                   */
/*   2. Redistributions in binary form must reproduce the above      */
/*      copyright notice, this list of conditions and the following  */
/*      disclaimer in the documentation and/or other materials       */
/*      provided with the distribution.                              */
/*                                                                   */
/*    THIS  SOFTWARE IS PROVIDED  BY THE  UNIVERSITY OF  TEXAS AT    */
/*    AUSTIN  ``AS IS''  AND ANY  EXPRESS OR  IMPLIED WARRANTIES,    */
/*    INCLUDING, BUT  NOT LIMITED  TO, THE IMPLIED  WARRANTIES OF    */
/*    MERCHANTABILITY  AND FITNESS FOR  A PARTICULAR  PURPOSE ARE    */
/*    DISCLAIMED.  IN  NO EVENT SHALL THE UNIVERSITY  OF TEXAS AT    */
/*    AUSTIN OR CONTRIBUTORS BE  LIABLE FOR ANY DIRECT, INDIRECT,    */
/*    INCIDENTAL,  SPECIAL, EXEMPLARY,  OR  CONSEQUENTIAL DAMAGES    */
/*    (INCLUDING, BUT  NOT LIMITED TO,  PROCUREMENT OF SUBSTITUTE    */
/*    GOODS  OR  SERVICES; LOSS  OF  USE,  DATA,  OR PROFITS;  OR    */
/*    BUSINESS INTERRUPTION) HOWEVER CAUSED  AND ON ANY THEORY OF    */
/*    LIABILITY, WHETHER  IN CONTRACT, STRICT  LIABILITY, OR TORT    */
/*    (INCLUDING NEGLIGENCE OR OTHERWISE)  ARISING IN ANY WAY OUT    */
/*    OF  THE  USE OF  THIS  SOFTWARE,  EVEN  IF ADVISED  OF  THE    */
/*    POSSIBILITY OF SUCH DAMAGE.                                    */
/*                                                                   */
/* The views and conclusions contained in the software and           */
/* documentation are those of the authors and should not be          */
/* interpreted as representing official policies, either expressed   */
/* or implied, of The University of Texas at Austin.                 */
/*********************************************************************/

#include <stdio.h>
#include "common.h"

#ifndef MINUS
#define a2	(a1 + 2)
#define a4	(a3 + 2)
#define a6	(a5 + 2)
#define a8	(a7 + 2)
#else
#define a2	(a1 - 2)
#define a4	(a3 - 2)
#define a6	(a5 - 2)
#define a8	(a7 - 2)
#endif

int CNAME(BLASLONG n, BLASLONG k1, BLASLONG k2, FLOAT dummy1, FLOAT dummy4,
	  FLOAT *a, BLASLONG lda,
	  FLOAT *dummy2, BLASLONG dumy3, blasint *ipiv, BLASLONG incx){

  BLASLONG i, j, ip1, ip2, rows;
  blasint *piv;
  FLOAT *a1, *a3, *a5, *a7;
  FLOAT *b1, *b2, *b3, *b4;
  FLOAT *b5, *b6, *b7, *b8;
  FLOAT A1, A2, B1, B2, A3, A4, B3, B4;
  FLOAT A5, A6, B5, B6, A7, A8, B7, B8;
  FLOAT A9, A10, B9, B10, A11, A12, B11, B12;
  FLOAT A13, A14, B13, B14, A15, A16, B15, B16;

  a   -= 2;
  lda *= 2;
  k1 --;

#ifndef MINUS
 ipiv += k1;
#else
  ipiv -= (k2 - 1) * incx;
#endif

  if (n  <= 0) return 0;
  rows = k2-k1;
  if (rows <=0) return 0;
  if (rows == 1) {
    //Only have 1 row
    ip1 = *ipiv * 2;

#ifndef MINUS
    a1 = a + (k1 + 1) * 2;
#else
    a1 = a + k2 * 2;
#endif

    b1 = a + ip1;

    if(a1 == b1) return 0;

    for(j=0; j<n; j++){
      A1 = *(a1 + 0);
      A2 = *(a1 + 1);
      B1 = *(b1 + 0);
      B2 = *(b1 + 1);

      *(a1 + 0) = B1;
      *(a1 + 1) = B2;
      *(b1 + 0) = A1;
      *(b1 + 1) = A2;

      a1 += lda;
      b1 += lda;
    }
    return 0;
  }

  j = (n >> 2);
  if (j > 0) {
    do {
      piv = ipiv;

#ifndef MINUS
      a1 = a + (k1 + 1) * 2;
#else
      a1 = a + k2 * 2;
#endif

      a3 = a1 + 1 * lda;
      a5 = a1 + 2 * lda;
      a7 = a1 + 3 * lda;

      ip1 = *piv * 2;
      piv += incx;
      ip2 = *piv * 2;
      piv += incx;

      b1 = a + ip1;
      b2 = a + ip2;

      b3 = b1 + 1 * lda;
      b4 = b2 + 1 * lda;
      b5 = b1 + 2 * lda;
      b6 = b2 + 2 * lda;
      b7 = b1 + 3 * lda;
      b8 = b2 + 3 * lda;

      i = (rows >> 1);
      i--;

      //Loop pipeline
      //Main Loop
      while (i > 0) {
	  A1 = *(a1 + 0);
	  A2 = *(a1 + 1);
	  A3 = *(a2 + 0);
	  A4 = *(a2 + 1);
	  A5 = *(a3 + 0);
	  A6 = *(a3 + 1);
	  A7 = *(a4 + 0);
	  A8 = *(a4 + 1);

	  A9  = *(a5 + 0);
	  A10 = *(a5 + 1);
	  A11 = *(a6 + 0);
	  A12 = *(a6 + 1);
	  A13 = *(a7 + 0);
	  A14 = *(a7 + 1);
	  A15 = *(a8 + 0);
	  A16 = *(a8 + 1);

	  B1 = *(b1 + 0);
	  B2 = *(b1 + 1);
	  B3 = *(b2 + 0);
	  B4 = *(b2 + 1);
	  B5 = *(b3 + 0);
	  B6 = *(b3 + 1);
	  B7 = *(b4 + 0);
	  B8 = *(b4 + 1);

	  B9  = *(b5 + 0);
	  B10 = *(b5 + 1);
	  B11 = *(b6 + 0);
	  B12 = *(b6 + 1);
	  B13 = *(b7 + 0);
	  B14 = *(b7 + 1);
	  B15 = *(b8 + 0);
	  B16 = *(b8 + 1);

	  ip1 = *piv * 2;
	  piv += incx;
	  ip2 = *piv * 2;
	  piv += incx;

	  if (b1 == a1) {
	    if (b2 == a1) {
	      *(a1 + 0) = A3;
	      *(a1 + 1) = A4;
	      *(a2 + 0) = A1;
	      *(a2 + 1) = A2;
	      *(a3 + 0) = A7;
	      *(a3 + 1) = A8;
	      *(a4 + 0) = A5;
	      *(a4 + 1) = A6;
	      *(a5 + 0) = A11;
	      *(a5 + 1) = A12;
	      *(a6 + 0) = A9;
	      *(a6 + 1) = A10;
	      *(a7 + 0) = A15;
	      *(a7 + 1) = A16;
	      *(a8 + 0) = A13;
	      *(a8 + 1) = A14;
	    } else
	      if (b2 != a2) {
		*(a2 + 0) = B3;
		*(a2 + 1) = B4;
		*(b2 + 0) = A3;
		*(b2 + 1) = A4;
		*(a4 + 0) = B7;
		*(a4 + 1) = B8;
		*(b4 + 0) = A7;
		*(b4 + 1) = A8;
		*(a6 + 0) = B11;
		*(a6 + 1) = B12;
		*(b6 + 0) = A11;
		*(b6 + 1) = A12;
		*(a8 + 0) = B15;
		*(a8 + 1) = B16;
		*(b8 + 0) = A15;
		*(b8 + 1) = A16;
	      }
	  } else
	    if (b1 == a2) {
	      if (b2 != a1) {
		if (b2 == a2) {
		  *(a1 + 0) = A3;
		  *(a1 + 1) = A4;
		  *(a2 + 0) = A1;
		  *(a2 + 1) = A2;
		  *(a3 + 0) = A7;
		  *(a3 + 1) = A8;
		  *(a4 + 0) = A5;
		  *(a4 + 1) = A6;
		  *(a5 + 0) = A11;
		  *(a5 + 1) = A12;
		  *(a6 + 0) = A9;
		  *(a6 + 1) = A10;
		  *(a7 + 0) = A15;
		  *(a7 + 1) = A16;
		  *(a8 + 0) = A13;
		  *(a8 + 1) = A14;
		} else {
		  *(a1 + 0) = A3;
		  *(a1 + 1) = A4;
		  *(a2 + 0) = B3;
		  *(a2 + 1) = B4;
		  *(b2 + 0) = A1;
		  *(b2 + 1) = A2;
		  *(a3 + 0) = A7;
		  *(a3 + 1) = A8;
		  *(a4 + 0) = B7;
		  *(a4 + 1) = B8;
		  *(b4 + 0) = A5;
		  *(b4 + 1) = A6;

		  *(a5 + 0) = A11;
		  *(a5 + 1) = A12;
		  *(a6 + 0) = B11;
		  *(a6 + 1) = B12;
		  *(b6 + 0) = A9;
		  *(b6 + 1) = A10;
		  *(a7 + 0) = A15;
		  *(a7 + 1) = A16;
		  *(a8 + 0) = B15;
		  *(a8 + 1) = B16;
		  *(b8 + 0) = A13;
		  *(b8 + 1) = A14;
		}
	      }
	    } else {
	      if (b2 == a1) {
		*(a1 + 0) = A3;
		*(a1 + 1) = A4;
		*(a2 + 0) = B1;
		*(a2 + 1) = B2;
		*(b1 + 0) = A1;
		*(b1 + 1) = A2;
		*(a3 + 0) = A7;
		*(a3 + 1) = A8;
		*(a4 + 0) = B5;
		*(a4 + 1) = B6;
		*(b3 + 0) = A5;
		*(b3 + 1) = A6;
		*(a5 + 0) = A11;
		*(a5 + 1) = A12;
		*(a6 + 0) = B9;
		*(a6 + 1) = B10;
		*(b5 + 0) = A9;
		*(b5 + 1) = A10;
		*(a7 + 0) = A15;
		*(a7 + 1) = A16;
		*(a8 + 0) = B13;
		*(a8 + 1) = B14;
		*(b7 + 0) = A13;
		*(b7 + 1) = A14;
	      } else
		if (b2 == a2) {
		  *(a1 + 0) = B1;
		  *(a1 + 1) = B2;
		  *(b1 + 0) = A1;
		  *(b1 + 1) = A2;
		  *(a3 + 0) = B5;
		  *(a3 + 1) = B6;
		  *(b3 + 0) = A5;
		  *(b3 + 1) = A6;
		  *(a5 + 0) = B9;
		  *(a5 + 1) = B10;
		  *(b5 + 0) = A9;
		  *(b5 + 1) = A10;
		  *(a7 + 0) = B13;
		  *(a7 + 1) = B14;
		  *(b7 + 0) = A13;
		  *(b7 + 1) = A14;
		} else
		  if (b2 == b1) {
		    *(a1 + 0) = B1;
		    *(a1 + 1) = B2;
		    *(a2 + 0) = A1;
		    *(a2 + 1) = A2;
		    *(b1 + 0) = A3;
		    *(b1 + 1) = A4;
		    *(a3 + 0) = B5;
		    *(a3 + 1) = B6;
		    *(a4 + 0) = A5;
		    *(a4 + 1) = A6;
		    *(b3 + 0) = A7;
		    *(b3 + 1) = A8;

		    *(a5 + 0) = B9;
		    *(a5 + 1) = B10;
		    *(a6 + 0) = A9;
		    *(a6 + 1) = A10;
		    *(b5 + 0) = A11;
		    *(b5 + 1) = A12;
		    *(a7 + 0) = B13;
		    *(a7 + 1) = B14;
		    *(a8 + 0) = A13;
		    *(a8 + 1) = A14;
		    *(b7 + 0) = A15;
		    *(b7 + 1) = A16;
		  } else {
		    *(a1 + 0) = B1;
		    *(a1 + 1) = B2;
		    *(a2 + 0) = B3;
		    *(a2 + 1) = B4;
		    *(b1 + 0) = A1;
		    *(b1 + 1) = A2;
		    *(b2 + 0) = A3;
		    *(b2 + 1) = A4;
		    *(a3 + 0) = B5;
		    *(a3 + 1) = B6;
		    *(a4 + 0) = B7;
		    *(a4 + 1) = B8;
		    *(b3 + 0) = A5;
		    *(b3 + 1) = A6;
		    *(b4 + 0) = A7;
		    *(b4 + 1) = A8;
		    *(a5 + 0) = B9;
		    *(a5 + 1) = B10;
		    *(a6 + 0) = B11;
		    *(a6 + 1) = B12;
		    *(b5 + 0) = A9;
		    *(b5 + 1) = A10;
		    *(b6 + 0) = A11;
		    *(b6 + 1) = A12;
		    *(a7 + 0) = B13;
		    *(a7 + 1) = B14;
		    *(a8 + 0) = B15;
		    *(a8 + 1) = B16;
		    *(b7 + 0) = A13;
		    *(b7 + 1) = A14;
		    *(b8 + 0) = A15;
		    *(b8 + 1) = A16;
		  }
	    }

	  b1 = a + ip1;
	  b2 = a + ip2;

	  b3 = b1 + 1 * lda;
	  b4 = b2 + 1 * lda;
	  b5 = b1 + 2 * lda;
	  b6 = b2 + 2 * lda;
	  b7 = b1 + 3 * lda;
	  b8 = b2 + 3 * lda;

#ifndef MINUS
	  a1 += 4;
	  a3 += 4;
	  a5 += 4;
	  a7 += 4;
#else
	  a1 -= 4;
	  a3 -= 4;
	  a5 -= 4;
	  a7 -= 4;
#endif
	i --;
      }

      //Loop Ending
      A1 = *(a1 + 0);
      A2 = *(a1 + 1);
      A3 = *(a2 + 0);
      A4 = *(a2 + 1);
      A5 = *(a3 + 0);
      A6 = *(a3 + 1);
      A7 = *(a4 + 0);
      A8 = *(a4 + 1);

      A9  = *(a5 + 0);
      A10 = *(a5 + 1);
      A11 = *(a6 + 0);
      A12 = *(a6 + 1);
      A13 = *(a7 + 0);
      A14 = *(a7 + 1);
      A15 = *(a8 + 0);
      A16 = *(a8 + 1);

      B1 = *(b1 + 0);
      B2 = *(b1 + 1);
      B3 = *(b2 + 0);
      B4 = *(b2 + 1);
      B5 = *(b3 + 0);
      B6 = *(b3 + 1);
      B7 = *(b4 + 0);
      B8 = *(b4 + 1);

      B9  = *(b5 + 0);
      B10 = *(b5 + 1);
      B11 = *(b6 + 0);
      B12 = *(b6 + 1);
      B13 = *(b7 + 0);
      B14 = *(b7 + 1);
      B15 = *(b8 + 0);
      B16 = *(b8 + 1);

      if (b1 == a1) {
	if (b2 == a1) {
	  *(a1 + 0) = A3;
	  *(a1 + 1) = A4;
	  *(a2 + 0) = A1;
	  *(a2 + 1) = A2;
	  *(a3 + 0) = A7;
	  *(a3 + 1) = A8;
	  *(a4 + 0) = A5;
	  *(a4 + 1) = A6;
	  *(a5 + 0) = A11;
	  *(a5 + 1) = A12;
	  *(a6 + 0) = A9;
	  *(a6 + 1) = A10;
	  *(a7 + 0) = A15;
	  *(a7 + 1) = A16;
	  *(a8 + 0) = A13;
	  *(a8 + 1) = A14;
	} else
	  if (b2 != a2) {
	    *(a2 + 0) = B3;
	    *(a2 + 1) = B4;
	    *(b2 + 0) = A3;
	    *(b2 + 1) = A4;
	    *(a4 + 0) = B7;
	    *(a4 + 1) = B8;
	    *(b4 + 0) = A7;
	    *(b4 + 1) = A8;
	    *(a6 + 0) = B11;
	    *(a6 + 1) = B12;
	    *(b6 + 0) = A11;
	    *(b6 + 1) = A12;
	    *(a8 + 0) = B15;
	    *(a8 + 1) = B16;
	    *(b8 + 0) = A15;
	    *(b8 + 1) = A16;
	  }
      } else
	if (b1 == a2) {
	  if (b2 != a1) {
	    if (b2 == a2) {
	      *(a1 + 0) = A3;
	      *(a1 + 1) = A4;
	      *(a2 + 0) = A1;
	      *(a2 + 1) = A2;
	      *(a3 + 0) = A7;
	      *(a3 + 1) = A8;
	      *(a4 + 0) = A5;
	      *(a4 + 1) = A6;
	      *(a5 + 0) = A11;
	      *(a5 + 1) = A12;
	      *(a6 + 0) = A9;
	      *(a6 + 1) = A10;
	      *(a7 + 0) = A15;
	      *(a7 + 1) = A16;
	      *(a8 + 0) = A13;
	      *(a8 + 1) = A14;
	    } else {
	      *(a1 + 0) = A3;
	      *(a1 + 1) = A4;
	      *(a2 + 0) = B3;
	      *(a2 + 1) = B4;
	      *(b2 + 0) = A1;
	      *(b2 + 1) = A2;
	      *(a3 + 0) = A7;
	      *(a3 + 1) = A8;
	      *(a4 + 0) = B7;
	      *(a4 + 1) = B8;
	      *(b4 + 0) = A5;
	      *(b4 + 1) = A6;

	      *(a5 + 0) = A11;
	      *(a5 + 1) = A12;
	      *(a6 + 0) = B11;
	      *(a6 + 1) = B12;
	      *(b6 + 0) = A9;
	      *(b6 + 1) = A10;
	      *(a7 + 0) = A15;
	      *(a7 + 1) = A16;
	      *(a8 + 0) = B15;
	      *(a8 + 1) = B16;
	      *(b8 + 0) = A13;
	      *(b8 + 1) = A14;
	    }
	  }
	} else {
	  if (b2 == a1) {
	    *(a1 + 0) = A3;
	    *(a1 + 1) = A4;
	    *(a2 + 0) = B1;
	    *(a2 + 1) = B2;
	    *(b1 + 0) = A1;
	    *(b1 + 1) = A2;
	    *(a3 + 0) = A7;
	    *(a3 + 1) = A8;
	    *(a4 + 0) = B5;
	    *(a4 + 1) = B6;
	    *(b3 + 0) = A5;
	    *(b3 + 1) = A6;
	    *(a5 + 0) = A11;
	    *(a5 + 1) = A12;
	    *(a6 + 0) = B9;
	    *(a6 + 1) = B10;
	    *(b5 + 0) = A9;
	    *(b5 + 1) = A10;
	    *(a7 + 0) = A15;
	    *(a7 + 1) = A16;
	    *(a8 + 0) = B13;
	    *(a8 + 1) = B14;
	    *(b7 + 0) = A13;
	    *(b7 + 1) = A14;
	  } else
	    if (b2 == a2) {
	      *(a1 + 0) = B1;
	      *(a1 + 1) = B2;
	      *(b1 + 0) = A1;
	      *(b1 + 1) = A2;
	      *(a3 + 0) = B5;
	      *(a3 + 1) = B6;
	      *(b3 + 0) = A5;
	      *(b3 + 1) = A6;
	      *(a5 + 0) = B9;
	      *(a5 + 1) = B10;
	      *(b5 + 0) = A9;
	      *(b5 + 1) = A10;
	      *(a7 + 0) = B13;
	      *(a7 + 1) = B14;
	      *(b7 + 0) = A13;
	      *(b7 + 1) = A14;
	    } else
	      if (b2 == b1) {
		*(a1 + 0) = B1;
		*(a1 + 1) = B2;
		*(a2 + 0) = A1;
		*(a2 + 1) = A2;
		*(b1 + 0) = A3;
		*(b1 + 1) = A4;
		*(a3 + 0) = B5;
		*(a3 + 1) = B6;
		*(a4 + 0) = A5;
		*(a4 + 1) = A6;
		*(b3 + 0) = A7;
		*(b3 + 1) = A8;

		*(a5 + 0) = B9;
		*(a5 + 1) = B10;
		*(a6 + 0) = A9;
		*(a6 + 1) = A10;
		*(b5 + 0) = A11;
		*(b5 + 1) = A12;
		*(a7 + 0) = B13;
		*(a7 + 1) = B14;
		*(a8 + 0) = A13;
		*(a8 + 1) = A14;
		*(b7 + 0) = A15;
		*(b7 + 1) = A16;
	      } else {
		*(a1 + 0) = B1;
		*(a1 + 1) = B2;
		*(a2 + 0) = B3;
		*(a2 + 1) = B4;
		*(b1 + 0) = A1;
		*(b1 + 1) = A2;
		*(b2 + 0) = A3;
		*(b2 + 1) = A4;
		*(a3 + 0) = B5;
		*(a3 + 1) = B6;
		*(a4 + 0) = B7;
		*(a4 + 1) = B8;
		*(b3 + 0) = A5;
		*(b3 + 1) = A6;
		*(b4 + 0) = A7;
		*(b4 + 1) = A8;
		*(a5 + 0) = B9;
		*(a5 + 1) = B10;
		*(a6 + 0) = B11;
		*(a6 + 1) = B12;
		*(b5 + 0) = A9;
		*(b5 + 1) = A10;
		*(b6 + 0) = A11;
		*(b6 + 1) = A12;
		*(a7 + 0) = B13;
		*(a7 + 1) = B14;
		*(a8 + 0) = B15;
		*(a8 + 1) = B16;
		*(b7 + 0) = A13;
		*(b7 + 1) = A14;
		*(b8 + 0) = A15;
		*(b8 + 1) = A16;
	      }
	}

#ifndef MINUS
      a1 += 4;
      a3 += 4;
      a5 += 4;
      a7 += 4;
#else
      a1 -= 4;
      a3 -= 4;
      a5 -= 4;
      a7 -= 4;
#endif
      //Remain
      i = (rows & 1);

      if (i > 0) {
	ip1 = *piv * 2;
	b1 = a + ip1;
	b3 = b1 + 1 * lda;
	b5 = b1 + 2 * lda;
	b7 = b1 + 3 * lda;



	A1 = *(a1 + 0);
	A2 = *(a1 + 1);
	A3 = *(a3 + 0);
	A4 = *(a3 + 1);
	B1 = *(b1 + 0);
	B2 = *(b1 + 1);
	B3 = *(b3 + 0);
	B4 = *(b3 + 1);
	A5 = *(a5 + 0);
	A6 = *(a5 + 1);
	A7 = *(a7 + 0);
	A8 = *(a7 + 1);
	B5 = *(b5 + 0);
	B6 = *(b5 + 1);
	B7 = *(b7 + 0);
	B8 = *(b7 + 1);

	*(a1 + 0) = B1;
	*(a1 + 1) = B2;
	*(a3 + 0) = B3;
	*(a3 + 1) = B4;
	*(b1 + 0) = A1;
	*(b1 + 1) = A2;
	*(b3 + 0) = A3;
	*(b3 + 1) = A4;
	*(a5 + 0) = B5;
	*(a5 + 1) = B6;
	*(a7 + 0) = B7;
	*(a7 + 1) = B8;
	*(b5 + 0) = A5;
	*(b5 + 1) = A6;
	*(b7 + 0) = A7;
	*(b7 + 1) = A8;
      }

      a += 4 * lda;

      j --;
    } while (j > 0);
  }

  if (n & 2) {
    piv = ipiv;

#ifndef MINUS
    a1 = a + (k1 + 1) * 2;
#else
    a1 = a + k2 * 2;
#endif

    a3 = a1 + lda;

    ip1 = *piv * 2;
    piv += incx;
    ip2 = *piv * 2;
    piv += incx;

    b1 = a + ip1;
    b2 = a + ip2;

    b3 = b1 + lda;
    b4 = b2 + lda;

    i = (rows >> 1);
    i--;

    //Loop pipeline
    //Main Loop
    while (i > 0) {
      A1 = *(a1 + 0);
      A2 = *(a1 + 1);
      A3 = *(a2 + 0);
      A4 = *(a2 + 1);

      A5 = *(a3 + 0);
      A6 = *(a3 + 1);
      A7 = *(a4 + 0);
      A8 = *(a4 + 1);

      B1 = *(b1 + 0);
      B2 = *(b1 + 1);
      B3 = *(b2 + 0);
      B4 = *(b2 + 1);

      B5 = *(b3 + 0);
      B6 = *(b3 + 1);
      B7 = *(b4 + 0);
      B8 = *(b4 + 1);

      ip1 = *piv * 2;
      piv += incx;
      ip2 = *piv * 2;
      piv += incx;

      if (b1 == a1) {
	if (b2 == a1) {
	  *(a1 + 0) = A3;
	  *(a1 + 1) = A4;
	  *(a2 + 0) = A1;
	  *(a2 + 1) = A2;
	  *(a3 + 0) = A7;
	  *(a3 + 1) = A8;
	  *(a4 + 0) = A5;
	  *(a4 + 1) = A6;
	} else
	  if (b2 != a2) {
	    *(a2 + 0) = B3;
	    *(a2 + 1) = B4;
	    *(b2 + 0) = A3;
	    *(b2 + 1) = A4;
	    *(a4 + 0) = B7;
	    *(a4 + 1) = B8;
	    *(b4 + 0) = A7;
	    *(b4 + 1) = A8;
	  }
      } else
	if (b1 == a2) {
	  if (b2 != a1) {
	    if (b2 == a2) {
	      *(a1 + 0) = A3;
	      *(a1 + 1) = A4;
	      *(a2 + 0) = A1;
	      *(a2 + 1) = A2;
	      *(a3 + 0) = A7;
	      *(a3 + 1) = A8;
	      *(a4 + 0) = A5;
	      *(a4 + 1) = A6;
	    } else {
	      *(a1 + 0) = A3;
	      *(a1 + 1) = A4;
	      *(a2 + 0) = B3;
	      *(a2 + 1) = B4;
	      *(b2 + 0) = A1;
	      *(b2 + 1) = A2;
	      *(a3 + 0) = A7;
	      *(a3 + 1) = A8;
	      *(a4 + 0) = B7;
	      *(a4 + 1) = B8;
	      *(b4 + 0) = A5;
	      *(b4 + 1) = A6;
	    }
	  }
	} else {
	  if (b2 == a1) {
	    *(a1 + 0) = A3;
	    *(a1 + 1) = A4;
	    *(a2 + 0) = B1;
	    *(a2 + 1) = B2;
	    *(b1 + 0) = A1;
	    *(b1 + 1) = A2;
	    *(a3 + 0) = A7;
	    *(a3 + 1) = A8;
	    *(a4 + 0) = B5;
	    *(a4 + 1) = B6;
	    *(b3 + 0) = A5;
	    *(b3 + 1) = A6;
	  } else
	    if (b2 == a2) {
	      *(a1 + 0) = B1;
	      *(a1 + 1) = B2;
	      *(b1 + 0) = A1;
	      *(b1 + 1) = A2;
	      *(a3 + 0) = B5;
	      *(a3 + 1) = B6;
	      *(b3 + 0) = A5;
	      *(b3 + 1) = A6;
	    } else
	      if (b2 == b1) {
		*(a1 + 0) = B1;
		*(a1 + 1) = B2;
		*(a2 + 0) = A1;
		*(a2 + 1) = A2;
		*(b1 + 0) = A3;
		*(b1 + 1) = A4;
		*(a3 + 0) = B5;
		*(a3 + 1) = B6;
		*(a4 + 0) = A5;
		*(a4 + 1) = A6;
		*(b3 + 0) = A7;
		*(b3 + 1) = A8;
	      } else {
		*(a1 + 0) = B1;
		*(a1 + 1) = B2;
		*(a2 + 0) = B3;
		*(a2 + 1) = B4;
		*(b1 + 0) = A1;
		*(b1 + 1) = A2;
		*(b2 + 0) = A3;
		*(b2 + 1) = A4;
		*(a3 + 0) = B5;
		*(a3 + 1) = B6;
		*(a4 + 0) = B7;
		*(a4 + 1) = B8;
		*(b3 + 0) = A5;
		*(b3 + 1) = A6;
		*(b4 + 0) = A7;
		*(b4 + 1) = A8;
	      }
	}

      b1 = a + ip1;
      b2 = a + ip2;

      b3 = b1 + lda;
      b4 = b2 + lda;

#ifndef MINUS
      a1 += 4;
      a3 += 4;
#else
      a1 -= 4;
      a3 -= 4;
#endif
      i --;
    }
    //Loop Ending
    A1 = *(a1 + 0);
    A2 = *(a1 + 1);
    A3 = *(a2 + 0);
    A4 = *(a2 + 1);

    A5 = *(a3 + 0);
    A6 = *(a3 + 1);
    A7 = *(a4 + 0);
    A8 = *(a4 + 1);

    B1 = *(b1 + 0);
    B2 = *(b1 + 1);
    B3 = *(b2 + 0);
    B4 = *(b2 + 1);

    B5 = *(b3 + 0);
    B6 = *(b3 + 1);
    B7 = *(b4 + 0);
    B8 = *(b4 + 1);


    if (b1 == a1) {
      if (b2 == a1) {
	*(a1 + 0) = A3;
	*(a1 + 1) = A4;
	*(a2 + 0) = A1;
	*(a2 + 1) = A2;
	*(a3 + 0) = A7;
	*(a3 + 1) = A8;
	*(a4 + 0) = A5;
	*(a4 + 1) = A6;
      } else
	if (b2 != a2) {
	  *(a2 + 0) = B3;
	  *(a2 + 1) = B4;
	  *(b2 + 0) = A3;
	  *(b2 + 1) = A4;
	  *(a4 + 0) = B7;
	  *(a4 + 1) = B8;
	  *(b4 + 0) = A7;
	  *(b4 + 1) = A8;
	}
    } else
      if (b1 == a2) {
	if (b2 != a1) {
	  if (b2 == a2) {
	    *(a1 + 0) = A3;
	    *(a1 + 1) = A4;
	    *(a2 + 0) = A1;
	    *(a2 + 1) = A2;
	    *(a3 + 0) = A7;
	    *(a3 + 1) = A8;
	    *(a4 + 0) = A5;
	    *(a4 + 1) = A6;
	  } else {
	    *(a1 + 0) = A3;
	    *(a1 + 1) = A4;
	    *(a2 + 0) = B3;
	    *(a2 + 1) = B4;
	    *(b2 + 0) = A1;
	    *(b2 + 1) = A2;
	    *(a3 + 0) = A7;
	    *(a3 + 1) = A8;
	    *(a4 + 0) = B7;
	    *(a4 + 1) = B8;
	    *(b4 + 0) = A5;
	    *(b4 + 1) = A6;
	  }
	}
      } else {
	if (b2 == a1) {
	  *(a1 + 0) = A3;
	  *(a1 + 1) = A4;
	  *(a2 + 0) = B1;
	  *(a2 + 1) = B2;
	  *(b1 + 0) = A1;
	  *(b1 + 1) = A2;
	  *(a3 + 0) = A7;
	  *(a3 + 1) = A8;
	  *(a4 + 0) = B5;
	  *(a4 + 1) = B6;
	  *(b3 + 0) = A5;
	  *(b3 + 1) = A6;
	} else
	  if (b2 == a2) {
	    *(a1 + 0) = B1;
	    *(a1 + 1) = B2;
	    *(b1 + 0) = A1;
	    *(b1 + 1) = A2;
	    *(a3 + 0) = B5;
	    *(a3 + 1) = B6;
	    *(b3 + 0) = A5;
	    *(b3 + 1) = A6;
	  } else
	    if (b2 == b1) {
	      *(a1 + 0) = B1;
	      *(a1 + 1) = B2;
	      *(a2 + 0) = A1;
	      *(a2 + 1) = A2;
	      *(b1 + 0) = A3;
	      *(b1 + 1) = A4;
	      *(a3 + 0) = B5;
	      *(a3 + 1) = B6;
	      *(a4 + 0) = A5;
	      *(a4 + 1) = A6;
	      *(b3 + 0) = A7;
	      *(b3 + 1) = A8;
	    } else {
	      *(a1 + 0) = B1;
	      *(a1 + 1) = B2;
	      *(a2 + 0) = B3;
	      *(a2 + 1) = B4;
	      *(b1 + 0) = A1;
	      *(b1 + 1) = A2;
	      *(b2 + 0) = A3;
	      *(b2 + 1) = A4;
	      *(a3 + 0) = B5;
	      *(a3 + 1) = B6;
	      *(a4 + 0) = B7;
	      *(a4 + 1) = B8;
	      *(b3 + 0) = A5;
	      *(b3 + 1) = A6;
	      *(b4 + 0) = A7;
	      *(b4 + 1) = A8;
	    }
      }

#ifndef MINUS
    a1 += 4;
    a3 += 4;
#else
    a1 -= 4;
    a3 -= 4;
#endif

    //Remain
    i = (rows & 1);

    if (i > 0) {
      ip1 = *piv * 2;

      b1 = a + ip1;
      b3 = b1 + lda;

      A1 = *(a1 + 0);
      A2 = *(a1 + 1);
      A3 = *(a3 + 0);
      A4 = *(a3 + 1);
      B1 = *(b1 + 0);
      B2 = *(b1 + 1);
      B3 = *(b3 + 0);
      B4 = *(b3 + 1);
      *(a1 + 0) = B1;
      *(a1 + 1) = B2;
      *(a3 + 0) = B3;
      *(a3 + 1) = B4;
      *(b1 + 0) = A1;
      *(b1 + 1) = A2;
      *(b3 + 0) = A3;
      *(b3 + 1) = A4;
    }

    a += 2 * lda;

  }

  if (n & 1) {
    piv = ipiv;

#ifndef MINUS
    a1 = a + (k1 + 1) * 2;
#else
    a1 = a + k2 * 2;
#endif

    ip1 = *piv * 2;
    piv += incx;
    ip2 = *piv * 2;
    piv += incx;

    b1 = a + ip1;
    b2 = a + ip2;

    i = (rows >> 1);
    i--;

    //Loop pipeline
    //Main Loop
    while (i > 0) {
	A1 = *(a1 + 0);
	A2 = *(a1 + 1);
	A3 = *(a2 + 0);
	A4 = *(a2 + 1);
	B1 = *(b1 + 0);
	B2 = *(b1 + 1);
	B3 = *(b2 + 0);
	B4 = *(b2 + 1);

	ip1 = *piv * 2;
	piv += incx;
	ip2 = *piv * 2;
	piv += incx;

	if (b1 == a1) {
	  if (b2 == a1) {
	    *(a1 + 0) = A3;
	    *(a1 + 1) = A4;
	    *(a2 + 0) = A1;
	    *(a2 + 1) = A2;
	  } else
	    if (b2 != a2) {
	      *(a2 + 0) = B3;
	      *(a2 + 1) = B4;
	      *(b2 + 0) = A3;
	      *(b2 + 1) = A4;
	    }
	} else
	  if (b1 == a2) {
	    if (b2 != a1) {
	      if (b2 == a2) {
		*(a1 + 0) = A3;
		*(a1 + 1) = A4;
		*(a2 + 0) = A1;
		*(a2 + 1) = A2;
	      } else {
		*(a1 + 0) = A3;
		*(a1 + 1) = A4;
		*(a2 + 0) = B3;
		*(a2 + 1) = B4;
		*(b2 + 0) = A1;
		*(b2 + 1) = A2;
	      }
	    }
	  } else {
	    if (b2 == a1) {
	      *(a1 + 0) = A3;
	      *(a1 + 1) = A4;
	      *(a2 + 0) = B1;
	      *(a2 + 1) = B2;
	      *(b1 + 0) = A1;
	      *(b1 + 1) = A2;
	    } else
	      if (b2 == a2) {
		*(a1 + 0) = B1;
		*(a1 + 1) = B2;
		*(b1 + 0) = A1;
		*(b1 + 1) = A2;
	      } else
		if (b2 == b1) {
		  *(a1 + 0) = B1;
		  *(a1 + 1) = B2;
		  *(a2 + 0) = A1;
		  *(a2 + 1) = A2;
		  *(b1 + 0) = A3;
		  *(b1 + 1) = A4;
		} else {
		  *(a1 + 0) = B1;
		  *(a1 + 1) = B2;
		  *(a2 + 0) = B3;
		  *(a2 + 1) = B4;
		  *(b1 + 0) = A1;
		  *(b1 + 1) = A2;
		  *(b2 + 0) = A3;
		  *(b2 + 1) = A4;
		}
	  }

	b1 = a + ip1;
	b2 = a + ip2;

#ifndef MINUS
	a1 += 4;
#else
	a1 -= 4;
#endif
	i --;
    }
    //Loop Ending
    A1 = *(a1 + 0);
    A2 = *(a1 + 1);
    A3 = *(a2 + 0);
    A4 = *(a2 + 1);
    B1 = *(b1 + 0);
    B2 = *(b1 + 1);
    B3 = *(b2 + 0);
    B4 = *(b2 + 1);

    if (b1 == a1) {
      if (b2 == a1) {
	*(a1 + 0) = A3;
	*(a1 + 1) = A4;
	*(a2 + 0) = A1;
	*(a2 + 1) = A2;
      } else
	if (b2 != a2) {
	  *(a2 + 0) = B3;
	  *(a2 + 1) = B4;
	  *(b2 + 0) = A3;
	  *(b2 + 1) = A4;
	}
    } else
      if (b1 == a2) {
	if (b2 != a1) {
	  if (b2 == a2) {
	    *(a1 + 0) = A3;
	    *(a1 + 1) = A4;
	    *(a2 + 0) = A1;
	    *(a2 + 1) = A2;
	  } else {
	    *(a1 + 0) = A3;
	    *(a1 + 1) = A4;
	    *(a2 + 0) = B3;
	    *(a2 + 1) = B4;
	    *(b2 + 0) = A1;
	    *(b2 + 1) = A2;
	  }
	}
      } else {
	if (b2 == a1) {
	  *(a1 + 0) = A3;
	  *(a1 + 1) = A4;
	  *(a2 + 0) = B1;
	  *(a2 + 1) = B2;
	  *(b1 + 0) = A1;
	  *(b1 + 1) = A2;
	} else
	  if (b2 == a2) {
	    *(a1 + 0) = B1;
	    *(a1 + 1) = B2;
	    *(b1 + 0) = A1;
	    *(b1 + 1) = A2;
	  } else
	    if (b2 == b1) {
	      *(a1 + 0) = B1;
	      *(a1 + 1) = B2;
	      *(a2 + 0) = A1;
	      *(a2 + 1) = A2;
	      *(b1 + 0) = A3;
	      *(b1 + 1) = A4;
	    } else {
	      *(a1 + 0) = B1;
	      *(a1 + 1) = B2;
	      *(a2 + 0) = B3;
	      *(a2 + 1) = B4;
	      *(b1 + 0) = A1;
	      *(b1 + 1) = A2;
	      *(b2 + 0) = A3;
	      *(b2 + 1) = A4;
	    }
      }

#ifndef MINUS
    a1 += 4;
#else
    a1 -= 4;
#endif

    //Remain
    i = (rows & 1);

    if (i > 0) {
      ip1 = *piv * 2;
      b1 = a + ip1;

      A1 = *(a1 + 0);
      A2 = *(a1 + 1);
      B1 = *(b1 + 0);
      B2 = *(b1 + 1);
      *(a1 + 0) = B1;
      *(a1 + 1) = B2;
      *(b1 + 0) = A1;
      *(b1 + 1) = A2;
    }
  }

  return 0;
}

