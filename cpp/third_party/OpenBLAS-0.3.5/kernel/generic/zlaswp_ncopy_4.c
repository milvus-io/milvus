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

#define a2	(a1 + 2)
#define a4	(a3 + 2)
#define a6	(a5 + 2)
#define a8	(a7 + 2)

int CNAME(BLASLONG n, BLASLONG k1, BLASLONG k2, FLOAT *a, BLASLONG lda, blasint *ipiv, FLOAT *buffer){

  BLASLONG i, j, ip1, ip2;
  blasint *piv;
  FLOAT *a1, *a3, *a5, *a7;
  FLOAT *b1, *b2, *b3, *b4;
  FLOAT *b5, *b6, *b7, *b8;
  FLOAT A1, A2, A3, A4, A5, A6, A7, A8;
  FLOAT B1, B2, B3, B4, B5, B6, B7, B8;

  FLOAT A9, A10, A11, A12, A13, A14, A15, A16;
  FLOAT B9, B10, B11, B12, B13, B14, B15, B16;

  a -= 2;
  lda *= 2;
  k1 --;

 ipiv += k1;

  if (n  <= 0) return 0;

  j = (n >> 2);
  if (j > 0) {
    do {
      piv = ipiv;

      a1 = a + (k1 + 1) * 2;

      a3 = a1 + 1 * lda;
      a5 = a1 + 2 * lda;
      a7 = a1 + 3 * lda;

      ip1 = *(piv + 0) * 2;
      ip2 = *(piv + 1) * 2;
      piv += 2;

      b1 = a + ip1;
      b2 = a + ip2;

      b3 = b1 + 1 * lda;
      b4 = b2 + 1 * lda;
      b5 = b1 + 2 * lda;
      b6 = b2 + 2 * lda;
      b7 = b1 + 3 * lda;
      b8 = b2 + 3 * lda;

      i = ((k2 - k1) >> 1);

      if (i > 0) {
	do {
	  A1  = *(a1 + 0);
	  A9  = *(a1 + 1);
	  A2  = *(a2 + 0);
	  A10 = *(a2 + 1);
	  A3  = *(a3 + 0);
	  A11 = *(a3 + 1);
	  A4  = *(a4 + 0);
	  A12 = *(a4 + 1);
	  A5  = *(a5 + 0);
	  A13 = *(a5 + 1);
	  A6  = *(a6 + 0);
	  A14 = *(a6 + 1);
	  A7  = *(a7 + 0);
	  A15 = *(a7 + 1);
	  A8  = *(a8 + 0);
	  A16 = *(a8 + 1);

	  B1  = *(b1 + 0);
	  B9  = *(b1 + 1);
	  B2  = *(b2 + 0);
	  B10 = *(b2 + 1);
	  B3  = *(b3 + 0);
	  B11 = *(b3 + 1);
	  B4  = *(b4 + 0);
	  B12 = *(b4 + 1);
	  B5  = *(b5 + 0);
	  B13 = *(b5 + 1);
	  B6  = *(b6 + 0);
	  B14 = *(b6 + 1);
	  B7  = *(b7 + 0);
	  B15 = *(b7 + 1);
	  B8  = *(b8 + 0);
	  B16 = *(b8 + 1);

	  ip1 = *(piv + 0) * 2;
	  ip2 = *(piv + 1) * 2;
	  piv += 2;

	if (b1 == a1) {
	    if (b2 == a2) {
	      *(buffer +  0) = A1;
	      *(buffer +  1) = A9;
	      *(buffer +  2) = A3;
	      *(buffer +  3) = A11;
	      *(buffer +  4) = A5;
	      *(buffer +  5) = A13;
	      *(buffer +  6) = A7;
	      *(buffer +  7) = A15;

	      *(buffer +  8) = A2;
	      *(buffer +  9) = A10;
	      *(buffer + 10) = A4;
	      *(buffer + 11) = A12;
	      *(buffer + 12) = A6;
	      *(buffer + 13) = A14;
	      *(buffer + 14) = A8;
	      *(buffer + 15) = A16;
	    } else {
	      *(buffer +  0) = A1;
	      *(buffer +  1) = A9;
	      *(buffer +  2) = A3;
	      *(buffer +  3) = A11;
	      *(buffer +  4) = A5;
	      *(buffer +  5) = A13;
	      *(buffer +  6) = A7;
	      *(buffer +  7) = A15;

	      *(buffer +  8) = B2;
	      *(buffer +  9) = B10;
	      *(buffer + 10) = B4;
	      *(buffer + 11) = B12;
	      *(buffer + 12) = B6;
	      *(buffer + 13) = B14;
	      *(buffer + 14) = B8;
	      *(buffer + 15) = B16;

	      *(b2 + 0) = A2;
	      *(b2 + 1) = A10;
	      *(b4 + 0) = A4;
	      *(b4 + 1) = A12;
	      *(b6 + 0) = A6;
	      *(b6 + 1) = A14;
	      *(b8 + 0) = A8;
	      *(b8 + 1) = A16;
	    }
	} else
	  if (b1 == a2) {
	      if (b2 == a2) {
		*(buffer +  0) = A2;
		*(buffer +  1) = A10;
		*(buffer +  2) = A4;
		*(buffer +  3) = A12;
		*(buffer +  4) = A6;
		*(buffer +  5) = A14;
		*(buffer +  6) = A8;
		*(buffer +  7) = A16;
		*(buffer +  8) = A1;
		*(buffer +  9) = A9;
		*(buffer + 10) = A3;
		*(buffer + 11) = A11;
		*(buffer + 12) = A5;
		*(buffer + 13) = A13;
		*(buffer + 14) = A7;
		*(buffer + 15) = A15;

	      } else {
		*(buffer +  0) = A2;
		*(buffer +  1) = A10;
		*(buffer +  2) = A4;
		*(buffer +  3) = A12;
		*(buffer +  4) = A6;
		*(buffer +  5) = A14;
		*(buffer +  6) = A8;
		*(buffer +  7) = A16;
		*(buffer +  8) = B2;
		*(buffer +  9) = B10;
		*(buffer + 10) = B4;
		*(buffer + 11) = B12;
		*(buffer + 12) = B6;
		*(buffer + 13) = B14;
		*(buffer + 14) = B8;
		*(buffer + 15) = B16;

		*(b2 + 0) = A1;
		*(b2 + 1) = A9;
		*(b4 + 0) = A3;
		*(b4 + 1) = A11;
		*(b6 + 0) = A5;
		*(b6 + 1) = A13;
		*(b8 + 0) = A7;
		*(b8 + 1) = A15;
	      }
	  } else {
	      if (b2 == a2) {
		*(buffer +  0) = B1;
		*(buffer +  1) = B9;
		*(buffer +  2) = B3;
		*(buffer +  3) = B11;
		*(buffer +  4) = B5;
		*(buffer +  5) = B13;
		*(buffer +  6) = B7;
		*(buffer +  7) = B15;
		*(buffer +  8) = A2;
		*(buffer +  9) = A10;
		*(buffer + 10) = A4;
		*(buffer + 11) = A12;
		*(buffer + 12) = A6;
		*(buffer + 13) = A14;
		*(buffer + 14) = A8;
		*(buffer + 15) = A16;

		*(b1 + 0) = A1;
		*(b1 + 1) = A9;
		*(b3 + 0) = A3;
		*(b3 + 1) = A11;
		*(b5 + 0) = A5;
		*(b5 + 1) = A13;
		*(b7 + 0) = A7;
		*(b7 + 1) = A15;
	      } else
		if (b2 == b1) {
		  *(buffer +  0) = B1;
		  *(buffer +  1) = B9;
		  *(buffer +  2) = B3;
		  *(buffer +  3) = B11;
		  *(buffer +  4) = B5;
		  *(buffer +  5) = B13;
		  *(buffer +  6) = B7;
		  *(buffer +  7) = B15;
		  *(buffer +  8) = A1;
		  *(buffer +  9) = A9;
		  *(buffer + 10) = A3;
		  *(buffer + 11) = A11;
		  *(buffer + 12) = A5;
		  *(buffer + 13) = A13;
		  *(buffer + 14) = A7;
		  *(buffer + 15) = A15;

		  *(b1 + 0) = A2;
		  *(b1 + 1) = A10;
		  *(b3 + 0) = A4;
		  *(b3 + 1) = A12;
		  *(b5 + 0) = A6;
		  *(b5 + 1) = A14;
		  *(b7 + 0) = A8;
		  *(b7 + 1) = A16;
		} else {
		  *(buffer +  0) = B1;
		  *(buffer +  1) = B9;
		  *(buffer +  2) = B3;
		  *(buffer +  3) = B11;
		  *(buffer +  4) = B5;
		  *(buffer +  5) = B13;
		  *(buffer +  6) = B7;
		  *(buffer +  7) = B15;
		  *(buffer +  8) = B2;
		  *(buffer +  9) = B10;
		  *(buffer + 10) = B4;
		  *(buffer + 11) = B12;
		  *(buffer + 12) = B6;
		  *(buffer + 13) = B14;
		  *(buffer + 14) = B8;
		  *(buffer + 15) = B16;

		  *(b1 + 0) = A1;
		  *(b1 + 1) = A9;
		  *(b2 + 0) = A2;
		  *(b2 + 1) = A10;
		  *(b3 + 0) = A3;
		  *(b3 + 1) = A11;
		  *(b4 + 0) = A4;
		  *(b4 + 1) = A12;
		  *(b5 + 0) = A5;
		  *(b5 + 1) = A13;
		  *(b6 + 0) = A6;
		  *(b6 + 1) = A14;
		  *(b7 + 0) = A7;
		  *(b7 + 1) = A15;
		  *(b8 + 0) = A8;
		  *(b8 + 1) = A16;
		}
	  }

	 buffer += 16;

	  b1 = a + ip1;
	  b2 = a + ip2;

	  b3 = b1 + 1 * lda;
	  b4 = b2 + 1 * lda;
	  b5 = b1 + 2 * lda;
	  b6 = b2 + 2 * lda;
	  b7 = b1 + 3 * lda;
	  b8 = b2 + 3 * lda;

	  a1 += 4;
	  a3 += 4;
	  a5 += 4;
	  a7 += 4;

	i --;
	} while (i > 0);
      }

      i = ((k2 - k1) & 1);

      if (i > 0) {
	A1  = *(a1 + 0);
	A9  = *(a1 + 1);
	B1  = *(b1 + 0);
	B9  = *(b1 + 1);
	A3  = *(a3 + 0);
	A11 = *(a3 + 1);
	B3  = *(b3 + 0);
	B11 = *(b3 + 1);
	A5  = *(a5 + 0);
	A13 = *(a5 + 1);
	B5  = *(b5 + 0);
	B13 = *(b5 + 1);
	A7  = *(a7 + 0);
	A15 = *(a7 + 1);
	B7  = *(b7 + 0);
	B15 = *(b7 + 1);

	if (a1 == b1) {
	  *(buffer + 0) = A1;
	  *(buffer + 1) = A9;
	  *(buffer + 2) = A3;
	  *(buffer + 3) = A11;
	  *(buffer + 4) = A5;
	  *(buffer + 5) = A13;
	  *(buffer + 6) = A7;
	  *(buffer + 7) = A15;
	} else {
	  *(buffer + 0) = B1;
	  *(buffer + 1) = B9;
	  *(buffer + 2) = B3;
	  *(buffer + 3) = B11;
	  *(buffer + 4) = B5;
	  *(buffer + 5) = B13;
	  *(buffer + 6) = B7;
	  *(buffer + 7) = B15;

	  *(b1 + 0) = A1;
	  *(b1 + 1) = A9;
	  *(b3 + 0) = A3;
	  *(b3 + 1) = A11;
	  *(b5 + 0) = A5;
	  *(b5 + 1) = A13;
	  *(b7 + 0) = A7;
	  *(b7 + 1) = A15;
	}
	buffer += 8;
      }

      a += 4 * lda;

      j --;
    } while (j > 0);
  }

  if (n & 2) {
    piv = ipiv;

    a1 = a + (k1 + 1) * 2;
    a3 = a1 + lda;

    ip1 = *(piv + 0) * 2;
    ip2 = *(piv + 1) * 2;
    piv += 2;

    b1 = a + ip1;
    b2 = a + ip2;

    b3 = b1 + lda;
    b4 = b2 + lda;

    i = ((k2 - k1) >> 1);

    if (i > 0) {
      do {
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

	ip1 = *(piv + 0) * 2;
	ip2 = *(piv + 1) * 2;
	piv += 2;

	if (b1 == a1) {
	  if (b2 == a2) {
	    *(buffer + 0) = A1;
	    *(buffer + 1) = A2;
	    *(buffer + 2) = A5;
	    *(buffer + 3) = A6;
	    *(buffer + 4) = A3;
	    *(buffer + 5) = A4;
	    *(buffer + 6) = A7;
	    *(buffer + 7) = A8;
	  } else {
	    *(buffer + 0) = A1;
	    *(buffer + 1) = A2;
	    *(buffer + 2) = A5;
	    *(buffer + 3) = A6;
	    *(buffer + 4) = B3;
	    *(buffer + 5) = B4;
	    *(buffer + 6) = B7;
	    *(buffer + 7) = B8;

	    *(b2 + 0) = A3;
	    *(b2 + 1) = A4;
	    *(b4 + 0) = A7;
	    *(b4 + 1) = A8;
	  }
	} else {
	  if (b1 == a2) {
	    if (b2 == a2) {
	      *(buffer + 0) = A3;
	      *(buffer + 1) = A4;
	      *(buffer + 2) = A7;
	      *(buffer + 3) = A8;
	      *(buffer + 4) = A1;
	      *(buffer + 5) = A2;
	      *(buffer + 6) = A5;
	      *(buffer + 7) = A6;
	    } else {
	      *(buffer + 0) = A3;
	      *(buffer + 1) = A4;
	      *(buffer + 2) = A7;
	      *(buffer + 3) = A8;
	      *(buffer + 4) = B3;
	      *(buffer + 5) = B4;
	      *(buffer + 6) = B7;
	      *(buffer + 7) = B8;

	      *(b2 + 0) = A1;
	      *(b2 + 1) = A2;
	      *(b4 + 0) = A5;
	      *(b4 + 1) = A6;
	    }
	  } else {
	    if (b2 == a2) {
	      *(buffer + 0) = B1;
	      *(buffer + 1) = B2;
	      *(buffer + 2) = B5;
	      *(buffer + 3) = B6;
	      *(buffer + 4) = A3;
	      *(buffer + 5) = A4;
	      *(buffer + 6) = A7;
	      *(buffer + 7) = A8;

	      *(b1 + 0) = A1;
	      *(b1 + 1) = A2;
	      *(b3 + 0) = A5;
	      *(b3 + 1) = A6;
	    } else {
	      if (b2 == b1) {
		*(buffer + 0) = B1;
		*(buffer + 1) = B2;
		*(buffer + 2) = B5;
		*(buffer + 3) = B6;
		*(buffer + 4) = A1;
		*(buffer + 5) = A2;
		*(buffer + 6) = A5;
		*(buffer + 7) = A6;

		*(b1 + 0) = A3;
		*(b1 + 1) = A4;
		*(b3 + 0) = A7;
		*(b3 + 1) = A8;
	      } else {
		*(buffer + 0) = B1;
		*(buffer + 1) = B2;
		*(buffer + 2) = B5;
		*(buffer + 3) = B6;
		*(buffer + 4) = B3;
		*(buffer + 5) = B4;
		*(buffer + 6) = B7;
		*(buffer + 7) = B8;
		*(b1 + 0) = A1;
		*(b1 + 1) = A2;
		*(b2 + 0) = A3;
		*(b2 + 1) = A4;
		*(b3 + 0) = A5;
		*(b3 + 1) = A6;
		*(b4 + 0) = A7;
		*(b4 + 1) = A8;
	      }
	    }
	  }
	  }

	  buffer += 8;

	  b1 = a + ip1;
	  b2 = a + ip2;

	  b3 = b1 + lda;
	  b4 = b2 + lda;

	  a1 += 4;
	  a3 += 4;

	  i --;
      } while (i > 0);
    }

    i = ((k2 - k1) & 1);

    if (i > 0) {
      A1 = *(a1 + 0);
      A2 = *(a1 + 1);
      B1 = *(b1 + 0);
      B2 = *(b1 + 1);
      A3 = *(a3 + 0);
      A4 = *(a3 + 1);
      B3 = *(b3 + 0);
      B4 = *(b3 + 1);

      if (a1 == b1) {
	*(buffer + 0) = A1;
	*(buffer + 1) = A2;
	*(buffer + 2) = A3;
	*(buffer + 3) = A4;

      } else {
	*(buffer + 0) = B1;
	*(buffer + 1) = B2;
	*(buffer + 2) = B3;
	*(buffer + 3) = B4;
	*(b1 + 0) = A1;
	*(b1 + 1) = A2;
	*(b3 + 0) = A3;
	*(b3 + 1) = A4;
      }
      buffer += 4;
    }

    a += 2 * lda;
  }

  if (n & 1) {
    piv = ipiv;

    a1 = a + (k1 + 1) * 2;

    ip1 = *(piv + 0) * 2;
    ip2 = *(piv + 1) * 2;
    piv += 2;

    b1 = a + ip1;
    b2 = a + ip2;

    i = ((k2 - k1) >> 1);

    if (i > 0) {
      do {
	A1 = *(a1 + 0);
	A2 = *(a1 + 1);
	A3 = *(a2 + 0);
	A4 = *(a2 + 1);
	B1 = *(b1 + 0);
	B2 = *(b1 + 1);
	B3 = *(b2 + 0);
	B4 = *(b2 + 1);

	ip1 = *(piv + 0) * 2;
	ip2 = *(piv + 1) * 2;
	piv += 2;

	if (b1 == a1) {
	  if (b2 == a2) {
	    *(buffer + 0) = A1;
	    *(buffer + 1) = A2;
	    *(buffer + 2) = A3;
	    *(buffer + 3) = A4;
	  } else {
	    *(buffer + 0) = A1;
	    *(buffer + 1) = A2;
	    *(buffer + 2) = B3;
	    *(buffer + 3) = B4;

	    *(b2 + 0) = A3;
	    *(b2 + 1) = A4;
	  }
	} else
	  if (b1 == a2) {
	    if (b2 == a2) {
	      *(buffer + 0) = A3;
	      *(buffer + 1) = A4;
	      *(buffer + 2) = A1;
	      *(buffer + 3) = A2;
	    } else {
	      *(buffer + 0) = A3;
	      *(buffer + 1) = A4;
	      *(buffer + 2) = B3;
	      *(buffer + 3) = B4;
	      *(b2 + 0) = A1;
	      *(b2 + 1) = A2;
	    }
	  } else {
	    if (b2 == a2) {
	      *(buffer + 0) = B1;
	      *(buffer + 1) = B2;
	      *(buffer + 2) = A3;
	      *(buffer + 3) = A4;
	      *(b1 + 0) = A1;
	      *(b1 + 1) = A2;
	    } else
	      if (b2 == b1) {
		*(buffer + 0) = B1;
		*(buffer + 1) = B2;
		*(buffer + 2) = A1;
		*(buffer + 3) = A2;
		*(b1 + 0) = A3;
		*(b1 + 1) = A4;
	      } else {
		*(buffer + 0) = B1;
		*(buffer + 1) = B2;
		*(buffer + 2) = B3;
		*(buffer + 3) = B4;
		*(b1 + 0) = A1;
		*(b1 + 1) = A2;
		*(b2 + 0) = A3;
		*(b2 + 1) = A4;
	      }
	  }

	buffer += 4;

	b1 = a + ip1;
	b2 = a + ip2;

	a1 += 4;

	i --;
      } while (i > 0);
    }

    i = ((k2 - k1) & 1);

    if (i > 0) {
      A1 = *(a1 + 0);
      A2 = *(a1 + 1);
      B1 = *(b1 + 0);
      B2 = *(b1 + 1);

      if (a1 == b1) {
	*(buffer + 0) = A1;
	*(buffer + 1) = A2;
      } else {
	*(buffer + 0) = B1;
	*(buffer + 1) = B2;
	*(b1 + 0) = A1;
	*(b1 + 1) = A2;
      }
      // buffer += 2;
    }
  }

  return 0;
}

