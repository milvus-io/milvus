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
#else
#define a2	(a1 - 2)
#endif

int CNAME(BLASLONG n, BLASLONG k1, BLASLONG k2, FLOAT dummy1, FLOAT dummy4,
	  FLOAT *a, BLASLONG lda,
	  FLOAT *dummy2, BLASLONG dumy3, blasint *ipiv, BLASLONG incx){

  BLASLONG i, j, ip1, ip2, rows;
  blasint *piv;
  FLOAT *a1;
  FLOAT *b1, *b2;
  FLOAT A1, A2, B1, B2, A3, A4, B3, B4;

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

  j = n;
  if (j > 0) {
    do {
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

      i = ((k2 - k1) >> 1);
      i --;
      //Loop pipeline
      //Main Loop
      while (i > 0) {
#ifdef OPTERON
#ifndef MINUS
	  asm volatile("prefetchw  2 * 128(%0)\n"  : : "r"(a1));
	  asm volatile("prefetchw  2 * 128(%0)\n"  : : "r"(b1));
#else
	  asm volatile("prefetchw -2 * 128(%0)\n"  : : "r"(a1));
	  asm volatile("prefetchw -2 * 128(%0)\n"  : : "r"(b1));
#endif
#endif

#ifdef CORE2
#ifndef MINUS
	  asm volatile("prefetcht1  2 * 128(%0)\n"  : : "r"(a1));
	  asm volatile("prefetcht1  2 * 128(%0)\n"  : : "r"(b1));
	  asm volatile("prefetcht1  2 * 128(%0)\n"  : : "r"(b2));
#else
	  asm volatile("prefetcht1 -2 * 128(%0)\n"  : : "r"(a1));
	  asm volatile("prefetcht1 -2 * 128(%0)\n"  : : "r"(b1));
	  asm volatile("prefetcht1 -2 * 128(%0)\n"  : : "r"(b2));
#endif
#endif
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

      a += lda;

      j --;
    } while (j > 0);
  }

  return 0;
}

