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

#include "common.h"

#ifdef SMP
#if !defined(USE64BITINT) || defined(ARCH_X86)
unsigned int blas_quick_divide_table[] = {
  0x00000000, 0x00000001, 0x80000001, 0x55555556,
  0x40000001, 0x33333334, 0x2aaaaaab, 0x24924925,
  0x20000001, 0x1c71c71d, 0x1999999a, 0x1745d175,
  0x15555556, 0x13b13b14, 0x12492493, 0x11111112,
  0x10000001, 0x0f0f0f10, 0x0e38e38f, 0x0d79435f,
  0x0ccccccd, 0x0c30c30d, 0x0ba2e8bb, 0x0b21642d,
  0x0aaaaaab, 0x0a3d70a4, 0x09d89d8a, 0x097b425f,
  0x0924924a, 0x08d3dcb1, 0x08888889, 0x08421085,
  0x08000001, 0x07c1f07d, 0x07878788, 0x07507508,
  0x071c71c8, 0x06eb3e46, 0x06bca1b0, 0x06906907,
  0x06666667, 0x063e7064, 0x06186187, 0x05f417d1,
  0x05d1745e, 0x05b05b06, 0x0590b217, 0x0572620b,
  0x05555556, 0x0539782a, 0x051eb852, 0x05050506,
  0x04ec4ec5, 0x04d4873f, 0x04bda130, 0x04a7904b,
  0x04924925, 0x047dc120, 0x0469ee59, 0x0456c798,
  0x04444445, 0x04325c54, 0x04210843, 0x04104105,
  0x04000001,
};
#else
BLASULONG blas_quick_divide_table[] = {
0x0000000000000000, 0x0000000000000001, 0x8000000000000001, 0x5555555555555557,
0x4000000000000001, 0x3333333333333335, 0x2aaaaaaaaaaaaaac, 0x2492492492492494,
0x2000000000000001, 0x1c71c71c71c71c73, 0x199999999999999b, 0x1745d1745d1745d3,
0x1555555555555557, 0x13b13b13b13b13b3, 0x124924924924924b, 0x1111111111111113,
0x1000000000000001, 0x0f0f0f0f0f0f0f11, 0x0e38e38e38e38e3a, 0x0d79435e50d79437,
0x0cccccccccccccce, 0x0c30c30c30c30c32, 0x0ba2e8ba2e8ba2ea, 0x0b21642c8590b218,
0x0aaaaaaaaaaaaaac, 0x0a3d70a3d70a3d72, 0x09d89d89d89d89da, 0x097b425ed097b427,
0x0924924924924926, 0x08d3dcb08d3dcb0a, 0x088888888888888a, 0x0842108421084212,
0x0800000000000001, 0x07c1f07c1f07c1f2, 0x0787878787878789, 0x0750750750750752,
0x071c71c71c71c71e, 0x06eb3e45306eb3e6, 0x06bca1af286bca1c, 0x0690690690690692,
0x0666666666666668, 0x063e7063e7063e72, 0x061861861861861a, 0x05f417d05f417d07,
0x05d1745d1745d176, 0x05b05b05b05b05b2, 0x0590b21642c8590d, 0x0572620ae4c415cb,
0x0555555555555557, 0x05397829cbc14e60, 0x051eb851eb851eba, 0x0505050505050507,
0x04ec4ec4ec4ec4ee, 0x04d4873ecade304f, 0x04bda12f684bda14, 0x04a7904a7904a792,
0x0492492492492494, 0x047dc11f7047dc13, 0x0469ee58469ee586, 0x0456c797dd49c343,
0x0444444444444446, 0x04325c53ef368eb2, 0x042108421084210a, 0x0410410410410412,
0x0400000000000001,
};
#endif
#endif
