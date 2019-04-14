#include <stdio.h>
#include <ctype.h>
#include <stdarg.h>
#include <string.h>
#include "cblas.h"
#include "cblas_test.h"

void cblas_xerbla(int info, const char *rout, const char *form, ...)
{
   extern int cblas_lerr, cblas_info, cblas_ok;
   extern int link_xerbla;
   extern int RowMajorStrg;
   extern char *cblas_rout;

   /* Initially, c__3chke will call this routine with
    * global variable link_xerbla=1, and F77_xerbla will set link_xerbla=0.
    * This is done to fool the linker into loading these subroutines first
    * instead of ones in the CBLAS or the legacy BLAS library.
    */
   if (link_xerbla) return;

   if (cblas_rout != NULL && strcmp(cblas_rout, rout) != 0){
      printf("***** XERBLA WAS CALLED WITH SRNAME = <%s> INSTEAD OF <%s> *******\n", rout, cblas_rout);
      cblas_ok = FALSE;
   }

   if (RowMajorStrg)
   {
      /* To properly check leading dimension problems in cblas__gemm, we
       * need to do the following trick. When cblas__gemm is called with
       * CblasRowMajor, the arguments A and B switch places in the call to
       * f77__gemm. Thus when we test for bad leading dimension problems
       * for A and B, lda is in position 11 instead of 9, and ldb is in
       * position 9 instead of 11.
       */
      if (strstr(rout,"gemm") != 0)
      {
         if      (info == 5 ) info =  4;
         else if (info == 4 ) info =  5;
         else if (info == 11) info =  9;
         else if (info == 9 ) info = 11;
      }
      else if (strstr(rout,"symm") != 0 || strstr(rout,"hemm") != 0)
      {
         if      (info == 5 ) info =  4;
         else if (info == 4 ) info =  5;
      }
      else if (strstr(rout,"trmm") != 0 || strstr(rout,"trsm") != 0)
      {
         if      (info == 7 ) info =  6;
         else if (info == 6 ) info =  7;
      }
      else if (strstr(rout,"gemv") != 0)
      {
         if      (info == 4)  info = 3;
         else if (info == 3)  info = 4;
      }
      else if (strstr(rout,"gbmv") != 0)
      {
         if      (info == 4)  info = 3;
         else if (info == 3)  info = 4;
         else if (info == 6)  info = 5;
         else if (info == 5)  info = 6;
      }
      else if (strstr(rout,"ger") != 0)
      {
         if      (info == 3) info = 2;
         else if (info == 2) info = 3;
         else if (info == 8) info = 6;
         else if (info == 6) info = 8;
      }
      else if ( ( strstr(rout,"her2") != 0 || strstr(rout,"hpr2") != 0 )
               && strstr(rout,"her2k") == 0 )
      {
         if      (info == 8) info = 6;
         else if (info == 6) info = 8;
      }
   }

   if (info != cblas_info){
      printf("***** XERBLA WAS CALLED WITH INFO = %d INSTEAD OF %d in %s *******\n",info, cblas_info, rout);
      cblas_lerr = PASSED;
      cblas_ok = FALSE;
   } else cblas_lerr = FAILED;
}

#ifdef F77_Char
void F77_xerbla(F77_Char F77_srname, void *vinfo)
#else
void F77_xerbla(char *srname, void *vinfo)
#endif
{
#ifdef F77_Char
   char *srname;
#endif

   char rout[] = {'c','b','l','a','s','_','\0','\0','\0','\0','\0','\0','\0'};

#ifdef F77_Integer
   F77_Integer *info=vinfo;
   F77_Integer i;
   extern F77_Integer link_xerbla;
#else
   int *info=vinfo;
   int i;
   extern int link_xerbla;
#endif
#ifdef F77_Char
   srname = F2C_STR(F77_srname, XerblaStrLen);
#endif

   /* See the comment in cblas_xerbla() above */
   if (link_xerbla)
   {
      link_xerbla = 0;
      return;
   }
   for(i=0;  i  < 6; i++) rout[i+6] = tolower(srname[i]);
   for(i=11; i >= 9; i--) if (rout[i] == ' ') rout[i] = '\0';

   /* We increment *info by 1 since the CBLAS interface adds one more
    * argument to all level 2 and 3 routines.
    */
   cblas_xerbla(*info+1,rout,"");
}
