c     dzasumsub.f
c
c     The program is a fortran wrapper for dzasum.
c     Witten by Keita Teranishi.  2/11/1998
c
      subroutine dzasumsub(n,x,incx,asum)
c
      external dzasum
      double precision dzasum,asum
      integer n,incx
      double complex x(*)
c
      asum=dzasum(n,x,incx)
      return
      end
