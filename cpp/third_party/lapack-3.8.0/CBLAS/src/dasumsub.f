c     dasumsun.f
c
c     The program is a fortran wrapper for dasum..
c     Witten by Keita Teranishi.  2/11/1998
c
      subroutine dasumsub(n,x,incx,asum)
c
      external dasum
      double precision dasum,asum
      integer n,incx
      double precision x(*)
c
      asum=dasum(n,x,incx)
      return
      end
