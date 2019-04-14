c     sasumsub.f
c
c     The program is a fortran wrapper for sasum.
c     Witten by Keita Teranishi.  2/11/1998
c
      subroutine sasumsub(n,x,incx,asum)
c
      external sasum
      real sasum,asum
      integer n,incx
      real x(*)
c
      asum=sasum(n,x,incx)
      return
      end
