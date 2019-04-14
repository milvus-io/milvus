c     scasumsub.f
c
c     The program is a fortran wrapper for scasum.
c     Witten by Keita Teranishi.  2/11/1998
c
      subroutine scasumsub(n,x,incx,asum)
c
      external scasum
      real scasum,asum
      integer n,incx
      complex x(*)
c
      asum=scasum(n,x,incx)
      return
      end
