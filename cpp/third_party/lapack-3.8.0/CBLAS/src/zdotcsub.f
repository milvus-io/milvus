c     zdotcsub.f
c
c     The program is a fortran wrapper for zdotc.
c     Witten by Keita Teranishi.  2/11/1998
c
      subroutine zdotcsub(n,x,incx,y,incy,dotc)
c
      external zdotc
      double complex zdotc,dotc
      integer n,incx,incy
      double complex x(*),y(*)
c
      dotc=zdotc(n,x,incx,y,incy)
      return
      end
